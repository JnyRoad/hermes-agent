"""飞书文档工具。

当前实现优先保证稳定闭环，同时逐步提升 Markdown 保真度：
- 读取：使用 docx raw_content
- 写入：使用 docx block API 重建文档内容

现阶段仍使用稳定的 text block 写回文档，但会先解析 Markdown 块结构，
尽量保留标题、列表、引用和代码块的语义，而不是把所有内容简单压平成纯段落。
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from typing import Any, Dict, List, Optional

from hermes_constants import get_hermes_home
from tools.feishu.client import feishu_api_request
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)

_DOC_URL_RE = re.compile(r"/docx/([A-Za-z0-9]+)")
_DOC_TOKEN_RE = re.compile(r"^[A-Za-z0-9]{10,}$")
_MD_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s*")
_MD_LIST_RE = re.compile(r"^\s*([-*+]|\d+\.)\s+")
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_MD_EMPHASIS_RE = re.compile(r"(\*\*|__|\*|_|~~|`)")
_MD_BLOCKQUOTE_RE = re.compile(r"^\s*>\s?")
_MD_FENCE_RE = re.compile(r"^\s*(```+|~~~+)\s*([A-Za-z0-9_+-]*)\s*$")
_DOC_TASK_ID_RE = re.compile(r"^doc_task_[a-f0-9]{12}$")

_ASYNC_DOC_MARKDOWN_THRESHOLD = 12000
_DOC_TASK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="feishu-doc-task")
_DOC_TASKS: Dict[str, Dict[str, Any]] = {}
_DOC_TASKS_LOCK = Lock()


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _doc_task_state_path() -> Any:
    return get_hermes_home() / "feishu_doc_tasks.json"


def _save_doc_tasks_snapshot() -> None:
    """Persist lightweight task snapshots so later polls can inspect finished tasks."""
    with _DOC_TASKS_LOCK:
        snapshot: Dict[str, Dict[str, Any]] = {}
        for task_id, item in _DOC_TASKS.items():
            snapshot[task_id] = {
                "status": item.get("status", "running"),
                "result": item.get("result"),
                "error": item.get("error"),
            }
    path = _doc_task_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_doc_tasks_snapshot() -> Dict[str, Dict[str, Any]]:
    path = _doc_task_state_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to load Feishu doc task snapshot from %s", path, exc_info=True)
        return {}
    return payload if isinstance(payload, dict) else {}


def _queue_async_doc_task(operation: str, payload: Dict[str, Any], fn: Any) -> str:
    """Run large document rewrites in the background and expose a pollable task_id."""
    task_id = f"doc_task_{uuid.uuid4().hex[:12]}"
    with _DOC_TASKS_LOCK:
        _DOC_TASKS[task_id] = {
            "status": "running",
            "operation": operation,
            "payload": dict(payload),
            "result": None,
            "error": None,
            "future": None,
        }

    def _runner() -> Dict[str, Any]:
        try:
            result = fn()
            with _DOC_TASKS_LOCK:
                entry = _DOC_TASKS.setdefault(task_id, {})
                entry["status"] = "success"
                entry["result"] = result
                entry["error"] = None
            _save_doc_tasks_snapshot()
            return result
        except Exception as exc:
            with _DOC_TASKS_LOCK:
                entry = _DOC_TASKS.setdefault(task_id, {})
                entry["status"] = "failed"
                entry["result"] = None
                entry["error"] = str(exc)
            _save_doc_tasks_snapshot()
            raise

    future = _DOC_TASK_EXECUTOR.submit(_runner)
    with _DOC_TASKS_LOCK:
        _DOC_TASKS[task_id]["future"] = future
    _save_doc_tasks_snapshot()
    return task_id


def _get_async_doc_task_status(task_id: str) -> Dict[str, Any]:
    normalized_task_id = str(task_id or "").strip()
    if not _DOC_TASK_ID_RE.match(normalized_task_id):
        raise ValueError("Invalid task_id format.")

    with _DOC_TASKS_LOCK:
        entry = _DOC_TASKS.get(normalized_task_id)

    if entry is not None:
        future = entry.get("future")
        if isinstance(future, Future) and future.done():
            try:
                future.result()
            except Exception:
                # Result/error state is already captured by the runner.
                pass
        return {
            "task_id": normalized_task_id,
            "status": entry.get("status", "running"),
            "result": entry.get("result"),
            "error": entry.get("error"),
        }

    snapshot = _load_doc_tasks_snapshot().get(normalized_task_id)
    if snapshot is not None:
        return {
            "task_id": normalized_task_id,
            "status": snapshot.get("status", "unknown"),
            "result": snapshot.get("result"),
            "error": snapshot.get("error"),
        }
    return {
        "task_id": normalized_task_id,
        "status": "not_found",
        "error": "Unknown task_id.",
    }


def _extract_doc_id(value: str) -> str:
    trimmed = str(value or "").strip()
    if not trimmed:
        raise ValueError("Document ID or URL is required.")
    match = _DOC_URL_RE.search(trimmed)
    if match:
        return match.group(1)
    if _DOC_TOKEN_RE.match(trimmed):
        return trimmed
    raise ValueError(f"Could not extract Feishu document ID from: {trimmed}")


def _strip_inline_markdown(text: str) -> str:
    """Best-effort inline Markdown cleanup for doc text blocks."""
    cleaned = _MD_LINK_RE.sub(lambda m: f"{m.group(1)} ({m.group(2)})", str(text or ""))
    cleaned = _MD_EMPHASIS_RE.sub("", cleaned)
    return cleaned.strip()


def _normalize_markdown_to_blocks(markdown: str) -> List[Dict[str, Any]]:
    """Parse Markdown into stable doc text blocks with preserved structural cues."""
    text = str(markdown or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    blocks: List[Dict[str, Any]] = []
    paragraph_buffer: List[str] = []
    code_fence: Optional[str] = None
    code_language = ""
    code_lines: List[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph_buffer
        if not paragraph_buffer:
            return
        merged = " ".join(item.strip() for item in paragraph_buffer if item.strip()).strip()
        paragraph_buffer = []
        if not merged:
            return
        blocks.append({"kind": "paragraph", "text": _strip_inline_markdown(merged)})

    def flush_code_block() -> None:
        nonlocal code_lines, code_fence, code_language
        if code_fence is None:
            return
        body = "\n".join(code_lines).rstrip("\n")
        prefix = f"[{code_language}]\n" if code_language else ""
        blocks.append({"kind": "code", "text": f"{prefix}{body}".strip()})
        code_lines = []
        code_fence = None
        code_language = ""

    for raw_line in lines:
        fence_match = _MD_FENCE_RE.match(raw_line)
        if code_fence is not None:
            if fence_match and fence_match.group(1) == code_fence:
                flush_code_block()
                continue
            code_lines.append(raw_line)
            continue
        if fence_match:
            flush_paragraph()
            code_fence = fence_match.group(1)
            code_language = str(fence_match.group(2) or "").strip()
            code_lines = []
            continue

        line = raw_line.rstrip()
        if not line.strip():
            flush_paragraph()
            continue

        heading_match = re.match(r"^\s{0,3}(#{1,6})\s+(.*)$", line)
        if heading_match:
            flush_paragraph()
            blocks.append(
                {
                    "kind": "heading",
                    "level": len(heading_match.group(1)),
                    "text": _strip_inline_markdown(heading_match.group(2)),
                }
            )
            continue

        quote_match = _MD_BLOCKQUOTE_RE.match(line)
        if quote_match:
            flush_paragraph()
            blocks.append({"kind": "quote", "text": _strip_inline_markdown(line[quote_match.end():])})
            continue

        list_match = re.match(r"^(\s*)([-*+]|\d+\.)\s+(.*)$", line)
        if list_match:
            flush_paragraph()
            indent_level = len(list_match.group(1).replace("\t", "    ")) // 2
            marker = list_match.group(2)
            blocks.append(
                {
                    "kind": "list",
                    "ordered": marker.endswith(".") and marker[:-1].isdigit(),
                    "marker": marker,
                    "indent": max(indent_level, 0),
                    "text": _strip_inline_markdown(list_match.group(3)),
                }
            )
            continue

        paragraph_buffer.append(line)

    flush_paragraph()
    flush_code_block()
    return blocks or [{"kind": "paragraph", "text": ""}]


def _render_doc_block_text(block: Dict[str, Any]) -> str:
    """Render a parsed Markdown block into stable text for Feishu doc text blocks."""
    kind = str(block.get("kind") or "paragraph")
    text = str(block.get("text") or "")
    if kind == "heading":
        level = int(block.get("level") or 1)
        return f"{'#' * min(max(level, 1), 6)} {text}".strip()
    if kind == "list":
        indent = "  " * int(block.get("indent") or 0)
        if block.get("ordered"):
            marker = str(block.get("marker") or "1.")
        else:
            marker = "•"
        return f"{indent}{marker} {text}".rstrip()
    if kind == "quote":
        return f"> {text}".rstrip()
    if kind == "code":
        return f"```\n{text}\n```".strip()
    return text


def _build_text_block(text: str) -> Dict[str, Any]:
    content = text if text else " "
    return {
        "block_type": 2,
        "text": {
            "elements": [
                {
                    "text_run": {
                        "content": content,
                    }
                }
            ],
            "style": {},
        },
    }


def _get_doc_meta(document_id: str) -> Dict[str, Any]:
    return feishu_api_request("GET", f"/open-apis/docx/v1/documents/{document_id}")


def _get_doc_raw_content(document_id: str) -> Dict[str, Any]:
    return feishu_api_request("GET", f"/open-apis/docx/v1/documents/{document_id}/raw_content")


def _list_root_children(document_id: str) -> List[Dict[str, Any]]:
    children: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        params: Dict[str, Any] = {
            "page_size": 500,
            "document_revision_id": -1,
        }
        if page_token:
            params["page_token"] = page_token
        data = feishu_api_request(
            "GET",
            f"/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/children",
            params=params,
        )
        payload = data.get("data") or {}
        batch = payload.get("items") or payload.get("children") or []
        if isinstance(batch, list):
            children.extend(item for item in batch if isinstance(item, dict))
        page_token = payload.get("page_token")
        if not payload.get("has_more"):
            break
    return children


def _delete_root_children(document_id: str, child_ids: List[str]) -> None:
    if not child_ids:
        return
    for index in range(0, len(child_ids), 200):
        batch = child_ids[index:index + 200]
        feishu_api_request(
            "DELETE",
            f"/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/children/batch_delete",
            params={"document_revision_id": -1, "client_token": str(uuid.uuid4())},
            json_body={"children": batch},
        )


def _append_blocks(document_id: str, paragraphs: List[str], *, start_index: int) -> None:
    blocks = [_build_text_block(item) for item in paragraphs]
    for index in range(0, len(blocks), 50):
        chunk = blocks[index:index + 50]
        feishu_api_request(
            "POST",
            f"/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/children",
            params={"document_revision_id": -1, "client_token": str(uuid.uuid4())},
            json_body={
                "index": start_index + index,
                "children": chunk,
            },
        )


def _rewrite_document_content(document_id: str, content: str) -> Dict[str, Any]:
    """用新的全文内容重建文档。

    规则：
    - 先读根子块，再删除，再按顺序插入新段落
    - 返回最终段落数，供上层工具结果使用
    """
    children = _list_root_children(document_id)
    child_ids = [str(item.get("block_id", "")).strip() for item in children if str(item.get("block_id", "")).strip()]
    _delete_root_children(document_id, child_ids)
    markdown_blocks = _normalize_markdown_to_blocks(content)
    paragraphs = [_render_doc_block_text(item) for item in markdown_blocks]
    _append_blocks(document_id, paragraphs, start_index=0)
    return {
        "paragraph_count": len(paragraphs),
        "block_count": len(markdown_blocks),
        "block_kinds": [str(item.get("kind") or "paragraph") for item in markdown_blocks],
    }


def _slice_content(content: str, offset: int = 0, limit: Optional[int] = None) -> str:
    if offset < 0:
        offset = 0
    if limit is None:
        return content[offset:]
    return content[offset:offset + limit]


def _resolve_selection(content: str, *, selection_with_ellipsis: str = "", selection_by_title: str = "") -> tuple[int, int]:
    if selection_with_ellipsis:
        if "..." not in selection_with_ellipsis:
            raise ValueError("selection_with_ellipsis must contain '...'.")
        start_text, end_text = selection_with_ellipsis.split("...", 1)
        start_idx = content.find(start_text)
        if start_idx < 0:
            raise ValueError("selection_with_ellipsis start marker not found.")
        end_idx = content.find(end_text, start_idx + len(start_text))
        if end_idx < 0:
            raise ValueError("selection_with_ellipsis end marker not found.")
        return start_idx, end_idx + len(end_text)

    if selection_by_title:
        lines = content.splitlines(keepends=True)
        stripped_title = selection_by_title.strip()
        cursor = 0
        for index, line in enumerate(lines):
            line_stripped = line.strip()
            if line_stripped == stripped_title:
                start = cursor
                end = len(content)
                for later in lines[index + 1:]:
                    if later.lstrip().startswith("#"):
                        end = cursor + len(line)
                        break
                    cursor += len(later)
                else:
                    cursor += len(line)
                if end == len(content):
                    running = 0
                    for item in lines[:index]:
                        running += len(item)
                    return running, len(content)
        raise ValueError("selection_by_title not found.")

    raise ValueError("A selection expression is required for this mode.")


def _apply_update_mode(
    *,
    current_content: str,
    mode: str,
    markdown: str,
    selection_with_ellipsis: str = "",
    selection_by_title: str = "",
) -> str:
    if mode in {"overwrite", "replace_all"}:
        return markdown
    if mode == "append":
        if not current_content.strip():
            return markdown
        return f"{current_content.rstrip()}\n\n{markdown.lstrip()}"

    start, end = _resolve_selection(
        current_content,
        selection_with_ellipsis=selection_with_ellipsis,
        selection_by_title=selection_by_title,
    )
    if mode == "replace_range":
        return current_content[:start] + markdown + current_content[end:]
    if mode == "insert_before":
        return current_content[:start] + markdown + "\n" + current_content[start:]
    if mode == "insert_after":
        return current_content[:end] + "\n" + markdown + current_content[end:]
    if mode == "delete_range":
        return current_content[:start] + current_content[end:]
    raise ValueError(f"Unsupported update mode: {mode}")


def _handle_fetch_doc(args: dict, **_kw) -> str:
    try:
        document_id = _extract_doc_id(args.get("doc_id", ""))
        offset = int(args.get("offset", 0) or 0)
        limit = args.get("limit")
        limit_value = int(limit) if limit is not None else None
        meta = _get_doc_meta(document_id)
        raw = _get_doc_raw_content(document_id)
        title = ((meta.get("data") or {}).get("document") or {}).get("title")
        content = ((raw.get("data") or {}).get("content") or "")
        sliced = _slice_content(str(content), offset=offset, limit=limit_value)
        return json.dumps(
            {
                "document_id": document_id,
                "title": title,
                "content": sliced,
                "total_chars": len(str(content)),
                "offset": offset,
                "limit": limit_value,
                "has_more": offset + len(sliced) < len(str(content)),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_fetch_doc error: %s", exc)
        return tool_error(f"Failed to fetch Feishu doc: {exc}")


def _handle_create_doc(args: dict, **_kw) -> str:
    try:
        task_id = str(args.get("task_id", "") or "").strip()
        if task_id:
            return json.dumps(_get_async_doc_task_status(task_id), ensure_ascii=False)
        title = str(args.get("title", "")).strip()
        markdown = str(args.get("markdown", "") or "")
        if not title:
            return tool_error("Missing required parameter: title")
        target_flags = [args.get("folder_token"), args.get("wiki_node"), args.get("wiki_space")]
        if len([item for item in target_flags if item]) > 1:
            return tool_error("folder_token, wiki_node, and wiki_space are mutually exclusive.")

        data = feishu_api_request(
            "POST",
            "/open-apis/docx/v1/documents",
            json_body={"title": title},
        )
        document = (data.get("data") or {}).get("document") or {}
        document_id = str(document.get("document_id") or document.get("document_id", "")).strip()
        if not document_id:
            document_id = str(document.get("document_id", "")).strip() or str(document.get("document_token", "")).strip()
        if not document_id:
            document_id = str((data.get("data") or {}).get("document_id", "")).strip()
        if not document_id:
            return tool_error("Document was created but no document_id was returned.")

        write_result = None
        if markdown.strip():
            if len(markdown) >= _ASYNC_DOC_MARKDOWN_THRESHOLD:
                async_task_id = _queue_async_doc_task(
                    "create_doc",
                    {"document_id": document_id, "title": title},
                    lambda: _rewrite_document_content(document_id, markdown),
                )
                return json.dumps(
                    {
                        "task_id": async_task_id,
                        "document_id": document_id,
                        "title": title,
                        "message": "Document creation content has been scheduled for async processing.",
                    },
                    ensure_ascii=False,
                )
            write_result = _rewrite_document_content(document_id, markdown)

        return json.dumps(
            {
                "document_id": document_id,
                "title": title,
                "initialized": bool(markdown.strip()),
                "write_result": write_result,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_create_doc error: %s", exc)
        return tool_error(f"Failed to create Feishu doc: {exc}")


def _handle_update_doc(args: dict, **_kw) -> str:
    try:
        task_id = str(args.get("task_id", "") or "").strip()
        if task_id:
            return json.dumps(_get_async_doc_task_status(task_id), ensure_ascii=False)
        mode = str(args.get("mode", "")).strip().lower()
        if not mode:
            return tool_error("Missing required parameter: mode")
        document_id = _extract_doc_id(args.get("doc_id", ""))
        markdown = str(args.get("markdown", "") or "")
        selection_with_ellipsis = str(args.get("selection_with_ellipsis", "") or "")
        selection_by_title = str(args.get("selection_by_title", "") or "")
        current = _get_doc_raw_content(document_id)
        current_content = str(((current.get("data") or {}).get("content") or ""))
        next_content = _apply_update_mode(
            current_content=current_content,
            mode=mode,
            markdown=markdown,
            selection_with_ellipsis=selection_with_ellipsis,
            selection_by_title=selection_by_title,
        )
        if len(markdown) >= _ASYNC_DOC_MARKDOWN_THRESHOLD:
            async_task_id = _queue_async_doc_task(
                "update_doc",
                {"document_id": document_id, "mode": mode},
                lambda: _rewrite_document_content(document_id, next_content),
            )
            return json.dumps(
                {
                    "task_id": async_task_id,
                    "document_id": document_id,
                    "mode": mode,
                    "message": "Document update has been scheduled for async processing.",
                },
                ensure_ascii=False,
            )
        write_result = _rewrite_document_content(document_id, next_content)
        result = {
            "document_id": document_id,
            "mode": mode,
            "updated": True,
            "paragraph_count": write_result["paragraph_count"],
        }
        new_title = str(args.get("new_title", "") or "").strip()
        if new_title:
            result["title_update_note"] = "Document title update is not implemented yet."
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        logger.error("feishu_update_doc error: %s", exc)
        return tool_error(f"Failed to update Feishu doc: {exc}")


FEISHU_FETCH_DOC_SCHEMA = {
    "name": "feishu_fetch_doc",
    "description": "Fetch a Feishu doc by document ID or URL. Returns title and raw text content, with optional pagination by character offset.",
    "parameters": {
        "type": "object",
        "properties": {
            "doc_id": {"type": "string", "description": "Feishu document ID or full document URL."},
            "offset": {"type": "integer", "minimum": 0, "description": "Character offset for paginated reads."},
            "limit": {"type": "integer", "minimum": 1, "description": "Maximum characters to return."},
        },
        "required": ["doc_id"],
    },
}

FEISHU_CREATE_DOC_SCHEMA = {
    "name": "feishu_create_doc",
    "description": "Create a Feishu doc. When markdown is provided, Hermes preserves headings, lists, quotes, and code fences while writing stable doc text blocks.",
    "parameters": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Document title."},
            "markdown": {"type": "string", "description": "Initial Markdown content to render into the document."},
            "folder_token": {"type": "string", "description": "Reserved for future folder placement support."},
            "wiki_node": {"type": "string", "description": "Reserved for future wiki placement support."},
            "wiki_space": {"type": "string", "description": "Reserved for future wiki space placement support."},
            "task_id": {"type": "string", "description": "Reserved for future async task polling support."},
        },
        "required": ["title"],
    },
}

FEISHU_UPDATE_DOC_SCHEMA = {
    "name": "feishu_update_doc",
    "description": "Update a Feishu doc by rewriting its content with stable Markdown-aware doc text blocks. Supports overwrite, append, replace_all, replace_range, insert_before, insert_after, and delete_range.",
    "parameters": {
        "type": "object",
        "properties": {
            "doc_id": {"type": "string", "description": "Feishu document ID or URL."},
            "markdown": {"type": "string", "description": "New content to apply for the selected mode."},
            "mode": {
                "type": "string",
                "enum": [
                    "overwrite",
                    "append",
                    "replace_range",
                    "replace_all",
                    "insert_before",
                    "insert_after",
                    "delete_range",
                ],
                "description": "Update mode.",
            },
            "selection_with_ellipsis": {
                "type": "string",
                "description": "Range locator in the form 'start...end'. Required for replace_range, insert_before, insert_after, delete_range unless selection_by_title is used.",
            },
            "selection_by_title": {
                "type": "string",
                "description": "Heading text used to select a section. Alternative to selection_with_ellipsis for range-based modes.",
            },
            "new_title": {"type": "string", "description": "Reserved for future document title update support."},
            "task_id": {"type": "string", "description": "Reserved for future async task polling support."},
        },
        "required": ["doc_id", "mode"],
    },
}

registry.register(
    name="feishu_fetch_doc",
    toolset="feishu",
    schema=FEISHU_FETCH_DOC_SCHEMA,
    handler=_handle_fetch_doc,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_create_doc",
    toolset="feishu",
    schema=FEISHU_CREATE_DOC_SCHEMA,
    handler=_handle_create_doc,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_update_doc",
    toolset="feishu",
    schema=FEISHU_UPDATE_DOC_SCHEMA,
    handler=_handle_update_doc,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
