"""飞书云文档评论工具。"""

from __future__ import annotations

import json
import logging
from typing import Any

from tools.feishu.client import feishu_api_request
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _resolve_doc_target(file_token: str, file_type: str) -> tuple[str, str]:
    """支持 wiki token 自动转换为实际文档对象。"""
    if file_type != "wiki":
        return file_token, file_type
    data = feishu_api_request(
        "GET",
        "/open-apis/wiki/v2/spaces/get_node",
        params={"token": file_token, "obj_type": "wiki"},
    )
    node = (data.get("data") or {}).get("node") or {}
    obj_token = str(node.get("obj_token", "")).strip()
    obj_type = str(node.get("obj_type", "")).strip()
    if not obj_token or not obj_type:
        raise ValueError(f'failed to resolve wiki token "{file_token}" to a document object')
    return obj_token, obj_type


def _convert_elements(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    converted: list[dict[str, Any]] = []
    for element in elements:
        if not isinstance(element, dict):
            raise ValueError("elements must contain objects only")
        kind = str(element.get("type", "")).strip()
        if kind == "text":
            text = str(element.get("text", ""))
            if not text:
                raise ValueError("text element requires non-empty text")
            converted.append({"type": "text_run", "text_run": {"text": text}})
            continue
        if kind == "mention":
            open_id = str(element.get("open_id", "")).strip()
            if not open_id:
                raise ValueError("mention element requires open_id")
            converted.append({"type": "person", "person": {"user_id": open_id}})
            continue
        if kind == "link":
            url = str(element.get("url", "")).strip()
            if not url:
                raise ValueError("link element requires url")
            converted.append({"type": "docs_link", "docs_link": {"url": url}})
            continue
        raise ValueError(f"unsupported element type: {kind}")
    return converted


def _list_replies(file_token: str, file_type: str, comment_id: str, user_id_type: str, page_token: str = "") -> dict[str, Any]:
    params: dict[str, str] = {"file_type": file_type, "page_size": "50", "user_id_type": user_id_type}
    if page_token:
        params["page_token"] = page_token
    data = feishu_api_request(
        "GET",
        f"/open-apis/drive/v1/files/{file_token}/comments/{comment_id}/replies",
        params=params,
    )
    return data.get("data") or {}


def _assemble_comments_with_replies(items: list[dict[str, Any]], file_token: str, file_type: str, user_id_type: str) -> list[dict[str, Any]]:
    assembled: list[dict[str, Any]] = []
    for comment in items:
        current = dict(comment)
        has_partial_replies = bool(((comment.get("reply_list") or {}).get("replies"))) or bool(comment.get("has_more"))
        if has_partial_replies and comment.get("comment_id"):
            replies: list[dict[str, Any]] = []
            page_token = ""
            while True:
                reply_payload = _list_replies(file_token, file_type, str(comment["comment_id"]), user_id_type, page_token)
                replies.extend(reply_payload.get("items", []))
                if not reply_payload.get("has_more"):
                    break
                page_token = str(reply_payload.get("page_token", "")).strip()
                if not page_token:
                    break
            current["reply_list"] = {"replies": replies}
        assembled.append(current)
    return assembled


def _handle_doc_comments(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    file_token = str(args.get("file_token", "")).strip()
    file_type = str(args.get("file_type", "")).strip()
    if not file_token or not file_type:
        return tool_error("Parameters 'file_token' and 'file_type' are required.")
    user_id_type = str(args.get("user_id_type", "open_id")).strip() or "open_id"
    try:
        actual_token, actual_type = _resolve_doc_target(file_token, file_type)

        if action == "list":
            params = {
                "file_type": actual_type,
                "page_size": str(max(1, min(int(args.get("page_size", 50) or 50), 200))),
                "user_id_type": user_id_type,
            }
            if args.get("is_whole") is not None:
                params["is_whole"] = str(bool(args.get("is_whole"))).lower()
            if args.get("is_solved") is not None:
                params["is_solved"] = str(bool(args.get("is_solved"))).lower()
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", f"/open-apis/drive/v1/files/{actual_token}/comments", params=params)
            payload = data.get("data") or {}
            items = payload.get("items", [])
            return json.dumps(
                {
                    "items": _assemble_comments_with_replies(items, actual_token, actual_type, user_id_type),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "list_replies":
            comment_id = str(args.get("comment_id", "")).strip()
            if not comment_id:
                return tool_error("Missing required parameter: comment_id")
            payload = _list_replies(
                actual_token,
                actual_type,
                comment_id,
                user_id_type,
                str(args.get("page_token", "")).strip(),
            )
            return json.dumps(
                {
                    "items": payload.get("items", []),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "create":
            elements = args.get("elements")
            if not isinstance(elements, list) or not elements:
                return tool_error("Parameter 'elements' must be a non-empty array.")
            data = feishu_api_request(
                "POST",
                f"/open-apis/drive/v1/files/{actual_token}/comments",
                params={"file_type": actual_type, "user_id_type": user_id_type},
                json_body={
                    "reply_list": {
                        "replies": [{"content": {"elements": _convert_elements(elements)}}],
                    }
                },
            )
            return json.dumps(data.get("data") or {}, ensure_ascii=False)

        if action == "reply":
            comment_id = str(args.get("comment_id", "")).strip()
            elements = args.get("elements")
            if not comment_id:
                return tool_error("Missing required parameter: comment_id")
            if not isinstance(elements, list) or not elements:
                return tool_error("Parameter 'elements' must be a non-empty array.")
            converted = _convert_elements(elements)
            path = f"/open-apis/drive/v1/files/{actual_token}/comments/{comment_id}/replies"
            params = {"file_type": actual_type, "user_id_type": user_id_type}
            try:
                data = feishu_api_request(
                    "POST",
                    path,
                    params=params,
                    json_body={"content": {"elements": converted}},
                )
            except Exception:
                data = feishu_api_request(
                    "POST",
                    path,
                    params=params,
                    json_body={"reply_elements": converted},
                )
            return json.dumps(data.get("data") or data, ensure_ascii=False)

        if action == "patch":
            comment_id = str(args.get("comment_id", "")).strip()
            if not comment_id:
                return tool_error("Missing required parameter: comment_id")
            if args.get("is_solved_value") is None:
                return tool_error("Missing required parameter: is_solved_value")
            feishu_api_request(
                "PATCH",
                f"/open-apis/drive/v1/files/{actual_token}/comments/{comment_id}",
                params={"file_type": actual_type},
                json_body={"is_solved": bool(args.get("is_solved_value"))},
            )
            return json.dumps({"success": True}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: list, list_replies, create, reply, patch")
    except Exception as exc:
        logger.error("feishu_doc_comments error: %s", exc)
        return tool_error(f"Failed to execute feishu_doc_comments: {exc}")


FEISHU_DOC_COMMENTS_SCHEMA = {
    "name": "feishu_doc_comments",
    "description": "Manage Feishu document comments. Hermes supports list, list_replies, create, reply, and patch. Wiki token is supported.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["list", "list_replies", "create", "reply", "patch"], "description": "Document comment action."},
            "file_token": {"type": "string", "description": "Document token or wiki node token."},
            "file_type": {"type": "string", "enum": ["doc", "docx", "sheet", "file", "slides", "wiki"], "description": "Document type. Wiki is resolved automatically."},
            "is_whole": {"type": "boolean", "description": "Whether to fetch only whole-document comments for list action."},
            "is_solved": {"type": "boolean", "description": "Whether to filter solved comments for list action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 200, "description": "Page size."},
            "page_token": {"type": "string", "description": "Pagination token."},
            "elements": {
                "type": "array",
                "description": "Comment content elements for create or reply action.",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["text", "mention", "link"]},
                        "text": {"type": "string"},
                        "open_id": {"type": "string"},
                        "url": {"type": "string"},
                    },
                    "required": ["type"],
                },
            },
            "comment_id": {"type": "string", "description": "Comment ID for list_replies, reply, or patch action."},
            "is_solved_value": {"type": "boolean", "description": "Solved state for patch action."},
            "user_id_type": {
                "type": "string",
                "enum": ["open_id", "union_id", "user_id"],
                "description": "User ID type. Defaults to open_id.",
            },
        },
        "required": ["action", "file_token", "file_type"],
    },
}

registry.register(
    name="feishu_doc_comments",
    toolset="feishu",
    schema=FEISHU_DOC_COMMENTS_SCHEMA,
    handler=_handle_doc_comments,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
