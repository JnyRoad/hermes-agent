"""飞书 Drive 基础工具。"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path

import httpx

from tools.feishu.client import feishu_api_request, get_feishu_base_url, get_tenant_access_token
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)
_SMALL_FILE_THRESHOLD = 15 * 1024 * 1024


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _upload_drive_file(*, file_name: str, content: bytes, parent_node: str = "") -> dict:
    token = get_tenant_access_token()
    url = f"{get_feishu_base_url()}/open-apis/drive/v1/files/upload_all"
    headers = {"Authorization": f"Bearer {token}"}
    data = {
        "file_name": file_name,
        "parent_type": "explorer",
        "parent_node": parent_node,
        "size": str(len(content)),
    }
    files = {"file": (file_name, content)}
    response = httpx.post(url, headers=headers, data=data, files=files, timeout=120)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Feishu drive upload returned a non-object response.")
    if payload.get("code") not in (0, None):
        raise RuntimeError(
            f"Feishu drive upload error: code={payload.get('code')} msg={payload.get('msg') or payload.get('message')}"
        )
    return payload


def _upload_prepare(*, file_name: str, size: int, parent_node: str = "") -> dict:
    token = get_tenant_access_token()
    url = f"{get_feishu_base_url()}/open-apis/drive/v1/files/upload_prepare"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    response = httpx.post(
        url,
        headers=headers,
        json={
            "file_name": file_name,
            "parent_type": "explorer",
            "parent_node": parent_node,
            "size": size,
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Feishu drive upload_prepare returned a non-object response.")
    if payload.get("code") not in (0, None):
        raise RuntimeError(
            f"Feishu drive upload_prepare error: code={payload.get('code')} msg={payload.get('msg') or payload.get('message')}"
        )
    return payload


def _upload_part(*, upload_id: str, seq: int, content: bytes) -> None:
    token = get_tenant_access_token()
    url = f"{get_feishu_base_url()}/open-apis/drive/v1/files/upload_part"
    headers = {"Authorization": f"Bearer {token}"}
    data = {
        "upload_id": upload_id,
        "seq": str(seq),
        "size": str(len(content)),
    }
    files = {"file": (f"part-{seq}", content)}
    response = httpx.post(url, headers=headers, data=data, files=files, timeout=120)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Feishu drive upload_part returned a non-object response.")
    if payload.get("code") not in (0, None):
        raise RuntimeError(
            f"Feishu drive upload_part error: code={payload.get('code')} msg={payload.get('msg') or payload.get('message')}"
        )


def _upload_finish(*, upload_id: str, block_num: int) -> dict:
    token = get_tenant_access_token()
    url = f"{get_feishu_base_url()}/open-apis/drive/v1/files/upload_finish"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    response = httpx.post(
        url,
        headers=headers,
        json={"upload_id": upload_id, "block_num": block_num},
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Feishu drive upload_finish returned a non-object response.")
    if payload.get("code") not in (0, None):
        raise RuntimeError(
            f"Feishu drive upload_finish error: code={payload.get('code')} msg={payload.get('msg') or payload.get('message')}"
        )
    return payload


def _handle_drive_file(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "list":
            params = {
                "page_size": max(1, min(int(args.get("page_size", 200) or 200), 200)),
            }
            folder_token = str(args.get("folder_token", "")).strip()
            if folder_token:
                params["folder_token"] = folder_token
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            order_by = str(args.get("order_by", "")).strip()
            if order_by:
                params["order_by"] = order_by
            direction = str(args.get("direction", "")).strip()
            if direction:
                params["direction"] = direction
            data = feishu_api_request("GET", "/open-apis/drive/v1/files", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "files": payload.get("files", []),
                    "has_more": bool(payload.get("has_more", False)),
                    "next_page_token": payload.get("next_page_token") or payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "get_meta":
            request_docs = args.get("request_docs")
            if not isinstance(request_docs, list) or not request_docs:
                return tool_error("Parameter 'request_docs' must be a non-empty array.")
            data = feishu_api_request(
                "POST",
                "/open-apis/drive/v1/metas/batch_query",
                json_body={"request_docs": request_docs},
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "metas": payload.get("metas", []),
                    "docs": payload.get("docs", payload.get("metas", [])),
                },
                ensure_ascii=False,
            )

        if action == "copy":
            file_token = str(args.get("file_token", "")).strip()
            name = str(args.get("name", "")).strip()
            file_type = str(args.get("type", "")).strip()
            folder_token = str(args.get("folder_token", "")).strip() or str(args.get("parent_node", "")).strip()
            if not file_token or not name or not file_type:
                return tool_error("Parameters 'file_token', 'name', and 'type' are required for copy.")
            data = feishu_api_request(
                "POST",
                f"/open-apis/drive/v1/files/{file_token}/copy",
                json_body={
                    "name": name,
                    "type": file_type,
                    **({"folder_token": folder_token} if folder_token else {}),
                },
            )
            payload = data.get("data") or {}
            return json.dumps({"file": payload.get("file", payload)}, ensure_ascii=False)

        if action == "move":
            file_token = str(args.get("file_token", "")).strip()
            file_type = str(args.get("type", "")).strip()
            folder_token = str(args.get("folder_token", "")).strip()
            if not file_token or not file_type or not folder_token:
                return tool_error("Parameters 'file_token', 'type', and 'folder_token' are required for move.")
            data = feishu_api_request(
                "POST",
                f"/open-apis/drive/v1/files/{file_token}/move",
                json_body={
                    "type": file_type,
                    "folder_token": folder_token,
                },
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "success": True,
                    "task_id": payload.get("task_id"),
                    "file_token": file_token,
                    "target_folder_token": folder_token,
                },
                ensure_ascii=False,
            )

        if action == "upload":
            file_path = str(args.get("file_path", "")).strip()
            file_content_base64 = str(args.get("file_content_base64", "")).strip()
            file_name = str(args.get("file_name", "")).strip()
            parent_node = str(args.get("parent_node", "")).strip()
            if file_path:
                local_path = Path(file_path)
                if not local_path.is_file():
                    return tool_error(f"Local file does not exist: {file_path}")
                content = local_path.read_bytes()
                resolved_name = file_name or local_path.name
            elif file_content_base64:
                if not file_name:
                    return tool_error("file_name is required when using file_content_base64.")
                try:
                    content = base64.b64decode(file_content_base64)
                except Exception as exc:
                    return tool_error(f"Failed to decode file_content_base64: {exc}")
                resolved_name = file_name
            else:
                return tool_error("Either file_path or file_content_base64 is required for upload.")

            if len(content) <= _SMALL_FILE_THRESHOLD:
                data = _upload_drive_file(file_name=resolved_name, content=content, parent_node=parent_node)
                payload = data.get("data") or data
                return json.dumps(
                    {
                        "file_token": payload.get("file_token"),
                        "file_name": resolved_name,
                        "size": len(content),
                        "upload_method": "upload_all",
                    },
                    ensure_ascii=False,
                )

            prepare = _upload_prepare(file_name=resolved_name, size=len(content), parent_node=parent_node)
            prepare_payload = prepare.get("data") or prepare
            upload_id = str(prepare_payload.get("upload_id", "")).strip()
            block_size = int(prepare_payload.get("block_size") or 0)
            block_num = int(prepare_payload.get("block_num") or 0)
            if not upload_id or block_size <= 0 or block_num <= 0:
                return tool_error("Invalid upload_prepare response.")
            for seq in range(block_num):
                start = seq * block_size
                end = min(start + block_size, len(content))
                _upload_part(upload_id=upload_id, seq=seq, content=content[start:end])
            finish = _upload_finish(upload_id=upload_id, block_num=block_num)
            finish_payload = finish.get("data") or finish
            return json.dumps(
                {
                    "file_token": finish_payload.get("file_token"),
                    "file_name": resolved_name,
                    "size": len(content),
                    "upload_method": "chunked",
                    "chunks_uploaded": block_num,
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: list, get_meta, copy, move, upload")
    except Exception as exc:
        logger.error("feishu_drive_file error: %s", exc)
        return tool_error(f"Failed to execute feishu_drive_file: {exc}")


FEISHU_DRIVE_FILE_SCHEMA = {
    "name": "feishu_drive_file",
    "description": "Manage Feishu Drive files. Supported actions in Hermes now: list, get_meta, copy, move, and upload.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["list", "get_meta", "copy", "move", "upload"],
                "description": "Drive action to execute.",
            },
            "folder_token": {"type": "string", "description": "Folder token for list action."},
            "parent_node": {"type": "string", "description": "Alias of folder_token for copy action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 200, "description": "Page size for list."},
            "page_token": {"type": "string", "description": "Pagination token for list."},
            "order_by": {
                "type": "string",
                "enum": ["EditedTime", "CreatedTime"],
                "description": "Sort key for list.",
            },
            "direction": {
                "type": "string",
                "enum": ["ASC", "DESC"],
                "description": "Sort direction for list.",
            },
            "request_docs": {
                "type": "array",
                "description": "Batch metadata query input for get_meta.",
                "items": {
                    "type": "object",
                    "properties": {
                        "doc_token": {"type": "string"},
                        "doc_type": {"type": "string"},
                    },
                        "required": ["doc_token", "doc_type"],
                },
            },
            "file_token": {"type": "string", "description": "File token for copy or move action."},
            "name": {"type": "string", "description": "Target file name for copy action."},
            "type": {
                "type": "string",
                "enum": ["doc", "sheet", "file", "bitable", "docx", "folder", "mindnote", "slides"],
                "description": "Drive file type for copy or move action.",
            },
            "file_path": {"type": "string", "description": "Absolute local file path for upload action."},
            "file_content_base64": {"type": "string", "description": "Base64-encoded file content for upload action."},
            "file_name": {"type": "string", "description": "Target file name for upload action, or override when using file_path."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_drive_file",
    toolset="feishu",
    schema=FEISHU_DRIVE_FILE_SCHEMA,
    handler=_handle_drive_file,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
