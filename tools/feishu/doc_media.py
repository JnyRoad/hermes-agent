"""飞书文档媒体工具。"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from tools.feishu.client import (
    feishu_api_request,
    feishu_api_request_bytes,
    get_feishu_base_url,
    get_tenant_access_token,
)
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)

_MAX_FILE_SIZE = 20 * 1024 * 1024
_ALIGN_MAP = {"left": 1, "center": 2, "right": 3}
_MIME_TO_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
    "video/mp4": ".mp4",
    "video/mpeg": ".mpeg",
    "video/quicktime": ".mov",
    "video/x-msvideo": ".avi",
    "video/webm": ".webm",
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-powerpoint": ".ppt",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/zip": ".zip",
    "application/x-rar-compressed": ".rar",
    "text/plain": ".txt",
    "application/json": ".json",
}


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _extract_document_id(value: str) -> str:
    """从文档 URL 或纯 token 中提取 document_id。"""
    trimmed = str(value or "").strip()
    if not trimmed:
        raise ValueError("doc_id is required")
    if "/docx/" not in trimmed:
        return trimmed
    parsed = urlparse(trimmed)
    parts = [part for part in parsed.path.split("/") if part]
    try:
        index = parts.index("docx")
        return parts[index + 1]
    except Exception as exc:
        raise ValueError(f"failed to extract document id from url: {trimmed}") from exc


def _upload_doc_media(*, file_path: str, document_id: str, parent_type: str, parent_node: str) -> dict[str, Any]:
    """上传文档素材。

    这里直接走 multipart 请求，避免为了单个工具引入更大的 SDK 抽象。
    上传成功后返回飞书原始 JSON，供上层继续拼接 block 更新流程。
    """
    token = get_tenant_access_token()
    url = f"{get_feishu_base_url()}/open-apis/drive/v1/medias/upload_all"
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    headers = {"Authorization": f"Bearer {token}"}
    data = {
        "file_name": file_name,
        "parent_type": parent_type,
        "parent_node": parent_node,
        "size": str(file_size),
        "extra": json.dumps({"drive_route_token": document_id}),
    }
    with open(file_path, "rb") as fh:
        files = {"file": (file_name, fh)}
        response = httpx.post(url, headers=headers, data=data, files=files, timeout=120)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Feishu doc media upload returned a non-object response.")
    if payload.get("code") not in (0, None):
        raise RuntimeError(
            f"Feishu doc media upload error: code={payload.get('code')} msg={payload.get('msg') or payload.get('message')}"
        )
    return payload


def _handle_doc_media(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "insert":
            document_id = _extract_document_id(str(args.get("doc_id", "")))
            file_path = str(args.get("file_path", "")).strip()
            media_type = str(args.get("type", "image")).strip().lower() or "image"
            if media_type not in {"image", "file"}:
                return tool_error("Parameter 'type' must be image or file.")
            if not file_path:
                return tool_error("Missing required parameter: file_path")
            local_path = Path(file_path)
            if not local_path.is_file():
                return tool_error(f"Local file does not exist: {file_path}")
            file_size = local_path.stat().st_size
            if file_size > _MAX_FILE_SIZE:
                return tool_error("File exceeds the 20MB limit.")

            # 先在文档尾部创建空 block，再把上传后的素材 token 回填到 block 上。
            config = {
                "image": {"block_type": 27, "block_data": {"image": {}}, "parent_type": "docx_image"},
                "file": {"block_type": 23, "block_data": {"file": {"token": ""}}, "parent_type": "docx_file"},
            }[media_type]
            create_data = feishu_api_request(
                "POST",
                f"/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/children",
                params={"document_revision_id": -1, "client_token": str(uuid.uuid4())},
                json_body={"children": [{"block_type": config["block_type"], **config["block_data"]}]},
            )
            payload = create_data.get("data") or {}
            children = payload.get("children") or []
            block_id = ""
            if media_type == "file":
                if children and isinstance(children[0], dict):
                    nested = children[0].get("children") or []
                    if nested:
                        block_id = str(nested[0])
            else:
                if children and isinstance(children[0], dict):
                    block_id = str(children[0].get("block_id", "")).strip()
            if not block_id:
                return tool_error("Failed to create a document media block.")

            upload_data = _upload_doc_media(
                file_path=file_path,
                document_id=document_id,
                parent_type=str(config["parent_type"]),
                parent_node=block_id,
            )
            file_token = str(upload_data.get("file_token") or (upload_data.get("data") or {}).get("file_token") or "").strip()
            if not file_token:
                return tool_error("Feishu media upload did not return file_token.")

            patch_request: dict[str, Any] = {"block_id": block_id}
            if media_type == "image":
                patch_request["replace_image"] = {
                    "token": file_token,
                    "align": _ALIGN_MAP.get(str(args.get("align", "center")).strip().lower() or "center", 2),
                }
                caption = str(args.get("caption", "")).strip()
                if caption:
                    patch_request["replace_image"]["caption"] = {"content": caption}
            else:
                patch_request["replace_file"] = {"token": file_token}

            feishu_api_request(
                "POST",
                f"/open-apis/docx/v1/documents/{document_id}/blocks/batch_update",
                params={"document_revision_id": -1},
                json_body={"requests": [patch_request]},
            )
            return json.dumps(
                {
                    "success": True,
                    "type": media_type,
                    "document_id": document_id,
                    "block_id": block_id,
                    "file_token": file_token,
                    "file_name": local_path.name,
                },
                ensure_ascii=False,
            )

        if action == "download":
            resource_token = str(args.get("resource_token", "")).strip()
            resource_type = str(args.get("resource_type", "")).strip().lower()
            output_path = str(args.get("output_path", "")).strip()
            if not resource_token or resource_type not in {"media", "whiteboard"} or not output_path:
                return tool_error("Parameters 'resource_token', 'resource_type(media|whiteboard)', and 'output_path' are required.")
            if resource_type == "media":
                content, headers = feishu_api_request_bytes("GET", f"/open-apis/drive/v1/medias/{resource_token}/download")
            else:
                content, headers = feishu_api_request_bytes(
                    "GET",
                    f"/open-apis/board/v1/whiteboards/{resource_token}/download_as_image",
                )
            content_type = str(headers.get("content-type", "")).split(";")[0].strip().lower()
            final_path = output_path
            if not Path(final_path).suffix:
                suggested = _MIME_TO_EXT.get(content_type) or (".png" if resource_type == "whiteboard" else "")
                if suggested:
                    final_path = final_path + suggested
            target = Path(final_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            target.chmod(0o600)
            return json.dumps(
                {
                    "resource_type": resource_type,
                    "resource_token": resource_token,
                    "size_bytes": len(content),
                    "content_type": content_type,
                    "saved_path": str(target),
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: insert, download")
    except Exception as exc:
        logger.error("feishu_doc_media error: %s", exc)
        return tool_error(f"Failed to execute feishu_doc_media: {exc}")


FEISHU_DOC_MEDIA_SCHEMA = {
    "name": "feishu_doc_media",
    "description": "Manage Feishu document media. Hermes supports insert and download.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["insert", "download"], "description": "Document media action."},
            "doc_id": {"type": "string", "description": "Document ID or doc URL for insert action."},
            "file_path": {"type": "string", "description": "Absolute local file path for insert action. Max 20MB."},
            "type": {"type": "string", "enum": ["image", "file"], "description": "Media type for insert action. Default image."},
            "align": {"type": "string", "enum": ["left", "center", "right"], "description": "Image alignment for insert action."},
            "caption": {"type": "string", "description": "Optional image caption for insert action."},
            "resource_token": {"type": "string", "description": "Media file token or whiteboard id for download action."},
            "resource_type": {"type": "string", "enum": ["media", "whiteboard"], "description": "Download resource type."},
            "output_path": {"type": "string", "description": "Target local output path for download action."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_doc_media",
    toolset="feishu",
    schema=FEISHU_DOC_MEDIA_SCHEMA,
    handler=_handle_doc_media,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
