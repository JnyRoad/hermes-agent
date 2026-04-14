"""飞书机器人身份资源下载工具。"""

from __future__ import annotations

import json
import mimetypes
import os
import tempfile
from pathlib import Path

from tools.feishu.client import feishu_api_request_bytes
from tools.registry import registry, tool_error


_MIME_TO_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
}


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _resolve_extension(headers: dict[str, str]) -> str:
    content_type = str(headers.get("content-type", "")).split(";", 1)[0].strip().lower()
    return _MIME_TO_EXT.get(content_type) or mimetypes.guess_extension(content_type) or ""


def _handle_im_bot_image(args: dict, **_kw) -> str:
    message_id = str(args.get("message_id", "")).strip()
    file_key = str(args.get("file_key", "")).strip()
    resource_type = str(args.get("type", "")).strip().lower()
    if not message_id:
        return tool_error("Missing required parameter: message_id")
    if not file_key:
        return tool_error("Missing required parameter: file_key")
    if resource_type not in {"image", "file"}:
        return tool_error("Parameter 'type' must be either 'image' or 'file'.")

    try:
        content, headers = feishu_api_request_bytes(
            "GET",
            f"/open-apis/im/v1/messages/{message_id}/resources/{file_key}",
            params={"type": resource_type},
        )
        suffix = _resolve_extension(headers)
        fd, temp_path = tempfile.mkstemp(prefix="feishu-bot-resource-", suffix=suffix)
        os.close(fd)
        Path(temp_path).write_bytes(content)
        Path(temp_path).chmod(0o600)
        return json.dumps(
            {
                "message_id": message_id,
                "file_key": file_key,
                "type": resource_type,
                "saved_path": temp_path,
                "size_bytes": len(content),
                "content_type": headers.get("content-type", ""),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        return tool_error(f"Failed to download Feishu bot resource: {exc}")


FEISHU_IM_BOT_IMAGE_SCHEMA = {
    "name": "feishu_im_bot_image",
    "description": "Download a Feishu IM image or file resource using bot credentials and save it to a local temp file.",
    "parameters": {
        "type": "object",
        "properties": {
            "message_id": {"type": "string", "description": "Feishu message ID such as om_xxx."},
            "file_key": {"type": "string", "description": "Image key or file key from the Feishu message."},
            "type": {
                "type": "string",
                "enum": ["image", "file"],
                "description": "Resource type to fetch from the IM message.",
            },
        },
        "required": ["message_id", "file_key", "type"],
    },
}

registry.register(
    name="feishu_im_bot_image",
    toolset="feishu",
    schema=FEISHU_IM_BOT_IMAGE_SCHEMA,
    handler=_handle_im_bot_image,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
