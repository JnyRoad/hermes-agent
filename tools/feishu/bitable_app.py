"""飞书多维表格应用工具。"""

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


def _handle_bitable_app(args: dict, **_kw) -> str:
    """处理多维表格应用级操作。"""
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "create":
            name = str(args.get("name", "")).strip()
            if not name:
                return tool_error("Missing required parameter: name")
            body: dict[str, Any] = {"name": name}
            folder_token = str(args.get("folder_token", "")).strip()
            if folder_token:
                body["folder_token"] = folder_token
            data = feishu_api_request("POST", "/open-apis/bitable/v1/apps", json_body=body)
            payload = data.get("data") or {}
            return json.dumps({"app": payload.get("app", payload)}, ensure_ascii=False)

        if action == "get":
            app_token = str(args.get("app_token", "")).strip()
            if not app_token:
                return tool_error("Missing required parameter: app_token")
            data = feishu_api_request("GET", f"/open-apis/bitable/v1/apps/{app_token}")
            payload = data.get("data") or {}
            return json.dumps({"app": payload.get("app", payload)}, ensure_ascii=False)

        if action == "list":
            params = {
                "page_size": str(max(1, min(int(args.get("page_size", 50) or 50), 200))),
            }
            folder_token = str(args.get("folder_token", "")).strip()
            if folder_token:
                params["folder_token"] = folder_token
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/drive/v1/files", params=params)
            payload = data.get("data") or {}
            files = payload.get("files", [])
            apps = [item for item in files if isinstance(item, dict) and item.get("type") == "bitable"]
            return json.dumps(
                {
                    "apps": apps,
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("next_page_token") or payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "patch":
            app_token = str(args.get("app_token", "")).strip()
            if not app_token:
                return tool_error("Missing required parameter: app_token")
            body: dict[str, Any] = {}
            if args.get("name") is not None:
                body["name"] = args.get("name")
            if args.get("is_advanced") is not None:
                body["is_advanced"] = bool(args.get("is_advanced"))
            if not body:
                return tool_error("At least one updatable field is required for patch.")
            data = feishu_api_request("PATCH", f"/open-apis/bitable/v1/apps/{app_token}", json_body=body)
            payload = data.get("data") or {}
            return json.dumps({"app": payload.get("app", payload)}, ensure_ascii=False)

        if action == "copy":
            app_token = str(args.get("app_token", "")).strip()
            name = str(args.get("name", "")).strip()
            if not app_token or not name:
                return tool_error("Parameters 'app_token' and 'name' are required for copy.")
            body: dict[str, Any] = {"name": name}
            folder_token = str(args.get("folder_token", "")).strip()
            if folder_token:
                body["folder_token"] = folder_token
            data = feishu_api_request("POST", f"/open-apis/bitable/v1/apps/{app_token}/copy", json_body=body)
            payload = data.get("data") or {}
            return json.dumps({"app": payload.get("app", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: create, get, list, patch, copy")
    except Exception as exc:
        logger.error("feishu_bitable_app error: %s", exc)
        return tool_error(f"Failed to execute feishu_bitable_app: {exc}")


FEISHU_BITABLE_APP_SCHEMA = {
    "name": "feishu_bitable_app",
    "description": "Manage Feishu bitable apps. Hermes currently supports create, get, list, patch, and copy.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "get", "list", "patch", "copy"], "description": "Bitable app action."},
            "name": {"type": "string", "description": "App name for create or patch action."},
            "app_token": {"type": "string", "description": "Bitable app token for get or patch action."},
            "folder_token": {"type": "string", "description": "Parent folder token for create, list, or copy action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 200, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
            "is_advanced": {"type": "boolean", "description": "Whether to enable advanced permission mode for patch action."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_bitable_app",
    toolset="feishu",
    schema=FEISHU_BITABLE_APP_SCHEMA,
    handler=_handle_bitable_app,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
