"""飞书多维表格视图工具。"""

from __future__ import annotations

import json
import logging

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


def _handle_bitable_app_table_view(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    app_token = str(args.get("app_token", "")).strip()
    table_id = str(args.get("table_id", "")).strip()
    if not app_token or not table_id:
        return tool_error("Parameters 'app_token' and 'table_id' are required.")
    try:
        if action == "create":
            view_name = str(args.get("view_name", "")).strip()
            if not view_name:
                return tool_error("Missing required parameter: view_name")
            body = {"view_name": view_name, "view_type": str(args.get("view_type", "grid")).strip() or "grid"}
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/views",
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps({"view": payload.get("view", payload)}, ensure_ascii=False)

        if action == "get":
            view_id = str(args.get("view_id", "")).strip()
            if not view_id:
                return tool_error("Missing required parameter: view_id")
            data = feishu_api_request("GET", f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/views/{view_id}")
            payload = data.get("data") or {}
            return json.dumps({"view": payload.get("view", payload)}, ensure_ascii=False)

        if action == "list":
            params = {"page_size": str(max(1, min(int(args.get("page_size", 50) or 50), 100)))}
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/views", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "views": payload.get("items", []),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "patch":
            view_id = str(args.get("view_id", "")).strip()
            view_name = str(args.get("view_name", "")).strip()
            if not view_id or not view_name:
                return tool_error("Parameters 'view_id' and 'view_name' are required for patch.")
            data = feishu_api_request(
                "PATCH",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/views/{view_id}",
                json_body={"view_name": view_name},
            )
            payload = data.get("data") or {}
            return json.dumps({"view": payload.get("view", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: create, get, list, patch")
    except Exception as exc:
        logger.error("feishu_bitable_app_table_view error: %s", exc)
        return tool_error(f"Failed to execute feishu_bitable_app_table_view: {exc}")


FEISHU_BITABLE_APP_TABLE_VIEW_SCHEMA = {
    "name": "feishu_bitable_app_table_view",
    "description": "Manage Feishu bitable views. Hermes currently supports create, get, list, and patch.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "get", "list", "patch"], "description": "Bitable view action."},
            "app_token": {"type": "string", "description": "Bitable app token."},
            "table_id": {"type": "string", "description": "Bitable table ID."},
            "view_id": {"type": "string", "description": "View ID for get or patch action."},
            "view_name": {"type": "string", "description": "View name for create or patch action."},
            "view_type": {
                "type": "string",
                "enum": ["grid", "kanban", "gallery", "gantt", "form"],
                "description": "View type for create action. Default grid.",
            },
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
        },
        "required": ["action", "app_token", "table_id"],
    },
}

registry.register(
    name="feishu_bitable_app_table_view",
    toolset="feishu",
    schema=FEISHU_BITABLE_APP_TABLE_VIEW_SCHEMA,
    handler=_handle_bitable_app_table_view,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
