"""飞书多维表格数据表工具。"""

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


def _sanitize_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """清理已知会触发飞书接口错误的字段属性。"""
    sanitized: list[dict[str, Any]] = []
    for field in fields:
        if not isinstance(field, dict):
            raise ValueError("table.fields must contain objects only")
        current = dict(field)
        if current.get("type") in {7, 15} and "property" in current:
            current.pop("property", None)
        sanitized.append(current)
    return sanitized


def _handle_bitable_app_table(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    app_token = str(args.get("app_token", "")).strip()
    if not app_token:
        return tool_error("Missing required parameter: app_token")
    try:
        if action == "create":
            table = args.get("table")
            if not isinstance(table, dict):
                return tool_error("Missing required parameter: table")
            body = {"table": dict(table)}
            if isinstance(body["table"].get("fields"), list):
                body["table"]["fields"] = _sanitize_fields(body["table"]["fields"])
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables",
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "table_id": payload.get("table_id"),
                    "default_view_id": payload.get("default_view_id"),
                    "field_id_list": payload.get("field_id_list"),
                },
                ensure_ascii=False,
            )

        if action == "list":
            params = {
                "page_size": str(max(1, min(int(args.get("page_size", 50) or 50), 100))),
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", f"/open-apis/bitable/v1/apps/{app_token}/tables", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "items": payload.get("items", []),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                    "total": payload.get("total"),
                },
                ensure_ascii=False,
            )

        if action == "patch":
            table_id = str(args.get("table_id", "")).strip()
            if not table_id:
                return tool_error("Missing required parameter: table_id")
            name = str(args.get("name", "")).strip()
            if not name:
                return tool_error("Missing required parameter: name")
            data = feishu_api_request(
                "PATCH",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}",
                json_body={"name": name},
            )
            payload = data.get("data") or {}
            return json.dumps({"table": payload.get("table", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: create, list, patch")
    except Exception as exc:
        logger.error("feishu_bitable_app_table error: %s", exc)
        return tool_error(f"Failed to execute feishu_bitable_app_table: {exc}")


FEISHU_BITABLE_APP_TABLE_SCHEMA = {
    "name": "feishu_bitable_app_table",
    "description": "Manage Feishu bitable tables. Hermes currently supports create, list, and patch.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "list", "patch"], "description": "Bitable table action."},
            "app_token": {"type": "string", "description": "Bitable app token."},
            "table_id": {"type": "string", "description": "Table ID for patch action."},
            "name": {"type": "string", "description": "Table name for patch action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
            "table": {
                "type": "object",
                "description": "Table definition for create action.",
                "properties": {
                    "name": {"type": "string"},
                    "default_view_name": {"type": "string"},
                    "fields": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "field_name": {"type": "string"},
                                "type": {"type": "integer"},
                                "property": {"type": "object"},
                            },
                            "required": ["field_name", "type"],
                        },
                    },
                },
                "required": ["name"],
            },
        },
        "required": ["action", "app_token"],
    },
}

registry.register(
    name="feishu_bitable_app_table",
    toolset="feishu",
    schema=FEISHU_BITABLE_APP_TABLE_SCHEMA,
    handler=_handle_bitable_app_table,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
