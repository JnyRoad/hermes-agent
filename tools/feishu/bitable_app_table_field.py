"""飞书多维表格字段工具。"""

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


def _sanitize_field_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """清理已知不允许携带 property 的字段类型。"""
    current = dict(payload)
    if current.get("type") in {7, 15} and "property" in current:
        current.pop("property", None)
    return current


def _list_fields(app_token: str, table_id: str, view_id: str = "") -> list[dict[str, Any]]:
    params: dict[str, str] = {"page_size": "100"}
    if view_id:
        params["view_id"] = view_id
    data = feishu_api_request("GET", f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields", params=params)
    payload = data.get("data") or {}
    return payload.get("items", [])


def _find_field(app_token: str, table_id: str, field_id: str) -> dict[str, Any]:
    for item in _list_fields(app_token, table_id):
        if isinstance(item, dict) and str(item.get("field_id", "")).strip() == field_id:
            return item
    raise ValueError(f"field_id not found: {field_id}")


def _handle_bitable_app_table_field(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    app_token = str(args.get("app_token", "")).strip()
    table_id = str(args.get("table_id", "")).strip()
    if not app_token or not table_id:
        return tool_error("Parameters 'app_token' and 'table_id' are required.")
    try:
        if action == "create":
            field_name = str(args.get("field_name", "")).strip()
            field_type = args.get("type")
            if not field_name or field_type is None:
                return tool_error("Parameters 'field_name' and 'type' are required for create.")
            body: dict[str, Any] = {
                "field_name": field_name,
                "type": int(field_type),
            }
            if args.get("property") is not None:
                body["property"] = args.get("property")
            body = _sanitize_field_payload(body)
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps({"field": payload.get("field", payload)}, ensure_ascii=False)

        if action == "list":
            params = {
                "page_size": str(max(1, min(int(args.get("page_size", 50) or 50), 100))),
            }
            view_id = str(args.get("view_id", "")).strip()
            if view_id:
                params["view_id"] = view_id
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "fields": payload.get("items", []),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "update":
            field_id = str(args.get("field_id", "")).strip()
            if not field_id:
                return tool_error("Missing required parameter: field_id")
            current = _find_field(app_token, table_id, field_id)
            field_name = args.get("field_name", current.get("field_name"))
            field_type = args.get("type", current.get("type"))
            if field_name is None or field_type is None:
                return tool_error("field_name/type could not be resolved for update.")
            body: dict[str, Any] = {
                "field_name": field_name,
                "type": int(field_type),
            }
            if args.get("property") is not None:
                body["property"] = args.get("property")
            elif current.get("property") is not None:
                body["property"] = current.get("property")
            body = _sanitize_field_payload(body)
            data = feishu_api_request(
                "PUT",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}",
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps({"field": payload.get("field", payload)}, ensure_ascii=False)

        if action == "delete":
            field_id = str(args.get("field_id", "")).strip()
            if not field_id:
                return tool_error("Missing required parameter: field_id")
            data = feishu_api_request(
                "DELETE",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}",
            )
            payload = data.get("data") or {}
            return json.dumps({"deleted": True, "field_id": field_id, "data": payload}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: create, list, update, delete")
    except Exception as exc:
        logger.error("feishu_bitable_app_table_field error: %s", exc)
        return tool_error(f"Failed to execute feishu_bitable_app_table_field: {exc}")


FEISHU_BITABLE_APP_TABLE_FIELD_SCHEMA = {
    "name": "feishu_bitable_app_table_field",
    "description": "Manage Feishu bitable fields. Hermes currently supports create, list, update, and delete.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "list", "update", "delete"], "description": "Bitable field action."},
            "app_token": {"type": "string", "description": "Bitable app token."},
            "table_id": {"type": "string", "description": "Bitable table ID."},
            "field_id": {"type": "string", "description": "Field ID for update or delete action."},
            "field_name": {"type": "string", "description": "Field name for create or update action."},
            "type": {"type": "integer", "description": "Field type code for create or update action."},
            "property": {"type": "object", "description": "Optional field property payload."},
            "view_id": {"type": "string", "description": "Optional view ID for list action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
        },
        "required": ["action", "app_token", "table_id"],
    },
}

registry.register(
    name="feishu_bitable_app_table_field",
    toolset="feishu",
    schema=FEISHU_BITABLE_APP_TABLE_FIELD_SCHEMA,
    handler=_handle_bitable_app_table_field,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
