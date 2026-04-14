"""飞书多维表格记录工具。"""

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


def _validate_fields(value: Any, field_name: str = "fields") -> dict[str, Any]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{field_name} must be a non-empty object")
    return value


def _validate_batch_records(value: Any, *, require_record_id: bool) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("records must be a non-empty array")
    if len(value) > 500:
        raise ValueError("records exceeds the maximum size of 500")
    validated: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"records[{index}] must be an object")
        current: dict[str, Any] = {}
        if require_record_id:
            record_id = str(item.get("record_id", "")).strip()
            if not record_id:
                raise ValueError(f"records[{index}].record_id is required")
            current["record_id"] = record_id
        current["fields"] = _validate_fields(item.get("fields"), field_name=f"records[{index}].fields")
        validated.append(current)
    return validated


def _validate_record_ids(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("record_ids must be a non-empty array")
    if len(value) > 500:
        raise ValueError("record_ids exceeds the maximum size of 500")
    result = [str(item).strip() for item in value if str(item).strip()]
    if len(result) != len(value):
        raise ValueError("record_ids cannot contain empty values")
    return result


def _handle_bitable_app_table_record(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    app_token = str(args.get("app_token", "")).strip()
    table_id = str(args.get("table_id", "")).strip()
    if not app_token or not table_id:
        return tool_error("Parameters 'app_token' and 'table_id' are required.")
    try:
        if action == "create":
            fields = _validate_fields(args.get("fields"))
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                json_body={"fields": fields},
            )
            payload = data.get("data") or {}
            return json.dumps({"record": payload.get("record", payload)}, ensure_ascii=False)

        if action == "list":
            body: dict[str, Any] = {}
            view_id = str(args.get("view_id", "")).strip()
            if view_id:
                body["view_id"] = view_id
            if isinstance(args.get("field_names"), list):
                body["field_names"] = args.get("field_names")
            if isinstance(args.get("filter"), dict):
                body["filter"] = args.get("filter")
            if isinstance(args.get("sort"), list):
                body["sort"] = args.get("sort")
            if args.get("automatic_fields") is not None:
                body["automatic_fields"] = bool(args.get("automatic_fields"))
            page_size = max(1, min(int(args.get("page_size", 50) or 50), 500))
            body["page_size"] = page_size
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                body["page_token"] = page_token
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search",
                json_body=body,
            )
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

        if action == "update":
            record_id = str(args.get("record_id", "")).strip()
            if not record_id:
                return tool_error("Missing required parameter: record_id")
            fields = _validate_fields(args.get("fields"))
            data = feishu_api_request(
                "PUT",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}",
                json_body={"fields": fields},
            )
            payload = data.get("data") or {}
            return json.dumps({"record": payload.get("record", payload)}, ensure_ascii=False)

        if action == "delete":
            record_id = str(args.get("record_id", "")).strip()
            if not record_id:
                return tool_error("Missing required parameter: record_id")
            data = feishu_api_request(
                "DELETE",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}",
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "deleted": True,
                    "record_id": record_id,
                    "data": payload,
                },
                ensure_ascii=False,
            )

        if action == "batch_create":
            records = _validate_batch_records(args.get("records"), require_record_id=False)
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
                json_body={"records": records},
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "records": payload.get("records", []),
                    "total": payload.get("total"),
                },
                ensure_ascii=False,
            )

        if action == "batch_update":
            records = _validate_batch_records(args.get("records"), require_record_id=True)
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update",
                json_body={"records": records},
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "records": payload.get("records", []),
                    "total": payload.get("total"),
                },
                ensure_ascii=False,
            )

        if action == "batch_delete":
            record_ids = _validate_record_ids(args.get("record_ids"))
            data = feishu_api_request(
                "POST",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete",
                json_body={"record_ids": record_ids},
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "deleted": True,
                    "record_ids": record_ids,
                    "total": payload.get("total"),
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: create, list, update, delete, batch_create, batch_update, batch_delete")
    except Exception as exc:
        logger.error("feishu_bitable_app_table_record error: %s", exc)
        return tool_error(f"Failed to execute feishu_bitable_app_table_record: {exc}")


FEISHU_BITABLE_APP_TABLE_RECORD_SCHEMA = {
    "name": "feishu_bitable_app_table_record",
    "description": "Manage Feishu bitable records. Hermes currently supports create, list, update, delete, batch_create, batch_update, and batch_delete.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "list", "update", "delete", "batch_create", "batch_update", "batch_delete"], "description": "Bitable record action."},
            "app_token": {"type": "string", "description": "Bitable app token."},
            "table_id": {"type": "string", "description": "Bitable table ID."},
            "record_id": {"type": "string", "description": "Record ID for update or delete action."},
            "fields": {
                "type": "object",
                "description": "Record fields for create or update action.",
                "additionalProperties": True,
            },
            "view_id": {"type": "string", "description": "Optional view ID for list action."},
            "field_names": {"type": "array", "description": "Optional field name whitelist for list action.", "items": {"type": "string"}},
            "filter": {"type": "object", "description": "Structured filter object for list action."},
            "sort": {"type": "array", "description": "Sort rules for list action.", "items": {"type": "object"}},
            "automatic_fields": {"type": "boolean", "description": "Whether to return automatic fields in list action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
            "records": {
                "type": "array",
                "description": "Batch record payload for batch_create or batch_update.",
                "items": {"type": "object"},
            },
            "record_ids": {
                "type": "array",
                "description": "Record ID list for batch_delete.",
                "items": {"type": "string"},
            },
        },
        "required": ["action", "app_token", "table_id"],
    },
}

registry.register(
    name="feishu_bitable_app_table_record",
    toolset="feishu",
    schema=FEISHU_BITABLE_APP_TABLE_RECORD_SCHEMA,
    handler=_handle_bitable_app_table_record,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
