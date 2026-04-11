"""飞书任务分组工具。"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

from tools.feishu.client import feishu_api_request
from tools.feishu.task import _to_timestamp_ms
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _normalize_time_filter(value: Any) -> str:
    """统一处理任务分组列表中的时间过滤字段。"""
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    if raw.isdigit():
        return raw
    return _to_timestamp_ms(raw)


def _handle_task_section(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    user_id_type = str(args.get("user_id_type", "open_id")).strip() or "open_id"
    try:
        if action == "create":
            name = str(args.get("name", "")).strip()
            resource_type = str(args.get("resource_type", "")).strip()
            if not name or not resource_type:
                return tool_error("Parameters 'name' and 'resource_type' are required for create.")
            body: Dict[str, Any] = {"name": name, "resource_type": resource_type}
            resource_id = str(args.get("resource_id", "")).strip()
            insert_before = str(args.get("insert_before", "")).strip()
            insert_after = str(args.get("insert_after", "")).strip()
            if resource_id:
                body["resource_id"] = resource_id
            if insert_before:
                body["insert_before"] = insert_before
            if insert_after:
                body["insert_after"] = insert_after
            data = feishu_api_request(
                "POST",
                "/open-apis/task/v2/sections",
                params={"user_id_type": user_id_type},
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps({"section": payload.get("section", payload)}, ensure_ascii=False)

        if action == "get":
            section_guid = str(args.get("section_guid", "")).strip()
            if not section_guid:
                return tool_error("Missing required parameter: section_guid")
            data = feishu_api_request(
                "GET",
                f"/open-apis/task/v2/sections/{section_guid}",
                params={"user_id_type": user_id_type},
            )
            payload = data.get("data") or {}
            return json.dumps({"section": payload.get("section", payload)}, ensure_ascii=False)

        if action == "patch":
            section_guid = str(args.get("section_guid", "")).strip()
            if not section_guid:
                return tool_error("Missing required parameter: section_guid")
            section_data: Dict[str, Any] = {}
            update_fields: List[str] = []
            if args.get("name") is not None:
                section_data["name"] = args.get("name")
                update_fields.append("name")
            if args.get("insert_before") is not None:
                section_data["insert_before"] = args.get("insert_before")
                update_fields.append("insert_before")
            if args.get("insert_after") is not None:
                section_data["insert_after"] = args.get("insert_after")
                update_fields.append("insert_after")
            if not update_fields:
                return tool_error("At least one updatable field is required for patch.")
            data = feishu_api_request(
                "PATCH",
                f"/open-apis/task/v2/sections/{section_guid}",
                params={"user_id_type": user_id_type},
                json_body={"section": section_data, "update_fields": update_fields},
            )
            payload = data.get("data") or {}
            return json.dumps({"section": payload.get("section", payload)}, ensure_ascii=False)

        if action == "list":
            resource_type = str(args.get("resource_type", "")).strip()
            if not resource_type:
                return tool_error("Missing required parameter: resource_type")
            params = {
                "resource_type": resource_type,
                "user_id_type": user_id_type,
            }
            resource_id = str(args.get("resource_id", "")).strip()
            if resource_id:
                params["resource_id"] = resource_id
            if args.get("page_size") is not None:
                params["page_size"] = max(1, min(int(args.get("page_size") or 50), 100))
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/task/v2/sections", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "sections": payload.get("items", payload.get("sections", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "tasks":
            section_guid = str(args.get("section_guid", "")).strip()
            if not section_guid:
                return tool_error("Missing required parameter: section_guid")
            params = {"user_id_type": user_id_type}
            if args.get("page_size") is not None:
                params["page_size"] = max(1, min(int(args.get("page_size") or 50), 100))
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            if args.get("completed") is not None:
                params["completed"] = str(bool(args.get("completed"))).lower()
            created_from = _normalize_time_filter(args.get("created_from"))
            created_to = _normalize_time_filter(args.get("created_to"))
            if created_from:
                params["created_from"] = created_from
            if created_to:
                params["created_to"] = created_to
            data = feishu_api_request(
                "GET",
                f"/open-apis/task/v2/sections/{section_guid}/tasks",
                params=params,
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "tasks": payload.get("items", payload.get("tasks", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: create, get, patch, list, tasks")
    except Exception as exc:
        logger.error("feishu_task_section error: %s", exc)
        return tool_error(f"Failed to execute feishu_task_section: {exc}")


FEISHU_TASK_SECTION_SCHEMA = {
    "name": "feishu_task_section",
    "description": "Manage Feishu task sections. Supported actions in Hermes now: create, get, patch, list, and tasks.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "get", "patch", "list", "tasks"], "description": "Task section action."},
            "section_guid": {"type": "string", "description": "Section GUID for get, patch, or tasks action."},
            "name": {"type": "string", "description": "Section name for create or patch action."},
            "resource_type": {"type": "string", "enum": ["tasklist", "my_tasks"], "description": "Section resource type for create or list action."},
            "resource_id": {"type": "string", "description": "Task list GUID when resource_type is tasklist."},
            "insert_before": {"type": "string", "description": "Insert current section before target section GUID."},
            "insert_after": {"type": "string", "description": "Insert current section after target section GUID."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list or tasks action."},
            "page_token": {"type": "string", "description": "Pagination token for list or tasks action."},
            "completed": {"type": "boolean", "description": "Completion filter for tasks action."},
            "created_from": {"type": "string", "description": "Created time filter lower bound for tasks action."},
            "created_to": {"type": "string", "description": "Created time filter upper bound for tasks action."},
            "user_id_type": {
                "type": "string",
                "enum": ["open_id", "union_id", "user_id"],
                "description": "User ID type. Defaults to open_id.",
            },
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_task_section",
    toolset="feishu",
    schema=FEISHU_TASK_SECTION_SCHEMA,
    handler=_handle_task_section,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
