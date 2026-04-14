"""飞书任务基础工具。"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from tools.feishu.client import feishu_api_request
from tools.feishu.scopes import ensure_authorization, handle_authorization_error
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _to_timestamp_ms(value: str) -> str:
    """将 ISO 8601 时间转换为毫秒时间戳字符串。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return str(int(dt.timestamp() * 1000))


def _normalize_schedule_field(raw_value: Any, field_name: str) -> Dict[str, Any]:
    """统一处理任务开始时间和截止时间字段。"""
    if not isinstance(raw_value, dict):
        raise ValueError(f"{field_name} must be an object with timestamp and optional is_all_day")
    timestamp = str(raw_value.get("timestamp", "")).strip()
    if not timestamp:
        raise ValueError(f"{field_name}.timestamp is required")
    return {
        "timestamp": _to_timestamp_ms(timestamp),
        "is_all_day": bool(raw_value.get("is_all_day", False)),
    }


def _handle_task(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    user_id_type = str(args.get("user_id_type", "open_id")).strip() or "open_id"
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_task_task",
            action=action,
            title="Feishu Task Authorization Required",
            tool_args=args,
        )
        if auth_result is not None:
            return auth_result

        if action == "create":
            summary = str(args.get("summary", "")).strip()
            if not summary:
                return tool_error("Missing required parameter: summary")
            task_data: Dict[str, Any] = {"summary": summary}
            description = args.get("description")
            if description is not None:
                task_data["description"] = description
            if args.get("due") is not None:
                task_data["due"] = _normalize_schedule_field(args.get("due"), "due")
            if args.get("start") is not None:
                task_data["start"] = _normalize_schedule_field(args.get("start"), "start")
            if isinstance(args.get("members"), list):
                task_data["members"] = args.get("members")
            if args.get("repeat_rule"):
                task_data["repeat_rule"] = args.get("repeat_rule")
            if isinstance(args.get("tasklists"), list):
                task_data["tasklists"] = args.get("tasklists")
            data = feishu_api_request(
                "POST",
                "/open-apis/task/v2/tasks",
                params={"user_id_type": user_id_type},
                json_body=task_data,
            )
            payload = data.get("data") or {}
            return json.dumps({"task": payload.get("task", payload)}, ensure_ascii=False)

        if action == "get":
            task_guid = str(args.get("task_guid", "")).strip()
            if not task_guid:
                return tool_error("Missing required parameter: task_guid")
            data = feishu_api_request(
                "GET",
                f"/open-apis/task/v2/tasks/{task_guid}",
                params={"user_id_type": user_id_type},
            )
            payload = data.get("data") or {}
            return json.dumps({"task": payload.get("task", payload)}, ensure_ascii=False)

        if action == "list":
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 100)),
                "user_id_type": user_id_type,
            }
            if args.get("completed") is not None:
                params["completed"] = str(bool(args.get("completed"))).lower()
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/task/v2/tasks", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "tasks": payload.get("items", payload.get("tasks", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "patch":
            task_guid = str(args.get("task_guid", "")).strip()
            if not task_guid:
                return tool_error("Missing required parameter: task_guid")
            update_data: Dict[str, Any] = {}
            if args.get("summary") is not None:
                update_data["summary"] = args.get("summary")
            if args.get("description") is not None:
                update_data["description"] = args.get("description")
            if args.get("due") is not None:
                update_data["due"] = _normalize_schedule_field(args.get("due"), "due")
            if args.get("start") is not None:
                update_data["start"] = _normalize_schedule_field(args.get("start"), "start")
            if args.get("completed_at") is not None:
                completed_at = str(args.get("completed_at")).strip()
                if completed_at == "0" or completed_at.isdigit():
                    update_data["completed_at"] = completed_at
                else:
                    update_data["completed_at"] = _to_timestamp_ms(completed_at)
            if isinstance(args.get("members"), list):
                update_data["members"] = args.get("members")
            if args.get("repeat_rule") is not None:
                update_data["repeat_rule"] = args.get("repeat_rule")
            if not update_data:
                return tool_error("At least one updatable field is required for patch.")
            data = feishu_api_request(
                "PATCH",
                f"/open-apis/task/v2/tasks/{task_guid}",
                params={"user_id_type": user_id_type},
                json_body={
                    "task": update_data,
                    "update_fields": list(update_data.keys()),
                },
            )
            payload = data.get("data") or {}
            return json.dumps({"task": payload.get("task", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: create, get, list, patch")
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_task_task",
            action=action,
            title="Feishu Task Authorization Required",
            tool_args=args,
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_task_task error: %s", exc)
        return tool_error(f"Failed to execute feishu_task_task: {exc}")


FEISHU_TASK_TASK_SCHEMA = {
    "name": "feishu_task_task",
    "description": "Manage Feishu tasks. Supported actions in Hermes now: create, get, list, and patch.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "get", "list", "patch"], "description": "Task action."},
            "task_guid": {"type": "string", "description": "Task GUID for get or patch action."},
            "summary": {"type": "string", "description": "Task summary for create or patch action."},
            "description": {"type": "string", "description": "Task description for create or patch action."},
            "due": {
                "type": "object",
                "description": "Due time for create or patch action.",
                "properties": {
                    "timestamp": {"type": "string", "description": "ISO 8601 timestamp with timezone."},
                    "is_all_day": {"type": "boolean", "description": "Whether the task is all day."},
                },
                "required": ["timestamp"],
            },
            "start": {
                "type": "object",
                "description": "Start time for create or patch action.",
                "properties": {
                    "timestamp": {"type": "string", "description": "ISO 8601 timestamp with timezone."},
                    "is_all_day": {"type": "boolean", "description": "Whether the task is all day."},
                },
                "required": ["timestamp"],
            },
            "completed_at": {
                "type": "string",
                "description": "Completion marker for patch action. Use ISO 8601, millisecond timestamp string, or 0 to mark unfinished.",
            },
            "members": {"type": "array", "description": "Task members list.", "items": {"type": "object"}},
            "repeat_rule": {"type": "string", "description": "RRULE repeat rule."},
            "tasklists": {"type": "array", "description": "Task list bindings for create action.", "items": {"type": "object"}},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
            "completed": {"type": "boolean", "description": "Completion filter for list action."},
            "user_id_type": {
                "type": "string",
                "enum": ["open_id", "union_id", "user_id"],
                "description": "User ID type for member-related fields. Defaults to open_id.",
            },
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_task_task",
    toolset="feishu",
    schema=FEISHU_TASK_TASK_SCHEMA,
    handler=_handle_task,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
