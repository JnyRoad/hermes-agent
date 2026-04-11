"""飞书子任务工具。"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

from tools.feishu.client import feishu_api_request
from tools.feishu.task import _normalize_schedule_field
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _normalize_members(raw_members: Any) -> List[Dict[str, Any]]:
    """统一子任务成员结构。"""
    if not isinstance(raw_members, list) or not raw_members:
        raise ValueError("members must be a non-empty array")
    members: List[Dict[str, Any]] = []
    for item in raw_members:
        if not isinstance(item, dict):
            raise ValueError("each member must be an object")
        member_id = str(item.get("id", "")).strip()
        if not member_id:
            raise ValueError("member.id is required")
        members.append(
            {
                "id": member_id,
                "type": "user",
                "role": str(item.get("role", "assignee")).strip() or "assignee",
            }
        )
    return members


def _handle_task_subtask(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "create":
            task_guid = str(args.get("task_guid", "")).strip()
            summary = str(args.get("summary", "")).strip()
            if not task_guid or not summary:
                return tool_error("Parameters 'task_guid' and 'summary' are required for create.")
            body: Dict[str, Any] = {"summary": summary}
            if args.get("description") is not None:
                body["description"] = args.get("description")
            if args.get("due") is not None:
                body["due"] = _normalize_schedule_field(args.get("due"), "due")
            if args.get("start") is not None:
                body["start"] = _normalize_schedule_field(args.get("start"), "start")
            if args.get("members") is not None:
                body["members"] = _normalize_members(args.get("members"))
            data = feishu_api_request(
                "POST",
                f"/open-apis/task/v2/tasks/{task_guid}/subtasks",
                params={"user_id_type": "open_id"},
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps({"subtask": payload.get("subtask", payload)}, ensure_ascii=False)

        if action == "list":
            task_guid = str(args.get("task_guid", "")).strip()
            if not task_guid:
                return tool_error("Missing required parameter: task_guid")
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 100)),
                "user_id_type": "open_id",
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request(
                "GET",
                f"/open-apis/task/v2/tasks/{task_guid}/subtasks",
                params=params,
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "subtasks": payload.get("items", payload.get("subtasks", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: create, list")
    except Exception as exc:
        logger.error("feishu_task_subtask error: %s", exc)
        return tool_error(f"Failed to execute feishu_task_subtask: {exc}")


FEISHU_TASK_SUBTASK_SCHEMA = {
    "name": "feishu_task_subtask",
    "description": "Manage Feishu task subtasks. Supported actions in Hermes now: create and list.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "list"], "description": "Task subtask action."},
            "task_guid": {"type": "string", "description": "Parent task GUID."},
            "summary": {"type": "string", "description": "Subtask summary for create action."},
            "description": {"type": "string", "description": "Subtask description for create action."},
            "due": {
                "type": "object",
                "description": "Due time for create action.",
                "properties": {
                    "timestamp": {"type": "string", "description": "ISO 8601 timestamp with timezone."},
                    "is_all_day": {"type": "boolean", "description": "Whether the subtask is all day."},
                },
                "required": ["timestamp"],
            },
            "start": {
                "type": "object",
                "description": "Start time for create action.",
                "properties": {
                    "timestamp": {"type": "string", "description": "ISO 8601 timestamp with timezone."},
                    "is_all_day": {"type": "boolean", "description": "Whether the subtask is all day."},
                },
                "required": ["timestamp"],
            },
            "members": {"type": "array", "description": "Subtask members for create action.", "items": {"type": "object"}},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
        },
        "required": ["action", "task_guid"],
    },
}

registry.register(
    name="feishu_task_subtask",
    toolset="feishu",
    schema=FEISHU_TASK_SUBTASK_SCHEMA,
    handler=_handle_task_subtask,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
