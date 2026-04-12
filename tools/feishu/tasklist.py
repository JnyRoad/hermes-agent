"""飞书任务清单工具。"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

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


def _normalize_members(raw_members: Any) -> List[Dict[str, Any]]:
    """统一任务清单成员结构。"""
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
                "role": str(item.get("role", "editor")).strip() or "editor",
            }
        )
    return members


def _handle_tasklist(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_task_tasklist",
            action=action,
            title="Feishu Task Authorization Required",
            tool_args=args,
        )
        if auth_result is not None:
            return auth_result

        if action == "create":
            name = str(args.get("name", "")).strip()
            if not name:
                return tool_error("Missing required parameter: name")
            body: Dict[str, Any] = {"name": name}
            if args.get("members") is not None:
                body["members"] = _normalize_members(args.get("members"))
            data = feishu_api_request(
                "POST",
                "/open-apis/task/v2/tasklists",
                params={"user_id_type": "open_id"},
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps({"tasklist": payload.get("tasklist", payload)}, ensure_ascii=False)

        if action == "get":
            tasklist_guid = str(args.get("tasklist_guid", "")).strip()
            if not tasklist_guid:
                return tool_error("Missing required parameter: tasklist_guid")
            data = feishu_api_request(
                "GET",
                f"/open-apis/task/v2/tasklists/{tasklist_guid}",
                params={"user_id_type": "open_id"},
            )
            payload = data.get("data") or {}
            return json.dumps({"tasklist": payload.get("tasklist", payload)}, ensure_ascii=False)

        if action == "list":
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 100)),
                "user_id_type": "open_id",
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/task/v2/tasklists", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "tasklists": payload.get("items", payload.get("tasklists", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "tasks":
            tasklist_guid = str(args.get("tasklist_guid", "")).strip()
            if not tasklist_guid:
                return tool_error("Missing required parameter: tasklist_guid")
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 100)),
                "user_id_type": "open_id",
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            if args.get("completed") is not None:
                params["completed"] = str(bool(args.get("completed"))).lower()
            data = feishu_api_request(
                "GET",
                f"/open-apis/task/v2/tasklists/{tasklist_guid}/tasks",
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

        if action == "patch":
            tasklist_guid = str(args.get("tasklist_guid", "")).strip()
            if not tasklist_guid:
                return tool_error("Missing required parameter: tasklist_guid")
            update_tasklist: Dict[str, Any] = {}
            update_fields: List[str] = []
            if args.get("name") is not None:
                update_tasklist["name"] = args.get("name")
                update_fields.append("name")
            if not update_fields:
                return tool_error("At least one updatable field is required for patch.")
            data = feishu_api_request(
                "PATCH",
                f"/open-apis/task/v2/tasklists/{tasklist_guid}",
                params={"user_id_type": "open_id"},
                json_body={"tasklist": update_tasklist, "update_fields": update_fields},
            )
            payload = data.get("data") or {}
            return json.dumps({"tasklist": payload.get("tasklist", payload)}, ensure_ascii=False)

        if action == "add_members":
            tasklist_guid = str(args.get("tasklist_guid", "")).strip()
            if not tasklist_guid:
                return tool_error("Missing required parameter: tasklist_guid")
            members = _normalize_members(args.get("members"))
            data = feishu_api_request(
                "POST",
                f"/open-apis/task/v2/tasklists/{tasklist_guid}/add_members",
                params={"user_id_type": "open_id"},
                json_body={"members": members},
            )
            payload = data.get("data") or {}
            return json.dumps({"tasklist": payload.get("tasklist", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: create, get, list, tasks, patch, add_members")
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_task_tasklist",
            action=action,
            title="Feishu Task Authorization Required",
            tool_args=args,
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_task_tasklist error: %s", exc)
        return tool_error(f"Failed to execute feishu_task_tasklist: {exc}")


FEISHU_TASK_TASKLIST_SCHEMA = {
    "name": "feishu_task_tasklist",
    "description": "Manage Feishu task lists. Supported actions in Hermes now: create, get, list, tasks, patch, and add_members.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "get", "list", "tasks", "patch", "add_members"], "description": "Task list action."},
            "tasklist_guid": {"type": "string", "description": "Task list GUID for get, tasks, patch, or add_members action."},
            "name": {"type": "string", "description": "Task list name for create or patch action."},
            "members": {"type": "array", "description": "Task list members for create or add_members action.", "items": {"type": "object"}},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list or tasks action."},
            "page_token": {"type": "string", "description": "Pagination token for list or tasks action."},
            "completed": {"type": "boolean", "description": "Completion filter for tasks action."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_task_tasklist",
    toolset="feishu",
    schema=FEISHU_TASK_TASKLIST_SCHEMA,
    handler=_handle_tasklist,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
