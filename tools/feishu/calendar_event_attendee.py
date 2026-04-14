"""飞书日程参会人工具。"""

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


def _normalize_attendees(raw_attendees: Any) -> List[Dict[str, Any]]:
    """把参会人输入统一映射到飞书接口格式。"""
    if not isinstance(raw_attendees, list) or not raw_attendees:
        raise ValueError("attendees must be a non-empty array")
    attendees: List[Dict[str, Any]] = []
    for item in raw_attendees:
        if not isinstance(item, dict):
            raise ValueError("each attendee must be an object")
        attendee_type = str(item.get("type", "")).strip()
        attendee_id = str(item.get("attendee_id", "")).strip() or str(item.get("id", "")).strip()
        if not attendee_type or not attendee_id:
            raise ValueError("attendee type and attendee_id are required")
        entry: Dict[str, Any] = {"type": attendee_type, "is_optional": False}
        if attendee_type == "user":
            entry["user_id"] = attendee_id
        elif attendee_type == "chat":
            entry["chat_id"] = attendee_id
        elif attendee_type == "resource":
            entry["room_id"] = attendee_id
        elif attendee_type == "third_party":
            entry["third_party_email"] = attendee_id
        else:
            raise ValueError(f"unsupported attendee type: {attendee_type}")
        attendees.append(entry)
    return attendees


def _handle_calendar_event_attendee(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        calendar_id = str(args.get("calendar_id", "")).strip()
        event_id = str(args.get("event_id", "")).strip()
        if not calendar_id or not event_id:
            return tool_error("Parameters 'calendar_id' and 'event_id' are required.")
        auth_result = ensure_authorization(
            tool_name="feishu_calendar_event_attendee",
            action=action,
            title="Feishu Calendar Authorization Required",
            tool_args=args,
        )
        if auth_result is not None:
            return auth_result

        if action == "create":
            attendees = _normalize_attendees(args.get("attendees"))
            data = feishu_api_request(
                "POST",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees",
                params={"user_id_type": "open_id"},
                json_body={
                    "attendees": attendees,
                    "need_notification": bool(args.get("need_notification", True)),
                },
            )
            payload = data.get("data") or {}
            return json.dumps({"attendees": payload.get("attendees", [])}, ensure_ascii=False)

        if action == "list":
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 500)),
                "user_id_type": str(args.get("user_id_type", "open_id")).strip() or "open_id",
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request(
                "GET",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees",
                params=params,
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "attendees": payload.get("items", payload.get("attendees", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: create, list")
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_calendar_event_attendee",
            action=action,
            title="Feishu Calendar Authorization Required",
            tool_args=args,
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_calendar_event_attendee error: %s", exc)
        return tool_error(f"Failed to execute feishu_calendar_event_attendee: {exc}")


FEISHU_CALENDAR_EVENT_ATTENDEE_SCHEMA = {
    "name": "feishu_calendar_event_attendee",
    "description": "Manage Feishu calendar event attendees. Supported actions in Hermes now: create and list.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "list"], "description": "Calendar attendee action."},
            "calendar_id": {"type": "string", "description": "Calendar ID."},
            "event_id": {"type": "string", "description": "Event ID."},
            "attendees": {
                "type": "array",
                "description": "Attendee list for create action.",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["user", "chat", "resource", "third_party"]},
                        "attendee_id": {"type": "string", "description": "Attendee identifier matching the type."},
                        "id": {"type": "string", "description": "Alias of attendee_id for compatibility."},
                    },
                    "required": ["type"],
                },
            },
            "need_notification": {"type": "boolean", "description": "Whether to notify attendees on create. Default true."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
            "user_id_type": {
                "type": "string",
                "enum": ["open_id", "union_id", "user_id"],
                "description": "User ID type for list action. Defaults to open_id.",
            },
        },
        "required": ["action", "calendar_id", "event_id"],
    },
}

registry.register(
    name="feishu_calendar_event_attendee",
    toolset="feishu",
    schema=FEISHU_CALENDAR_EVENT_ATTENDEE_SCHEMA,
    handler=_handle_calendar_event_attendee,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
