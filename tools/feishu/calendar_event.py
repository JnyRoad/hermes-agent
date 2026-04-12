"""飞书日程事件工具。"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
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


def _to_unix_seconds(value: str) -> str:
    """将 ISO 8601 时间转换为秒级 Unix 时间戳。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return str(int(dt.timestamp()))


def _seconds_to_iso(value: Any) -> str:
    """将飞书日程时间字段统一转换为 ISO 8601。"""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc).astimezone().isoformat()
    except Exception:
        return raw


def _normalize_event_time_fields(event: Dict[str, Any]) -> Dict[str, Any]:
    """把飞书返回中的时间字段标准化为可读格式。"""
    normalized = dict(event)
    for key in ("create_time",):
        if key in normalized:
            normalized[key] = _seconds_to_iso(normalized.get(key))
    for key in ("start_time", "end_time"):
        value = normalized.get(key)
        if isinstance(value, dict):
            normalized[key] = _seconds_to_iso(value.get("timestamp")) or value.get("date") or value
        else:
            normalized[key] = _seconds_to_iso(value)
    return normalized


def _resolve_primary_calendar_id() -> str:
    """获取当前用户主日历。"""
    data = feishu_api_request("POST", "/open-apis/calendar/v4/calendars/primary", json_body={})
    calendars = data.get("data", {}).get("calendars", [])
    if not calendars:
        raise ValueError("Could not determine primary calendar")
    calendar = calendars[0].get("calendar") if isinstance(calendars[0], dict) else None
    if isinstance(calendar, dict) and calendar.get("calendar_id"):
        return str(calendar["calendar_id"])
    if isinstance(calendars[0], dict) and calendars[0].get("calendar_id"):
        return str(calendars[0]["calendar_id"])
    raise ValueError("Could not determine primary calendar")


def _calendar_id(args: dict) -> str:
    """优先使用显式 calendar_id，否则回退主日历。"""
    calendar_id = str(args.get("calendar_id", "")).strip()
    return calendar_id or _resolve_primary_calendar_id()


def _build_attendees(raw_attendees: Any, user_open_id: str) -> List[Dict[str, str]]:
    """合并显式参与人与当前用户。"""
    attendees: List[Dict[str, str]] = []
    if isinstance(raw_attendees, list):
        for item in raw_attendees:
            if not isinstance(item, dict):
                continue
            attendee_type = str(item.get("type", "")).strip()
            attendee_id = str(item.get("id", "")).strip()
            if attendee_type and attendee_id:
                attendees.append({"type": attendee_type, "id": attendee_id})
    if user_open_id and not any(item["type"] == "user" and item["id"] == user_open_id for item in attendees):
        attendees.append({"type": "user", "id": user_open_id})
    return attendees


def _build_attendee_batch(attendees: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """转换为飞书参会人批量创建格式。"""
    result: List[Dict[str, Any]] = []
    for attendee in attendees:
        entry: Dict[str, Any] = {"type": attendee["type"]}
        if attendee["type"] == "user":
            entry["user_id"] = attendee["id"]
        elif attendee["type"] == "chat":
            entry["chat_id"] = attendee["id"]
        elif attendee["type"] == "resource":
            entry["room_id"] = attendee["id"]
        elif attendee["type"] == "third_party":
            entry["third_party_email"] = attendee["id"]
        else:
            continue
        result.append(entry)
    return result


def _handle_calendar_event(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_calendar_event",
            action=action,
            title="Feishu Calendar Authorization Required",
        )
        if auth_result is not None:
            return auth_result

        if action == "create":
            summary = str(args.get("summary", "")).strip()
            start_time = str(args.get("start_time", "")).strip()
            end_time = str(args.get("end_time", "")).strip()
            if not summary or not start_time or not end_time:
                return tool_error("Parameters 'summary', 'start_time', and 'end_time' are required for create.")
            calendar_id = _calendar_id(args)
            event_body: Dict[str, Any] = {
                "summary": summary,
                "start_time": {"timestamp": _to_unix_seconds(start_time)},
                "end_time": {"timestamp": _to_unix_seconds(end_time)},
                "need_notification": True,
            }
            if args.get("description") is not None:
                event_body["description"] = args.get("description")
            if args.get("visibility"):
                event_body["visibility"] = args.get("visibility")
            if args.get("free_busy_status"):
                event_body["free_busy_status"] = args.get("free_busy_status")
            if args.get("attendee_ability"):
                event_body["attendee_ability"] = args.get("attendee_ability")
            if isinstance(args.get("location"), dict):
                event_body["location"] = args.get("location")
            if isinstance(args.get("reminders"), list):
                event_body["reminders"] = args.get("reminders")
            if args.get("recurrence"):
                event_body["recurrence"] = args.get("recurrence")
            if isinstance(args.get("vchat"), dict):
                event_body["vchat"] = args.get("vchat")
            data = feishu_api_request(
                "POST",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events",
                json_body=event_body,
            )
            payload = data.get("data") or {}
            event = payload.get("event", payload)
            event_id = str(event.get("event_id", "")).strip() if isinstance(event, dict) else ""
            attendees = _build_attendees(args.get("attendees"), str(args.get("user_open_id", "")).strip())
            warning = None
            if event_id and attendees:
                try:
                    feishu_api_request(
                        "POST",
                        f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees/batch_create",
                        params={"user_id_type": "open_id"},
                        json_body={"attendees": _build_attendee_batch(attendees), "need_notification": True},
                    )
                except Exception as exc:
                    warning = str(exc)
            result: Dict[str, Any] = {"event": _normalize_event_time_fields(event) if isinstance(event, dict) else event}
            if attendees:
                result["attendees"] = attendees
            if warning:
                result["warning"] = f"Event created but attendee sync failed: {warning}"
            return json.dumps(result, ensure_ascii=False)

        if action == "list":
            start_time = str(args.get("start_time", "")).strip()
            end_time = str(args.get("end_time", "")).strip()
            if not start_time or not end_time:
                return tool_error("Parameters 'start_time' and 'end_time' are required for list.")
            calendar_id = _calendar_id(args)
            data = feishu_api_request(
                "GET",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/instance_view",
                params={
                    "start_time": _to_unix_seconds(start_time),
                    "end_time": _to_unix_seconds(end_time),
                    "user_id_type": "open_id",
                },
            )
            payload = data.get("data") or {}
            items = payload.get("items", [])
            return json.dumps(
                {
                    "events": [
                        _normalize_event_time_fields(item) for item in items if isinstance(item, dict)
                    ],
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "get":
            event_id = str(args.get("event_id", "")).strip()
            if not event_id:
                return tool_error("Missing required parameter: event_id")
            calendar_id = _calendar_id(args)
            data = feishu_api_request(
                "GET",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}",
            )
            payload = data.get("data") or {}
            event = payload.get("event", payload)
            return json.dumps({"event": _normalize_event_time_fields(event) if isinstance(event, dict) else event}, ensure_ascii=False)

        if action == "patch":
            event_id = str(args.get("event_id", "")).strip()
            if not event_id:
                return tool_error("Missing required parameter: event_id")
            calendar_id = _calendar_id(args)
            update_body: Dict[str, Any] = {}
            if args.get("summary") is not None:
                update_body["summary"] = args.get("summary")
            if args.get("description") is not None:
                update_body["description"] = args.get("description")
            if args.get("start_time") is not None:
                update_body["start_time"] = {"timestamp": _to_unix_seconds(str(args.get("start_time")))}
            if args.get("end_time") is not None:
                update_body["end_time"] = {"timestamp": _to_unix_seconds(str(args.get("end_time")))}
            if args.get("location") is not None:
                update_body["location"] = {"name": str(args.get("location"))}
            if not update_body:
                return tool_error("At least one updatable field is required for patch.")
            data = feishu_api_request(
                "PATCH",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}",
                json_body=update_body,
            )
            payload = data.get("data") or {}
            event = payload.get("event", payload)
            return json.dumps({"event": _normalize_event_time_fields(event) if isinstance(event, dict) else event}, ensure_ascii=False)

        if action == "delete":
            event_id = str(args.get("event_id", "")).strip()
            if not event_id:
                return tool_error("Missing required parameter: event_id")
            calendar_id = _calendar_id(args)
            feishu_api_request(
                "DELETE",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}",
                params={"need_notification": str(bool(args.get("need_notification", True))).lower()},
            )
            return json.dumps({"success": True, "event_id": event_id}, ensure_ascii=False)

        if action == "search":
            query = str(args.get("query", "")).strip()
            if not query:
                return tool_error("Missing required parameter: query")
            calendar_id = _calendar_id(args)
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 500)),
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request(
                "POST",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/search",
                params=params,
                json_body={"query": query},
            )
            payload = data.get("data") or {}
            items = payload.get("items", [])
            return json.dumps(
                {
                    "events": [_normalize_event_time_fields(item) for item in items if isinstance(item, dict)],
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "reply":
            event_id = str(args.get("event_id", "")).strip()
            rsvp_status = str(args.get("rsvp_status", "")).strip().lower()
            if not event_id or not rsvp_status:
                return tool_error("Parameters 'event_id' and 'rsvp_status' are required for reply.")
            calendar_id = _calendar_id(args)
            feishu_api_request(
                "POST",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}/reply",
                json_body={"rsvp_status": rsvp_status},
            )
            return json.dumps({"success": True, "event_id": event_id, "rsvp_status": rsvp_status}, ensure_ascii=False)

        if action == "instances":
            event_id = str(args.get("event_id", "")).strip()
            start_time = str(args.get("start_time", "")).strip()
            end_time = str(args.get("end_time", "")).strip()
            if not event_id or not start_time or not end_time:
                return tool_error("Parameters 'event_id', 'start_time', and 'end_time' are required for instances.")
            calendar_id = _calendar_id(args)
            params = {
                "start_time": _to_unix_seconds(start_time),
                "end_time": _to_unix_seconds(end_time),
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 500)),
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request(
                "GET",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}/instances",
                params=params,
            )
            payload = data.get("data") or {}
            items = payload.get("items", [])
            return json.dumps(
                {
                    "instances": [_normalize_event_time_fields(item) for item in items if isinstance(item, dict)],
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "instance_view":
            start_time = str(args.get("start_time", "")).strip()
            end_time = str(args.get("end_time", "")).strip()
            if not start_time or not end_time:
                return tool_error("Parameters 'start_time' and 'end_time' are required for instance_view.")
            calendar_id = _calendar_id(args)
            params = {
                "start_time": _to_unix_seconds(start_time),
                "end_time": _to_unix_seconds(end_time),
                "user_id_type": "open_id",
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 500)),
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request(
                "GET",
                f"/open-apis/calendar/v4/calendars/{calendar_id}/events/instance_view",
                params=params,
            )
            payload = data.get("data") or {}
            items = payload.get("items", [])
            return json.dumps(
                {
                    "events": [_normalize_event_time_fields(item) for item in items if isinstance(item, dict)],
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: create, list, get, patch, delete, search, reply, instances, instance_view")
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_calendar_event",
            action=action,
            title="Feishu Calendar Authorization Required",
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_calendar_event error: %s", exc)
        return tool_error(f"Failed to execute feishu_calendar_event: {exc}")


FEISHU_CALENDAR_EVENT_SCHEMA = {
    "name": "feishu_calendar_event",
    "description": "Manage Feishu calendar events. Supported actions in Hermes now: create, list, get, patch, delete, search, reply, instances, and instance_view.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "list", "get", "patch", "delete", "search", "reply", "instances", "instance_view"], "description": "Calendar event action."},
            "calendar_id": {"type": "string", "description": "Calendar ID. Defaults to the primary calendar."},
            "event_id": {"type": "string", "description": "Event ID for get, patch, or delete action."},
            "summary": {"type": "string", "description": "Event summary for create or patch action."},
            "description": {"type": "string", "description": "Event description for create or patch action."},
            "start_time": {"type": "string", "description": "ISO 8601 start time for create, list, or patch action."},
            "end_time": {"type": "string", "description": "ISO 8601 end time for create, list, or patch action."},
            "query": {"type": "string", "description": "Keyword query for search action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Page size for search, instances, or instance_view action."},
            "page_token": {"type": "string", "description": "Pagination token for search, instances, or instance_view action."},
            "rsvp_status": {"type": "string", "enum": ["accept", "decline", "tentative"], "description": "RSVP status for reply action."},
            "user_open_id": {"type": "string", "description": "Current user open_id for attendee backfill during create."},
            "attendees": {"type": "array", "description": "Attendees for create action.", "items": {"type": "object"}},
            "vchat": {"type": "object", "description": "Video meeting config for create action."},
            "visibility": {"type": "string", "enum": ["default", "public", "private"], "description": "Visibility for create action."},
            "attendee_ability": {
                "type": "string",
                "enum": ["none", "can_see_others", "can_invite_others", "can_modify_event"],
                "description": "Attendee ability for create action.",
            },
            "free_busy_status": {"type": "string", "enum": ["busy", "free"], "description": "Free/busy state for create action."},
            "location": {"description": "Location object for create action or plain string name for patch action."},
            "reminders": {"type": "array", "description": "Reminder list for create action.", "items": {"type": "object"}},
            "recurrence": {"type": "string", "description": "RFC5545 RRULE for recurring events."},
            "need_notification": {"type": "boolean", "description": "Whether delete should notify attendees. Default true."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_calendar_event",
    toolset="feishu",
    schema=FEISHU_CALENDAR_EVENT_SCHEMA,
    handler=_handle_calendar_event,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
