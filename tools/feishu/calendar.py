"""飞书日历基础工具。"""

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


def _handle_calendar(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "list":
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 1000)),
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/calendar/v4/calendars", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "calendars": payload.get("calendar_list", payload.get("items", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "get":
            calendar_id = str(args.get("calendar_id", "")).strip()
            if not calendar_id:
                return tool_error("Missing required parameter: calendar_id")
            data = feishu_api_request("GET", f"/open-apis/calendar/v4/calendars/{calendar_id}")
            payload = data.get("data") or {}
            return json.dumps({"calendar": payload.get("calendar", payload)}, ensure_ascii=False)

        if action == "primary":
            data = feishu_api_request("POST", "/open-apis/calendar/v4/calendars/primary", json_body={})
            payload = data.get("data") or {}
            return json.dumps({"calendars": payload.get("calendars", [])}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: list, get, primary")
    except Exception as exc:
        logger.error("feishu_calendar_calendar error: %s", exc)
        return tool_error(f"Failed to execute feishu_calendar_calendar: {exc}")


FEISHU_CALENDAR_CALENDAR_SCHEMA = {
    "name": "feishu_calendar_calendar",
    "description": "Manage Feishu calendars. Supported actions in Hermes now: list, get, and primary.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["list", "get", "primary"], "description": "Calendar action."},
            "calendar_id": {"type": "string", "description": "Calendar ID for get action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_calendar_calendar",
    toolset="feishu",
    schema=FEISHU_CALENDAR_CALENDAR_SCHEMA,
    handler=_handle_calendar,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
