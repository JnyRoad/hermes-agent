"""飞书日历忙闲查询工具。"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

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


def _to_rfc3339(value: str) -> str:
    """将 ISO 8601 时间统一输出为 RFC3339 格式。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone().replace(microsecond=0).isoformat()


def _handle_calendar_freebusy(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    if action != "list":
        return tool_error("Unsupported action. Supported actions: list")
    try:
        time_min = str(args.get("time_min", "")).strip()
        time_max = str(args.get("time_max", "")).strip()
        user_ids = [str(item).strip() for item in (args.get("user_ids") or []) if str(item).strip()]
        if not time_min or not time_max:
            return tool_error("Parameters 'time_min' and 'time_max' are required.")
        if not user_ids:
            return tool_error("Parameter 'user_ids' must contain 1-10 user open_ids.")
        if len(user_ids) > 10:
            return tool_error(f"user_ids count exceeds limit, maximum 10 users (current: {len(user_ids)})")
        data = feishu_api_request(
            "POST",
            "/open-apis/calendar/v4/freebusy/batch",
            json_body={
                "time_min": _to_rfc3339(time_min),
                "time_max": _to_rfc3339(time_max),
                "user_ids": user_ids,
                "include_external_calendar": True,
                "only_busy": True,
            },
        )
        payload = data.get("data") or {}
        return json.dumps(
            {
                "freebusy_lists": payload.get("freebusy_lists", []),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_calendar_freebusy error: %s", exc)
        return tool_error(f"Failed to execute feishu_calendar_freebusy: {exc}")


FEISHU_CALENDAR_FREEBUSY_SCHEMA = {
    "name": "feishu_calendar_freebusy",
    "description": "Query Feishu user free/busy windows for a time range. Supported action in Hermes now: list.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["list"], "description": "Freebusy action."},
            "time_min": {"type": "string", "description": "Start time in ISO 8601 / RFC3339 format."},
            "time_max": {"type": "string", "description": "End time in ISO 8601 / RFC3339 format."},
            "user_ids": {
                "type": "array",
                "description": "1-10 user open_ids to query.",
                "items": {"type": "string"},
                "minItems": 1,
                "maxItems": 10,
            },
        },
        "required": ["action", "time_min", "time_max", "user_ids"],
    },
}

registry.register(
    name="feishu_calendar_freebusy",
    toolset="feishu",
    schema=FEISHU_CALENDAR_FREEBUSY_SCHEMA,
    handler=_handle_calendar_freebusy,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
