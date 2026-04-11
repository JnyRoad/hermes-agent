"""飞书用户目录工具。"""

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


def _handle_get_user(args: dict, **_kw) -> str:
    user_id = str(args.get("user_id", "")).strip()
    user_id_type = str(args.get("user_id_type", "open_id")).strip() or "open_id"
    try:
        if not user_id:
            return tool_error(
                "Missing required parameter: user_id. Hermes currently resolves explicit users only."
            )
        data = feishu_api_request(
            "GET",
            f"/open-apis/contact/v3/users/{user_id}",
            params={"user_id_type": user_id_type},
        )
        return json.dumps({"user": (data.get("data") or {}).get("user")}, ensure_ascii=False)
    except Exception as exc:
        logger.error("feishu_get_user error: %s", exc)
        return tool_error(f"Failed to get user: {exc}")


def _handle_search_user(args: dict, **_kw) -> str:
    query = str(args.get("query", "")).strip()
    if not query:
        return tool_error("Missing required parameter: query")
    page_size = int(args.get("page_size", 20) or 20)
    page_size = max(1, min(page_size, 200))
    page_token = str(args.get("page_token", "")).strip()
    try:
        params = {"query": query, "page_size": str(page_size)}
        if page_token:
            params["page_token"] = page_token
        data = feishu_api_request("GET", "/open-apis/search/v1/user", params=params)
        payload = data.get("data") or {}
        return json.dumps(
            {
                "users": payload.get("users", []),
                "has_more": bool(payload.get("has_more", False)),
                "page_token": payload.get("page_token"),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_search_user error: %s", exc)
        return tool_error(f"Failed to search user: {exc}")


FEISHU_GET_USER_SCHEMA = {
    "name": "feishu_get_user",
    "description": "Get a Feishu user by explicit ID. Returns profile fields such as name, avatar, email, department, and IDs.",
    "parameters": {
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "description": "Feishu user ID such as open_id / union_id / user_id."},
            "user_id_type": {
                "type": "string",
                "enum": ["open_id", "union_id", "user_id"],
                "description": "The ID type used in user_id. Defaults to open_id.",
            },
        },
        "required": ["user_id"],
    },
}

FEISHU_SEARCH_USER_SCHEMA = {
    "name": "feishu_search_user",
    "description": "Search Feishu users by keyword. Returns matching users with paging metadata.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Search keyword for name, email, or phone."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 200, "description": "Page size. Default 20."},
            "page_token": {"type": "string", "description": "Pagination token returned by the previous call."},
        },
        "required": ["query"],
    },
}

registry.register(
    name="feishu_get_user",
    toolset="feishu",
    schema=FEISHU_GET_USER_SCHEMA,
    handler=_handle_get_user,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_search_user",
    toolset="feishu",
    schema=FEISHU_SEARCH_USER_SCHEMA,
    handler=_handle_search_user,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
