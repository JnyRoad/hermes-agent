"""飞书群聊基础工具。"""

from __future__ import annotations

import json
import logging

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


def _handle_chat(args: dict, **_kw) -> str:
    """处理飞书群聊查询。

    这里仅暴露 OpenClaw 中最稳定的两个只读动作，避免在消息平台工具层引入
    不必要的写操作副作用。
    """
    action = str(args.get("action", "")).strip().lower()
    user_id_type = str(args.get("user_id_type", "open_id")).strip() or "open_id"
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_chat",
            action=action,
            title="Feishu Chat Authorization Required",
            tool_args=args,
        )
        if auth_result is not None:
            return auth_result

        if action == "search":
            query = str(args.get("query", "")).strip()
            if not query:
                return tool_error("Missing required parameter: query")
            page_size = max(1, min(int(args.get("page_size", 20) or 20), 100))
            params = {
                "query": query,
                "page_size": str(page_size),
                "user_id_type": user_id_type,
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/im/v1/chats/search", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "items": payload.get("items", []),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "get":
            chat_id = str(args.get("chat_id", "")).strip()
            if not chat_id:
                return tool_error("Missing required parameter: chat_id")
            data = feishu_api_request(
                "GET",
                f"/open-apis/im/v1/chats/{chat_id}",
                params={"user_id_type": user_id_type},
            )
            payload = data.get("data") or {}
            return json.dumps({"chat": payload.get("chat", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: search, get")
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_chat",
            action=action,
            title="Feishu Chat Authorization Required",
            tool_args=args,
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_chat error: %s", exc)
        return tool_error(f"Failed to execute feishu_chat: {exc}")


FEISHU_CHAT_SCHEMA = {
    "name": "feishu_chat",
    "description": "Search Feishu chats by keyword or get a specific chat by chat_id.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["search", "get"], "description": "Chat action."},
            "query": {"type": "string", "description": "Search keyword for the search action."},
            "chat_id": {"type": "string", "description": "Chat ID for the get action."},
            "page_size": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "description": "Page size for the search action. Default 20.",
            },
            "page_token": {"type": "string", "description": "Pagination token for the search action."},
            "user_id_type": {
                "type": "string",
                "enum": ["open_id", "union_id", "user_id"],
                "description": "User ID type used by the API. Defaults to open_id.",
            },
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_chat",
    toolset="feishu",
    schema=FEISHU_CHAT_SCHEMA,
    handler=_handle_chat,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
