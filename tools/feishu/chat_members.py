"""飞书群成员工具。"""

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


def _handle_chat_members(args: dict, **_kw) -> str:
    """列出指定群聊的成员。

    成员列表是后续权限判断、会话富化和群上下文说明的基础数据，因此保持原始字段
    返回，避免工具层过度裁剪。
    """
    chat_id = str(args.get("chat_id", "")).strip()
    if not chat_id:
        return tool_error("Missing required parameter: chat_id")
    member_id_type = str(args.get("member_id_type", "open_id")).strip() or "open_id"
    page_size = max(1, min(int(args.get("page_size", 50) or 50), 200))
    page_token = str(args.get("page_token", "")).strip()
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_chat_members",
            action="default",
            title="Feishu Chat Authorization Required",
            tool_args=args,
        )
        if auth_result is not None:
            return auth_result
        params = {
            "member_id_type": member_id_type,
            "page_size": str(page_size),
        }
        if page_token:
            params["page_token"] = page_token
        data = feishu_api_request("GET", f"/open-apis/im/v1/chats/{chat_id}/members", params=params)
        payload = data.get("data") or {}
        return json.dumps(
            {
                "items": payload.get("items", []),
                "member_total": payload.get("member_total"),
                "has_more": bool(payload.get("has_more", False)),
                "page_token": payload.get("page_token"),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_chat_members",
            action="default",
            title="Feishu Chat Authorization Required",
            tool_args=args,
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_chat_members error: %s", exc)
        return tool_error(f"Failed to execute feishu_chat_members: {exc}")


FEISHU_CHAT_MEMBERS_SCHEMA = {
    "name": "feishu_chat_members",
    "description": "List members in a Feishu chat with paging metadata.",
    "parameters": {
        "type": "object",
        "properties": {
            "chat_id": {"type": "string", "description": "Chat ID to inspect."},
            "member_id_type": {
                "type": "string",
                "enum": ["open_id", "union_id", "user_id"],
                "description": "The ID type returned for members. Defaults to open_id.",
            },
            "page_size": {
                "type": "integer",
                "minimum": 1,
                "maximum": 200,
                "description": "Page size. Default 50.",
            },
            "page_token": {"type": "string", "description": "Pagination token from a previous call."},
        },
        "required": ["chat_id"],
    },
}

registry.register(
    name="feishu_chat_members",
    toolset="feishu",
    schema=FEISHU_CHAT_MEMBERS_SCHEMA,
    handler=_handle_chat_members,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
