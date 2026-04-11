"""飞书授权提示工具。

这里先落一层 Hermes 原生的人机闭环：
- 工具声明所需 scopes
- 飞书适配器发送说明卡片
- 用户完成后台授权后点击“已完成授权”
- 网关注入 synthetic message，驱动模型重试

真正的自动换取 user token / scope 自动补齐可在此结构上继续扩展。
"""

from __future__ import annotations

import json
import logging
import asyncio

from tools.feishu.runtime import get_active_feishu_adapter, require_feishu_session
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)


def _run_async(coro):
    """在同步工具 handler 中执行异步协程。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=30)
    return asyncio.run(coro)


def _check_feishu_runtime() -> bool:
    try:
        require_feishu_session()
        get_active_feishu_adapter()
        return True
    except Exception:
        return False


def _normalize_scopes(raw_scopes) -> list[str]:
    if not isinstance(raw_scopes, list):
        return []
    return [str(item).strip() for item in raw_scopes if str(item).strip()]


def _handle_feishu_oauth(args: dict, **_kw) -> str:
    scopes = _normalize_scopes(args.get("scopes"))
    if not scopes:
        return tool_error("Parameter 'scopes' must be a non-empty array.")
    reason = str(args.get("reason", "")).strip() or "This action requires additional Feishu permissions."
    try:
        adapter = get_active_feishu_adapter()
        session = require_feishu_session()
        result = _run_async(
            adapter.send_oauth_request_card(
                chat_id=session["chat_id"],
                scopes=scopes,
                reason=reason,
                metadata={"thread_id": session["thread_id"] or None},
            )
        )
        if not result.success:
            return tool_error(result.error or "Failed to send Feishu authorization card.")
        return json.dumps(
            {
                "status": "pending",
                "scopes": scopes,
                "request_id": ((result.raw_response or {}) if isinstance(result.raw_response, dict) else {}).get("request_id"),
                "message_id": result.message_id,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_oauth error: %s", exc)
        return tool_error(f"Failed to request Feishu authorization: {exc}")


def _handle_feishu_oauth_batch(args: dict, **_kw) -> str:
    scopes = _normalize_scopes(args.get("scopes"))
    if not scopes:
        return tool_error("Parameter 'scopes' must be a non-empty array.")
    title = str(args.get("title", "")).strip() or "Feishu Authorization Required"
    reason = str(args.get("reason", "")).strip() or "The requested batch of Feishu actions needs extra permissions."
    try:
        adapter = get_active_feishu_adapter()
        session = require_feishu_session()
        result = _run_async(
            adapter.send_oauth_request_card(
                chat_id=session["chat_id"],
                scopes=scopes,
                reason=reason,
                title=title,
                metadata={"thread_id": session["thread_id"] or None},
            )
        )
        if not result.success:
            return tool_error(result.error or "Failed to send Feishu batch authorization card.")
        return json.dumps(
            {
                "status": "pending",
                "scopes": scopes,
                "request_id": ((result.raw_response or {}) if isinstance(result.raw_response, dict) else {}).get("request_id"),
                "message_id": result.message_id,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_oauth_batch_auth error: %s", exc)
        return tool_error(f"Failed to request Feishu batch authorization: {exc}")


FEISHU_OAUTH_SCHEMA = {
    "name": "feishu_oauth",
    "description": "Request additional Feishu permissions from the current user by sending an authorization guidance card into the active Feishu chat.",
    "parameters": {
        "type": "object",
        "properties": {
            "scopes": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of Feishu scopes required by the action.",
            },
            "reason": {"type": "string", "description": "Why these scopes are needed."},
        },
        "required": ["scopes"],
    },
}

FEISHU_OAUTH_BATCH_SCHEMA = {
    "name": "feishu_oauth_batch_auth",
    "description": "Request a batch of Feishu scopes in one user-facing authorization card.",
    "parameters": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Card title shown to the user."},
            "scopes": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Batch of Feishu scopes required by upcoming actions.",
            },
            "reason": {"type": "string", "description": "Why these scopes are required together."},
        },
        "required": ["scopes"],
    },
}

registry.register(
    name="feishu_oauth",
    toolset="feishu",
    schema=FEISHU_OAUTH_SCHEMA,
    handler=_handle_feishu_oauth,
    check_fn=_check_feishu_runtime,
    emoji="🪽",
)

registry.register(
    name="feishu_oauth_batch_auth",
    toolset="feishu",
    schema=FEISHU_OAUTH_BATCH_SCHEMA,
    handler=_handle_feishu_oauth_batch,
    check_fn=_check_feishu_runtime,
    emoji="🪽",
)
