"""飞书工具运行态辅助函数。"""

from __future__ import annotations

import uuid
from typing import Any, Dict

from gateway.adapter_registry import get_adapter
from gateway.config import Platform, load_gateway_config
from gateway.session_context import get_session_env


def get_current_feishu_session() -> Dict[str, str]:
    """返回当前会话的飞书上下文。"""
    return {
        "platform": get_session_env("HERMES_SESSION_PLATFORM", "").strip().lower(),
        "chat_id": get_session_env("HERMES_SESSION_CHAT_ID", "").strip(),
        "chat_name": get_session_env("HERMES_SESSION_CHAT_NAME", "").strip(),
        "thread_id": get_session_env("HERMES_SESSION_THREAD_ID", "").strip(),
        "user_id": get_session_env("HERMES_SESSION_USER_ID", "").strip(),
    }


def require_feishu_session() -> Dict[str, str]:
    """确保当前工具调用发生在飞书会话中。"""
    session = get_current_feishu_session()
    if session["platform"] != "feishu" or not session["chat_id"]:
        raise RuntimeError("This tool requires an active Feishu gateway session.")
    return session


def get_active_feishu_adapter() -> Any:
    """获取当前活跃的飞书适配器。"""
    adapter = get_adapter(Platform.FEISHU)
    if adapter is None:
        raise RuntimeError("Feishu gateway adapter is not running.")
    return adapter


def get_feishu_platform_extra() -> Dict[str, Any]:
    """读取飞书平台配置。"""
    config = load_gateway_config()
    platform_config = config.platforms.get(Platform.FEISHU)
    if not platform_config:
        return {}
    return dict(platform_config.extra or {})


def register_pending_feishu_tool_replay(
    *,
    tool_name: str,
    args: Dict[str, Any],
    session: Dict[str, str] | None = None,
) -> str:
    """为后续授权完成后的工具自动重放登记一条请求。

    数据保存在活跃飞书适配器实例上，生命周期跟随网关进程。这里不做跨进程持久化，
    因为 replay 只针对当前会话内、当前聊天上下文中的短周期授权闭环。
    """
    adapter = get_active_feishu_adapter()
    current_session = session or require_feishu_session()
    pending = getattr(adapter, "_pending_tool_replays", None)
    if pending is None:
        pending = {}
        setattr(adapter, "_pending_tool_replays", pending)
    replay_id = f"fr_{uuid.uuid4().hex[:12]}"
    pending[replay_id] = {
        "tool_name": str(tool_name or "").strip(),
        "args": dict(args or {}),
        "chat_id": str(current_session.get("chat_id", "") or "").strip(),
        "thread_id": str(current_session.get("thread_id", "") or "").strip(),
        "user_id": str(current_session.get("user_id", "") or "").strip(),
    }
    return replay_id


def pop_pending_feishu_tool_replay(replay_id: str) -> Dict[str, Any] | None:
    """取出并移除待自动重放的工具调用。"""
    adapter = get_active_feishu_adapter()
    pending = getattr(adapter, "_pending_tool_replays", None)
    if not isinstance(pending, dict):
        return None
    return pending.pop(str(replay_id or "").strip(), None)
