"""飞书工具运行态辅助函数。"""

from __future__ import annotations

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
