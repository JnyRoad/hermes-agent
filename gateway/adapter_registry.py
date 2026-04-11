"""运行中的网关适配器注册表。

为工具层提供一个轻量、线程安全的方式来获取当前活跃的平台适配器。
飞书工具需要依赖实时适配器发送卡片、回写消息、推进等待中的交互状态，
因此不能只依赖静态配置。
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from gateway.config import Platform

_lock = threading.RLock()
_adapters: Dict[Platform, Any] = {}


def register_adapter(platform: Platform, adapter: Any) -> None:
    """登记一个已连接的平台适配器。"""
    with _lock:
        _adapters[platform] = adapter


def unregister_adapter(platform: Platform, adapter: Any | None = None) -> None:
    """注销平台适配器。

    当调用方提供 *adapter* 时，仅在注册表中仍是同一实例时才移除，
    避免重连期间误删新实例。
    """
    with _lock:
        existing = _adapters.get(platform)
        if existing is None:
            return
        if adapter is not None and existing is not adapter:
            return
        _adapters.pop(platform, None)


def get_adapter(platform: Platform) -> Optional[Any]:
    """读取当前活跃的平台适配器。"""
    with _lock:
        return _adapters.get(platform)


def clear_adapters() -> None:
    """清空注册表。用于网关整体关闭阶段。"""
    with _lock:
        _adapters.clear()
