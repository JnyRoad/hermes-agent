"""飞书工具 scope 映射与自动授权辅助。

这一层负责两件事：

1. 维护工具动作到飞书 user scope 的映射。
2. 在飞书网关会话里，工具执行前根据当前用户的本地授权状态判断是否需要
   自动发起授权卡片。

这里故意把“scope 识别”和“具体业务 API 调用”解耦，避免每个工具都重复拼接
授权逻辑，也方便后续继续对齐官方插件的 auto-auth / scope merge / 重放机制。
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from tools.feishu.runtime import get_active_feishu_adapter, require_feishu_session
from tools.registry import tool_error

logger = logging.getLogger(__name__)


# 与官方插件 tool-scopes.ts 保持同粒度的工具动作键。
TOOL_SCOPES: Dict[str, List[str]] = {
    "feishu_calendar_event.create": ["calendar:calendar.event:create", "calendar:calendar.event:update"],
    "feishu_calendar_event.list": ["calendar:calendar.event:read"],
    "feishu_calendar_event.get": ["calendar:calendar.event:read"],
    "feishu_calendar_event.patch": ["calendar:calendar.event:update"],
    "feishu_calendar_event.delete": ["calendar:calendar.event:delete"],
    "feishu_calendar_event.search": ["calendar:calendar.event:read"],
    "feishu_calendar_event.reply": ["calendar:calendar.event:reply"],
    "feishu_calendar_event.instances": ["calendar:calendar.event:read"],
    "feishu_calendar_event.instance_view": ["calendar:calendar.event:read"],
    "feishu_drive_file.list": ["space:document:retrieve"],
    "feishu_drive_file.get_meta": ["drive:drive.metadata:readonly"],
    "feishu_drive_file.copy": ["docs:document:copy"],
    "feishu_drive_file.move": ["space:document:move"],
    "feishu_drive_file.delete": ["space:document:delete"],
    "feishu_drive_file.upload": ["drive:file:upload"],
    "feishu_drive_file.download": ["drive:file:download"],
    "feishu_im_user_get_messages.default": [
        "im:chat:read",
        "im:message:readonly",
        "im:message.group_msg:get_as_user",
        "im:message.p2p_msg:get_as_user",
        "contact:contact.base:readonly",
        "contact:user.base:readonly",
    ],
    "feishu_im_user_get_thread_messages.default": [
        "im:chat:read",
        "im:message:readonly",
        "im:message.group_msg:get_as_user",
        "im:message.p2p_msg:get_as_user",
        "contact:contact.base:readonly",
        "contact:user.base:readonly",
    ],
    "feishu_im_user_search_messages.default": [
        "im:chat:read",
        "im:message:readonly",
        "im:message.group_msg:get_as_user",
        "im:message.p2p_msg:get_as_user",
        "contact:contact.base:readonly",
        "contact:user.base:readonly",
        "search:message",
    ],
    "feishu_im_user_message.send": ["im:message", "im:message.send_as_user"],
    "feishu_im_user_message.reply": ["im:message", "im:message.send_as_user"],
    "feishu_im_user_fetch_resource.default": [
        "im:message.group_msg:get_as_user",
        "im:message.p2p_msg:get_as_user",
        "im:message:readonly",
    ],
    "feishu_task_task.create": ["task:task:write", "task:task:writeonly"],
    "feishu_task_task.get": ["task:task:read", "task:task:write"],
    "feishu_task_task.list": ["task:task:read", "task:task:write"],
    "feishu_task_task.patch": ["task:task:write", "task:task:writeonly"],
}

SENSITIVE_SCOPES = {
    "im:message.send_as_user",
    "space:document:delete",
    "calendar:calendar.event:delete",
    "base:table:delete",
}


def _run_async(coro):
    """在同步工具 handler 中安全执行协程。"""
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


def _normalize_scopes(scopes: List[str]) -> List[str]:
    """去重并保持顺序稳定，保证卡片和状态判断可预期。"""
    result: List[str] = []
    seen: set[str] = set()
    for item in scopes:
        scope = str(item or "").strip()
        if not scope or scope in seen:
            continue
        seen.add(scope)
        result.append(scope)
    return result


def get_scope_action_key(tool_name: str, action: Optional[str] = None) -> str:
    """将工具名和动作归一为统一 key。

    大部分飞书工具是 action 型；少数工具没有 action 字段，使用 default。
    """
    normalized_tool = str(tool_name or "").strip()
    normalized_action = str(action or "").strip().lower() or "default"
    return f"{normalized_tool}.{normalized_action}"


def get_required_scopes(tool_name: str, action: Optional[str] = None) -> List[str]:
    """返回某个工具动作要求的全部 user scopes。"""
    return list(TOOL_SCOPES.get(get_scope_action_key(tool_name, action), []))


def get_missing_scopes(tool_name: str, action: Optional[str] = None, granted_scopes: Optional[List[str]] = None) -> List[str]:
    """根据已授权 scope 计算仍然缺失的权限。"""
    required = get_required_scopes(tool_name, action)
    granted = {str(item or "").strip() for item in granted_scopes or [] if str(item or "").strip()}
    return [scope for scope in required if scope not in granted]


def split_sensitive_scopes(scopes: List[str]) -> Tuple[List[str], List[str]]:
    """区分普通权限与高敏感权限，便于后续做分批授权。"""
    safe_scopes: List[str] = []
    sensitive_scopes: List[str] = []
    for scope in _normalize_scopes(scopes):
        if scope in SENSITIVE_SCOPES:
            sensitive_scopes.append(scope)
        else:
            safe_scopes.append(scope)
    return safe_scopes, sensitive_scopes


def ensure_authorization(
    *,
    tool_name: str,
    action: Optional[str],
    title: Optional[str] = None,
    reason: Optional[str] = None,
) -> Optional[str]:
    """在飞书网关会话里自动检查用户授权状态。

    返回：
    - `None`：说明无需拦截，工具可继续执行。
    - JSON string：说明已发起授权请求，调用方应直接返回该结果。

    当前阶段只在“存在飞书会话 + 存在活跃适配器”时拦截。这样不会破坏 CLI
    或其他离线环境中直接使用 tenant token 的既有行为。
    """
    required_scopes = get_required_scopes(tool_name, action)
    if not required_scopes:
        return None

    try:
        session = require_feishu_session()
        adapter = get_active_feishu_adapter()
    except Exception:
        return None

    user_open_id = str(session.get("user_id", "") or "").strip()
    chat_id = str(session.get("chat_id", "") or "").strip()
    if not user_open_id or not chat_id:
        return None

    status = adapter.get_authorization_status(user_open_id, required_scopes)
    if status.get("authorized"):
        return None

    missing_scopes = _normalize_scopes(list(status.get("missing_scopes") or []))
    if not missing_scopes:
        return None

    safe_scopes, sensitive_scopes = split_sensitive_scopes(missing_scopes)
    request_scopes = safe_scopes + sensitive_scopes
    if not request_scopes:
        return None

    action_name = str(action or "").strip().lower() or "default"
    title_text = str(title or "").strip() or "Feishu Authorization Required"
    if reason:
        reason_text = str(reason).strip()
    else:
        reason_text = (
            f"The tool `{tool_name}` action `{action_name}` needs additional Feishu user permissions "
            "before it can continue."
        )
        if sensitive_scopes:
            reason_text += " Some requested scopes are sensitive and should be reviewed carefully."

    result = _run_async(
        adapter.send_oauth_request_card(
            chat_id=chat_id,
            scopes=request_scopes,
            reason=reason_text,
            title=title_text,
            metadata={
                "thread_id": session.get("thread_id") or None,
                "requester_open_id": user_open_id,
                "tool_name": tool_name,
                "action": action_name,
            },
        )
    )
    if not result.success:
        logger.error(
            "feishu authorization card send failed: tool=%s action=%s user_open_id=%s error=%s",
            tool_name,
            action_name,
            user_open_id,
            result.error,
        )
        return tool_error(result.error or "Failed to send Feishu authorization card.")

    raw_response = result.raw_response if isinstance(result.raw_response, dict) else {}
    return json.dumps(
        {
            "status": "pending_authorization",
            "tool": tool_name,
            "action": action_name,
            "user_open_id": user_open_id,
            "requested_scopes": required_scopes,
            "missing_scopes": missing_scopes,
            "safe_scopes": safe_scopes,
            "sensitive_scopes": sensitive_scopes,
            "message_id": result.message_id,
            "request_id": raw_response.get("request_id"),
        },
        ensure_ascii=False,
    )
