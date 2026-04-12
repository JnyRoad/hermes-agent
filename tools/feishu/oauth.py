"""飞书授权工具。

当前这层实现的目标不是直接复刻官方 Device Flow，而是先把 Hermes 的
授权生命周期补齐为稳定可追踪的运行态：

- `authorize`：在当前飞书会话发送授权卡片，并记录待确认请求
- `status`：查看当前用户在本应用上的本地授权状态
- `revoke`：撤销当前用户的全部或部分本地授权状态

后续自动授权与失败重放会继续基于这套状态模型扩展。
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List

from tools.feishu.runtime import get_active_feishu_adapter, require_feishu_session
from tools.feishu.scopes import get_required_scopes
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


def _normalize_scopes(raw_scopes: Any) -> List[str]:
    if raw_scopes is None:
        return []
    if not isinstance(raw_scopes, list):
        return []
    result: List[str] = []
    seen: set[str] = set()
    for item in raw_scopes:
        scope = str(item).strip()
        if not scope or scope in seen:
            continue
        seen.add(scope)
        result.append(scope)
    return result


def _normalize_tool_actions(raw_tool_actions: Any) -> List[Dict[str, str]]:
    """把批量工具动作输入统一为稳定结构。"""
    if not isinstance(raw_tool_actions, list):
        return []
    result: List[Dict[str, str]] = []
    for item in raw_tool_actions:
        if not isinstance(item, dict):
            continue
        tool_name = str(item.get("tool_name", "")).strip()
        action = str(item.get("action", "")).strip().lower() or "default"
        if not tool_name:
            continue
        result.append({"tool_name": tool_name, "action": action})
    return result


def _resolve_scope_request(args: Dict[str, Any]) -> Dict[str, Any]:
    """支持显式 scopes 与 tool/action 推导两种授权输入方式。"""
    scopes = _normalize_scopes(args.get("scopes"))
    targets: List[Dict[str, str]] = []

    tool_name = str(args.get("tool_name", "")).strip()
    action_name = str(args.get("action_name", "")).strip().lower() or "default"
    if tool_name:
        scopes.extend(get_required_scopes(tool_name, action_name))
        targets.append({"tool_name": tool_name, "action": action_name})

    for item in _normalize_tool_actions(args.get("tool_actions")):
        scopes.extend(get_required_scopes(item["tool_name"], item["action"]))
        targets.append(item)

    return {
        "scopes": _normalize_scopes(scopes),
        "targets": targets,
    }


def _resolve_target_open_id(args: Dict[str, Any], session: Dict[str, str]) -> str:
    """优先使用显式 user_open_id，否则回退到当前会话发送者。"""
    explicit = str(args.get("user_open_id", "") or "").strip()
    if explicit:
        return explicit
    current = str(session.get("user_id", "") or "").strip()
    if current:
        return current
    raise RuntimeError("Missing Feishu user identity. Provide user_open_id or call from a Feishu gateway session.")


def _handle_authorize(args: Dict[str, Any]) -> str:
    scope_request = _resolve_scope_request(args)
    scopes = scope_request["scopes"]
    if not scopes:
        return tool_error("Provide non-empty scopes, or specify tool_name/action_name, or tool_actions.")
    reason = str(args.get("reason", "")).strip() or "This action requires additional Feishu permissions."
    title = str(args.get("title", "")).strip() or "Feishu Authorization Required"
    try:
        adapter = get_active_feishu_adapter()
        session = require_feishu_session()
        requester_open_id = _resolve_target_open_id(args, session)
        status = adapter.get_authorization_status(requester_open_id, scopes)
        if status["authorized"]:
            return json.dumps(
                {
                    "status": "authorized",
                    "user_open_id": requester_open_id,
                    "granted_scopes": status["granted_scopes"],
                    "requested_scopes": status["requested_scopes"],
                    "missing_scopes": [],
                    "targets": scope_request["targets"],
                    "updated_at": status["updated_at"],
                    "updated_by": status["updated_by"],
                    "source": status["source"],
                },
                ensure_ascii=False,
            )
        result = _run_async(
            adapter.send_oauth_request_card(
                chat_id=session["chat_id"],
                scopes=scopes,
                reason=reason,
                title=title,
                metadata={
                    "thread_id": session["thread_id"] or None,
                    "requester_open_id": requester_open_id,
                },
            )
        )
        if not result.success:
            return tool_error(result.error or "Failed to send Feishu authorization card.")
        return json.dumps(
            {
                "status": "pending",
                "user_open_id": requester_open_id,
                "scopes": scopes,
                "missing_scopes": status["missing_scopes"],
                "targets": scope_request["targets"],
                "request_id": ((result.raw_response or {}) if isinstance(result.raw_response, dict) else {}).get("request_id"),
                "message_id": result.message_id,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_oauth authorize error: %s", exc)
        return tool_error(f"Failed to request Feishu authorization: {exc}")


def _handle_status(args: Dict[str, Any]) -> str:
    try:
        adapter = get_active_feishu_adapter()
        session = require_feishu_session()
        user_open_id = _resolve_target_open_id(args, session)
        scope_request = _resolve_scope_request(args)
        scopes = scope_request["scopes"]
        status = adapter.get_authorization_status(user_open_id, scopes)
        return json.dumps(
            {
                "status": "authorized" if status["authorized"] else "not_authorized",
                "user_open_id": user_open_id,
                "targets": scope_request["targets"],
                **status,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_oauth status error: %s", exc)
        return tool_error(f"Failed to read Feishu authorization status: {exc}")


def _handle_revoke(args: Dict[str, Any]) -> str:
    try:
        adapter = get_active_feishu_adapter()
        session = require_feishu_session()
        user_open_id = _resolve_target_open_id(args, session)
        scopes = _normalize_scopes(args.get("scopes"))
        status = adapter.revoke_authorization(user_open_id, scopes or None)
        return json.dumps(
            {
                "status": "revoked",
                "user_open_id": user_open_id,
                "revoked_scopes": scopes,
                "remaining_scopes": status["granted_scopes"],
                "authorized": status["authorized"],
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_oauth revoke error: %s", exc)
        return tool_error(f"Failed to revoke Feishu authorization: {exc}")


def _handle_feishu_oauth(args: Dict[str, Any], **_kw) -> str:
    action = str(args.get("action", "authorize") or "authorize").strip().lower()
    if action == "authorize":
        return _handle_authorize(args)
    if action == "status":
        return _handle_status(args)
    if action == "revoke":
        return _handle_revoke(args)
    return tool_error("Unsupported action. Supported actions: authorize, status, revoke")


def _handle_feishu_oauth_batch(args: Dict[str, Any], **_kw) -> str:
    scope_request = _resolve_scope_request(args)
    scopes = scope_request["scopes"]
    if not scopes:
        return tool_error("Provide non-empty scopes, or specify tool_name/action_name, or tool_actions.")
    title = str(args.get("title", "")).strip() or "Feishu Batch Authorization Required"
    reason = str(args.get("reason", "")).strip() or "The requested batch of Feishu actions needs extra permissions."
    try:
        adapter = get_active_feishu_adapter()
        session = require_feishu_session()
        requester_open_id = _resolve_target_open_id(args, session)
        status = adapter.get_authorization_status(requester_open_id, scopes)
        missing_scopes = list(status["missing_scopes"])
        if not missing_scopes:
            return json.dumps(
                {
                    "status": "authorized",
                    "user_open_id": requester_open_id,
                    "granted_scopes": status["granted_scopes"],
                    "requested_scopes": scopes,
                    "missing_scopes": [],
                    "targets": scope_request["targets"],
                },
                ensure_ascii=False,
            )
        result = _run_async(
            adapter.send_oauth_request_card(
                chat_id=session["chat_id"],
                scopes=missing_scopes,
                reason=reason,
                title=title,
                metadata={
                    "thread_id": session["thread_id"] or None,
                    "requester_open_id": requester_open_id,
                },
            )
        )
        if not result.success:
            return tool_error(result.error or "Failed to send Feishu batch authorization card.")
        return json.dumps(
            {
                "status": "pending",
                "user_open_id": requester_open_id,
                "requested_scopes": scopes,
                "missing_scopes": missing_scopes,
                "targets": scope_request["targets"],
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
    "description": "Manage Feishu authorization for the current user. Actions: authorize, status, revoke.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "description": "authorize to request permissions, status to inspect current grants, revoke to clear grants.",
                "enum": ["authorize", "status", "revoke"],
            },
            "user_open_id": {
                "type": "string",
                "description": "Optional target user open_id. Defaults to the current Feishu session sender.",
            },
            "title": {"type": "string", "description": "Card title shown to the user during authorize."},
            "scopes": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of Feishu scopes involved in this authorization action.",
            },
            "tool_name": {
                "type": "string",
                "description": "Optional tool name used to derive required scopes automatically.",
            },
            "action_name": {
                "type": "string",
                "description": "Optional action name used with tool_name. Defaults to default.",
            },
            "tool_actions": {
                "type": "array",
                "description": "Optional list of tool/action pairs used to derive scopes automatically.",
                "items": {
                    "type": "object",
                    "properties": {
                        "tool_name": {"type": "string"},
                        "action": {"type": "string"},
                    },
                    "required": ["tool_name"],
                },
            },
            "reason": {"type": "string", "description": "Why these scopes are needed."},
        },
        "required": [],
    },
}

FEISHU_OAUTH_BATCH_SCHEMA = {
    "name": "feishu_oauth_batch_auth",
    "description": "Request a batch of Feishu scopes in one user-facing authorization card. Already granted scopes are skipped automatically.",
    "parameters": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Card title shown to the user."},
            "user_open_id": {
                "type": "string",
                "description": "Optional target user open_id. Defaults to the current Feishu session sender.",
            },
            "scopes": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Batch of Feishu scopes required by upcoming actions.",
            },
            "tool_name": {
                "type": "string",
                "description": "Optional single tool name used to derive scopes automatically.",
            },
            "action_name": {
                "type": "string",
                "description": "Optional single action used with tool_name. Defaults to default.",
            },
            "tool_actions": {
                "type": "array",
                "description": "Optional list of tool/action pairs used to derive scopes automatically.",
                "items": {
                    "type": "object",
                    "properties": {
                        "tool_name": {"type": "string"},
                        "action": {"type": "string"},
                    },
                    "required": ["tool_name"],
                },
            },
            "reason": {"type": "string", "description": "Why these scopes are required together."},
        },
        "required": [],
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
