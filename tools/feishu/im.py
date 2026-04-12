"""飞书 IM 基础工具。"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote

from gateway.platforms.feishu import normalize_feishu_message
from tools.feishu.client import feishu_api_request, feishu_api_request_bytes
from tools.feishu.scopes import ensure_authorization, handle_authorization_error
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)

_MIME_TO_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "application/pdf": ".pdf",
    "text/plain": ".txt",
    "application/json": ".json",
    "audio/mpeg": ".mp3",
    "audio/wav": ".wav",
    "audio/ogg": ".ogg",
    "video/mp4": ".mp4",
}


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _millis_to_iso(value: Any) -> str:
    try:
        millis = int(str(value))
    except Exception:
        return ""
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).astimezone().isoformat()


def _datetime_to_seconds_string(value: str) -> str:
    """将 ISO 8601 时间转换为秒级时间戳字符串。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return str(int(dt.timestamp()))


def _format_beijing_iso(dt: datetime) -> str:
    """统一输出北京时间格式，便于和飞书搜索接口保持一致。"""
    dt = dt.astimezone(timezone(timedelta(hours=8)))
    return dt.replace(microsecond=0).isoformat()


def _parse_relative_time_to_seconds(value: str) -> Dict[str, str]:
    """解析常用相对时间范围。

    这里采用北京时间作为自然日、自然周、自然月的边界，和飞书参考实现保持一致。
    """
    now = datetime.now(timezone.utc)
    bj_tz = timezone(timedelta(hours=8))
    bj_now = now.astimezone(bj_tz)

    def start_of_day(dt: datetime) -> datetime:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def end_of_day(dt: datetime) -> datetime:
        return dt.replace(hour=23, minute=59, second=59, microsecond=0)

    if value == "today":
        start = start_of_day(bj_now)
        end = bj_now
    elif value == "yesterday":
        base = bj_now - timedelta(days=1)
        start = start_of_day(base)
        end = end_of_day(base)
    elif value == "day_before_yesterday":
        base = bj_now - timedelta(days=2)
        start = start_of_day(base)
        end = end_of_day(base)
    elif value == "this_week":
        start = start_of_day(bj_now - timedelta(days=bj_now.weekday()))
        end = bj_now
    elif value == "last_week":
        this_week_start = start_of_day(bj_now - timedelta(days=bj_now.weekday()))
        start = this_week_start - timedelta(days=7)
        end = this_week_start - timedelta(seconds=1)
    elif value == "this_month":
        start = bj_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = bj_now
    elif value == "last_month":
        this_month_start = bj_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        prev_month_end = this_month_start - timedelta(seconds=1)
        start = prev_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = prev_month_end
    else:
        import re

        match = re.match(r"^last_(\d+)_(minutes?|hours?|days?)$", value)
        if not match:
            raise ValueError(
                'Unsupported relative_time. Use today, yesterday, day_before_yesterday, '
                'this_week, last_week, this_month, last_month, or last_{N}_{unit}.'
            )
        amount = int(match.group(1))
        unit = match.group(2).rstrip("s")
        delta_map = {
            "minute": timedelta(minutes=amount),
            "hour": timedelta(hours=amount),
            "day": timedelta(days=amount),
        }
        start = bj_now - delta_map[unit]
        end = bj_now

    return {
        "start": str(int(start.timestamp())),
        "end": str(int(end.timestamp())),
        "start_iso": _format_beijing_iso(start),
        "end_iso": _format_beijing_iso(end),
    }


def _resolve_time_range(args: dict) -> Dict[str, str]:
    """统一解析时间过滤参数。

    relative_time 与 start_time/end_time 互斥；返回值中的 start/end 直接用于飞书 API。
    """
    relative_time = str(args.get("relative_time", "")).strip().lower()
    start_time = str(args.get("start_time", "")).strip()
    end_time = str(args.get("end_time", "")).strip()
    if relative_time and (start_time or end_time):
        raise ValueError("cannot use both relative_time and start_time/end_time")
    if relative_time:
        return _parse_relative_time_to_seconds(relative_time)
    result: Dict[str, str] = {}
    if start_time:
        result["start"] = _datetime_to_seconds_string(start_time)
    if end_time:
        result["end"] = _datetime_to_seconds_string(end_time)
    if start_time:
        result["start_iso"] = start_time
    if end_time:
        result["end_iso"] = end_time
    return result


def _validate_json_content(raw_content: str) -> str:
    """校验消息体必须是合法 JSON 字符串。

    飞书 IM 发送接口不同消息类型的 content 结构差异很大，这里只负责语法校验，
    避免在工具层错误改写业务数据。
    """
    try:
        json.loads(raw_content)
    except Exception as exc:
        raise ValueError(f"content must be a valid JSON string: {exc}") from exc
    return raw_content


def _resolve_p2p_chat_id(open_id: str) -> str:
    """将单聊对象的 open_id 解析为 p2p chat_id。"""
    payload = feishu_api_request(
        "POST",
        "/open-apis/im/v1/chat_p2p/batch_query",
        params={"user_id_type": "open_id"},
        json_body={"chatter_ids": [open_id]},
    )
    chats = payload.get("data", {}).get("p2p_chats", [])
    if not chats or not isinstance(chats[0], dict) or not chats[0].get("chat_id"):
        raise ValueError(f"no 1-on-1 chat found with open_id={open_id}")
    return str(chats[0]["chat_id"])


def _fetch_chat_contexts(chat_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量拉取会话元信息，用于补充搜索结果的上下文。"""
    unique_chat_ids = [chat_id for chat_id in dict.fromkeys(chat_ids) if chat_id]
    if not unique_chat_ids:
        return {}
    try:
        payload = feishu_api_request(
            "POST",
            "/open-apis/im/v1/chats/batch_query",
            params={"user_id_type": "open_id"},
            json_body={"chat_ids": unique_chat_ids},
        )
    except Exception as exc:
        logger.info("feishu_im_user_search_messages chat context fetch skipped: %s", exc)
        return {}
    items = payload.get("data", {}).get("items", [])
    result: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict) or not item.get("chat_id"):
            continue
        result[str(item["chat_id"])] = item
    return result


def _resolve_user_names(open_ids: List[str]) -> Dict[str, str]:
    """批量解析用户名称。

    搜索结果常见场景是少量单聊会话，这里不引入复杂缓存，优先保证返回内容完整。
    """
    unique_open_ids = [open_id for open_id in dict.fromkeys(open_ids) if open_id]
    if not unique_open_ids:
        return {}
    result: Dict[str, str] = {}
    for index in range(0, len(unique_open_ids), 10):
        chunk = unique_open_ids[index : index + 10]
        try:
            payload = feishu_api_request(
                "POST",
                "/open-apis/contact/v3/users/basic_batch",
                params={"user_id_type": "open_id"},
                json_body={"user_ids": chunk},
            )
        except Exception as exc:
            logger.info("feishu_im_user_search_messages user name resolve skipped: %s", exc)
            continue
        users = payload.get("data", {}).get("users", [])
        for user in users:
            if not isinstance(user, dict):
                continue
            open_id = str(user.get("user_id", "")).strip()
            raw_name = user.get("name")
            name = raw_name if isinstance(raw_name, str) else (
                str(raw_name.get("value", "")).strip() if isinstance(raw_name, dict) else ""
            )
            if open_id and name:
                result[open_id] = name
    return result


def _format_message_item(item: Dict[str, Any]) -> Dict[str, Any]:
    msg_type = str(item.get("msg_type", "unknown"))
    body = item.get("body") if isinstance(item.get("body"), dict) else {}
    raw_content = body.get("content") if isinstance(body, dict) else ""
    normalized = normalize_feishu_message(
        message_type=msg_type,
        raw_content=raw_content if isinstance(raw_content, str) else "",
    )
    sender = item.get("sender") if isinstance(item.get("sender"), dict) else {}
    sender_id = ""
    if isinstance(sender.get("id"), str):
        sender_id = sender["id"]
    elif isinstance(sender.get("sender_id"), dict):
        sender_id = str(
            sender["sender_id"].get("open_id")
            or sender["sender_id"].get("user_id")
            or sender["sender_id"].get("union_id")
            or ""
        )
    result = {
        "message_id": item.get("message_id", ""),
        "msg_type": msg_type,
        "content": normalized.text_content or (raw_content if isinstance(raw_content, str) else ""),
        "sender": {
            "id": sender_id,
            "sender_type": sender.get("sender_type", ""),
        },
        "create_time": _millis_to_iso(item.get("create_time")),
        "deleted": bool(item.get("deleted", False)),
        "updated": bool(item.get("updated", False)),
    }
    if item.get("thread_id"):
        result["thread_id"] = item["thread_id"]
    elif item.get("parent_id"):
        result["reply_to"] = item["parent_id"]
    return result


def _list_messages(
    *,
    container_type: str,
    container_id: str,
    sort_rule: str,
    page_size: int,
    page_token: str = "",
    start_time: str = "",
    end_time: str = "",
) -> str:
    params: Dict[str, Any] = {
        "container_id_type": container_type,
        "container_id": container_id,
        "sort_type": "ByCreateTimeAsc" if sort_rule == "create_time_asc" else "ByCreateTimeDesc",
        "page_size": max(1, min(page_size, 50)),
        "card_msg_content_type": "raw_card_content",
    }
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time
    if page_token:
        params["page_token"] = page_token
    data = feishu_api_request("GET", "/open-apis/im/v1/messages", params=params)
    payload = data.get("data") or {}
    items = payload.get("items", [])
    messages = [_format_message_item(item) for item in items if isinstance(item, dict)]
    return json.dumps(
        {
            "messages": messages,
            "has_more": bool(payload.get("has_more", False)),
            "page_token": payload.get("page_token"),
        },
        ensure_ascii=False,
    )


def _handle_get_messages(args: dict, **_kw) -> str:
    chat_id = str(args.get("chat_id", "")).strip()
    open_id = str(args.get("open_id", "")).strip()
    if chat_id and open_id:
        return tool_error("Parameters chat_id and open_id are mutually exclusive.")
    if not chat_id and not open_id:
        return tool_error("Missing required parameter: chat_id or open_id")
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_im_user_get_messages",
            action="default",
            title="Feishu Message Authorization Required",
        )
        if auth_result is not None:
            return auth_result
        if open_id:
            chat_id = _resolve_p2p_chat_id(open_id)
        time_range = _resolve_time_range(args)
        return _list_messages(
            container_type="chat",
            container_id=chat_id,
            sort_rule=str(args.get("sort_rule", "create_time_desc")).strip().lower(),
            page_size=int(args.get("page_size", 50) or 50),
            page_token=str(args.get("page_token", "")).strip(),
            start_time=time_range.get("start", ""),
            end_time=time_range.get("end", ""),
        )
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_im_user_get_messages",
            action="default",
            title="Feishu Message Authorization Required",
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_im_user_get_messages error: %s", exc)
        return tool_error(f"Failed to get Feishu messages: {exc}")


def _handle_get_thread_messages(args: dict, **_kw) -> str:
    thread_id = str(args.get("thread_id", "")).strip()
    if not thread_id:
        return tool_error("Missing required parameter: thread_id")
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_im_user_get_thread_messages",
            action="default",
            title="Feishu Message Authorization Required",
        )
        if auth_result is not None:
            return auth_result
        time_range = _resolve_time_range(args)
        return _list_messages(
            container_type="thread",
            container_id=thread_id,
            sort_rule=str(args.get("sort_rule", "create_time_desc")).strip().lower(),
            page_size=int(args.get("page_size", 50) or 50),
            page_token=str(args.get("page_token", "")).strip(),
            start_time=time_range.get("start", ""),
            end_time=time_range.get("end", ""),
        )
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_im_user_get_thread_messages",
            action="default",
            title="Feishu Message Authorization Required",
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_im_user_get_thread_messages error: %s", exc)
        return tool_error(f"Failed to get Feishu thread messages: {exc}")


def _handle_search_messages(args: dict, **_kw) -> str:
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_im_user_search_messages",
            action="default",
            title="Feishu Search Authorization Required",
        )
        if auth_result is not None:
            return auth_result
        time_range = _resolve_time_range(args)
        page_size = max(1, min(int(args.get("page_size", 50) or 50), 50))
        page_token = str(args.get("page_token", "")).strip()
        sender_ids = [
            str(item).strip() for item in (args.get("sender_ids") or []) if str(item).strip()
        ]
        mention_ids = [
            str(item).strip() for item in (args.get("mention_ids") or []) if str(item).strip()
        ]
        query = str(args.get("query", "") or "")
        chat_id = str(args.get("chat_id", "")).strip()
        message_type = str(args.get("message_type", "")).strip().lower()
        sender_type = str(args.get("sender_type", "user")).strip().lower()
        chat_type = str(args.get("chat_type", "")).strip().lower()

        search_data: Dict[str, Any] = {
            "query": query,
            "start_time": time_range.get("start", "978307200"),
            "end_time": time_range.get("end", str(int(datetime.now(timezone.utc).timestamp()))),
        }
        if sender_ids:
            search_data["from_ids"] = sender_ids
        if chat_id:
            search_data["chat_ids"] = [chat_id]
        if mention_ids:
            search_data["at_chatter_ids"] = mention_ids
        if message_type:
            search_data["message_type"] = message_type
        if sender_type and sender_type != "all":
            search_data["from_type"] = sender_type
        if chat_type:
            search_data["chat_type"] = "group_chat" if chat_type == "group" else "p2p_chat"

        search_result = feishu_api_request(
            "POST",
            "/open-apis/search/v2/message",
            params={
                "user_id_type": "open_id",
                "page_size": page_size,
                **({"page_token": page_token} if page_token else {}),
            },
            json_body=search_data,
        )
        payload = search_result.get("data") or {}
        message_ids = [str(item).strip() for item in (payload.get("items") or []) if str(item).strip()]
        has_more = bool(payload.get("has_more", False))
        next_page_token = payload.get("page_token")
        if not message_ids:
            return json.dumps({"messages": [], "has_more": has_more, "page_token": next_page_token}, ensure_ascii=False)

        query_string = "&".join(f"message_ids={quote(message_id, safe='')}" for message_id in message_ids)
        details = feishu_api_request(
            "GET",
            f"/open-apis/im/v1/messages/mget?{query_string}",
            params={"user_id_type": "open_id", "card_msg_content_type": "raw_card_content"},
        )
        items = details.get("data", {}).get("items", [])
        chat_contexts = _fetch_chat_contexts(
            [str(item.get("chat_id", "")).strip() for item in items if isinstance(item, dict)]
        )
        p2p_partner_ids = []
        for chat_context in chat_contexts.values():
            if not isinstance(chat_context, dict):
                continue
            if str(chat_context.get("chat_mode", "")).strip().lower() == "p2p":
                partner_id = str(chat_context.get("p2p_target_id", "")).strip()
                if partner_id:
                    p2p_partner_ids.append(partner_id)
        p2p_partner_names = _resolve_user_names(p2p_partner_ids)

        messages = []
        for item in items:
            if not isinstance(item, dict):
                continue
            formatted = _format_message_item(item)
            chat_id_value = str(item.get("chat_id", "")).strip()
            if chat_id_value:
                formatted["chat_id"] = chat_id_value
                chat_context = chat_contexts.get(chat_id_value) or {}
                chat_mode = str(chat_context.get("chat_mode", "")).strip().lower()
                chat_name = str(chat_context.get("name", "")).strip()
                p2p_target_id = str(chat_context.get("p2p_target_id", "")).strip()
                if chat_mode:
                    formatted["chat_type"] = "p2p" if chat_mode == "p2p" else chat_mode
                if chat_name:
                    formatted["chat_name"] = chat_name
                if chat_mode == "p2p" and p2p_target_id:
                    partner_name = p2p_partner_names.get(p2p_target_id) or chat_name or None
                    formatted["chat_partner"] = {"open_id": p2p_target_id, "name": partner_name}
                    if partner_name:
                        formatted["chat_name"] = partner_name
            if item.get("thread_id"):
                formatted["thread_id"] = item.get("thread_id")
            messages.append(formatted)

        return json.dumps(
            {
                "messages": messages,
                "has_more": has_more,
                "page_token": next_page_token,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_im_user_search_messages",
            action="default",
            title="Feishu Search Authorization Required",
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_im_user_search_messages error: %s", exc)
        return tool_error(f"Failed to search Feishu messages: {exc}")


def _handle_im_message(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    msg_type = str(args.get("msg_type", "")).strip()
    content = str(args.get("content", "")).strip()
    if action not in {"send", "reply"}:
        return tool_error("Parameter action must be send or reply.")
    if not msg_type or not content:
        return tool_error("Parameters msg_type and content are required.")
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_im_user_message",
            action=action,
            title="Feishu Send Message Authorization Required",
        )
        if auth_result is not None:
            return auth_result
        processed_content = _validate_json_content(content)
        if action == "send":
            receive_id_type = str(args.get("receive_id_type", "")).strip().lower()
            receive_id = str(args.get("receive_id", "")).strip()
            if receive_id_type not in {"open_id", "chat_id"} or not receive_id:
                return tool_error("Parameters receive_id_type(open_id|chat_id) and receive_id are required for send.")
            response = feishu_api_request(
                "POST",
                "/open-apis/im/v1/messages",
                params={"receive_id_type": receive_id_type},
                json_body={
                    "receive_id": receive_id,
                    "msg_type": msg_type,
                    "content": processed_content,
                    **({"uuid": str(args.get("uuid")).strip()} if str(args.get("uuid", "")).strip() else {}),
                },
            )
        else:
            message_id = str(args.get("message_id", "")).strip()
            if not message_id:
                return tool_error("Parameter message_id is required for reply.")
            response = feishu_api_request(
                "POST",
                f"/open-apis/im/v1/messages/{message_id}/reply",
                json_body={
                    "msg_type": msg_type,
                    "content": processed_content,
                    "reply_in_thread": bool(args.get("reply_in_thread", False)),
                    **({"uuid": str(args.get("uuid")).strip()} if str(args.get("uuid", "")).strip() else {}),
                },
            )

        data = response.get("data") or {}
        return json.dumps(
            {
                "message_id": data.get("message_id"),
                "chat_id": data.get("chat_id"),
                "create_time": _millis_to_iso(data.get("create_time")) if data.get("create_time") else "",
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_im_user_message",
            action=action,
            title="Feishu Send Message Authorization Required",
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_im_user_message error: %s", exc)
        return tool_error(f"Failed to send Feishu IM message: {exc}")


def _handle_fetch_resource(args: dict, **_kw) -> str:
    message_id = str(args.get("message_id", "")).strip()
    file_key = str(args.get("file_key", "")).strip()
    resource_type = str(args.get("type", "")).strip().lower()
    if not message_id or not file_key or resource_type not in {"image", "file"}:
        return tool_error("Parameters message_id, file_key, and type(image|file) are required.")
    try:
        auth_result = ensure_authorization(
            tool_name="feishu_im_user_fetch_resource",
            action="default",
            title="Feishu Resource Authorization Required",
        )
        if auth_result is not None:
            return auth_result
        content, headers = feishu_api_request_bytes(
            "GET",
            f"/open-apis/im/v1/messages/{message_id}/resources/{file_key}",
            params={"type": resource_type},
        )
        content_type = str(headers.get("content-type", "")).split(";")[0].strip().lower()
        suffix = _MIME_TO_EXT.get(content_type, "")
        tmp_dir = Path(tempfile.gettempdir()) / "hermes-feishu"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        fd, path = tempfile.mkstemp(prefix="im-resource-", suffix=suffix, dir=str(tmp_dir))
        Path(path).write_bytes(content)
        Path(path).chmod(0o600)
        return json.dumps(
            {
                "message_id": message_id,
                "file_key": file_key,
                "type": resource_type,
                "size_bytes": len(content),
                "content_type": content_type,
                "saved_path": path,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        auth_error = handle_authorization_error(
            exc,
            tool_name="feishu_im_user_fetch_resource",
            action="default",
            title="Feishu Resource Authorization Required",
        )
        if auth_error is not None:
            return auth_error
        logger.error("feishu_im_user_fetch_resource error: %s", exc)
        return tool_error(f"Failed to fetch Feishu message resource: {exc}")


FEISHU_IM_GET_MESSAGES_SCHEMA = {
    "name": "feishu_im_user_get_messages",
    "description": "List Feishu chat history for a chat_id. Returns AI-readable message content, sender info, and paging metadata.",
    "parameters": {
        "type": "object",
        "properties": {
            "chat_id": {"type": "string", "description": "Feishu chat_id such as oc_xxx. Mutually exclusive with open_id."},
            "open_id": {"type": "string", "description": "Feishu user open_id for a 1-on-1 chat. Mutually exclusive with chat_id."},
            "sort_rule": {
                "type": "string",
                "enum": ["create_time_asc", "create_time_desc"],
                "description": "Sort order. Default create_time_desc.",
            },
            "page_size": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Page size. Default 50."},
            "page_token": {"type": "string", "description": "Pagination token."},
            "relative_time": {
                "type": "string",
                "description": "Relative time range such as today, yesterday, this_week, last_week, this_month, last_month, or last_3_days.",
            },
            "start_time": {"type": "string", "description": "Inclusive ISO 8601 start time."},
            "end_time": {"type": "string", "description": "Inclusive ISO 8601 end time."},
        },
    },
}

FEISHU_IM_GET_THREAD_MESSAGES_SCHEMA = {
    "name": "feishu_im_user_get_thread_messages",
    "description": "List Feishu thread messages for a thread_id. Returns the same message shape as feishu_im_user_get_messages.",
    "parameters": {
        "type": "object",
        "properties": {
            "thread_id": {"type": "string", "description": "Feishu thread_id such as omt_xxx."},
            "sort_rule": {
                "type": "string",
                "enum": ["create_time_asc", "create_time_desc"],
                "description": "Sort order. Default create_time_desc.",
            },
            "page_size": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Page size. Default 50."},
            "page_token": {"type": "string", "description": "Pagination token."},
            "relative_time": {
                "type": "string",
                "description": "Relative time range such as today, yesterday, this_week, last_week, this_month, last_month, or last_3_days.",
            },
            "start_time": {"type": "string", "description": "Inclusive ISO 8601 start time."},
            "end_time": {"type": "string", "description": "Inclusive ISO 8601 end time."},
        },
        "required": ["thread_id"],
    },
}

FEISHU_IM_SEARCH_MESSAGES_SCHEMA = {
    "name": "feishu_im_user_search_messages",
    "description": "Search Feishu messages across chats using query, sender, mention, chat, type, and time filters. Returns AI-readable messages and paging metadata.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Message content keyword query."},
            "sender_ids": {"type": "array", "items": {"type": "string"}, "description": "Sender open_id list."},
            "chat_id": {"type": "string", "description": "Optional chat_id to scope the search."},
            "mention_ids": {"type": "array", "items": {"type": "string"}, "description": "Mentioned user open_id list."},
            "message_type": {"type": "string", "enum": ["file", "image", "media"], "description": "Optional message type filter."},
            "sender_type": {"type": "string", "enum": ["user", "bot", "all"], "description": "Sender type filter. Default user."},
            "chat_type": {"type": "string", "enum": ["group", "p2p"], "description": "Chat type filter."},
            "relative_time": {
                "type": "string",
                "description": "Relative time range such as today, yesterday, this_week, last_week, this_month, last_month, or last_3_days.",
            },
            "start_time": {"type": "string", "description": "Inclusive ISO 8601 start time."},
            "end_time": {"type": "string", "description": "Inclusive ISO 8601 end time."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Page size. Default 50."},
            "page_token": {"type": "string", "description": "Pagination token."},
        },
    },
}

FEISHU_IM_FETCH_RESOURCE_SCHEMA = {
    "name": "feishu_im_user_fetch_resource",
    "description": "Download a Feishu IM message resource to a local temp file. Supports image and file resources.",
    "parameters": {
        "type": "object",
        "properties": {
            "message_id": {"type": "string", "description": "Message ID such as om_xxx."},
            "file_key": {"type": "string", "description": "Resource key such as image_key or file_key."},
            "type": {"type": "string", "enum": ["image", "file"], "description": "Resource type."},
        },
        "required": ["message_id", "file_key", "type"],
    },
}

FEISHU_IM_MESSAGE_SCHEMA = {
    "name": "feishu_im_user_message",
    "description": "Send or reply to a Feishu IM message when the user explicitly asked to send a message as themselves. content must be a valid JSON string matching msg_type.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["send", "reply"], "description": "Operation type."},
            "receive_id_type": {"type": "string", "enum": ["open_id", "chat_id"], "description": "Required for send."},
            "receive_id": {"type": "string", "description": "Target open_id or chat_id for send."},
            "message_id": {"type": "string", "description": "Target message_id for reply."},
            "msg_type": {
                "type": "string",
                "enum": ["text", "post", "image", "file", "audio", "media", "interactive", "share_chat", "share_user"],
                "description": "Feishu message type.",
            },
            "content": {"type": "string", "description": "JSON string content matching msg_type."},
            "reply_in_thread": {"type": "boolean", "description": "Whether reply should go into thread mode."},
            "uuid": {"type": "string", "description": "Optional idempotency key."},
        },
        "required": ["action", "msg_type", "content"],
    },
}

registry.register(
    name="feishu_im_user_get_messages",
    toolset="feishu",
    schema=FEISHU_IM_GET_MESSAGES_SCHEMA,
    handler=_handle_get_messages,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_im_user_get_thread_messages",
    toolset="feishu",
    schema=FEISHU_IM_GET_THREAD_MESSAGES_SCHEMA,
    handler=_handle_get_thread_messages,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_im_user_fetch_resource",
    toolset="feishu",
    schema=FEISHU_IM_FETCH_RESOURCE_SCHEMA,
    handler=_handle_fetch_resource,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_im_user_search_messages",
    toolset="feishu",
    schema=FEISHU_IM_SEARCH_MESSAGES_SCHEMA,
    handler=_handle_search_messages,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_im_user_message",
    toolset="feishu",
    schema=FEISHU_IM_MESSAGE_SCHEMA,
    handler=_handle_im_message,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
