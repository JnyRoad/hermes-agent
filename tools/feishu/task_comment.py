"""飞书任务评论工具。"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

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


def _handle_task_comment(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "create":
            task_guid = str(args.get("task_guid", "")).strip()
            content = str(args.get("content", "")).strip()
            if not task_guid or not content:
                return tool_error("Parameters 'task_guid' and 'content' are required for create.")
            body: Dict[str, Any] = {
                "content": content,
                "resource_type": "task",
                "resource_id": task_guid,
            }
            reply_to_comment_id = str(args.get("reply_to_comment_id", "")).strip()
            if reply_to_comment_id:
                body["reply_to_comment_id"] = reply_to_comment_id
            data = feishu_api_request(
                "POST",
                "/open-apis/task/v2/comments",
                params={"user_id_type": "open_id"},
                json_body=body,
            )
            payload = data.get("data") or {}
            return json.dumps({"comment": payload.get("comment", payload)}, ensure_ascii=False)

        if action == "list":
            resource_id = str(args.get("resource_id", "")).strip()
            if not resource_id:
                return tool_error("Missing required parameter: resource_id")
            params = {
                "resource_type": "task",
                "resource_id": resource_id,
                "direction": str(args.get("direction", "asc")).strip() or "asc",
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 100)),
                "user_id_type": "open_id",
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/task/v2/comments", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "comments": payload.get("items", payload.get("comments", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "get":
            comment_id = str(args.get("comment_id", "")).strip()
            if not comment_id:
                return tool_error("Missing required parameter: comment_id")
            data = feishu_api_request(
                "GET",
                f"/open-apis/task/v2/comments/{comment_id}",
                params={"user_id_type": "open_id"},
            )
            payload = data.get("data") or {}
            return json.dumps({"comment": payload.get("comment", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: create, list, get")
    except Exception as exc:
        logger.error("feishu_task_comment error: %s", exc)
        return tool_error(f"Failed to execute feishu_task_comment: {exc}")


FEISHU_TASK_COMMENT_SCHEMA = {
    "name": "feishu_task_comment",
    "description": "Manage Feishu task comments. Supported actions in Hermes now: create, list, and get.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["create", "list", "get"], "description": "Task comment action."},
            "task_guid": {"type": "string", "description": "Task GUID for create action."},
            "content": {"type": "string", "description": "Comment content for create action."},
            "reply_to_comment_id": {"type": "string", "description": "Reply target comment ID for create action."},
            "resource_id": {"type": "string", "description": "Task GUID for list action."},
            "direction": {"type": "string", "enum": ["asc", "desc"], "description": "List order. Default asc."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
            "comment_id": {"type": "string", "description": "Comment ID for get action."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_task_comment",
    toolset="feishu",
    schema=FEISHU_TASK_COMMENT_SCHEMA,
    handler=_handle_task_comment,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
