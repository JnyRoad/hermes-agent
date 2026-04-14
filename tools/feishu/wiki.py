"""飞书 Wiki 基础工具。"""

from __future__ import annotations

import json
import logging

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


def _handle_wiki_space(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "list":
            params = {
                "page_size": max(1, min(int(args.get("page_size", 10) or 10), 50)),
            }
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", "/open-apis/wiki/v2/spaces", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "spaces": payload.get("items", payload.get("spaces", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "get":
            space_id = str(args.get("space_id", "")).strip()
            if not space_id:
                return tool_error("Missing required parameter: space_id")
            data = feishu_api_request("GET", f"/open-apis/wiki/v2/spaces/{space_id}")
            payload = data.get("data") or {}
            return json.dumps({"space": payload.get("space", payload)}, ensure_ascii=False)

        if action == "create":
            name = str(args.get("name", "")).strip()
            description = str(args.get("description", "")).strip()
            data = feishu_api_request(
                "POST",
                "/open-apis/wiki/v2/spaces",
                json_body={
                    **({"name": name} if name else {}),
                    **({"description": description} if description else {}),
                },
            )
            payload = data.get("data") or {}
            return json.dumps({"space": payload.get("space", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: list, get, create")
    except Exception as exc:
        logger.error("feishu_wiki_space error: %s", exc)
        return tool_error(f"Failed to execute feishu_wiki_space: {exc}")


def _handle_wiki_space_node(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "list":
            space_id = str(args.get("space_id", "")).strip()
            if not space_id:
                return tool_error("Missing required parameter: space_id")
            params = {
                "page_size": max(1, min(int(args.get("page_size", 50) or 50), 200)),
            }
            parent_node_token = str(args.get("parent_node_token", "")).strip()
            if parent_node_token:
                params["parent_node_token"] = parent_node_token
            page_token = str(args.get("page_token", "")).strip()
            if page_token:
                params["page_token"] = page_token
            data = feishu_api_request("GET", f"/open-apis/wiki/v2/spaces/{space_id}/nodes", params=params)
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "nodes": payload.get("items", payload.get("nodes", [])),
                    "has_more": bool(payload.get("has_more", False)),
                    "page_token": payload.get("page_token"),
                },
                ensure_ascii=False,
            )

        if action == "get":
            token = str(args.get("token", "")).strip()
            if not token:
                return tool_error("Missing required parameter: token")
            obj_type = str(args.get("obj_type", "wiki")).strip() or "wiki"
            data = feishu_api_request(
                "GET",
                "/open-apis/wiki/v2/spaces/get_node",
                params={"token": token, "obj_type": obj_type},
            )
            payload = data.get("data") or {}
            return json.dumps({"node": payload.get("node", payload)}, ensure_ascii=False)

        if action == "create":
            space_id = str(args.get("space_id", "")).strip()
            obj_type = str(args.get("obj_type", "")).strip()
            node_type = str(args.get("node_type", "")).strip()
            if not space_id or not obj_type or not node_type:
                return tool_error("Parameters 'space_id', 'obj_type', and 'node_type' are required for create.")
            parent_node_token = str(args.get("parent_node_token", "")).strip()
            origin_node_token = str(args.get("origin_node_token", "")).strip()
            title = str(args.get("title", "")).strip()
            data = feishu_api_request(
                "POST",
                f"/open-apis/wiki/v2/spaces/{space_id}/nodes",
                json_body={
                    "obj_type": obj_type,
                    "node_type": node_type,
                    **({"parent_node_token": parent_node_token} if parent_node_token else {}),
                    **({"origin_node_token": origin_node_token} if origin_node_token else {}),
                    **({"title": title} if title else {}),
                },
            )
            payload = data.get("data") or {}
            return json.dumps({"node": payload.get("node", payload)}, ensure_ascii=False)

        if action == "move":
            space_id = str(args.get("space_id", "")).strip()
            node_token = str(args.get("node_token", "")).strip()
            if not space_id or not node_token:
                return tool_error("Parameters 'space_id' and 'node_token' are required for move.")
            target_parent_token = str(args.get("target_parent_token", "")).strip()
            data = feishu_api_request(
                "POST",
                f"/open-apis/wiki/v2/spaces/{space_id}/nodes/{node_token}/move",
                json_body={**({"target_parent_token": target_parent_token} if target_parent_token else {})},
            )
            payload = data.get("data") or {}
            return json.dumps({"node": payload.get("node", payload)}, ensure_ascii=False)

        if action == "copy":
            space_id = str(args.get("space_id", "")).strip()
            node_token = str(args.get("node_token", "")).strip()
            if not space_id or not node_token:
                return tool_error("Parameters 'space_id' and 'node_token' are required for copy.")
            target_space_id = str(args.get("target_space_id", "")).strip()
            target_parent_token = str(args.get("target_parent_token", "")).strip()
            title = str(args.get("title", "")).strip()
            data = feishu_api_request(
                "POST",
                f"/open-apis/wiki/v2/spaces/{space_id}/nodes/{node_token}/copy",
                json_body={
                    **({"target_space_id": target_space_id} if target_space_id else {}),
                    **({"target_parent_token": target_parent_token} if target_parent_token else {}),
                    **({"title": title} if title else {}),
                },
            )
            payload = data.get("data") or {}
            return json.dumps({"node": payload.get("node", payload)}, ensure_ascii=False)

        return tool_error("Unsupported action. Supported actions: list, get, create, move, copy")
    except Exception as exc:
        logger.error("feishu_wiki_space_node error: %s", exc)
        return tool_error(f"Failed to execute feishu_wiki_space_node: {exc}")


FEISHU_WIKI_SPACE_SCHEMA = {
    "name": "feishu_wiki_space",
    "description": "Manage Feishu Wiki spaces. Supported actions in Hermes now: list, get, and create.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["list", "get", "create"], "description": "Wiki space action."},
            "space_id": {"type": "string", "description": "Space ID for get action."},
            "name": {"type": "string", "description": "Space name for create action."},
            "description": {"type": "string", "description": "Space description for create action."},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Page size for list."},
            "page_token": {"type": "string", "description": "Pagination token for list."},
        },
        "required": ["action"],
    },
}

FEISHU_WIKI_SPACE_NODE_SCHEMA = {
    "name": "feishu_wiki_space_node",
    "description": "Manage Feishu Wiki space nodes. Supported actions in Hermes now: list, get, create, move, and copy.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["list", "get", "create", "move", "copy"], "description": "Wiki node action."},
            "space_id": {"type": "string", "description": "Space ID for list action."},
            "parent_node_token": {"type": "string", "description": "Parent node token for list action."},
            "page_size": {"type": "integer", "minimum": 1, "description": "Page size for list action."},
            "page_token": {"type": "string", "description": "Pagination token for list action."},
            "token": {"type": "string", "description": "Node token for get action."},
            "obj_type": {
                "type": "string",
                "enum": ["doc", "sheet", "mindnote", "bitable", "file", "docx", "slides", "wiki"],
                "description": "Object type for get action. Defaults to wiki.",
            },
            "node_type": {"type": "string", "enum": ["origin", "shortcut"], "description": "Node type for create action."},
            "origin_node_token": {"type": "string", "description": "Origin node token for shortcut creation."},
            "title": {"type": "string", "description": "Optional title for create or copy action."},
            "node_token": {"type": "string", "description": "Node token for move or copy action."},
            "target_parent_token": {"type": "string", "description": "Target parent token for move or copy action."},
            "target_space_id": {"type": "string", "description": "Target space for copy action. Defaults to current space."},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_wiki_space",
    toolset="feishu",
    schema=FEISHU_WIKI_SPACE_SCHEMA,
    handler=_handle_wiki_space,
    check_fn=_check_feishu_available,
    emoji="🪽",
)

registry.register(
    name="feishu_wiki_space_node",
    toolset="feishu",
    schema=FEISHU_WIKI_SPACE_NODE_SCHEMA,
    handler=_handle_wiki_space_node,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
