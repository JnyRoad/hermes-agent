"""飞书搜索工具。"""

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


def _handle_search_doc_wiki(args: dict, **_kw) -> str:
    query = str(args.get("query", "") or "")
    page_size = int(args.get("page_size", 15) or 15)
    page_size = max(1, min(page_size, 20))
    page_token = str(args.get("page_token", "")).strip() or None
    filter_arg = args.get("filter") if isinstance(args.get("filter"), dict) else None
    try:
        request_data = {
            "query": query,
            "page_size": page_size,
            "doc_filter": dict(filter_arg or {}),
            "wiki_filter": dict(filter_arg or {}),
        }
        if page_token:
            request_data["page_token"] = page_token
        data = feishu_api_request(
            "POST",
            "/open-apis/search/v2/doc_wiki/search",
            json_body=request_data,
        )
        payload = data.get("data") or {}
        return json.dumps(
            {
                "total": payload.get("total"),
                "has_more": bool(payload.get("has_more", False)),
                "page_token": payload.get("page_token"),
                "results": payload.get("res_units", []),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_search_doc_wiki error: %s", exc)
        return tool_error(f"Failed to search Feishu docs/wiki: {exc}")


FEISHU_SEARCH_DOC_WIKI_SCHEMA = {
    "name": "feishu_search_doc_wiki",
    "description": "Search Feishu documents and wikis. Supports keyword search, paging, and optional shared filters for docs and wikis.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Search keyword. Empty string is allowed for broad search."},
            "filter": {
                "type": "object",
                "description": "Optional shared filter object applied to both doc_filter and wiki_filter.",
            },
            "page_token": {"type": "string", "description": "Pagination token from a previous call."},
            "page_size": {
                "type": "integer",
                "minimum": 1,
                "maximum": 20,
                "description": "Page size. Default 15, max 20.",
            },
        },
        "required": [],
    },
}

registry.register(
    name="feishu_search_doc_wiki",
    toolset="feishu",
    schema=FEISHU_SEARCH_DOC_WIKI_SCHEMA,
    handler=_handle_search_doc_wiki,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
