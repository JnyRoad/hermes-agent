"""飞书电子表格工具。"""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import quote, urlparse, parse_qs

from tools.feishu.client import feishu_api_request, get_feishu_base_url
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)

_MAX_READ_ROWS = 200
_MAX_WRITE_ROWS = 5000
_MAX_WRITE_COLS = 100


def _check_feishu_available() -> bool:
    try:
        from tools.feishu.client import get_feishu_credentials

        get_feishu_credentials()
        return True
    except Exception:
        return False


def _parse_sheet_url(url: str) -> tuple[str, str]:
    """从飞书电子表格链接中提取 token 和可选 sheet_id。"""
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2 or parts[0] not in {"sheets", "wiki"}:
        raise ValueError(f"Failed to parse spreadsheet token from URL: {url}")
    token = parts[1]
    sheet_id = parse_qs(parsed.query).get("sheet", [""])[0]
    return token, sheet_id


def _extract_token_and_sheet(args: dict) -> tuple[str, str]:
    spreadsheet_token = str(args.get("spreadsheet_token", "")).strip()
    sheet_id = str(args.get("sheet_id", "")).strip()
    url = str(args.get("url", "")).strip()
    if spreadsheet_token:
        return spreadsheet_token, sheet_id
    if not url:
        raise ValueError("url or spreadsheet_token is required")
    token, url_sheet_id = _parse_sheet_url(url)
    return token, sheet_id or url_sheet_id


def _resolve_real_spreadsheet_token(token: str) -> str:
    """支持知识库 wiki URL 自动解析到真实的 spreadsheet_token。"""
    if not token.startswith("wik"):
        return token
    data = feishu_api_request(
        "GET",
        "/open-apis/wiki/v2/spaces/get_node",
        params={"token": token, "obj_type": "wiki"},
    )
    node = (data.get("data") or {}).get("node") or {}
    obj_token = str(node.get("obj_token", "")).strip()
    if not obj_token:
        raise ValueError(f"Failed to resolve spreadsheet token from wiki token: {token}")
    return obj_token


def _get_sheets(token: str) -> list[dict[str, Any]]:
    data = feishu_api_request("GET", f"/open-apis/sheets/v3/spreadsheets/{token}/sheets/query")
    payload = data.get("data") or {}
    return payload.get("sheets", [])


def _get_default_sheet_id(token: str) -> str:
    sheets = _get_sheets(token)
    if not sheets or not isinstance(sheets[0], dict) or not sheets[0].get("sheet_id"):
        raise ValueError("spreadsheet has no worksheets")
    return str(sheets[0]["sheet_id"])


def _resolve_range(token: str, args: dict) -> str:
    explicit_range = str(args.get("range", "")).strip()
    if explicit_range:
        return explicit_range
    sheet_id = str(args.get("sheet_id", "")).strip()
    if sheet_id:
        return sheet_id
    return _get_default_sheet_id(token)


def _flatten_cell_value(cell: Any) -> Any:
    """将富文本单元格压平成纯文本，减少模型上下文体积。"""
    if not isinstance(cell, list):
        return cell
    if cell and all(isinstance(item, dict) and "text" in item for item in cell):
        return "".join(str(item.get("text", "")) for item in cell)
    return cell


def _flatten_values(values: list[list[Any]]) -> list[list[Any]]:
    return [[_flatten_cell_value(cell) for cell in row] for row in values]


def _col_letter(col_index: int) -> str:
    result = ""
    value = col_index
    while value > 0:
        value -= 1
        result = chr(65 + (value % 26)) + result
        value //= 26
    return result


def _normalize_values(args: dict) -> list[list[Any]]:
    values = args.get("values")
    if not isinstance(values, list) or not values:
        raise ValueError("values must be a non-empty 2D array")
    normalized: list[list[Any]] = []
    max_cols = 0
    for row in values:
        if not isinstance(row, list):
            raise ValueError("values must be a 2D array")
        normalized.append(row)
        max_cols = max(max_cols, len(row))
    if len(normalized) > _MAX_WRITE_ROWS:
        raise ValueError(f"values has too many rows, max {_MAX_WRITE_ROWS}")
    if max_cols > _MAX_WRITE_COLS:
        raise ValueError(f"values has too many columns, max {_MAX_WRITE_COLS}")
    return normalized


def _build_write_range(token: str, args: dict, values: list[list[Any]]) -> str:
    explicit_range = str(args.get("range", "")).strip()
    if explicit_range:
        return explicit_range
    sheet_id = str(args.get("sheet_id", "")).strip() or _get_default_sheet_id(token)
    row_count = max(1, len(values))
    col_count = max((len(row) for row in values), default=1)
    return f"{sheet_id}!A1:{_col_letter(col_count)}{row_count}"


def _sheet_web_url(token: str) -> str:
    base_url = get_feishu_base_url()
    if base_url.endswith("open.larksuite.com"):
        return f"https://www.larksuite.com/sheets/{token}"
    return f"https://www.feishu.cn/sheets/{token}"


def _handle_sheet(args: dict, **_kw) -> str:
    action = str(args.get("action", "")).strip().lower()
    try:
        if action == "create":
            title = str(args.get("title", "")).strip()
            if not title:
                return tool_error("Missing required parameter: title")
            body = {"title": title}
            folder_token = str(args.get("folder_token", "")).strip()
            if folder_token:
                body["folder_token"] = folder_token
            data = feishu_api_request("POST", "/open-apis/sheets/v3/spreadsheets", json_body=body)
            spreadsheet = (data.get("data") or {}).get("spreadsheet") or {}
            token = str(spreadsheet.get("spreadsheet_token", "")).strip()
            result: dict[str, Any] = {
                "spreadsheet": spreadsheet,
                "spreadsheet_token": token,
                "url": _sheet_web_url(token) if token else None,
            }

            headers = args.get("headers")
            rows = args.get("data")
            if token and (isinstance(headers, list) or isinstance(rows, list)):
                values: list[list[Any]] = []
                if isinstance(headers, list) and headers:
                    values.append([str(item) for item in headers])
                if isinstance(rows, list) and rows:
                    if not all(isinstance(row, list) for row in rows):
                        return tool_error("data must be a 2D array")
                    values.extend(rows)
                if values:
                    range_value = _build_write_range(token, {}, values)
                    write_data = feishu_api_request(
                        "PUT",
                        f"/open-apis/sheets/v2/spreadsheets/{token}/values",
                        json_body={"valueRange": {"range": range_value, "values": values}},
                    )
                    result["initial_write"] = (write_data.get("data") or {}).get("updatedRange")
            return json.dumps(result, ensure_ascii=False)

        token, sheet_id = _extract_token_and_sheet(args)
        token = _resolve_real_spreadsheet_token(token)

        if action == "info":
            spreadsheet_data = feishu_api_request("GET", f"/open-apis/sheets/v3/spreadsheets/{token}")
            spreadsheet = (spreadsheet_data.get("data") or {}).get("spreadsheet") or {}
            sheets = _get_sheets(token)
            return json.dumps(
                {
                    "title": spreadsheet.get("title"),
                    "spreadsheet_token": token,
                    "url": _sheet_web_url(token),
                    "sheets": sheets,
                },
                ensure_ascii=False,
            )

        if action == "read":
            range_value = str(args.get("range", "")).strip() or sheet_id or _get_default_sheet_id(token)
            params = {}
            value_render_option = str(args.get("value_render_option", "ToString")).strip()
            if value_render_option:
                params["valueRenderOption"] = value_render_option
            data = feishu_api_request(
                "GET",
                f"/open-apis/sheets/v2/spreadsheets/{token}/values/{quote(range_value, safe='')}",
                params=params,
            )
            value_range = (data.get("data") or {}).get("valueRange") or {}
            values = _flatten_values(value_range.get("values", []))
            truncated = len(values) > _MAX_READ_ROWS
            if truncated:
                values = values[:_MAX_READ_ROWS]
            return json.dumps(
                {
                    "range": value_range.get("range", range_value),
                    "revision": value_range.get("revision"),
                    "values": values,
                    "truncated": truncated,
                },
                ensure_ascii=False,
            )

        if action == "write":
            values = _normalize_values(args)
            range_value = _build_write_range(token, {"range": args.get("range"), "sheet_id": sheet_id}, values)
            data = feishu_api_request(
                "PUT",
                f"/open-apis/sheets/v2/spreadsheets/{token}/values",
                json_body={"valueRange": {"range": range_value, "values": values}},
            )
            payload = data.get("data") or {}
            return json.dumps(
                {
                    "spreadsheet_token": payload.get("spreadsheetToken", token),
                    "revision": payload.get("revision"),
                    "updated_range": payload.get("updatedRange", range_value),
                    "updated_rows": payload.get("updatedRows"),
                    "updated_columns": payload.get("updatedColumns"),
                    "updated_cells": payload.get("updatedCells"),
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: info, read, write, create")
    except Exception as exc:
        logger.error("feishu_sheet error: %s", exc)
        return tool_error(f"Failed to execute feishu_sheet: {exc}")


FEISHU_SHEET_SCHEMA = {
    "name": "feishu_sheet",
    "description": "Operate Feishu spreadsheets. Hermes currently supports info, read, write, and create.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["info", "read", "write", "create"], "description": "Sheet action."},
            "url": {"type": "string", "description": "Spreadsheet URL. Can also be a wiki URL pointing to a spreadsheet."},
            "spreadsheet_token": {"type": "string", "description": "Spreadsheet token. Use this instead of url when already known."},
            "sheet_id": {"type": "string", "description": "Worksheet ID used when range is omitted."},
            "range": {"type": "string", "description": "Worksheet range such as <sheetId>!A1:D10 or just <sheetId>."},
            "value_render_option": {
                "type": "string",
                "enum": ["ToString", "FormattedValue", "Formula", "UnformattedValue"],
                "description": "Render option for read. Default ToString.",
            },
            "values": {"type": "array", "description": "2D array for write action.", "items": {"type": "array"}},
            "title": {"type": "string", "description": "Spreadsheet title for create action."},
            "folder_token": {"type": "string", "description": "Optional parent folder token for create action."},
            "headers": {"type": "array", "items": {"type": "string"}, "description": "Optional header row written after create."},
            "data": {"type": "array", "description": "Optional initial data rows written after create.", "items": {"type": "array"}},
        },
        "required": ["action"],
    },
}

registry.register(
    name="feishu_sheet",
    toolset="feishu",
    schema=FEISHU_SHEET_SCHEMA,
    handler=_handle_sheet,
    check_fn=_check_feishu_available,
    emoji="🪽",
)
