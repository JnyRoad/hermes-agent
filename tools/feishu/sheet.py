"""飞书电子表格工具。"""

from __future__ import annotations

import json
import logging
import time
from typing import Any
from urllib.parse import quote, urlparse, parse_qs

from tools.feishu.client import feishu_api_request, feishu_api_request_bytes, get_feishu_base_url
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)

_MAX_READ_ROWS = 200
_MAX_WRITE_ROWS = 5000
_MAX_WRITE_COLS = 100
_EXPORT_POLL_INTERVAL_SECONDS = 1
_EXPORT_POLL_MAX_RETRIES = 30


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


def _append_range(token: str, args: dict) -> str:
    explicit_range = str(args.get("range", "")).strip()
    if explicit_range:
        return explicit_range
    sheet_id = str(args.get("sheet_id", "")).strip() or _get_default_sheet_id(token)
    return sheet_id


def _split_sheet_range(range_value: str) -> tuple[str, str]:
    if "!" not in range_value:
        return range_value, ""
    sheet_id, cell_range = range_value.split("!", 1)
    return sheet_id, cell_range


def _cell_matches(value: Any, needle: str, *, match_case: bool, match_entire_cell: bool, search_by_regex: bool) -> bool:
    import re

    text = "" if value is None else str(value)
    target = needle
    flags = 0 if match_case else re.IGNORECASE
    if search_by_regex:
        pattern = re.compile(target, flags)
        if match_entire_cell:
            return bool(pattern.fullmatch(text))
        return bool(pattern.search(text))
    if not match_case:
        text = text.lower()
        target = target.lower()
    if match_entire_cell:
        return text == target
    return target in text


def _find_matches(values: list[list[Any]], needle: str, *, match_case: bool, match_entire_cell: bool, search_by_regex: bool) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for row_index, row in enumerate(values, start=1):
        for col_index, cell in enumerate(row, start=1):
            if _cell_matches(
                cell,
                needle,
                match_case=match_case,
                match_entire_cell=match_entire_cell,
                search_by_regex=search_by_regex,
            ):
                matches.append(
                    {
                        "row": row_index,
                        "column": col_index,
                        "value": cell,
                        "a1": f"{_col_letter(col_index)}{row_index}",
                    }
                )
    return matches


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

        if action == "append":
            values = _normalize_values(args)
            range_value = _append_range(token, {"range": args.get("range"), "sheet_id": sheet_id})
            data = feishu_api_request(
                "POST",
                f"/open-apis/sheets/v2/spreadsheets/{token}/values_append",
                json_body={"valueRange": {"range": range_value, "values": values}},
            )
            payload = data.get("data") or {}
            updates = payload.get("updates") or {}
            return json.dumps(
                {
                    "table_range": payload.get("tableRange"),
                    "updated_range": updates.get("updatedRange"),
                    "updated_rows": updates.get("updatedRows"),
                    "updated_columns": updates.get("updatedColumns"),
                    "updated_cells": updates.get("updatedCells"),
                    "revision": updates.get("revision"),
                },
                ensure_ascii=False,
            )

        if action == "find":
            sheet_id_for_find = str(args.get("sheet_id", "")).strip()
            needle = str(args.get("find", "")).strip()
            if not sheet_id_for_find or not needle:
                return tool_error("Parameters 'sheet_id' and 'find' are required for find.")
            suffix_range = str(args.get("range", "")).strip()
            read_range = f"{sheet_id_for_find}!{suffix_range}" if suffix_range else sheet_id_for_find
            data = feishu_api_request(
                "GET",
                f"/open-apis/sheets/v2/spreadsheets/{token}/values/{quote(read_range, safe='')}",
                params={"valueRenderOption": "ToString"},
            )
            value_range = (data.get("data") or {}).get("valueRange") or {}
            values = _flatten_values(value_range.get("values", []))
            matches = _find_matches(
                values,
                needle,
                match_case=bool(args.get("match_case", True)),
                match_entire_cell=bool(args.get("match_entire_cell", False)),
                search_by_regex=bool(args.get("search_by_regex", False)),
            )
            base_sheet_id, _ = _split_sheet_range(value_range.get("range", read_range))
            for item in matches:
                item["range"] = f"{base_sheet_id}!{item['a1']}"
            return json.dumps(
                {
                    "matched_cells": matches,
                    "rows_count": len(values),
                },
                ensure_ascii=False,
            )

        if action == "export":
            file_extension = str(args.get("file_extension", "")).strip().lower()
            if file_extension not in {"xlsx", "csv"}:
                return tool_error("Parameter 'file_extension' must be xlsx or csv.")
            export_sheet_id = str(args.get("sheet_id", "")).strip()
            if file_extension == "csv" and not export_sheet_id:
                return tool_error("sheet_id is required for CSV export.")
            create_data = {
                "file_extension": file_extension,
                "token": token,
                "type": "sheet",
            }
            if export_sheet_id:
                create_data["sub_id"] = export_sheet_id
            task = feishu_api_request("POST", "/open-apis/drive/v1/export_tasks", json_body=create_data)
            ticket = str((task.get("data") or {}).get("ticket", "")).strip()
            if not ticket:
                return tool_error("Failed to create export task: no ticket returned.")
            file_token = ""
            file_name = ""
            file_size = None
            for _ in range(_EXPORT_POLL_MAX_RETRIES):
                time.sleep(_EXPORT_POLL_INTERVAL_SECONDS)
                poll = feishu_api_request(
                    "GET",
                    f"/open-apis/drive/v1/export_tasks/{ticket}",
                    params={"token": token},
                )
                result = ((poll.get("data") or {}).get("result") or {})
                job_status = result.get("job_status")
                if job_status == 0:
                    file_token = str(result.get("file_token", "")).strip()
                    file_name = str(result.get("file_name", "")).strip()
                    file_size = result.get("file_size")
                    break
                if isinstance(job_status, int) and job_status >= 3:
                    return tool_error(result.get("job_error_msg") or f"Export failed with status={job_status}")
            if not file_token:
                return tool_error("Export timeout: task did not complete within 30 seconds.")
            output_path = str(args.get("output_path", "")).strip()
            if not output_path:
                return json.dumps(
                    {
                        "file_token": file_token,
                        "file_name": file_name,
                        "file_size": file_size,
                    },
                    ensure_ascii=False,
                )
            content, _headers = feishu_api_request_bytes("GET", f"/open-apis/drive/v1/export_tasks/file/{file_token}/download")
            from pathlib import Path

            target = Path(output_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            target.chmod(0o600)
            return json.dumps(
                {
                    "file_path": str(target),
                    "file_name": file_name,
                    "file_size": file_size,
                },
                ensure_ascii=False,
            )

        return tool_error("Unsupported action. Supported actions: info, read, write, append, find, create, export")
    except Exception as exc:
        logger.error("feishu_sheet error: %s", exc)
        return tool_error(f"Failed to execute feishu_sheet: {exc}")


FEISHU_SHEET_SCHEMA = {
    "name": "feishu_sheet",
    "description": "Operate Feishu spreadsheets. Hermes currently supports info, read, write, append, find, create, and export.",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["info", "read", "write", "append", "find", "create", "export"], "description": "Sheet action."},
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
            "find": {"type": "string", "description": "Find text or regex pattern for find action."},
            "match_case": {"type": "boolean", "description": "Whether find is case-sensitive. Default true."},
            "match_entire_cell": {"type": "boolean", "description": "Whether find must match the whole cell. Default false."},
            "search_by_regex": {"type": "boolean", "description": "Whether find uses regex matching. Default false."},
            "title": {"type": "string", "description": "Spreadsheet title for create action."},
            "folder_token": {"type": "string", "description": "Optional parent folder token for create action."},
            "headers": {"type": "array", "items": {"type": "string"}, "description": "Optional header row written after create."},
            "data": {"type": "array", "description": "Optional initial data rows written after create.", "items": {"type": "array"}},
            "file_extension": {"type": "string", "enum": ["xlsx", "csv"], "description": "Export format for export action."},
            "output_path": {"type": "string", "description": "Optional local file path for export action."},
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
