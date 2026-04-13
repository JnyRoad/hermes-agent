"""飞书工具注册与调用测试。"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from gateway.adapter_registry import register_adapter, unregister_adapter
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import SendResult


def test_feishu_runtime_session_exposes_account_id(monkeypatch):
    from tools.feishu.runtime import get_current_feishu_session

    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat")
    monkeypatch.setenv("HERMES_SESSION_ACCOUNT_ID", "feishu-cn")

    session = get_current_feishu_session()

    assert session["platform"] == "feishu"
    assert session["chat_id"] == "oc_chat"
    assert session["account_id"] == "feishu-cn"


def test_feishu_toolset_is_included_in_hermes_feishu():
    from toolsets import TOOLSETS, resolve_toolset

    assert "feishu" in TOOLSETS
    assert "feishu" in TOOLSETS["hermes-feishu"]["includes"]

    resolved = set(resolve_toolset("hermes-feishu"))
    assert "feishu_get_user" in resolved
    assert "feishu_search_doc_wiki" in resolved
    assert "feishu_bitable_app" in resolved
    assert "feishu_bitable_app_table" in resolved
    assert "feishu_bitable_app_table_field" in resolved
    assert "feishu_bitable_app_table_record" in resolved
    assert "feishu_bitable_app_table_view" in resolved
    assert "feishu_sheet" in resolved
    assert "feishu_doc_comments" in resolved
    assert "feishu_doc_media" in resolved
    assert "feishu_fetch_doc" in resolved
    assert "feishu_ask_user_question" in resolved
    assert "feishu_chat" in resolved
    assert "feishu_chat_members" in resolved
    assert "feishu_im_user_search_messages" in resolved
    assert "feishu_im_user_message" in resolved
    assert "feishu_im_bot_image" in resolved
    assert "feishu_calendar_calendar" in resolved
    assert "feishu_calendar_event" in resolved
    assert "feishu_calendar_event_attendee" in resolved
    assert "feishu_calendar_freebusy" in resolved
    assert "feishu_task_task" in resolved
    assert "feishu_task_tasklist" in resolved
    assert "feishu_task_comment" in resolved
    assert "feishu_task_subtask" in resolved
    assert "feishu_task_section" in resolved


def test_feishu_get_user_handler(monkeypatch):
    from tools.feishu.people import _handle_get_user

    monkeypatch.setattr(
        "tools.feishu.people.feishu_api_request",
        lambda *a, **kw: {"data": {"user": {"name": "Alice", "open_id": "ou_alice"}}},
    )
    payload = json.loads(_handle_get_user({"user_id": "ou_alice", "user_id_type": "open_id"}))
    assert payload["user"]["name"] == "Alice"


def test_feishu_search_doc_wiki_handler(monkeypatch):
    from tools.feishu.search import _handle_search_doc_wiki

    monkeypatch.setattr(
        "tools.feishu.search.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "total": 1,
                "has_more": False,
                "page_token": None,
                "res_units": [{"title": "Spec"}],
            }
        },
    )
    payload = json.loads(_handle_search_doc_wiki({"query": "spec"}))
    assert payload["total"] == 1
    assert payload["results"][0]["title"] == "Spec"


def test_feishu_sheet_info_handler(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/sheets/v3/spreadsheets/sht_1":
            return {"data": {"spreadsheet": {"title": "Budget"}}}
        if path == "/open-apis/sheets/v3/spreadsheets/sht_1/sheets/query":
            return {"data": {"sheets": [{"sheet_id": "sheet_1", "title": "Sheet1"}]}}
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.sheet.feishu_api_request", _fake_request)
    payload = json.loads(_handle_sheet({"action": "info", "spreadsheet_token": "sht_1"}))
    assert payload["title"] == "Budget"
    assert payload["sheets"][0]["sheet_id"] == "sheet_1"


def test_feishu_sheet_read_handler(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    monkeypatch.setattr(
        "tools.feishu.sheet.feishu_api_request",
        lambda *a, **kw: {
            "data": {"valueRange": {"range": "sheet_1!A1:B2", "values": [["A", "B"], ["1", "2"]], "revision": 3}}
        },
    )
    payload = json.loads(_handle_sheet({"action": "read", "spreadsheet_token": "sht_1", "range": "sheet_1!A1:B2"}))
    assert payload["range"] == "sheet_1!A1:B2"
    assert payload["values"][1][1] == "2"


def test_feishu_sheet_write_handler(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    monkeypatch.setattr(
        "tools.feishu.sheet.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "spreadsheetToken": "sht_1",
                "revision": 5,
                "updatedRange": "sheet_1!A1:B2",
                "updatedRows": 2,
                "updatedColumns": 2,
                "updatedCells": 4,
            }
        },
    )
    payload = json.loads(
        _handle_sheet(
            {
                "action": "write",
                "spreadsheet_token": "sht_1",
                "range": "sheet_1!A1:B2",
                "values": [["A", "B"], ["1", "2"]],
            }
        )
    )
    assert payload["updated_cells"] == 4


def test_feishu_sheet_append_handler(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    monkeypatch.setattr(
        "tools.feishu.sheet.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "tableRange": "sheet_1!A1:B3",
                "updates": {
                    "updatedRange": "sheet_1!A3:B3",
                    "updatedRows": 1,
                    "updatedColumns": 2,
                    "updatedCells": 2,
                    "revision": 6,
                },
            }
        },
    )
    payload = json.loads(
        _handle_sheet({"action": "append", "spreadsheet_token": "sht_1", "sheet_id": "sheet_1", "values": [["3", "4"]]})
    )
    assert payload["updated_range"] == "sheet_1!A3:B3"
    assert payload["updated_cells"] == 2


def test_feishu_sheet_find_handler(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    monkeypatch.setattr(
        "tools.feishu.sheet.feishu_api_request",
        lambda *a, **kw: {
            "data": {"valueRange": {"range": "sheet_1!A1:B2", "values": [["Alice", "Dev"], ["Bob", "QA"]]}}
        },
    )
    payload = json.loads(
        _handle_sheet({"action": "find", "spreadsheet_token": "sht_1", "sheet_id": "sheet_1", "find": "Bob"})
    )
    assert payload["matched_cells"][0]["range"] == "sheet_1!A2"
    assert payload["matched_cells"][0]["value"] == "Bob"


def test_feishu_sheet_create_handler(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    responses = [
        {"data": {"spreadsheet": {"spreadsheet_token": "sht_new", "title": "Budget"}}},
        {"data": {"updatedRange": "sheet_1!A1:B2"}},
    ]

    monkeypatch.setattr("tools.feishu.sheet.feishu_api_request", lambda *a, **kw: responses.pop(0))
    monkeypatch.setattr("tools.feishu.sheet._get_default_sheet_id", lambda token: "sheet_1")
    payload = json.loads(
        _handle_sheet({"action": "create", "title": "Budget", "headers": ["A", "B"], "data": [["1", "2"]]})
    )
    assert payload["spreadsheet_token"] == "sht_new"
    assert payload["initial_write"] == "sheet_1!A1:B2"


def test_feishu_sheet_export_handler(monkeypatch, tmp_path):
    from tools.feishu.sheet import _handle_sheet

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if path == "/open-apis/drive/v1/export_tasks":
            return {"data": {"ticket": "ticket_1"}}
        if path == "/open-apis/drive/v1/export_tasks/ticket_1":
            return {"data": {"result": {"job_status": 0, "file_token": "file_1", "file_name": "Budget.xlsx", "file_size": 10}}}
        raise AssertionError((method, path))

    monkeypatch.setattr("tools.feishu.sheet.feishu_api_request", _fake_request)
    monkeypatch.setattr("tools.feishu.sheet.feishu_api_request_bytes", lambda *a, **kw: (b"xlsx-bytes", {}))
    monkeypatch.setattr("tools.feishu.sheet.time.sleep", lambda _seconds: None)
    output = tmp_path / "budget.xlsx"
    payload = json.loads(
        _handle_sheet(
            {
                "action": "export",
                "spreadsheet_token": "sht_1",
                "file_extension": "xlsx",
                "output_path": str(output),
            }
        )
    )
    assert payload["file_path"] == str(output)
    assert output.read_bytes() == b"xlsx-bytes"


def test_feishu_sheet_find_requires_sheet_id(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    payload = json.loads(_handle_sheet({"action": "find", "spreadsheet_token": "sht_1", "find": "Bob"}))
    assert "sheet_id" in payload["error"]


def test_feishu_sheet_export_requires_sheet_id_for_csv(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    payload = json.loads(_handle_sheet({"action": "export", "spreadsheet_token": "sht_1", "file_extension": "csv"}))
    assert "sheet_id is required" in payload["error"]


def test_feishu_sheet_export_rejects_invalid_extension(monkeypatch):
    from tools.feishu.sheet import _handle_sheet

    payload = json.loads(_handle_sheet({"action": "export", "spreadsheet_token": "sht_1", "file_extension": "pdf"}))
    assert "file_extension" in payload["error"]


def test_feishu_drive_delete_handler(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"data": {}}

    monkeypatch.setattr("tools.feishu.drive.feishu_api_request", _fake_request)
    payload = json.loads(_handle_drive_file({"action": "delete", "file_token": "file_1", "type": "file"}))
    assert payload["success"] is True
    assert calls[0][0] == "DELETE"
    assert calls[0][1].endswith("/drive/v1/files/file_1")


def test_feishu_im_bot_image_handler(monkeypatch):
    from tools.feishu.im_bot_image import _handle_im_bot_image

    monkeypatch.setattr(
        "tools.feishu.im_bot_image.feishu_api_request_bytes",
        lambda *a, **kw: (b"image-bytes", {"content-type": "image/png"}),
    )
    payload = json.loads(_handle_im_bot_image({"message_id": "om_1", "file_key": "img_1", "type": "image"}))
    assert payload["message_id"] == "om_1"
    assert payload["content_type"] == "image/png"
    assert Path(payload["saved_path"]).exists()


def test_feishu_im_bot_image_requires_valid_type(monkeypatch):
    from tools.feishu.im_bot_image import _handle_im_bot_image

    payload = json.loads(_handle_im_bot_image({"message_id": "om_1", "file_key": "img_1", "type": "video"}))
    assert "type" in payload["error"]


def test_feishu_doc_comments_list_handler(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/drive/v1/files/dox_1/comments":
            return {"data": {"items": [{"comment_id": "c1", "reply_list": {"replies": [{}]}}], "has_more": False}}
        if path == "/open-apis/drive/v1/files/dox_1/comments/c1/replies":
            return {"data": {"items": [{"reply_id": "r1"}], "has_more": False}}
        raise AssertionError((method, path))

    monkeypatch.setattr("tools.feishu.doc_comments.feishu_api_request", _fake_request)
    payload = json.loads(_handle_doc_comments({"action": "list", "file_token": "dox_1", "file_type": "docx"}))
    assert payload["items"][0]["reply_list"]["replies"][0]["reply_id"] == "r1"


def test_feishu_doc_comments_requires_file_target(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    payload = json.loads(_handle_doc_comments({"action": "list", "file_type": "docx"}))
    assert "file_token" in payload["error"]
    assert "file_type" in payload["error"]


def test_feishu_doc_comments_reply_handler_fallback(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append(kwargs.get("json_body"))
        if len(calls) == 1:
            raise RuntimeError("first format rejected")
        return {"data": {"reply_id": "r1"}}

    monkeypatch.setattr("tools.feishu.doc_comments.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_doc_comments(
            {
                "action": "reply",
                "file_token": "dox_1",
                "file_type": "docx",
                "comment_id": "c1",
                "elements": [{"type": "text", "text": "ok"}],
            }
        )
    )
    assert calls[0] == {"content": {"elements": [{"type": "text_run", "text_run": {"text": "ok"}}]}}
    assert calls[1] == {"reply_elements": [{"type": "text_run", "text_run": {"text": "ok"}}]}
    assert payload["reply_id"] == "r1"


def test_feishu_doc_comments_reply_requires_comment_id(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    payload = json.loads(
        _handle_doc_comments(
            {"action": "reply", "file_token": "dox_1", "file_type": "docx", "elements": [{"type": "text", "text": "ok"}]}
        )
    )
    assert "comment_id" in payload["error"]


def test_feishu_doc_comments_patch_handler(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    monkeypatch.setattr("tools.feishu.doc_comments.feishu_api_request", lambda *a, **kw: {"data": {}})
    payload = json.loads(
        _handle_doc_comments(
            {
                "action": "patch",
                "file_token": "dox_1",
                "file_type": "docx",
                "comment_id": "c1",
                "is_solved_value": True,
            }
        )
    )
    assert payload["success"] is True


def test_feishu_doc_comments_patch_requires_is_solved_value(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    payload = json.loads(
        _handle_doc_comments({"action": "patch", "file_token": "dox_1", "file_type": "docx", "comment_id": "c1"})
    )
    assert "is_solved_value" in payload["error"]


def test_feishu_doc_comments_rejects_unsupported_action(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    payload = json.loads(_handle_doc_comments({"action": "noop", "file_token": "dox_1", "file_type": "docx"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_doc_comments_list_replies_handler(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    monkeypatch.setattr(
        "tools.feishu.doc_comments.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"reply_id": "r1"}], "has_more": False}},
    )
    payload = json.loads(
        _handle_doc_comments({"action": "list_replies", "file_token": "dox_1", "file_type": "docx", "comment_id": "c1"})
    )
    assert payload["items"][0]["reply_id"] == "r1"


def test_feishu_doc_comments_create_handler(monkeypatch):
    from tools.feishu.doc_comments import _handle_doc_comments

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"comment_id": "c1"}}

    monkeypatch.setattr("tools.feishu.doc_comments.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_doc_comments(
            {
                "action": "create",
                "file_token": "dox_1",
                "file_type": "docx",
                "elements": [{"type": "text", "text": "hello"}],
            }
        )
    )
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/files/dox_1/comments")
    replies = captured["json_body"]["reply_list"]["replies"]
    assert replies[0]["content"]["elements"][0]["text_run"]["text"] == "hello"
    assert payload["comment_id"] == "c1"


def test_feishu_doc_media_insert_handler(monkeypatch, tmp_path):
    from tools.feishu.doc_media import _handle_doc_media

    media_file = tmp_path / "demo.png"
    media_file.write_bytes(b"png-data")

    responses = [
        {"data": {"children": [{"block_id": "blk_1"}]}},
        {"data": {}},
    ]

    monkeypatch.setattr("tools.feishu.doc_media.feishu_api_request", lambda *a, **kw: responses.pop(0))
    monkeypatch.setattr(
        "tools.feishu.doc_media._upload_doc_media",
        lambda **kw: {"file_token": "file_1"},
    )
    payload = json.loads(
        _handle_doc_media(
            {
                "action": "insert",
                "doc_id": "dox_1",
                "file_path": str(media_file),
                "type": "image",
                "align": "center",
            }
        )
    )
    assert payload["success"] is True
    assert payload["file_token"] == "file_1"


def test_feishu_doc_media_download_handler(monkeypatch, tmp_path):
    from tools.feishu.doc_media import _handle_doc_media

    monkeypatch.setattr(
        "tools.feishu.doc_media.feishu_api_request_bytes",
        lambda *a, **kw: (b"hello", {"content-type": "text/plain"}),
    )
    output = tmp_path / "artifact"
    payload = json.loads(
        _handle_doc_media(
            {
                "action": "download",
                "resource_token": "file_1",
                "resource_type": "media",
                "output_path": str(output),
            }
        )
    )
    assert payload["saved_path"].endswith(".txt")
    assert Path(payload["saved_path"]).read_bytes() == b"hello"


def test_feishu_doc_media_insert_rejects_invalid_type(monkeypatch):
    from tools.feishu.doc_media import _handle_doc_media

    payload = json.loads(_handle_doc_media({"action": "insert", "doc_id": "dox_1", "file_path": "/tmp/a", "type": "video"}))
    assert "must be image or file" in payload["error"]


def test_feishu_doc_media_download_requires_output_path(monkeypatch):
    from tools.feishu.doc_media import _handle_doc_media

    payload = json.loads(_handle_doc_media({"action": "download", "resource_token": "file_1", "resource_type": "media"}))
    assert "output_path" in payload["error"]


def test_feishu_doc_media_rejects_unsupported_action(monkeypatch):
    from tools.feishu.doc_media import _handle_doc_media

    payload = json.loads(_handle_doc_media({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_bitable_app_list_handler(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    monkeypatch.setattr(
        "tools.feishu.bitable_app.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "files": [
                    {"token": "app_1", "type": "bitable", "name": "CRM"},
                    {"token": "doc_1", "type": "docx", "name": "Spec"},
                ],
                "has_more": False,
            }
        },
    )
    payload = json.loads(_handle_bitable_app({"action": "list"}))
    assert len(payload["apps"]) == 1
    assert payload["apps"][0]["token"] == "app_1"


def test_feishu_bitable_app_get_handler(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    monkeypatch.setattr(
        "tools.feishu.bitable_app.feishu_api_request",
        lambda *a, **kw: {"data": {"app": {"app_token": "app_1", "name": "CRM"}}},
    )
    payload = json.loads(_handle_bitable_app({"action": "get", "app_token": "app_1"}))
    assert payload["app"]["name"] == "CRM"


def test_feishu_bitable_app_create_handler(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"app": {"app_token": "app_new", "name": "CRM"}}}

    monkeypatch.setattr("tools.feishu.bitable_app.feishu_api_request", _fake_request)
    payload = json.loads(_handle_bitable_app({"action": "create", "name": "CRM", "folder_token": "fld_root"}))
    assert captured["method"] == "POST"
    assert captured["path"] == "/open-apis/bitable/v1/apps"
    assert captured["json_body"] == {"name": "CRM", "folder_token": "fld_root"}
    assert payload["app"]["app_token"] == "app_new"


def test_feishu_bitable_app_patch_handler(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"app": {"app_token": "app_1", "name": "CRM Pro", "is_advanced": True}}}

    monkeypatch.setattr("tools.feishu.bitable_app.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app({"action": "patch", "app_token": "app_1", "name": "CRM Pro", "is_advanced": True})
    )
    assert captured["method"] == "PATCH"
    assert captured["path"].endswith("/apps/app_1")
    assert captured["json_body"] == {"name": "CRM Pro", "is_advanced": True}
    assert payload["app"]["name"] == "CRM Pro"


def test_feishu_bitable_app_patch_requires_app_token(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    payload = json.loads(_handle_bitable_app({"action": "patch", "name": "CRM"}))
    assert "app_token" in payload["error"]


def test_feishu_bitable_app_patch_requires_updatable_field(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    payload = json.loads(_handle_bitable_app({"action": "patch", "app_token": "app_1"}))
    assert "At least one updatable field is required for patch" in payload["error"]


def test_feishu_bitable_app_copy_handler(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    monkeypatch.setattr(
        "tools.feishu.bitable_app.feishu_api_request",
        lambda *a, **kw: {"data": {"app": {"app_token": "app_copy", "name": "CRM Copy"}}},
    )
    payload = json.loads(_handle_bitable_app({"action": "copy", "app_token": "app_1", "name": "CRM Copy"}))
    assert payload["app"]["app_token"] == "app_copy"


def test_feishu_bitable_app_copy_requires_name(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    payload = json.loads(_handle_bitable_app({"action": "copy", "app_token": "app_1"}))
    assert "app_token" in payload["error"]
    assert "name" in payload["error"]


def test_feishu_bitable_app_rejects_unsupported_action(monkeypatch):
    from tools.feishu.bitable_app import _handle_bitable_app

    payload = json.loads(_handle_bitable_app({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_bitable_app_table_list_handler(monkeypatch):
    from tools.feishu.bitable_app_table import _handle_bitable_app_table

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"table_id": "tbl_1", "name": "Leads"}], "has_more": False}},
    )
    payload = json.loads(_handle_bitable_app_table({"action": "list", "app_token": "app_1"}))
    assert payload["items"][0]["table_id"] == "tbl_1"


def test_feishu_bitable_app_table_create_sanitizes_field_property(monkeypatch):
    from tools.feishu.bitable_app_table import _handle_bitable_app_table

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"table_id": "tbl_1", "default_view_id": "viw_1", "field_id_list": ["fld_1"]}}

    monkeypatch.setattr("tools.feishu.bitable_app_table.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table(
            {
                "action": "create",
                "app_token": "app_1",
                "table": {
                    "name": "Leads",
                    "fields": [
                        {"field_name": "Done", "type": 7, "property": {"formatter": "x"}},
                        {"field_name": "Link", "type": 15, "property": {"formatter": "y"}},
                    ],
                },
            }
        )
    )
    fields = captured["json_body"]["table"]["fields"]
    assert "property" not in fields[0]
    assert "property" not in fields[1]
    assert payload["table_id"] == "tbl_1"


def test_feishu_bitable_app_table_batch_create_handler(monkeypatch):
    from tools.feishu.bitable_app_table import _handle_bitable_app_table

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"tables": [{"table_id": "tbl_1"}, {"table_id": "tbl_2"}], "total": 2}}

    monkeypatch.setattr("tools.feishu.bitable_app_table.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table(
            {
                "action": "batch_create",
                "app_token": "app_1",
                "tables": [
                    {"name": "Leads", "fields": [{"field_name": "Done", "type": 7, "property": {"formatter": "x"}}]},
                    {"name": "Accounts"},
                ],
            }
        )
    )
    assert captured["json_body"]["tables"][0]["fields"][0].get("property") is None
    assert payload["total"] == 2
    assert payload["tables"][1]["table_id"] == "tbl_2"


def test_feishu_bitable_app_table_batch_create_requires_table_name(monkeypatch):
    from tools.feishu.bitable_app_table import _handle_bitable_app_table

    payload = json.loads(
        _handle_bitable_app_table(
            {
                "action": "batch_create",
                "app_token": "app_1",
                "tables": [{"default_view_name": "All"}],
            }
        )
    )
    assert "tables[0].name is required" in payload["error"]


def test_feishu_bitable_app_table_batch_create_rejects_non_object_item(monkeypatch):
    from tools.feishu.bitable_app_table import _handle_bitable_app_table

    payload = json.loads(
        _handle_bitable_app_table({"action": "batch_create", "app_token": "app_1", "tables": ["bad"]})
    )
    assert "tables[0] must be an object" in payload["error"]


def test_feishu_bitable_app_table_rejects_unsupported_action(monkeypatch):
    from tools.feishu.bitable_app_table import _handle_bitable_app_table

    payload = json.loads(_handle_bitable_app_table({"action": "noop", "app_token": "app_1"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_bitable_app_table_patch_handler(monkeypatch):
    from tools.feishu.bitable_app_table import _handle_bitable_app_table

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"table": {"table_id": "tbl_1", "name": "Customers"}}}

    monkeypatch.setattr("tools.feishu.bitable_app_table.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table({"action": "patch", "app_token": "app_1", "table_id": "tbl_1", "name": "Customers"})
    )
    assert captured["method"] == "PATCH"
    assert captured["path"].endswith("/apps/app_1/tables/tbl_1")
    assert captured["json_body"] == {"name": "Customers"}
    assert payload["table"]["name"] == "Customers"


def test_feishu_bitable_app_table_record_list_handler(monkeypatch):
    from tools.feishu.bitable_app_table_record import _handle_bitable_app_table_record

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_record.feishu_api_request",
        lambda *a, **kw: {
            "data": {"items": [{"record_id": "rec_1", "fields": {"Name": "Alice"}}], "has_more": False}
        },
    )
    payload = json.loads(
        _handle_bitable_app_table_record({"action": "list", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert payload["items"][0]["record_id"] == "rec_1"


def test_feishu_bitable_app_table_record_create_handler(monkeypatch):
    from tools.feishu.bitable_app_table_record import _handle_bitable_app_table_record

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_record.feishu_api_request",
        lambda *a, **kw: {"data": {"record": {"record_id": "rec_1", "fields": {"Name": "Alice"}}}},
    )
    payload = json.loads(
        _handle_bitable_app_table_record(
            {"action": "create", "app_token": "app_1", "table_id": "tbl_1", "fields": {"Name": "Alice"}}
        )
    )
    assert payload["record"]["record_id"] == "rec_1"


def test_feishu_bitable_app_table_record_update_handler(monkeypatch):
    from tools.feishu.bitable_app_table_record import _handle_bitable_app_table_record

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"record": {"record_id": "rec_1", "fields": {"Name": "Alice 2"}}}}

    monkeypatch.setattr("tools.feishu.bitable_app_table_record.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table_record(
            {
                "action": "update",
                "app_token": "app_1",
                "table_id": "tbl_1",
                "record_id": "rec_1",
                "fields": {"Name": "Alice 2"},
            }
        )
    )
    assert captured["method"] == "PUT"
    assert captured["path"].endswith("/apps/app_1/tables/tbl_1/records/rec_1")
    assert captured["json_body"] == {"fields": {"Name": "Alice 2"}}
    assert payload["record"]["fields"]["Name"] == "Alice 2"


def test_feishu_bitable_app_table_record_delete_handler(monkeypatch):
    from tools.feishu.bitable_app_table_record import _handle_bitable_app_table_record

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_record.feishu_api_request",
        lambda *a, **kw: {"data": {}},
    )
    payload = json.loads(
        _handle_bitable_app_table_record(
            {"action": "delete", "app_token": "app_1", "table_id": "tbl_1", "record_id": "rec_1"}
        )
    )
    assert payload["deleted"] is True
    assert payload["record_id"] == "rec_1"


def test_feishu_bitable_app_table_record_batch_create_handler(monkeypatch):
    from tools.feishu.bitable_app_table_record import _handle_bitable_app_table_record

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_record.feishu_api_request",
        lambda *a, **kw: {"data": {"records": [{"record_id": "rec_1"}, {"record_id": "rec_2"}], "total": 2}},
    )
    payload = json.loads(
        _handle_bitable_app_table_record(
            {
                "action": "batch_create",
                "app_token": "app_1",
                "table_id": "tbl_1",
                "records": [{"fields": {"Name": "Alice"}}, {"fields": {"Name": "Bob"}}],
            }
        )
    )
    assert payload["total"] == 2
    assert payload["records"][1]["record_id"] == "rec_2"


def test_feishu_bitable_app_table_record_batch_update_handler(monkeypatch):
    from tools.feishu.bitable_app_table_record import _handle_bitable_app_table_record

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_record.feishu_api_request",
        lambda *a, **kw: {"data": {"records": [{"record_id": "rec_1"}], "total": 1}},
    )
    payload = json.loads(
        _handle_bitable_app_table_record(
            {
                "action": "batch_update",
                "app_token": "app_1",
                "table_id": "tbl_1",
                "records": [{"record_id": "rec_1", "fields": {"Name": "Alice 2"}}],
            }
        )
    )
    assert payload["total"] == 1
    assert payload["records"][0]["record_id"] == "rec_1"


def test_feishu_bitable_app_table_record_batch_delete_handler(monkeypatch):
    from tools.feishu.bitable_app_table_record import _handle_bitable_app_table_record

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_record.feishu_api_request",
        lambda *a, **kw: {"data": {"total": 2}},
    )
    payload = json.loads(
        _handle_bitable_app_table_record(
            {
                "action": "batch_delete",
                "app_token": "app_1",
                "table_id": "tbl_1",
                "record_ids": ["rec_1", "rec_2"],
            }
        )
    )
    assert payload["deleted"] is True
    assert payload["record_ids"] == ["rec_1", "rec_2"]


def test_feishu_bitable_app_table_field_list_handler(monkeypatch):
    from tools.feishu.bitable_app_table_field import _handle_bitable_app_table_field

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_field.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"field_id": "fld_1", "field_name": "Name"}], "has_more": False}},
    )
    payload = json.loads(
        _handle_bitable_app_table_field({"action": "list", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert payload["fields"][0]["field_id"] == "fld_1"


def test_feishu_bitable_app_table_field_create_handler(monkeypatch):
    from tools.feishu.bitable_app_table_field import _handle_bitable_app_table_field

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"field": {"field_id": "fld_1", "field_name": "Name", "type": 1}}}

    monkeypatch.setattr("tools.feishu.bitable_app_table_field.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table_field(
            {"action": "create", "app_token": "app_1", "table_id": "tbl_1", "field_name": "Name", "type": 1}
        )
    )
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/apps/app_1/tables/tbl_1/fields")
    assert captured["json_body"]["field_name"] == "Name"
    assert payload["field"]["field_id"] == "fld_1"


def test_feishu_bitable_app_table_field_update_autofills_missing_properties(monkeypatch):
    from tools.feishu.bitable_app_table_field import _handle_bitable_app_table_field

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if method == "GET":
            return {
                "data": {
                    "items": [
                        {"field_id": "fld_1", "field_name": "Name", "type": 1, "property": {"formatter": "text"}}
                    ]
                }
            }
        return {"data": {"field": {"field_id": "fld_1", "field_name": "Full Name", "type": 1}}}

    monkeypatch.setattr("tools.feishu.bitable_app_table_field.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table_field(
            {"action": "update", "app_token": "app_1", "table_id": "tbl_1", "field_id": "fld_1", "field_name": "Full Name"}
        )
    )
    update_body = calls[-1][2]["json_body"]
    assert update_body["type"] == 1
    assert update_body["property"]["formatter"] == "text"
    assert payload["field"]["field_name"] == "Full Name"


def test_feishu_bitable_app_table_field_delete_handler(monkeypatch):
    from tools.feishu.bitable_app_table_field import _handle_bitable_app_table_field

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        return {"data": {}}

    monkeypatch.setattr("tools.feishu.bitable_app_table_field.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table_field(
            {"action": "delete", "app_token": "app_1", "table_id": "tbl_1", "field_id": "fld_1"}
        )
    )
    assert captured["method"] == "DELETE"
    assert captured["path"].endswith("/apps/app_1/tables/tbl_1/fields/fld_1")
    assert payload["deleted"] is True
    assert payload["field_id"] == "fld_1"


def test_feishu_bitable_app_table_field_update_requires_field_id(monkeypatch):
    from tools.feishu.bitable_app_table_field import _handle_bitable_app_table_field

    payload = json.loads(
        _handle_bitable_app_table_field({"action": "update", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert "field_id" in payload["error"]


def test_feishu_bitable_app_table_field_delete_requires_field_id(monkeypatch):
    from tools.feishu.bitable_app_table_field import _handle_bitable_app_table_field

    payload = json.loads(
        _handle_bitable_app_table_field({"action": "delete", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert "field_id" in payload["error"]


def test_feishu_bitable_app_table_field_rejects_unsupported_action(monkeypatch):
    from tools.feishu.bitable_app_table_field import _handle_bitable_app_table_field

    payload = json.loads(
        _handle_bitable_app_table_field({"action": "noop", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert "Unsupported action" in payload["error"]


def test_feishu_bitable_app_table_view_list_handler(monkeypatch):
    from tools.feishu.bitable_app_table_view import _handle_bitable_app_table_view

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_view.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"view_id": "viw_1", "view_name": "Grid"}], "has_more": False}},
    )
    payload = json.loads(
        _handle_bitable_app_table_view({"action": "list", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert payload["views"][0]["view_id"] == "viw_1"


def test_feishu_bitable_app_table_view_get_handler(monkeypatch):
    from tools.feishu.bitable_app_table_view import _handle_bitable_app_table_view

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_view.feishu_api_request",
        lambda *a, **kw: {"data": {"view": {"view_id": "viw_1", "view_name": "Grid"}}},
    )
    payload = json.loads(
        _handle_bitable_app_table_view(
            {"action": "get", "app_token": "app_1", "table_id": "tbl_1", "view_id": "viw_1"}
        )
    )
    assert payload["view"]["view_name"] == "Grid"


def test_feishu_bitable_app_table_view_create_handler(monkeypatch):
    from tools.feishu.bitable_app_table_view import _handle_bitable_app_table_view

    monkeypatch.setattr(
        "tools.feishu.bitable_app_table_view.feishu_api_request",
        lambda *a, **kw: {"data": {"view": {"view_id": "viw_1", "view_name": "Board"}}},
    )
    payload = json.loads(
        _handle_bitable_app_table_view(
            {"action": "create", "app_token": "app_1", "table_id": "tbl_1", "view_name": "Board", "view_type": "kanban"}
        )
    )
    assert payload["view"]["view_name"] == "Board"


def test_feishu_bitable_app_table_view_patch_handler(monkeypatch):
    from tools.feishu.bitable_app_table_view import _handle_bitable_app_table_view

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"view": {"view_id": "viw_1", "view_name": "Board v2"}}}

    monkeypatch.setattr("tools.feishu.bitable_app_table_view.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_bitable_app_table_view(
            {"action": "patch", "app_token": "app_1", "table_id": "tbl_1", "view_id": "viw_1", "view_name": "Board v2"}
        )
    )
    assert captured["method"] == "PATCH"
    assert captured["path"].endswith("/apps/app_1/tables/tbl_1/views/viw_1")
    assert captured["json_body"] == {"view_name": "Board v2"}
    assert payload["view"]["view_name"] == "Board v2"


def test_feishu_bitable_app_table_view_get_requires_view_id(monkeypatch):
    from tools.feishu.bitable_app_table_view import _handle_bitable_app_table_view

    payload = json.loads(
        _handle_bitable_app_table_view({"action": "get", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert "view_id" in payload["error"]


def test_feishu_bitable_app_table_view_patch_requires_view_name(monkeypatch):
    from tools.feishu.bitable_app_table_view import _handle_bitable_app_table_view

    payload = json.loads(
        _handle_bitable_app_table_view(
            {"action": "patch", "app_token": "app_1", "table_id": "tbl_1", "view_id": "viw_1"}
        )
    )
    assert "view_id" in payload["error"]
    assert "view_name" in payload["error"]


def test_feishu_bitable_app_table_view_rejects_unsupported_action(monkeypatch):
    from tools.feishu.bitable_app_table_view import _handle_bitable_app_table_view

    payload = json.loads(
        _handle_bitable_app_table_view({"action": "noop", "app_token": "app_1", "table_id": "tbl_1"})
    )
    assert "Unsupported action" in payload["error"]


def test_feishu_drive_file_list_handler(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    monkeypatch.setattr(
        "tools.feishu.drive.feishu_api_request",
        lambda *a, **kw: {"data": {"files": [{"name": "A"}], "has_more": False, "next_page_token": None}},
    )
    payload = json.loads(_handle_drive_file({"action": "list", "page_size": 10}))
    assert payload["files"][0]["name"] == "A"


def test_feishu_chat_search_handler(monkeypatch):
    from tools.feishu.chat import _handle_chat

    monkeypatch.setattr(
        "tools.feishu.chat.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"chat_id": "oc_1", "name": "研发群"}], "has_more": False}},
    )
    payload = json.loads(_handle_chat({"action": "search", "query": "研发"}))
    assert payload["items"][0]["chat_id"] == "oc_1"


def test_feishu_chat_search_requires_query(monkeypatch):
    from tools.feishu.chat import _handle_chat

    payload = json.loads(_handle_chat({"action": "search"}))
    assert "query" in payload["error"]


def test_feishu_chat_get_handler(monkeypatch):
    from tools.feishu.chat import _handle_chat

    monkeypatch.setattr(
        "tools.feishu.chat.feishu_api_request",
        lambda *a, **kw: {"data": {"chat": {"chat_id": "oc_1", "name": "研发群"}}},
    )
    payload = json.loads(_handle_chat({"action": "get", "chat_id": "oc_1"}))
    assert payload["chat"]["name"] == "研发群"


def test_feishu_chat_get_requires_chat_id(monkeypatch):
    from tools.feishu.chat import _handle_chat

    payload = json.loads(_handle_chat({"action": "get"}))
    assert "chat_id" in payload["error"]


def test_feishu_chat_rejects_unsupported_action(monkeypatch):
    from tools.feishu.chat import _handle_chat

    payload = json.loads(_handle_chat({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_chat_members_handler(monkeypatch):
    from tools.feishu.chat_members import _handle_chat_members

    monkeypatch.setattr(
        "tools.feishu.chat_members.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"member_id": "ou_1", "name": "Alice"}], "member_total": 1}},
    )
    payload = json.loads(_handle_chat_members({"chat_id": "oc_1"}))
    assert payload["member_total"] == 1
    assert payload["items"][0]["member_id"] == "ou_1"


def test_feishu_chat_members_requires_chat_id(monkeypatch):
    from tools.feishu.chat_members import _handle_chat_members

    payload = json.loads(_handle_chat_members({}))
    assert "chat_id" in payload["error"]


def test_feishu_drive_file_get_meta_handler(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    monkeypatch.setattr(
        "tools.feishu.drive.feishu_api_request",
        lambda *a, **kw: {"data": {"metas": [{"title": "Spec"}]}},
    )
    payload = json.loads(
        _handle_drive_file(
            {"action": "get_meta", "request_docs": [{"doc_token": "doxcn1", "doc_type": "docx"}]}
        )
    )
    assert payload["metas"][0]["title"] == "Spec"


def test_feishu_drive_file_get_meta_requires_request_docs(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(_handle_drive_file({"action": "get_meta"}))
    assert "request_docs" in payload["error"]


def test_feishu_drive_file_copy_handler(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    monkeypatch.setattr(
        "tools.feishu.drive.feishu_api_request",
        lambda *a, **kw: {"data": {"file": {"token": "fld_new", "name": "Copy"}}},
    )
    payload = json.loads(
        _handle_drive_file(
            {"action": "copy", "file_token": "fld_old", "name": "Copy", "type": "docx", "folder_token": "fld_root"}
        )
    )
    assert payload["file"]["token"] == "fld_new"


def test_feishu_drive_file_copy_requires_required_fields(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(_handle_drive_file({"action": "copy", "file_token": "fld_old"}))
    assert "file_token" in payload["error"]
    assert "name" in payload["error"]
    assert "type" in payload["error"]


def test_feishu_drive_file_move_handler(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    monkeypatch.setattr(
        "tools.feishu.drive.feishu_api_request",
        lambda *a, **kw: {"data": {"task_id": "task_1"}},
    )
    payload = json.loads(
        _handle_drive_file(
            {"action": "move", "file_token": "fld_old", "type": "docx", "folder_token": "fld_root"}
        )
    )
    assert payload["success"] is True
    assert payload["task_id"] == "task_1"


def test_feishu_drive_file_move_requires_required_fields(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(_handle_drive_file({"action": "move", "file_token": "fld_old"}))
    assert "file_token" in payload["error"]
    assert "type" in payload["error"]
    assert "folder_token" in payload["error"]


def test_feishu_drive_file_upload_handler(monkeypatch, tmp_path):
    from tools.feishu.drive import _handle_drive_file

    artifact = tmp_path / "demo.txt"
    artifact.write_text("hello", encoding="utf-8")
    monkeypatch.setattr(
        "tools.feishu.drive._upload_drive_file",
        lambda **kw: {"data": {"file_token": "file_1"}},
    )
    payload = json.loads(
        _handle_drive_file({"action": "upload", "file_path": str(artifact), "parent_node": "fld_root"})
    )
    assert payload["file_token"] == "file_1"
    assert payload["file_name"] == "demo.txt"


def test_feishu_drive_file_upload_handler_base64(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    captured = {}

    def _fake_upload(**kwargs):
        captured.update(kwargs)
        return {"data": {"file_token": "file_1"}}

    monkeypatch.setattr("tools.feishu.drive._upload_drive_file", _fake_upload)
    payload = json.loads(
        _handle_drive_file(
            {"action": "upload", "file_content_base64": "aGVsbG8=", "file_name": "demo.txt", "parent_node": "fld_root"}
        )
    )
    assert captured["file_name"] == "demo.txt"
    assert captured["content"] == b"hello"
    assert captured["parent_node"] == "fld_root"
    assert payload["upload_method"] == "upload_all"
    assert payload["size"] == 5


def test_feishu_drive_file_upload_requires_file_name_for_base64(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(_handle_drive_file({"action": "upload", "file_content_base64": "aGVsbG8="}))
    assert "file_name is required" in payload["error"]


def test_feishu_drive_file_upload_rejects_invalid_base64(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(
        _handle_drive_file(
            {"action": "upload", "file_content_base64": "not-base64!!", "file_name": "demo.txt"}
        )
    )
    assert "Failed to decode file_content_base64" in payload["error"]


def test_feishu_drive_file_upload_requires_file_input(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(_handle_drive_file({"action": "upload"}))
    assert "Either file_path or file_content_base64 is required" in payload["error"]


def test_feishu_drive_file_upload_handler_chunked(monkeypatch, tmp_path):
    from tools.feishu.drive import _handle_drive_file

    artifact = tmp_path / "big.bin"
    artifact.write_bytes(b"abcdefgh")
    part_calls = []

    monkeypatch.setattr("tools.feishu.drive._SMALL_FILE_THRESHOLD", 4)
    monkeypatch.setattr(
        "tools.feishu.drive._upload_prepare",
        lambda **kw: {"data": {"upload_id": "up_1", "block_size": 3, "block_num": 3}},
    )
    monkeypatch.setattr(
        "tools.feishu.drive._upload_part",
        lambda **kw: part_calls.append((kw["seq"], kw["content"])),
    )
    monkeypatch.setattr(
        "tools.feishu.drive._upload_finish",
        lambda **kw: {"data": {"file_token": "file_big"}},
    )
    payload = json.loads(
        _handle_drive_file({"action": "upload", "file_path": str(artifact), "parent_node": "fld_root"})
    )
    assert payload["file_token"] == "file_big"
    assert payload["upload_method"] == "chunked"
    assert payload["chunks_uploaded"] == 3
    assert part_calls == [(0, b"abc"), (1, b"def"), (2, b"gh")]


def test_feishu_drive_file_download_handler(monkeypatch, tmp_path):
    from tools.feishu.drive import _handle_drive_file

    monkeypatch.setattr(
        "tools.feishu.drive.feishu_api_request_bytes",
        lambda *a, **kw: (b"hello", {"content-type": "text/plain"}),
    )
    target = tmp_path / "demo.txt"
    payload = json.loads(
        _handle_drive_file({"action": "download", "file_token": "file_1", "output_path": str(target)})
    )
    assert payload["saved_path"] == str(target)
    assert target.read_bytes() == b"hello"


def test_feishu_drive_file_download_requires_file_token(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(_handle_drive_file({"action": "download"}))
    assert "file_token" in payload["error"]


def test_feishu_drive_file_rejects_unsupported_action(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    payload = json.loads(_handle_drive_file({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_wiki_space_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"space_id": "sp1"}], "has_more": False}},
    )
    payload = json.loads(_handle_wiki_space({"action": "list"}))
    assert payload["spaces"][0]["space_id"] == "sp1"


def test_feishu_wiki_space_create_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"space": {"space_id": "sp_new", "name": "KB"}}},
    )
    payload = json.loads(_handle_wiki_space({"action": "create", "name": "KB"}))
    assert payload["space"]["space_id"] == "sp_new"


def test_feishu_wiki_space_get_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"space": {"space_id": "sp1", "name": "KB"}}},
    )
    payload = json.loads(_handle_wiki_space({"action": "get", "space_id": "sp1"}))
    assert payload["space"]["name"] == "KB"


def test_feishu_wiki_space_get_requires_space_id(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space

    payload = json.loads(_handle_wiki_space({"action": "get"}))
    assert "space_id" in payload["error"]


def test_feishu_wiki_space_rejects_unsupported_action(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space

    payload = json.loads(_handle_wiki_space({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_wiki_space_node_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"node": {"node_token": "wikcn1"}}},
    )
    payload = json.loads(_handle_wiki_space_node({"action": "get", "token": "wikcn1"}))
    assert payload["node"]["node_token"] == "wikcn1"


def test_feishu_wiki_space_node_list_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"node_token": "wik_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_wiki_space_node({"action": "list", "space_id": "sp1"}))
    assert payload["nodes"][0]["node_token"] == "wik_1"


def test_feishu_wiki_space_node_get_requires_token(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    payload = json.loads(_handle_wiki_space_node({"action": "get"}))
    assert "token" in payload["error"]


def test_feishu_wiki_space_node_create_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"node": {"node_token": "wik_new"}}},
    )
    payload = json.loads(
        _handle_wiki_space_node(
            {"action": "create", "space_id": "sp1", "obj_type": "docx", "node_type": "origin", "title": "Doc"}
        )
    )
    assert payload["node"]["node_token"] == "wik_new"


def test_feishu_wiki_space_node_create_requires_core_fields(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    payload = json.loads(_handle_wiki_space_node({"action": "create", "space_id": "sp1"}))
    assert "obj_type" in payload["error"]
    assert "node_type" in payload["error"]


def test_feishu_wiki_space_node_move_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"node": {"node_token": "wik_old", "parent_node_token": "wik_parent"}}},
    )
    payload = json.loads(
        _handle_wiki_space_node(
            {"action": "move", "space_id": "sp1", "node_token": "wik_old", "target_parent_token": "wik_parent"}
        )
    )
    assert payload["node"]["parent_node_token"] == "wik_parent"


def test_feishu_wiki_space_node_move_requires_node_token(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    payload = json.loads(_handle_wiki_space_node({"action": "move", "space_id": "sp1"}))
    assert "space_id" in payload["error"]
    assert "node_token" in payload["error"]


def test_feishu_wiki_space_node_copy_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"node": {"node_token": "wik_copy"}}},
    )
    payload = json.loads(
        _handle_wiki_space_node(
            {"action": "copy", "space_id": "sp1", "node_token": "wik_old", "target_space_id": "sp2"}
        )
    )
    assert payload["node"]["node_token"] == "wik_copy"


def test_feishu_wiki_space_node_copy_requires_node_token(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    payload = json.loads(_handle_wiki_space_node({"action": "copy", "space_id": "sp1"}))
    assert "space_id" in payload["error"]
    assert "node_token" in payload["error"]


def test_feishu_wiki_space_node_rejects_unsupported_action(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    payload = json.loads(_handle_wiki_space_node({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_calendar_calendar_list_handler(monkeypatch):
    from tools.feishu.calendar import _handle_calendar

    monkeypatch.setattr(
        "tools.feishu.calendar.feishu_api_request",
        lambda *a, **kw: {"data": {"calendar_list": [{"calendar_id": "cal_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_calendar({"action": "list"}))
    assert payload["calendars"][0]["calendar_id"] == "cal_1"


def test_feishu_calendar_calendar_primary_handler(monkeypatch):
    from tools.feishu.calendar import _handle_calendar

    monkeypatch.setattr(
        "tools.feishu.calendar.feishu_api_request",
        lambda *a, **kw: {"data": {"calendars": [{"calendar_id": "cal_primary"}]}},
    )
    payload = json.loads(_handle_calendar({"action": "primary"}))
    assert payload["calendars"][0]["calendar_id"] == "cal_primary"


def test_feishu_calendar_calendar_get_handler(monkeypatch):
    from tools.feishu.calendar import _handle_calendar

    monkeypatch.setattr(
        "tools.feishu.calendar.feishu_api_request",
        lambda *a, **kw: {"data": {"calendar": {"calendar_id": "cal_1", "summary": "团队日历"}}},
    )
    payload = json.loads(_handle_calendar({"action": "get", "calendar_id": "cal_1"}))
    assert payload["calendar"]["summary"] == "团队日历"


def test_feishu_calendar_calendar_get_requires_calendar_id(monkeypatch):
    from tools.feishu.calendar import _handle_calendar

    payload = json.loads(_handle_calendar({"action": "get"}))
    assert "calendar_id" in payload["error"]


def test_feishu_calendar_calendar_rejects_unsupported_action(monkeypatch):
    from tools.feishu.calendar import _handle_calendar

    payload = json.loads(_handle_calendar({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_calendar_event_list_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/instance_view":
            return {
                "data": {
                    "items": [
                        {
                            "event_id": "evt_1",
                            "summary": "Demo",
                            "start_time": {"timestamp": "1710000000"},
                            "end_time": {"timestamp": "1710003600"},
                        }
                    ],
                    "has_more": False,
                }
            }
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_calendar_event(
            {"action": "list", "start_time": "2024-03-01T10:00:00+08:00", "end_time": "2024-03-01T11:00:00+08:00"}
        )
    )
    assert payload["events"][0]["event_id"] == "evt_1"


def test_feishu_calendar_event_create_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events":
            return {"data": {"event": {"event_id": "evt_1", "summary": "Demo"}}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/evt_1/attendees/batch_create":
            return {"data": {}}
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_calendar_event(
            {
                "action": "create",
                "summary": "Demo",
                "start_time": "2024-03-01T10:00:00+08:00",
                "end_time": "2024-03-01T11:00:00+08:00",
                "user_open_id": "ou_1",
            }
        )
    )
    assert payload["event"]["event_id"] == "evt_1"
    assert payload["attendees"][0]["id"] == "ou_1"


def test_feishu_calendar_event_create_requires_core_fields(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "create", "summary": "Demo"}))
    assert "start_time" in payload["error"]
    assert "end_time" in payload["error"]


def test_feishu_calendar_event_delete_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/evt_1":
            return {"data": {}}
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(_handle_calendar_event({"action": "delete", "event_id": "evt_1"}))
    assert payload["success"] is True


def test_feishu_calendar_event_list_requires_time_range(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "list"}))
    assert "start_time" in payload["error"]
    assert "end_time" in payload["error"]


def test_feishu_calendar_event_search_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/search":
            return {"data": {"items": [{"event_id": "evt_2", "summary": "Match"}], "has_more": False}}
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(_handle_calendar_event({"action": "search", "query": "Match"}))
    assert payload["events"][0]["event_id"] == "evt_2"


def test_feishu_calendar_event_search_requires_query(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "search"}))
    assert "query" in payload["error"]


def test_feishu_calendar_event_reply_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/evt_1/reply":
            return {"data": {}}
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(_handle_calendar_event({"action": "reply", "event_id": "evt_1", "rsvp_status": "accept"}))
    assert payload["success"] is True
    assert payload["rsvp_status"] == "accept"


def test_feishu_calendar_event_get_requires_event_id(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "get"}))
    assert "event_id" in payload["error"]


def test_feishu_calendar_event_delete_requires_event_id(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "delete"}))
    assert "event_id" in payload["error"]


def test_feishu_calendar_event_instances_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/evt_1/instances":
            return {
                "data": {
                    "items": [{"event_id": "evt_1", "start_time": {"timestamp": "1710000000"}}],
                    "has_more": False,
                }
            }
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_calendar_event(
            {
                "action": "instances",
                "event_id": "evt_1",
                "start_time": "2024-03-01T10:00:00+08:00",
                "end_time": "2024-03-01T11:00:00+08:00",
            }
        )
    )
    assert payload["instances"][0]["event_id"] == "evt_1"


def test_feishu_calendar_event_instances_requires_time_range(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "instances", "event_id": "evt_1"}))
    assert "start_time" in payload["error"]
    assert "end_time" in payload["error"]


def test_feishu_calendar_event_instance_view_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/instance_view":
            return {"data": {"items": [{"event_id": "evt_3"}], "has_more": False}}
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_calendar_event(
            {
                "action": "instance_view",
                "start_time": "2024-03-01T10:00:00+08:00",
                "end_time": "2024-03-01T11:00:00+08:00",
            }
        )
    )
    assert payload["events"][0]["event_id"] == "evt_3"


def test_feishu_calendar_event_get_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        if path == "/open-apis/calendar/v4/calendars/cal_1/events/evt_1":
            return {"data": {"event": {"event_id": "evt_1", "summary": "Demo"}}}
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(_handle_calendar_event({"action": "get", "event_id": "evt_1"}))
    assert payload["event"]["summary"] == "Demo"


def test_feishu_calendar_event_patch_handler(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    captured = {}

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/calendar/v4/calendars/primary":
            return {"data": {"calendars": [{"calendar": {"calendar_id": "cal_1"}}]}}
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"event": {"event_id": "evt_1", "summary": "Updated"}}}

    monkeypatch.setattr("tools.feishu.calendar_event.feishu_api_request", _fake_request)
    payload = json.loads(_handle_calendar_event({"action": "patch", "event_id": "evt_1", "summary": "Updated"}))
    assert captured["method"] == "PATCH"
    assert captured["path"].endswith("/calendars/cal_1/events/evt_1")
    assert captured["json_body"]["summary"] == "Updated"
    assert payload["event"]["summary"] == "Updated"


def test_feishu_calendar_event_patch_requires_updatable_field(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    monkeypatch.setattr("tools.feishu.calendar_event._calendar_id", lambda _args: "cal_1")
    payload = json.loads(_handle_calendar_event({"action": "patch", "event_id": "evt_1"}))
    assert "At least one updatable field is required for patch" in payload["error"]


def test_feishu_calendar_event_reply_requires_rsvp_status(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "reply", "event_id": "evt_1"}))
    assert "rsvp_status" in payload["error"]


def test_feishu_calendar_event_instance_view_requires_time_range(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "instance_view"}))
    assert "start_time" in payload["error"]
    assert "end_time" in payload["error"]


def test_feishu_calendar_event_rejects_unsupported_action(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    payload = json.loads(_handle_calendar_event({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_calendar_freebusy_list_handler(monkeypatch):
    from tools.feishu.calendar_freebusy import _handle_calendar_freebusy

    monkeypatch.setattr(
        "tools.feishu.calendar_freebusy.feishu_api_request",
        lambda *a, **kw: {"data": {"freebusy_lists": [{"user_id": "ou_1", "busy": []}]}},
    )
    payload = json.loads(
        _handle_calendar_freebusy(
            {
                "action": "list",
                "time_min": "2024-03-01T10:00:00+08:00",
                "time_max": "2024-03-01T11:00:00+08:00",
                "user_ids": ["ou_1"],
            }
        )
    )
    assert payload["freebusy_lists"][0]["user_id"] == "ou_1"


def test_feishu_calendar_freebusy_requires_user_ids(monkeypatch):
    from tools.feishu.calendar_freebusy import _handle_calendar_freebusy

    payload = json.loads(
        _handle_calendar_freebusy(
            {"action": "list", "time_min": "2024-03-01T10:00:00+08:00", "time_max": "2024-03-01T11:00:00+08:00", "user_ids": []}
        )
    )
    assert "user_ids" in payload["error"]


def test_feishu_calendar_freebusy_rejects_more_than_ten_users(monkeypatch):
    from tools.feishu.calendar_freebusy import _handle_calendar_freebusy

    user_ids = [f"ou_{idx}" for idx in range(11)]
    payload = json.loads(
        _handle_calendar_freebusy(
            {"action": "list", "time_min": "2024-03-01T10:00:00+08:00", "time_max": "2024-03-01T11:00:00+08:00", "user_ids": user_ids}
        )
    )
    assert "maximum 10 users" in payload["error"]


def test_feishu_calendar_freebusy_rejects_unsupported_action(monkeypatch):
    from tools.feishu.calendar_freebusy import _handle_calendar_freebusy

    payload = json.loads(_handle_calendar_freebusy({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_calendar_event_attendee_create_handler(monkeypatch):
    from tools.feishu.calendar_event_attendee import _handle_calendar_event_attendee

    monkeypatch.setattr(
        "tools.feishu.calendar_event_attendee.feishu_api_request",
        lambda *a, **kw: {"data": {"attendees": [{"type": "user", "user_id": "ou_1"}]}},
    )
    payload = json.loads(
        _handle_calendar_event_attendee(
            {
                "action": "create",
                "calendar_id": "cal_1",
                "event_id": "evt_1",
                "attendees": [{"type": "user", "attendee_id": "ou_1"}],
            }
        )
    )
    assert payload["attendees"][0]["user_id"] == "ou_1"


def test_feishu_calendar_event_attendee_list_handler(monkeypatch):
    from tools.feishu.calendar_event_attendee import _handle_calendar_event_attendee

    monkeypatch.setattr(
        "tools.feishu.calendar_event_attendee.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"type": "user", "user_id": "ou_1"}], "has_more": False}},
    )
    payload = json.loads(
        _handle_calendar_event_attendee({"action": "list", "calendar_id": "cal_1", "event_id": "evt_1"})
    )
    assert payload["attendees"][0]["user_id"] == "ou_1"


def test_feishu_calendar_event_attendee_rejects_unsupported_attendee_type(monkeypatch):
    from tools.feishu.calendar_event_attendee import _handle_calendar_event_attendee

    payload = json.loads(
        _handle_calendar_event_attendee(
            {
                "action": "create",
                "calendar_id": "cal_1",
                "event_id": "evt_1",
                "attendees": [{"type": "robot", "attendee_id": "rb_1"}],
            }
        )
    )
    assert "unsupported attendee type" in payload["error"]


def test_feishu_calendar_event_attendee_requires_calendar_and_event_id(monkeypatch):
    from tools.feishu.calendar_event_attendee import _handle_calendar_event_attendee

    payload = json.loads(_handle_calendar_event_attendee({"action": "list"}))
    assert "calendar_id" in payload["error"]
    assert "event_id" in payload["error"]


def test_feishu_calendar_event_attendee_rejects_unsupported_action(monkeypatch):
    from tools.feishu.calendar_event_attendee import _handle_calendar_event_attendee

    payload = json.loads(_handle_calendar_event_attendee({"action": "noop", "calendar_id": "cal_1", "event_id": "evt_1"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_task_task_create_handler(monkeypatch):
    from tools.feishu.task import _handle_task

    monkeypatch.setattr(
        "tools.feishu.task.feishu_api_request",
        lambda *a, **kw: {"data": {"task": {"guid": "task_1", "summary": "Ship"}}},
    )
    payload = json.loads(_handle_task({"action": "create", "summary": "Ship"}))
    assert payload["task"]["guid"] == "task_1"


def test_feishu_task_task_create_requires_summary(monkeypatch):
    from tools.feishu.task import _handle_task

    payload = json.loads(_handle_task({"action": "create"}))
    assert "summary" in payload["error"]


def test_feishu_task_task_list_handler(monkeypatch):
    from tools.feishu.task import _handle_task

    monkeypatch.setattr(
        "tools.feishu.task.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "task_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task({"action": "list"}))
    assert payload["tasks"][0]["guid"] == "task_1"


def test_feishu_task_task_get_handler(monkeypatch):
    from tools.feishu.task import _handle_task

    monkeypatch.setattr(
        "tools.feishu.task.feishu_api_request",
        lambda *a, **kw: {"data": {"task": {"guid": "task_1", "summary": "Ship"}}},
    )
    payload = json.loads(_handle_task({"action": "get", "task_guid": "task_1"}))
    assert payload["task"]["summary"] == "Ship"


def test_feishu_task_task_get_requires_task_guid(monkeypatch):
    from tools.feishu.task import _handle_task

    payload = json.loads(_handle_task({"action": "get"}))
    assert "task_guid" in payload["error"]


def test_feishu_task_task_patch_handler(monkeypatch):
    from tools.feishu.task import _handle_task

    monkeypatch.setattr(
        "tools.feishu.task.feishu_api_request",
        lambda *a, **kw: {"data": {"task": {"guid": "task_1", "summary": "Updated"}}},
    )
    payload = json.loads(_handle_task({"action": "patch", "task_guid": "task_1", "summary": "Updated"}))
    assert payload["task"]["summary"] == "Updated"


def test_feishu_task_task_patch_requires_updatable_field(monkeypatch):
    from tools.feishu.task import _handle_task

    payload = json.loads(_handle_task({"action": "patch", "task_guid": "task_1"}))
    assert "At least one updatable field is required for patch" in payload["error"]


def test_feishu_task_task_rejects_unsupported_action(monkeypatch):
    from tools.feishu.task import _handle_task

    payload = json.loads(_handle_task({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_task_tasklist_create_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    monkeypatch.setattr(
        "tools.feishu.tasklist.feishu_api_request",
        lambda *a, **kw: {"data": {"tasklist": {"guid": "tl_1", "name": "Inbox"}}},
    )
    payload = json.loads(_handle_tasklist({"action": "create", "name": "Inbox"}))
    assert payload["tasklist"]["guid"] == "tl_1"


def test_feishu_task_tasklist_get_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    monkeypatch.setattr(
        "tools.feishu.tasklist.feishu_api_request",
        lambda *a, **kw: {"data": {"tasklist": {"guid": "tl_1", "name": "Inbox"}}},
    )
    payload = json.loads(_handle_tasklist({"action": "get", "tasklist_guid": "tl_1"}))
    assert payload["tasklist"]["name"] == "Inbox"


def test_feishu_task_tasklist_get_requires_tasklist_guid(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(_handle_tasklist({"action": "get"}))
    assert "tasklist_guid" in payload["error"]


def test_feishu_task_tasklist_list_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    monkeypatch.setattr(
        "tools.feishu.tasklist.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "tl_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_tasklist({"action": "list"}))
    assert payload["tasklists"][0]["guid"] == "tl_1"


def test_feishu_task_tasklist_tasks_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    monkeypatch.setattr(
        "tools.feishu.tasklist.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "task_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_tasklist({"action": "tasks", "tasklist_guid": "tl_1"}))
    assert payload["tasks"][0]["guid"] == "task_1"


def test_feishu_task_tasklist_tasks_requires_tasklist_guid(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(_handle_tasklist({"action": "tasks"}))
    assert "tasklist_guid" in payload["error"]


def test_feishu_task_tasklist_patch_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"tasklist": {"guid": "tl_1", "name": "Inbox 2"}}}

    monkeypatch.setattr("tools.feishu.tasklist.feishu_api_request", _fake_request)
    payload = json.loads(_handle_tasklist({"action": "patch", "tasklist_guid": "tl_1", "name": "Inbox 2"}))
    assert captured["method"] == "PATCH"
    assert captured["path"].endswith("/tasklists/tl_1")
    assert captured["json_body"]["update_fields"] == ["name"]
    assert payload["tasklist"]["name"] == "Inbox 2"


def test_feishu_task_tasklist_patch_requires_tasklist_guid(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(_handle_tasklist({"action": "patch", "name": "Inbox 2"}))
    assert "tasklist_guid" in payload["error"]


def test_feishu_task_tasklist_patch_requires_updatable_field(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(_handle_tasklist({"action": "patch", "tasklist_guid": "tl_1"}))
    assert "At least one updatable field is required for patch" in payload["error"]


def test_feishu_task_tasklist_add_members_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    monkeypatch.setattr(
        "tools.feishu.tasklist.feishu_api_request",
        lambda *a, **kw: {"data": {"tasklist": {"guid": "tl_1"}}},
    )
    payload = json.loads(
        _handle_tasklist(
            {
                "action": "add_members",
                "tasklist_guid": "tl_1",
                "members": [{"id": "ou_1", "role": "editor"}],
            }
        )
    )
    assert payload["tasklist"]["guid"] == "tl_1"


def test_feishu_task_tasklist_add_members_requires_tasklist_guid(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(_handle_tasklist({"action": "add_members", "members": [{"id": "ou_1"}]}))
    assert "tasklist_guid" in payload["error"]


def test_feishu_task_tasklist_add_members_requires_non_empty_members(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(_handle_tasklist({"action": "add_members", "tasklist_guid": "tl_1", "members": []}))
    assert "members must be a non-empty array" in payload["error"]


def test_feishu_task_tasklist_add_members_requires_member_id(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(
        _handle_tasklist(
            {
                "action": "add_members",
                "tasklist_guid": "tl_1",
                "members": [{"role": "editor"}],
            }
        )
    )
    assert "member.id is required" in payload["error"]


def test_feishu_task_tasklist_rejects_unsupported_action(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    payload = json.loads(_handle_tasklist({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_task_comment_create_handler(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    monkeypatch.setattr(
        "tools.feishu.task_comment.feishu_api_request",
        lambda *a, **kw: {"data": {"comment": {"id": "c_1", "content": "ok"}}},
    )
    payload = json.loads(_handle_task_comment({"action": "create", "task_guid": "task_1", "content": "ok"}))
    assert payload["comment"]["id"] == "c_1"


def test_feishu_task_comment_create_requires_task_guid_and_content(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    payload = json.loads(_handle_task_comment({"action": "create", "task_guid": "task_1"}))
    assert "task_guid" in payload["error"]
    assert "content" in payload["error"]


def test_feishu_task_comment_list_handler(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    monkeypatch.setattr(
        "tools.feishu.task_comment.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"id": "c_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_comment({"action": "list", "resource_id": "task_1"}))
    assert payload["comments"][0]["id"] == "c_1"


def test_feishu_task_comment_list_requires_resource_id(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    payload = json.loads(_handle_task_comment({"action": "list"}))
    assert "resource_id" in payload["error"]


def test_feishu_task_comment_get_handler(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    monkeypatch.setattr(
        "tools.feishu.task_comment.feishu_api_request",
        lambda *a, **kw: {"data": {"comment": {"id": "c_1", "content": "ok"}}},
    )
    payload = json.loads(_handle_task_comment({"action": "get", "comment_id": "c_1"}))
    assert payload["comment"]["content"] == "ok"


def test_feishu_task_comment_get_requires_comment_id(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    payload = json.loads(_handle_task_comment({"action": "get"}))
    assert "comment_id" in payload["error"]


def test_feishu_task_comment_rejects_unsupported_action(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    payload = json.loads(_handle_task_comment({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_task_subtask_create_handler(monkeypatch):
    from tools.feishu.task_subtask import _handle_task_subtask

    monkeypatch.setattr(
        "tools.feishu.task_subtask.feishu_api_request",
        lambda *a, **kw: {"data": {"subtask": {"guid": "sub_1", "summary": "child"}}},
    )
    payload = json.loads(
        _handle_task_subtask({"action": "create", "task_guid": "task_1", "summary": "child"})
    )
    assert payload["subtask"]["guid"] == "sub_1"


def test_feishu_task_subtask_create_requires_summary(monkeypatch):
    from tools.feishu.task_subtask import _handle_task_subtask

    payload = json.loads(_handle_task_subtask({"action": "create", "task_guid": "task_1"}))
    assert "task_guid" in payload["error"]
    assert "summary" in payload["error"]


def test_feishu_task_subtask_list_handler(monkeypatch):
    from tools.feishu.task_subtask import _handle_task_subtask

    monkeypatch.setattr(
        "tools.feishu.task_subtask.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "sub_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_subtask({"action": "list", "task_guid": "task_1"}))
    assert payload["subtasks"][0]["guid"] == "sub_1"


def test_feishu_task_subtask_list_requires_task_guid(monkeypatch):
    from tools.feishu.task_subtask import _handle_task_subtask

    payload = json.loads(_handle_task_subtask({"action": "list"}))
    assert "task_guid" in payload["error"]


def test_feishu_task_subtask_rejects_unsupported_action(monkeypatch):
    from tools.feishu.task_subtask import _handle_task_subtask

    payload = json.loads(_handle_task_subtask({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_task_section_create_handler(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    monkeypatch.setattr(
        "tools.feishu.task_section.feishu_api_request",
        lambda *a, **kw: {"data": {"section": {"guid": "sec_1", "name": "Doing"}}},
    )
    payload = json.loads(
        _handle_task_section({"action": "create", "name": "Doing", "resource_type": "tasklist", "resource_id": "tl_1"})
    )
    assert payload["section"]["guid"] == "sec_1"


def test_feishu_task_section_create_requires_name_and_resource_type(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    payload = json.loads(_handle_task_section({"action": "create", "name": "Doing"}))
    assert "resource_type" in payload["error"]


def test_feishu_task_section_get_handler(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    monkeypatch.setattr(
        "tools.feishu.task_section.feishu_api_request",
        lambda *a, **kw: {"data": {"section": {"guid": "sec_1", "name": "Doing"}}},
    )
    payload = json.loads(_handle_task_section({"action": "get", "section_guid": "sec_1"}))
    assert payload["section"]["name"] == "Doing"


def test_feishu_task_section_get_requires_section_guid(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    payload = json.loads(_handle_task_section({"action": "get"}))
    assert "section_guid" in payload["error"]


def test_feishu_task_section_patch_handler(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    captured = {}

    def _fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return {"data": {"section": {"guid": "sec_1", "name": "Done"}}}

    monkeypatch.setattr("tools.feishu.task_section.feishu_api_request", _fake_request)
    payload = json.loads(_handle_task_section({"action": "patch", "section_guid": "sec_1", "name": "Done"}))
    assert captured["method"] == "PATCH"
    assert captured["path"].endswith("/sections/sec_1")
    assert captured["json_body"]["update_fields"] == ["name"]
    assert payload["section"]["name"] == "Done"


def test_feishu_task_section_patch_requires_updatable_field(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    payload = json.loads(_handle_task_section({"action": "patch", "section_guid": "sec_1"}))
    assert "At least one updatable field is required for patch" in payload["error"]


def test_feishu_task_section_list_handler(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    monkeypatch.setattr(
        "tools.feishu.task_section.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "sec_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_section({"action": "list", "resource_type": "my_tasks"}))
    assert payload["sections"][0]["guid"] == "sec_1"


def test_feishu_task_section_list_requires_resource_type(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    payload = json.loads(_handle_task_section({"action": "list"}))
    assert "resource_type" in payload["error"]


def test_feishu_task_section_tasks_handler(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    monkeypatch.setattr(
        "tools.feishu.task_section.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "task_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_section({"action": "tasks", "section_guid": "sec_1"}))
    assert payload["tasks"][0]["guid"] == "task_1"


def test_feishu_task_section_tasks_requires_section_guid(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    payload = json.loads(_handle_task_section({"action": "tasks"}))
    assert "section_guid" in payload["error"]


def test_feishu_task_section_rejects_unsupported_action(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    payload = json.loads(_handle_task_section({"action": "noop"}))
    assert "Unsupported action" in payload["error"]


def test_feishu_im_get_messages_handler(monkeypatch):
    from tools.feishu.im import _handle_get_messages

    monkeypatch.setattr(
        "tools.feishu.im.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "items": [
                    {
                        "message_id": "om_1",
                        "msg_type": "text",
                        "body": {"content": '{"text":"hello"}'},
                        "sender": {"id": "ou_a", "sender_type": "user"},
                        "create_time": "1710000000000",
                    }
                ],
                "has_more": False,
            }
        },
    )
    payload = json.loads(_handle_get_messages({"chat_id": "oc_chat_1"}))
    assert payload["messages"][0]["message_id"] == "om_1"
    assert "hello" in payload["messages"][0]["content"]


def test_feishu_im_get_messages_handler_with_open_id(monkeypatch):
    from tools.feishu.im import _handle_get_messages

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if path == "/open-apis/im/v1/chat_p2p/batch_query":
            return {"data": {"p2p_chats": [{"chat_id": "oc_p2p_1"}]}}
        if path == "/open-apis/im/v1/messages":
            return {
                "data": {
                    "items": [
                        {
                            "message_id": "om_2",
                            "msg_type": "text",
                            "body": {"content": '{"text":"p2p hello"}'},
                            "sender": {"id": "ou_b", "sender_type": "user"},
                            "create_time": "1710000000000",
                        }
                    ],
                    "has_more": False,
                }
            }
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.im.feishu_api_request", _fake_request)
    payload = json.loads(_handle_get_messages({"open_id": "ou_b"}))
    assert payload["messages"][0]["message_id"] == "om_2"
    assert calls[0][1] == "/open-apis/im/v1/chat_p2p/batch_query"
    assert calls[1][2]["params"]["container_id"] == "oc_p2p_1"


def test_feishu_im_get_messages_rejects_chat_id_and_open_id_together(monkeypatch):
    from tools.feishu.im import _handle_get_messages

    payload = json.loads(_handle_get_messages({"chat_id": "oc_1", "open_id": "ou_1"}))
    assert "mutually exclusive" in payload["error"]


def test_feishu_im_get_thread_messages_handler(monkeypatch):
    from tools.feishu.im import _handle_get_thread_messages

    monkeypatch.setattr(
        "tools.feishu.im.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "items": [
                    {
                        "message_id": "om_t1",
                        "msg_type": "text",
                        "body": {"content": '{"text":"in thread"}'},
                        "sender": {"id": "ou_a", "sender_type": "user"},
                        "create_time": "1710000000000",
                        "thread_id": "omt_1",
                    }
                ],
                "has_more": False,
            }
        },
    )
    payload = json.loads(_handle_get_thread_messages({"thread_id": "omt_1"}))
    assert payload["messages"][0]["thread_id"] == "omt_1"


def test_feishu_im_search_messages_handler(monkeypatch):
    from tools.feishu.im import _handle_search_messages

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if path == "/open-apis/search/v2/message":
            return {
                "data": {
                    "items": ["om_1"],
                    "has_more": False,
                    "page_token": None,
                }
            }
        if path.startswith("/open-apis/im/v1/messages/mget?"):
            return {
                "data": {
                    "items": [
                        {
                            "message_id": "om_1",
                            "chat_id": "oc_chat_1",
                            "msg_type": "text",
                            "body": {"content": '{"text":"matched"}'},
                            "sender": {"id": "ou_a", "sender_type": "user"},
                            "create_time": "1710000000000",
                        }
                    ]
                }
            }
        if path == "/open-apis/im/v1/chats/batch_query":
            return {
                "data": {
                    "items": [
                        {
                            "chat_id": "oc_chat_1",
                            "name": "Backend Chat",
                            "chat_mode": "group",
                        }
                    ]
                }
            }
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.im.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_search_messages(
            {
                "query": "matched",
                "sender_ids": ["ou_a"],
                "relative_time": "last_3_days",
            }
        )
    )
    assert payload["messages"][0]["chat_id"] == "oc_chat_1"
    assert payload["messages"][0]["content"] == "matched"
    assert payload["messages"][0]["chat_type"] == "group"
    assert payload["messages"][0]["chat_name"] == "Backend Chat"
    search_call = calls[0]
    assert search_call[0] == "POST"
    assert search_call[1] == "/open-apis/search/v2/message"
    assert search_call[2]["json_body"]["from_ids"] == ["ou_a"]


def test_feishu_im_search_messages_handler_enriches_p2p_partner(monkeypatch):
    from tools.feishu.im import _handle_search_messages

    def _fake_request(method, path, **kwargs):
        if path == "/open-apis/search/v2/message":
            return {"data": {"items": ["om_p2p"], "has_more": False, "page_token": None}}
        if path.startswith("/open-apis/im/v1/messages/mget?"):
            return {
                "data": {
                    "items": [
                        {
                            "message_id": "om_p2p",
                            "chat_id": "oc_p2p_1",
                            "msg_type": "text",
                            "body": {"content": '{"text":"hi partner"}'},
                            "sender": {"id": "ou_me", "sender_type": "user"},
                            "create_time": "1710000000000",
                        }
                    ]
                }
            }
        if path == "/open-apis/im/v1/chats/batch_query":
            return {
                "data": {
                    "items": [
                        {
                            "chat_id": "oc_p2p_1",
                            "name": "",
                            "chat_mode": "p2p",
                            "p2p_target_id": "ou_partner",
                        }
                    ]
                }
            }
        if path == "/open-apis/contact/v3/users/basic_batch":
            return {
                "data": {
                    "users": [
                        {"user_id": "ou_partner", "name": "Alice"},
                    ]
                }
            }
        raise AssertionError(path)

    monkeypatch.setattr("tools.feishu.im.feishu_api_request", _fake_request)
    payload = json.loads(_handle_search_messages({"query": "partner"}))
    assert payload["messages"][0]["chat_type"] == "p2p"
    assert payload["messages"][0]["chat_name"] == "Alice"
    assert payload["messages"][0]["chat_partner"] == {"open_id": "ou_partner", "name": "Alice"}


def test_feishu_im_fetch_resource_handler(monkeypatch):
    from tools.feishu.im import _handle_fetch_resource

    monkeypatch.setattr(
        "tools.feishu.im.feishu_api_request_bytes",
        lambda *a, **kw: (b"abc", {"content-type": "text/plain"}),
    )
    payload = json.loads(
        _handle_fetch_resource({"message_id": "om_1", "file_key": "file_1", "type": "file"})
    )
    assert payload["size_bytes"] == 3
    assert payload["content_type"] == "text/plain"


def test_feishu_im_message_send_handler(monkeypatch):
    from tools.feishu.im import _handle_im_message

    monkeypatch.setattr(
        "tools.feishu.im.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "message_id": "om_new",
                "chat_id": "oc_chat_1",
                "create_time": "1710000000000",
            }
        },
    )
    payload = json.loads(
        _handle_im_message(
            {
                "action": "send",
                "receive_id_type": "chat_id",
                "receive_id": "oc_chat_1",
                "msg_type": "text",
                "content": '{"text":"hello"}',
            }
        )
    )
    assert payload["message_id"] == "om_new"
    assert payload["chat_id"] == "oc_chat_1"


def test_feishu_im_message_reply_handler(monkeypatch):
    from tools.feishu.im import _handle_im_message

    monkeypatch.setattr(
        "tools.feishu.im.feishu_api_request",
        lambda *a, **kw: {
            "data": {
                "message_id": "om_reply",
                "chat_id": "oc_chat_1",
                "create_time": "1710000000000",
            }
        },
    )
    payload = json.loads(
        _handle_im_message(
            {
                "action": "reply",
                "message_id": "om_parent",
                "msg_type": "text",
                "content": '{"text":"roger"}',
                "reply_in_thread": True,
            }
        )
    )
    assert payload["message_id"] == "om_reply"
    assert payload["chat_id"] == "oc_chat_1"


def test_feishu_im_message_send_requires_receive_id(monkeypatch):
    from tools.feishu.im import _handle_im_message

    payload = json.loads(_handle_im_message({"action": "send", "msg_type": "text", "content": '{"text":"hello"}'}))
    assert "receive_id" in payload["error"]


def test_feishu_im_message_reply_requires_message_id(monkeypatch):
    from tools.feishu.im import _handle_im_message

    payload = json.loads(_handle_im_message({"action": "reply", "msg_type": "text", "content": '{"text":"hello"}'}))
    assert "message_id" in payload["error"]


def test_feishu_fetch_doc_handler(monkeypatch):
    from tools.feishu.docs import _handle_fetch_doc

    def _fake_request(method, path, **_kwargs):
        if path.endswith("/raw_content"):
            return {"data": {"content": "alpha\nbeta\ngamma"}}
        return {"data": {"document": {"title": "Test Doc"}}}

    monkeypatch.setattr("tools.feishu.docs.feishu_api_request", _fake_request)
    payload = json.loads(_handle_fetch_doc({"doc_id": "doxcn1234567890", "offset": 6, "limit": 4}))
    assert payload["title"] == "Test Doc"
    assert payload["content"] == "beta"
    assert payload["has_more"] is True


def test_feishu_create_doc_handler(monkeypatch):
    from tools.feishu.docs import _handle_create_doc

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if method == "POST" and path == "/open-apis/docx/v1/documents":
            return {"data": {"document": {"document_id": "doxcn_new"}}}
        if method == "GET" and path.endswith("/children"):
            return {"data": {"items": [], "has_more": False}}
        return {"data": {}}

    monkeypatch.setattr("tools.feishu.docs.feishu_api_request", _fake_request)
    payload = json.loads(_handle_create_doc({"title": "Hello", "markdown": "# Title\nBody"}))
    assert payload["document_id"] == "doxcn_new"
    assert payload["initialized"] is True
    assert payload["write_result"]["block_count"] == 2
    assert payload["write_result"]["block_kinds"] == ["heading", "paragraph"]
    assert any(item[0] == "POST" and item[1].endswith("/children") for item in calls)


def test_feishu_create_doc_rejects_conflicting_targets(monkeypatch):
    from tools.feishu.docs import _handle_create_doc

    payload = json.loads(_handle_create_doc({"title": "Hello", "folder_token": "fld_1", "wiki_node": "wik_1"}))
    assert "mutually exclusive" in payload["error"]


def test_feishu_create_doc_returns_async_task_for_large_markdown(monkeypatch):
    from tools.feishu.docs import _handle_create_doc

    monkeypatch.setattr("tools.feishu.docs._ASYNC_DOC_MARKDOWN_THRESHOLD", 1)
    monkeypatch.setattr(
        "tools.feishu.docs.feishu_api_request",
        lambda method, path, **kwargs: {"data": {"document": {"document_id": "doxcn_async"}}},
    )
    monkeypatch.setattr("tools.feishu.docs._queue_async_doc_task", lambda *a, **kw: "doc_task_123456abcdef")

    payload = json.loads(_handle_create_doc({"title": "Hello", "markdown": "Large body"}))
    assert payload["task_id"] == "doc_task_123456abcdef"
    assert payload["document_id"] == "doxcn_async"


def test_feishu_create_doc_polls_task_id(monkeypatch):
    from tools.feishu.docs import _handle_create_doc

    monkeypatch.setattr(
        "tools.feishu.docs._get_async_doc_task_status",
        lambda task_id: {"task_id": task_id, "status": "success", "result": {"paragraph_count": 3}},
    )

    payload = json.loads(_handle_create_doc({"task_id": "doc_task_123456abcdef"}))
    assert payload["task_id"] == "doc_task_123456abcdef"
    assert payload["status"] == "success"


def test_feishu_update_doc_handler_replace_range(monkeypatch):
    from tools.feishu.docs import _handle_update_doc

    calls = []

    def _fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if method == "GET" and path.endswith("/raw_content"):
            return {"data": {"content": "Intro\n## Scope\nOld Content\n## End\nBye"}}
        if method == "GET" and path.endswith("/children"):
            return {"data": {"items": [{"block_id": "blk_1"}], "has_more": False}}
        return {"data": {}}

    monkeypatch.setattr("tools.feishu.docs.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_update_doc(
            {
                "doc_id": "doxcn1234567890",
                "mode": "replace_range",
                "markdown": "New Content",
                "selection_with_ellipsis": "## Scope...## End",
            }
        )
    )
    assert payload["updated"] is True
    assert any(item[0] == "DELETE" for item in calls)
    assert any(item[0] == "POST" and item[1].endswith("/children") for item in calls)


def test_feishu_doc_markdown_blocks_preserve_structural_cues():
    from tools.feishu.docs import _normalize_markdown_to_blocks, _render_doc_block_text

    blocks = _normalize_markdown_to_blocks(
        "# Title\n"
        "Intro line\n\n"
        "- Alpha\n"
        "1. Beta\n"
        "> Quote\n\n"
        "```python\n"
        "print('hi')\n"
        "```"
    )

    assert [block["kind"] for block in blocks] == ["heading", "paragraph", "list", "list", "quote", "code"]
    assert [_render_doc_block_text(block) for block in blocks] == [
        "# Title",
        "Intro line",
        "• Alpha",
        "1. Beta",
        "> Quote",
        "```\n[python]\nprint('hi')\n```",
    ]


def test_feishu_doc_markdown_blocks_extract_tables():
    from tools.feishu.docs import _normalize_markdown_to_blocks, _render_doc_block_text

    blocks = _normalize_markdown_to_blocks(
        "Intro\n\n| name | value |\n| --- | --- |\n| a | 1 |\n| b | 2 |\n\nTail"
    )

    assert [block["kind"] for block in blocks] == ["paragraph", "table", "paragraph"]
    assert _render_doc_block_text(blocks[1]) == "```md\n| name | value |\n| --- | --- |\n| a | 1 |\n| b | 2 |\n```"


def test_feishu_create_doc_renders_list_quote_and_code_blocks(monkeypatch):
    from tools.feishu.docs import _handle_create_doc

    create_children_payloads = []

    def _fake_request(method, path, **kwargs):
        if method == "POST" and path == "/open-apis/docx/v1/documents":
            return {"data": {"document": {"document_id": "doxcn_new"}}}
        if method == "GET" and path.endswith("/children"):
            return {"data": {"items": [], "has_more": False}}
        if method == "POST" and path.endswith("/children"):
            create_children_payloads.append(kwargs.get("json_body") or {})
            return {"data": {}}
        return {"data": {}}

    monkeypatch.setattr("tools.feishu.docs.feishu_api_request", _fake_request)

    payload = json.loads(
        _handle_create_doc(
            {
                "title": "Hello",
                "markdown": "# **Title**\n- *Alpha*\n> ~~Quote~~\n```python\nprint('hi')\n```",
            }
        )
    )

    children = create_children_payloads[0]["children"]
    assert children[0]["block_type"] == 3
    assert children[0]["heading1"]["elements"][0]["text_run"]["content"] == "Title"
    assert children[0]["heading1"]["elements"][0]["text_run"]["text_element_style"] == {"bold": True}
    assert children[1]["block_type"] == 12
    assert children[1]["bullet"]["elements"][0]["text_run"]["content"] == "Alpha"
    assert children[1]["bullet"]["elements"][0]["text_run"]["text_element_style"] == {"italic": True}
    assert children[2]["block_type"] == 15
    assert children[2]["quote"]["elements"][0]["text_run"]["content"] == "Quote"
    assert children[2]["quote"]["elements"][0]["text_run"]["text_element_style"] == {"strikethrough": True}
    assert children[3]["block_type"] == 14
    assert children[3]["code"]["elements"][0]["text_run"]["content"] == "print('hi')"
    assert children[3]["code"]["style"]["language"] == "python"
    assert children[3]["code"]["style"]["wrap"] is True
    assert payload["write_result"]["block_kinds"] == ["heading", "list", "quote", "code"]


def test_feishu_doc_build_native_blocks_supports_ordered_and_inline_code():
    from tools.feishu.docs import _build_native_doc_block

    ordered_block = _build_native_doc_block(
        {
            "kind": "list",
            "ordered": True,
            "marker": "1.",
            "source_text": "`one`",
            "text": "one",
        }
    )
    assert ordered_block["block_type"] == 13
    assert ordered_block["ordered"]["elements"][0]["text_run"]["content"] == "one"
    assert ordered_block["ordered"]["elements"][0]["text_run"]["text_element_style"] == {"inline_code": True}


def test_feishu_doc_build_text_elements_preserves_links():
    from tools.feishu.docs import _build_text_elements

    elements = _build_text_elements("See [Spec](https://example.com/spec) now")

    assert elements[0]["text_run"]["content"] == "See "
    assert elements[1]["text_run"]["content"] == "Spec"
    assert elements[1]["text_run"]["text_element_style"] == {"link": {"url": "https://example.com/spec"}}
    assert elements[2]["text_run"]["content"] == " now"


def test_feishu_doc_build_native_blocks_preserves_nested_list_indent():
    from tools.feishu.docs import _build_native_doc_block

    nested_block = _build_native_doc_block(
        {
            "kind": "list",
            "ordered": False,
            "indent": 2,
            "source_text": "Nested item",
            "text": "Nested item",
        }
    )

    assert nested_block["block_type"] == 12
    assert nested_block["bullet"]["elements"][0]["text_run"]["content"] == "    "
    assert nested_block["bullet"]["elements"][1]["text_run"]["content"] == "Nested item"


def test_feishu_doc_build_native_blocks_downgrades_table_to_markdown_code():
    from tools.feishu.docs import _build_native_doc_block

    table_block = _build_native_doc_block(
        {
            "kind": "table",
            "text": "| name | value |\n| --- | --- |\n| a | 1 |",
        }
    )

    assert table_block["block_type"] == 14
    assert table_block["code"]["style"]["language"] == "markdown"
    assert table_block["code"]["elements"][0]["text_run"]["content"].startswith("| name | value |")


def test_feishu_doc_build_native_blocks_omits_unknown_code_language():
    from tools.feishu.docs import _build_native_doc_block

    code_block = _build_native_doc_block({"kind": "code", "text": "echo ok", "language": "foobar"})

    assert code_block["block_type"] == 14
    assert "language" not in code_block["code"]["style"]


def test_feishu_doc_build_native_blocks_normalizes_code_language_alias():
    from tools.feishu.docs import _build_native_doc_block

    code_block = _build_native_doc_block({"kind": "code", "text": "print('hi')", "language": "py"})

    assert code_block["code"]["style"]["language"] == "python"


def test_feishu_update_doc_reports_title_update_note(monkeypatch):
    from tools.feishu.docs import _handle_update_doc

    def _fake_request(method, path, **kwargs):
        if method == "GET" and path.endswith("/raw_content"):
            return {"data": {"content": "Intro"}}
        if method == "GET" and path.endswith("/children"):
            return {"data": {"items": [{"block_id": "blk_1"}], "has_more": False}}
        return {"data": {}}

    monkeypatch.setattr("tools.feishu.docs.feishu_api_request", _fake_request)
    payload = json.loads(
        _handle_update_doc(
            {
                "doc_id": "doxcn1234567890",
                "mode": "overwrite",
                "markdown": "Body",
                "new_title": "Renamed",
            }
        )
    )
    assert payload["updated"] is True
    assert "title_update_note" in payload


def test_feishu_update_doc_returns_async_task_for_large_markdown(monkeypatch):
    from tools.feishu.docs import _handle_update_doc

    def _fake_request(method, path, **kwargs):
        if method == "GET" and path.endswith("/raw_content"):
            return {"data": {"content": "Intro"}}
        return {"data": {}}

    monkeypatch.setattr("tools.feishu.docs._ASYNC_DOC_MARKDOWN_THRESHOLD", 1)
    monkeypatch.setattr("tools.feishu.docs.feishu_api_request", _fake_request)
    monkeypatch.setattr("tools.feishu.docs._queue_async_doc_task", lambda *a, **kw: "doc_task_fedcba654321")

    payload = json.loads(
        _handle_update_doc({"doc_id": "doxcn1234567890", "mode": "overwrite", "markdown": "Large body"})
    )
    assert payload["task_id"] == "doc_task_fedcba654321"
    assert payload["document_id"] == "doxcn1234567890"
    assert payload["mode"] == "overwrite"


def test_feishu_update_doc_polls_task_id(monkeypatch):
    from tools.feishu.docs import _handle_update_doc

    monkeypatch.setattr(
        "tools.feishu.docs._get_async_doc_task_status",
        lambda task_id: {"task_id": task_id, "status": "running"},
    )

    payload = json.loads(_handle_update_doc({"task_id": "doc_task_fedcba654321"}))
    assert payload["task_id"] == "doc_task_fedcba654321"
    assert payload["status"] == "running"


def test_feishu_update_doc_requires_mode(monkeypatch):
    from tools.feishu.docs import _handle_update_doc

    payload = json.loads(_handle_update_doc({"doc_id": "doxcn1234567890"}))
    assert "Missing required parameter: mode" in payload["error"]


def test_feishu_ask_user_question_uses_active_adapter(monkeypatch):
    from tools.feishu.ask_user_question import _handle_ask_user_question

    adapter = SimpleNamespace(
        send_question_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_1",
                raw_response={"question_id": "fq_123"},
            )
        )
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")

    try:
        payload = json.loads(
            _handle_ask_user_question(
                {
                    "header": "Need Input",
                    "question": "Which environment?",
                    "options": ["staging", "prod"],
                }
            )
        )
        assert payload["status"] == "pending"
        assert payload["question_id"] == "fq_123"
        adapter.send_question_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_ask_user_question_rejects_too_many_options(monkeypatch):
    from tools.feishu.ask_user_question import _handle_ask_user_question

    payload = json.loads(
        _handle_ask_user_question({"question": "Which one?", "options": ["1", "2", "3", "4", "5", "6"]})
    )
    assert "at most 5 options" in payload["error"]


def test_feishu_oauth_uses_active_adapter(monkeypatch):
    from tools.feishu.oauth import _handle_feishu_oauth

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth",
                raw_response={"request_id": "fo_123"},
            )
        )
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(_handle_feishu_oauth({"action": "authorize", "scopes": ["contact:user.base:readonly"]}))
        assert payload["status"] == "pending"
        assert payload["request_id"] == "fo_123"
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_oauth_requires_scopes(monkeypatch):
    from tools.feishu.oauth import _handle_feishu_oauth

    payload = json.loads(_handle_feishu_oauth({"action": "authorize", "scopes": []}))
    assert "scopes" in payload["error"]


def test_feishu_oauth_status_handler(monkeypatch):
    from tools.feishu.oauth import _handle_feishu_oauth

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": True,
            "granted_scopes": ["contact:user.base:readonly"],
            "requested_scopes": list(scopes or []),
            "missing_scopes": [],
            "updated_at": 123.0,
            "updated_by": "ou_user_1",
            "source": "interactive_confirm",
        }
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(_handle_feishu_oauth({"action": "status", "scopes": ["contact:user.base:readonly"]}))
        assert payload["status"] == "authorized"
        assert payload["authorized"] is True
        assert payload["granted_scopes"] == ["contact:user.base:readonly"]
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_oauth_revoke_handler(monkeypatch):
    from tools.feishu.oauth import _handle_feishu_oauth

    adapter = SimpleNamespace(
        revoke_authorization=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": [],
            "missing_scopes": [],
            "updated_at": None,
            "updated_by": "",
            "source": "",
        }
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(_handle_feishu_oauth({"action": "revoke", "scopes": ["contact:user.base:readonly"]}))
        assert payload["status"] == "revoked"
        assert payload["authorized"] is False
        assert payload["remaining_scopes"] == []
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_scope_mapping_matches_official_granularity():
    from tools.feishu.scopes import get_required_scopes

    assert get_required_scopes("feishu_calendar_event", "delete") == ["calendar:calendar.event:delete"]
    assert get_required_scopes("feishu_drive_file", "delete") == ["space:document:delete"]
    assert get_required_scopes("feishu_im_user_message", "send") == ["im:message", "im:message.send_as_user"]
    assert "search:message" in get_required_scopes("feishu_im_user_search_messages", "default")


def test_feishu_drive_auto_auth_pending(monkeypatch):
    from tools.feishu.drive import _handle_drive_file

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_drive",
                raw_response={"request_id": "fo_drive_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(_handle_drive_file({"action": "delete", "file_token": "file_1", "type": "file"}))
        assert payload["status"] == "pending_authorization"
        assert payload["tool"] == "feishu_drive_file"
        assert payload["action"] == "delete"
        assert payload["missing_scopes"] == ["space:document:delete"]
        assert payload["replay_id"].startswith("fr_")
        pending = getattr(adapter, "_pending_tool_replays")
        assert pending[payload["replay_id"]]["tool_name"] == "feishu_drive_file"
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_calendar_event_auto_auth_pending(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_calendar",
                raw_response={"request_id": "fo_calendar_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(
            _handle_calendar_event(
                {
                    "action": "reply",
                    "calendar_id": "cal_1",
                    "event_id": "evt_1",
                    "rsvp_status": "accept",
                }
            )
        )
        assert payload["status"] == "pending_authorization"
        assert payload["tool"] == "feishu_calendar_event"
        assert payload["action"] == "reply"
        assert payload["missing_scopes"] == ["calendar:calendar.event:reply"]
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_im_send_auto_auth_marks_sensitive_scope(monkeypatch):
    from tools.feishu.im import _handle_im_message

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_im",
                raw_response={"request_id": "fo_im_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(
            _handle_im_message(
                {
                    "action": "send",
                    "receive_id_type": "chat_id",
                    "receive_id": "oc_chat_2",
                    "msg_type": "text",
                    "content": json.dumps({"text": "hello"}),
                }
            )
        )
        assert payload["status"] == "pending_authorization"
        assert payload["safe_scopes"] == ["im:message"]
        assert payload["sensitive_scopes"] == ["im:message.send_as_user"]
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_oauth_resolves_scopes_from_tool_action(monkeypatch):
    from tools.feishu.oauth import _handle_feishu_oauth

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_oauth_tool",
                raw_response={"request_id": "fo_tool_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(
            _handle_feishu_oauth(
                {
                    "action": "authorize",
                    "tool_name": "feishu_calendar_event",
                    "action_name": "delete",
                }
            )
        )
        assert payload["status"] == "pending"
        assert payload["scopes"] == ["calendar:calendar.event:delete"]
        assert payload["targets"] == [{"tool_name": "feishu_calendar_event", "action": "delete"}]
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_oauth_batch_resolves_multiple_tool_actions(monkeypatch):
    from tools.feishu.oauth import _handle_feishu_oauth_batch

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_batch_tool",
                raw_response={"request_id": "fo_batch_tool_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(
            _handle_feishu_oauth_batch(
                {
                    "tool_actions": [
                        {"tool_name": "feishu_drive_file", "action": "delete"},
                        {"tool_name": "feishu_im_user_message", "action": "send"},
                    ]
                }
            )
        )
        assert payload["status"] == "pending"
        assert payload["requested_scopes"] == ["space:document:delete", "im:message", "im:message.send_as_user"]
        assert payload["targets"] == [
            {"tool_name": "feishu_drive_file", "action": "delete"},
            {"tool_name": "feishu_im_user_message", "action": "send"},
        ]
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_tasklist_auto_auth_pending(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_tasklist",
                raw_response={"request_id": "fo_tasklist_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(_handle_tasklist({"action": "list"}))
        assert payload["status"] == "pending_authorization"
        assert payload["tool"] == "feishu_task_tasklist"
        assert payload["action"] == "list"
        assert payload["missing_scopes"] == ["task:tasklist:read", "task:tasklist:write"]
        assert payload["replay_id"].startswith("fr_")
        pending = getattr(adapter, "_pending_tool_replays")
        assert pending[payload["replay_id"]]["tool_name"] == "feishu_task_tasklist"
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_chat_members_auto_auth_pending(monkeypatch):
    from tools.feishu.chat_members import _handle_chat_members

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": [],
            "requested_scopes": list(scopes or []),
            "missing_scopes": list(scopes or []),
            "updated_at": None,
            "updated_by": "",
            "source": "",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_chat_members",
                raw_response={"request_id": "fo_chat_members_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")

    try:
        payload = json.loads(_handle_chat_members({"chat_id": "oc_chat_2"}))
        assert payload["status"] == "pending_authorization"
        assert payload["tool"] == "feishu_chat_members"
        assert payload["missing_scopes"] == ["im:chat.members:read"]
        assert payload["replay_id"].startswith("fr_")
        pending = getattr(adapter, "_pending_tool_replays")
        assert pending[payload["replay_id"]]["tool_name"] == "feishu_chat_members"
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_chat_members_handles_user_scope_error_after_api_response(monkeypatch):
    from tools.feishu.chat_members import _handle_chat_members
    from tools.feishu.client import FeishuAPIError

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": True,
            "granted_scopes": list(scopes or []),
            "requested_scopes": list(scopes or []),
            "missing_scopes": [],
            "updated_at": 1.0,
            "updated_by": "ou_user_1",
            "source": "interactive_confirm",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_retry_chat_members",
                raw_response={"request_id": "fo_retry_chat_members_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")
    monkeypatch.setattr(
        "tools.feishu.chat_members.feishu_api_request",
        lambda *a, **kw: (_ for _ in ()).throw(
            FeishuAPIError(
                code=99991679,
                message="missing user scopes [im:chat.members:read]",
                missing_scopes=["im:chat.members:read"],
            )
        ),
    )

    try:
        payload = json.loads(_handle_chat_members({"chat_id": "oc_chat_2"}))
        assert payload["status"] == "pending_authorization"
        assert payload["missing_scopes"] == ["im:chat.members:read"]
        assert payload["replay_id"].startswith("fr_")
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_drive_handles_user_scope_error_after_api_response(monkeypatch):
    from tools.feishu.client import FeishuAPIError
    from tools.feishu.drive import _handle_drive_file

    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": True,
            "granted_scopes": list(scopes or []),
            "requested_scopes": list(scopes or []),
            "missing_scopes": [],
            "updated_at": 1.0,
            "updated_by": "ou_user_1",
            "source": "interactive_confirm",
        },
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_auth_retry_drive",
                raw_response={"request_id": "fo_retry_drive_1"},
            )
        ),
    )

    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_user_1")
    monkeypatch.setattr(
        "tools.feishu.drive.feishu_api_request",
        lambda *a, **kw: (_ for _ in ()).throw(
            FeishuAPIError(
                code=99991679,
                message="missing user scopes [space:document:delete]",
                missing_scopes=["space:document:delete"],
            )
        ),
    )

    try:
        payload = json.loads(_handle_drive_file({"action": "delete", "file_token": "file_1", "type": "file"}))
        assert payload["status"] == "pending_authorization"
        assert payload["missing_scopes"] == ["space:document:delete"]
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_calendar_handles_app_scope_missing(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event
    from tools.feishu.client import FeishuAPIError

    monkeypatch.setattr("tools.feishu.scopes.get_app_granted_scopes", lambda: ["calendar:calendar.event:read"])
    monkeypatch.setattr(
        "tools.feishu.client.get_app_info",
        lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
    )
    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None, account_id=None: {
            "authorized": True,
            "granted_scopes": list(scopes or []),
            "requested_scopes": list(scopes or []),
            "missing_scopes": [],
            "updated_at": 1.0,
            "updated_by": "ou_owner",
            "source": "interactive_confirm",
        },
        send_app_scope_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_app_scope_1",
                raw_response={"request_id": "fas_1"},
            )
        )
    )
    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_owner")
    monkeypatch.setenv("HERMES_SESSION_ACCOUNT_ID", "feishu-cn")
    monkeypatch.setattr(
        "tools.feishu.calendar_event.feishu_api_request",
        lambda *a, **kw: (_ for _ in ()).throw(
            FeishuAPIError(
                code=99991672,
                message="app missing scopes [calendar:calendar.event:delete]",
                missing_scopes=["calendar:calendar.event:delete"],
            )
        ),
    )
    payload = json.loads(
        _handle_calendar_event(
            {
                "action": "delete",
                "calendar_id": "cal_1",
                "event_id": "evt_1",
            }
        )
    )
    try:
        assert payload["error_type"] == "app_scope_missing"
        assert payload["missing_app_scopes"] == ["calendar:calendar.event:delete"]
        assert payload["owner_open_id"] == "ou_owner"
        assert payload["requester_is_owner"] is True
        assert payload["resolution_command"] == "/feishu auth batch"
        assert payload["account_id"] == "feishu-cn"
        assert payload["request_created"] is True
        assert payload["request_id"] == "fas_1"
        assert payload["message_id"] == "msg_app_scope_1"
        assert payload["replay_id"].startswith("fr_")
        adapter.send_app_scope_request_card.assert_awaited_once()
        kwargs = adapter.send_app_scope_request_card.await_args.kwargs
        assert kwargs["metadata"]["tool_name"] == "feishu_calendar_event"
        assert kwargs["metadata"]["action"] == "delete"
        assert kwargs["metadata"]["owner_open_id"] == "ou_owner"
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_calendar_app_scope_missing_for_non_owner_has_owner_hint(monkeypatch):
    from tools.feishu.calendar_event import _handle_calendar_event
    from tools.feishu.client import FeishuAPIError

    monkeypatch.setattr("tools.feishu.scopes.get_app_granted_scopes", lambda: ["calendar:calendar.event:read"])
    monkeypatch.setattr(
        "tools.feishu.client.get_app_info",
        lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
    )
    adapter = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None, account_id=None: {
            "authorized": True,
            "granted_scopes": list(scopes or []),
            "requested_scopes": list(scopes or []),
            "missing_scopes": [],
            "updated_at": 1.0,
            "updated_by": "ou_owner",
            "source": "interactive_confirm",
        },
        send_app_scope_request_card=AsyncMock(
            return_value=SendResult(
                success=True,
                message_id="msg_app_scope_2",
                raw_response={"request_id": "fas_2"},
            )
        )
    )
    register_adapter(Platform.FEISHU, adapter)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "feishu")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "oc_chat_1")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "ou_member")
    monkeypatch.setenv("HERMES_SESSION_ACCOUNT_ID", "feishu-cn")
    monkeypatch.setattr(
        "tools.feishu.calendar_event.feishu_api_request",
        lambda *a, **kw: (_ for _ in ()).throw(
            FeishuAPIError(
                code=99991672,
                message="app missing scopes [calendar:calendar.event:delete]",
                missing_scopes=["calendar:calendar.event:delete"],
            )
        ),
    )

    payload = json.loads(
        _handle_calendar_event(
            {
                "action": "delete",
                "calendar_id": "cal_1",
                "event_id": "evt_1",
            }
        )
    )
    try:
        assert payload["error_type"] == "app_scope_missing"
        assert payload["owner_open_id"] == "ou_owner"
        assert payload["requester_open_id"] == "ou_member"
        assert payload["requester_is_owner"] is False
        assert payload["resolution_command"] == ""
        assert "Ask the Feishu app owner" in payload["resolution_hint"]
        assert payload["request_created"] is True
        assert payload["request_id"] == "fas_2"
        adapter.send_app_scope_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


def test_feishu_adapter_promotes_app_scope_request_to_user_oauth(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingAppScopeRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._pending_app_scope_requests["fas_1"] = FeishuPendingAppScopeRequest(
        request_id="fas_1",
        chat_id="oc_chat_1",
        message_id="msg_app_scope_1",
        scopes=["calendar:calendar.event:delete"],
        reason="App scopes are required.",
        title="Feishu App Authorization Required",
        owner_open_id="ou_owner",
        requester_open_id="ou_requester",
        thread_id="omt_1",
        account_id="feishu-cn",
        tool_name="feishu_calendar_event",
        tool_action="delete",
        replay_id="fr_1",
        replay_ids=["fr_1"],
    )
    adapter.record_authorization_grant(
        user_open_id="ou_requester",
        scopes=["calendar:calendar:read"],
        account_id="feishu-cn",
    )
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_owner", "user_name": "Alice", "user_id_alt": "ou_owner"}
    )
    adapter._update_interactive_card = AsyncMock()
    adapter.send_oauth_request_card = AsyncMock(return_value=SendResult(success=True, message_id="msg_oauth_1"))
    monkeypatch.setattr(
        "tools.feishu.client.get_app_granted_scopes_by_token_type",
        lambda *args, **kwargs: ["calendar:calendar:read", "calendar:calendar.event:delete"],
    )

    event = SimpleNamespace(
        token="tok_app_scope_1",
        context=SimpleNamespace(open_chat_id="oc_chat_1"),
        operator=SimpleNamespace(open_id="ou_owner"),
        action=SimpleNamespace(tag="button", value={"hermes_action": "complete_app_scope_request", "request_id": "fas_1"}),
    )

    import asyncio

    asyncio.run(adapter._handle_card_action_event(SimpleNamespace(event=event)))

    assert "fas_1" not in adapter._pending_app_scope_requests
    adapter._update_interactive_card.assert_awaited_once()
    adapter.send_oauth_request_card.assert_awaited_once()
    kwargs = adapter.send_oauth_request_card.await_args.kwargs
    assert kwargs["chat_id"] == "oc_chat_1"
    assert kwargs["scopes"] == ["calendar:calendar.event:delete"]
    assert kwargs["metadata"]["requester_open_id"] == "ou_requester"
    assert kwargs["metadata"]["replay_id"] == "fr_1"
    assert kwargs["metadata"]["replay_ids"] == ["fr_1"]


def test_feishu_adapter_merges_pending_app_scope_request_replay_ids(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingAppScopeRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._client = object()
    adapter._pending_app_scope_requests["fas_existing"] = FeishuPendingAppScopeRequest(
        request_id="fas_existing",
        chat_id="oc_chat_1",
        message_id="msg_app_scope_1",
        scopes=["calendar:calendar.event:delete"],
        reason="Need app scopes.",
        title="App Auth",
        owner_open_id="ou_owner",
        requester_open_id="ou_user_1",
        thread_id="omt_1",
        account_id="feishu-cn",
        tool_name="feishu_calendar_event",
        tool_action="delete",
        replay_id="fr_existing",
        replay_ids=["fr_existing"],
    )
    adapter._update_interactive_card = AsyncMock()

    import asyncio

    result = asyncio.run(
        adapter.send_app_scope_request_card(
            chat_id="oc_chat_1",
            scopes=["im:message:send_as_bot"],
            reason="Need more app scopes.",
            title="App Auth",
            metadata={
                "thread_id": "omt_1",
                "account_id": "feishu-cn",
                "owner_open_id": "ou_owner",
                "requester_open_id": "ou_user_1",
                "tool_name": "feishu_im_user_message",
                "action": "send",
                "replay_id": "fr_later",
            },
        )
    )

    assert result.success is True
    state = adapter._pending_app_scope_requests["fas_existing"]
    assert state.replay_id == "fr_existing"
    assert state.replay_ids == ["fr_existing", "fr_later"]
    adapter._update_interactive_card.assert_awaited_once()


def test_feishu_adapter_rejects_app_scope_completion_before_scopes_exist(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingAppScopeRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._pending_app_scope_requests["fas_1"] = FeishuPendingAppScopeRequest(
        request_id="fas_1",
        chat_id="oc_chat_1",
        message_id="msg_app_scope_1",
        scopes=["calendar:calendar.event:delete"],
        reason="App scopes are required.",
        title="Feishu App Authorization Required",
        owner_open_id="ou_owner",
        requester_open_id="ou_requester",
        account_id="feishu-cn",
    )
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_owner", "user_name": "Alice", "user_id_alt": "ou_owner"}
    )
    adapter._update_interactive_card = AsyncMock()
    adapter.send_oauth_request_card = AsyncMock()
    monkeypatch.setattr(
        "tools.feishu.client.get_app_granted_scopes_by_token_type",
        lambda *args, **kwargs: ["calendar:calendar:read"],
    )

    event = SimpleNamespace(
        token="tok_app_scope_2",
        context=SimpleNamespace(open_chat_id="oc_chat_1"),
        operator=SimpleNamespace(open_id="ou_owner"),
        action=SimpleNamespace(tag="button", value={"hermes_action": "complete_app_scope_request", "request_id": "fas_1"}),
    )

    import asyncio

    asyncio.run(adapter._handle_card_action_event(SimpleNamespace(event=event)))

    assert "fas_1" in adapter._pending_app_scope_requests
    adapter._update_interactive_card.assert_awaited_once()
    adapter.send_oauth_request_card.assert_not_awaited()


def test_feishu_adapter_merges_pending_oauth_request(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingOAuthRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._client = object()
    adapter._pending_oauth_requests["fo_existing"] = FeishuPendingOAuthRequest(
        request_id="fo_existing",
        chat_id="oc_chat_1",
        message_id="msg_auth_1",
        scopes=["contact:user.base:readonly"],
        reason="Need basic profile.",
        title="Auth",
        thread_id="omt_1",
        requester_open_id="ou_user_1",
        account_id="feishu-cn",
        tool_name="feishu_get_user",
        tool_action="default",
        replay_id="fr_existing",
        replay_ids=["fr_existing"],
    )
    adapter._update_interactive_card = AsyncMock()

    import asyncio

    result = asyncio.run(
        adapter.send_oauth_request_card(
            chat_id="oc_chat_1",
            scopes=["contact:user.base:readonly", "calendar:calendar.readonly"],
            reason="Need calendar too.",
            title="Auth",
            metadata={
                "thread_id": "omt_1",
                "account_id": "feishu-cn",
                "requester_open_id": "ou_user_1",
                "tool_name": "feishu_calendar_event",
                "action": "get",
                "replay_id": "fr_later",
            },
        )
    )

    assert result.success is True
    assert result.raw_response["request_id"] == "fo_existing"
    assert result.raw_response["merged"] is True
    state = adapter._pending_oauth_requests["fo_existing"]
    assert state.scopes == ["contact:user.base:readonly", "calendar:calendar.readonly"]
    assert state.tool_name == "feishu_get_user"
    assert state.tool_action == "default"
    assert state.replay_id == "fr_existing"
    assert state.replay_ids == ["fr_existing", "fr_later"]
    adapter._update_interactive_card.assert_awaited_once()
    assert adapter._update_interactive_card.await_args.kwargs["account_id"] == "feishu-cn"


def test_feishu_adapter_records_oauth_completion(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingOAuthRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._pending_oauth_requests["fo_1"] = FeishuPendingOAuthRequest(
        request_id="fo_1",
        chat_id="oc_chat_1",
        message_id="msg_auth_1",
        scopes=["contact:user.base:readonly"],
        reason="Need basic profile.",
        title="Auth",
        requester_open_id="ou_requester",
        account_id="feishu-cn",
        tool_name="feishu_drive_file",
        tool_action="delete",
        replay_id="fr_1",
        replay_ids=["fr_1"],
    )
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_operator", "user_name": "Alice", "user_id_alt": "ou_operator"}
    )
    adapter.get_chat_info = AsyncMock(return_value={"name": "Backend Chat", "chat_type": "group"})
    adapter._update_interactive_card = AsyncMock()
    adapter._handle_message_with_guards = AsyncMock()
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="msg_result"))
    adapter._pending_tool_replays = {
        "fr_1": {
            "tool_name": "feishu_drive_file",
            "args": {"action": "delete", "file_token": "file_1", "type": "file"},
            "chat_id": "oc_chat_1",
            "thread_id": "",
            "user_id": "ou_requester",
        }
    }
    monkeypatch.setattr(
        "tools.registry.registry.dispatch",
        lambda name, args, **kwargs: json.dumps({"success": True, "tool": name, "args": args}, ensure_ascii=False),
    )

    event = SimpleNamespace(
        token="tok_auth_1",
        context=SimpleNamespace(open_chat_id="oc_chat_1"),
        operator=SimpleNamespace(open_id="ou_requester"),
        action=SimpleNamespace(tag="button", value={"hermes_action": "complete_oauth", "request_id": "fo_1"}),
    )

    asyncio.run(adapter._handle_card_action_event(SimpleNamespace(event=event)))

    status = adapter.get_authorization_status("ou_requester", ["contact:user.base:readonly"])
    assert status["authorized"] is True
    assert status["missing_scopes"] == []
    adapter._update_interactive_card.assert_awaited_once()
    assert adapter._update_interactive_card.await_args.kwargs["account_id"] == "feishu-cn"
    adapter.send.assert_awaited_once()
    sent_text = adapter.send.await_args.args[1]
    assert "Feishu authorized tool replay completed" in sent_text
    assert '"tool_name": "feishu_drive_file"' in sent_text
    assert adapter.send.await_args.kwargs["metadata"]["account_id"] == "feishu-cn"
    adapter._handle_message_with_guards.assert_not_awaited()


def test_feishu_adapter_replays_all_merged_oauth_actions(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingOAuthRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._pending_oauth_requests["fo_1"] = FeishuPendingOAuthRequest(
        request_id="fo_1",
        chat_id="oc_chat_1",
        message_id="msg_auth_1",
        scopes=["contact:user.base:readonly", "calendar:calendar.event:delete"],
        reason="Need multiple scopes.",
        title="Auth",
        requester_open_id="ou_requester",
        account_id="feishu-cn",
        tool_name="feishu_drive_file",
        tool_action="delete",
        replay_id="fr_1",
        replay_ids=["fr_1", "fr_2"],
    )
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_operator", "user_name": "Alice", "user_id_alt": "ou_operator"}
    )
    adapter.get_chat_info = AsyncMock(return_value={"name": "Backend Chat", "chat_type": "group"})
    adapter._update_interactive_card = AsyncMock()
    adapter._handle_message_with_guards = AsyncMock()
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="msg_result"))
    adapter._pending_tool_replays = {
        "fr_1": {
            "tool_name": "feishu_drive_file",
            "args": {"action": "delete", "file_token": "file_1", "type": "file"},
            "chat_id": "oc_chat_1",
            "thread_id": "",
            "user_id": "ou_requester",
        },
        "fr_2": {
            "tool_name": "feishu_calendar_event",
            "args": {"action": "delete", "calendar_id": "cal_1", "event_id": "evt_1"},
            "chat_id": "oc_chat_1",
            "thread_id": "",
            "user_id": "ou_requester",
        },
    }
    monkeypatch.setattr(
        "tools.registry.registry.dispatch",
        lambda name, args, **kwargs: json.dumps({"success": True, "tool": name, "args": args}, ensure_ascii=False),
    )

    event = SimpleNamespace(
        token="tok_auth_multi",
        context=SimpleNamespace(open_chat_id="oc_chat_1"),
        operator=SimpleNamespace(open_id="ou_requester"),
        action=SimpleNamespace(tag="button", value={"hermes_action": "complete_oauth", "request_id": "fo_1"}),
    )

    import asyncio

    asyncio.run(adapter._handle_card_action_event(SimpleNamespace(event=event)))

    adapter.send.assert_awaited_once()
    sent_text = adapter.send.await_args.args[1]
    assert "completed" in sent_text
    assert '"tool_name": "feishu_drive_file"' in sent_text
    assert '"tool_name": "feishu_calendar_event"' in sent_text
    assert adapter._update_interactive_card.await_args.kwargs["template"] == "green"


def test_feishu_adapter_marks_oauth_card_warning_when_replay_has_issues(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingOAuthRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._pending_oauth_requests["fo_1"] = FeishuPendingOAuthRequest(
        request_id="fo_1",
        chat_id="oc_chat_1",
        message_id="msg_auth_1",
        scopes=["contact:user.base:readonly", "calendar:calendar.event:delete"],
        reason="Need multiple scopes.",
        title="Auth",
        requester_open_id="ou_requester",
        account_id="feishu-cn",
        tool_name="feishu_drive_file",
        tool_action="delete",
        replay_id="fr_1",
        replay_ids=["fr_1", "fr_2"],
    )
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_operator", "user_name": "Alice", "user_id_alt": "ou_operator"}
    )
    adapter.get_chat_info = AsyncMock(return_value={"name": "Backend Chat", "chat_type": "group"})
    adapter._update_interactive_card = AsyncMock()
    adapter._handle_message_with_guards = AsyncMock()
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="msg_result"))
    adapter._pending_tool_replays = {
        "fr_1": {
            "tool_name": "feishu_drive_file",
            "args": {"action": "delete", "file_token": "file_1", "type": "file"},
            "chat_id": "oc_chat_1",
            "thread_id": "",
            "user_id": "ou_requester",
        },
        "fr_2": {
            "tool_name": "feishu_calendar_event",
            "args": {"action": "delete", "calendar_id": "cal_1", "event_id": "evt_1"},
            "chat_id": "oc_chat_1",
            "thread_id": "",
            "user_id": "ou_requester",
        },
    }
    monkeypatch.setattr(
        "tools.registry.registry.dispatch",
        lambda name, args, **kwargs: (
            json.dumps({"error": True, "tool": name}, ensure_ascii=False)
            if name == "feishu_calendar_event"
            else json.dumps({"success": True, "tool": name, "args": args}, ensure_ascii=False)
        ),
    )

    event = SimpleNamespace(
        token="tok_auth_partial",
        context=SimpleNamespace(open_chat_id="oc_chat_1"),
        operator=SimpleNamespace(open_id="ou_requester"),
        action=SimpleNamespace(tag="button", value={"hermes_action": "complete_oauth", "request_id": "fo_1"}),
    )

    import asyncio

    asyncio.run(adapter._handle_card_action_event(SimpleNamespace(event=event)))

    assert adapter._update_interactive_card.await_args.kwargs["template"] == "orange"
    card_body = adapter._update_interactive_card.await_args.kwargs["body_markdown"]
    assert "need attention" in card_body
    adapter.send.assert_awaited_once()


def test_feishu_adapter_rejects_oauth_completion_from_other_user(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingOAuthRequest

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._pending_oauth_requests["fo_1"] = FeishuPendingOAuthRequest(
        request_id="fo_1",
        chat_id="oc_chat_1",
        message_id="msg_auth_1",
        scopes=["contact:user.base:readonly"],
        reason="Need basic profile.",
        title="Auth",
        requester_open_id="ou_requester",
    )
    adapter._update_interactive_card = AsyncMock()
    adapter._handle_message_with_guards = AsyncMock()

    event = SimpleNamespace(
        token="tok_auth_1",
        context=SimpleNamespace(open_chat_id="oc_chat_1"),
        operator=SimpleNamespace(open_id="ou_other"),
        action=SimpleNamespace(tag="button", value={"hermes_action": "complete_oauth", "request_id": "fo_1"}),
    )

    asyncio.run(adapter._handle_card_action_event(SimpleNamespace(event=event)))

    status = adapter.get_authorization_status("ou_requester", ["contact:user.base:readonly"])
    assert status["authorized"] is False
    assert "fo_1" in adapter._pending_oauth_requests
    adapter._update_interactive_card.assert_not_awaited()
    adapter._handle_message_with_guards.assert_not_awaited()


def test_feishu_adapter_authorization_status_is_scoped_by_account():
    from gateway.platforms.feishu import FeishuAdapter

    config = PlatformConfig(
        enabled=True,
        extra={
            "app_id": "cli_primary",
            "app_secret": "secret_primary",
            "accounts": {
                "feishu-cn": {
                    "app_id": "cli_secondary",
                    "app_secret": "secret_secondary",
                }
            },
        },
    )
    adapter = FeishuAdapter(config)

    adapter.record_authorization_grant(
        user_open_id="ou_requester",
        scopes=["contact:user.base:readonly"],
        account_id="feishu-cn",
    )

    primary_status = adapter.get_authorization_status(
        "ou_requester",
        ["contact:user.base:readonly"],
        account_id="default",
    )
    secondary_status = adapter.get_authorization_status(
        "ou_requester",
        ["contact:user.base:readonly"],
        account_id="feishu-cn",
    )

    assert primary_status["authorized"] is False
    assert secondary_status["authorized"] is True
    assert primary_status["app_id"] == "cli_primary"
    assert secondary_status["app_id"] == "cli_secondary"


def test_feishu_adapter_routes_question_answer(monkeypatch):
    from gateway.platforms.feishu import FeishuAdapter, FeishuPendingQuestion

    config = PlatformConfig(
        enabled=True,
        extra={"app_id": "cli_xxx", "app_secret": "secret_xxx"},
    )
    adapter = FeishuAdapter(config)
    adapter._pending_questions["fq_1"] = FeishuPendingQuestion(
        question_id="fq_1",
        chat_id="oc_chat_1",
        message_id="msg_1",
        question="Which environment?",
        options=["staging", "prod"],
        header="Need Input",
    )
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_user", "user_name": "Alice", "user_id_alt": "ou_user"}
    )
    adapter.get_chat_info = AsyncMock(return_value={"name": "Backend Chat", "chat_type": "group"})
    adapter._update_interactive_card = AsyncMock()
    adapter._handle_message_with_guards = AsyncMock()

    event = SimpleNamespace(
        token="tok_1",
        context=SimpleNamespace(open_chat_id="oc_chat_1"),
        operator=SimpleNamespace(open_id="ou_user"),
        action=SimpleNamespace(
            tag="button",
            value={"hermes_action": "answer_question", "question_id": "fq_1", "answer": "staging"},
        ),
    )
    data = SimpleNamespace(event=event)

    import asyncio

    asyncio.run(adapter._handle_card_action_event(data))

    adapter._update_interactive_card.assert_awaited_once()
    adapter._handle_message_with_guards.assert_awaited_once()
    injected_event = adapter._handle_message_with_guards.await_args.args[0]
    assert injected_event.text == "Which environment?\nAnswer: staging"
