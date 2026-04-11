"""飞书工具注册与调用测试。"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from gateway.adapter_registry import register_adapter, unregister_adapter
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import SendResult


def test_feishu_toolset_is_included_in_hermes_feishu():
    from toolsets import TOOLSETS, resolve_toolset

    assert "feishu" in TOOLSETS
    assert "feishu" in TOOLSETS["hermes-feishu"]["includes"]

    resolved = set(resolve_toolset("hermes-feishu"))
    assert "feishu_get_user" in resolved
    assert "feishu_search_doc_wiki" in resolved
    assert "feishu_sheet" in resolved
    assert "feishu_fetch_doc" in resolved
    assert "feishu_ask_user_question" in resolved
    assert "feishu_chat" in resolved
    assert "feishu_chat_members" in resolved
    assert "feishu_im_user_search_messages" in resolved
    assert "feishu_im_user_message" in resolved
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


def test_feishu_chat_get_handler(monkeypatch):
    from tools.feishu.chat import _handle_chat

    monkeypatch.setattr(
        "tools.feishu.chat.feishu_api_request",
        lambda *a, **kw: {"data": {"chat": {"chat_id": "oc_1", "name": "研发群"}}},
    )
    payload = json.loads(_handle_chat({"action": "get", "chat_id": "oc_1"}))
    assert payload["chat"]["name"] == "研发群"


def test_feishu_chat_members_handler(monkeypatch):
    from tools.feishu.chat_members import _handle_chat_members

    monkeypatch.setattr(
        "tools.feishu.chat_members.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"member_id": "ou_1", "name": "Alice"}], "member_total": 1}},
    )
    payload = json.loads(_handle_chat_members({"chat_id": "oc_1"}))
    assert payload["member_total"] == 1
    assert payload["items"][0]["member_id"] == "ou_1"


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


def test_feishu_wiki_space_node_handler(monkeypatch):
    from tools.feishu.wiki import _handle_wiki_space_node

    monkeypatch.setattr(
        "tools.feishu.wiki.feishu_api_request",
        lambda *a, **kw: {"data": {"node": {"node_token": "wikcn1"}}},
    )
    payload = json.loads(_handle_wiki_space_node({"action": "get", "token": "wikcn1"}))
    assert payload["node"]["node_token"] == "wikcn1"


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


def test_feishu_task_task_create_handler(monkeypatch):
    from tools.feishu.task import _handle_task

    monkeypatch.setattr(
        "tools.feishu.task.feishu_api_request",
        lambda *a, **kw: {"data": {"task": {"guid": "task_1", "summary": "Ship"}}},
    )
    payload = json.loads(_handle_task({"action": "create", "summary": "Ship"}))
    assert payload["task"]["guid"] == "task_1"


def test_feishu_task_task_list_handler(monkeypatch):
    from tools.feishu.task import _handle_task

    monkeypatch.setattr(
        "tools.feishu.task.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "task_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task({"action": "list"}))
    assert payload["tasks"][0]["guid"] == "task_1"


def test_feishu_task_task_patch_handler(monkeypatch):
    from tools.feishu.task import _handle_task

    monkeypatch.setattr(
        "tools.feishu.task.feishu_api_request",
        lambda *a, **kw: {"data": {"task": {"guid": "task_1", "summary": "Updated"}}},
    )
    payload = json.loads(_handle_task({"action": "patch", "task_guid": "task_1", "summary": "Updated"}))
    assert payload["task"]["summary"] == "Updated"


def test_feishu_task_tasklist_create_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    monkeypatch.setattr(
        "tools.feishu.tasklist.feishu_api_request",
        lambda *a, **kw: {"data": {"tasklist": {"guid": "tl_1", "name": "Inbox"}}},
    )
    payload = json.loads(_handle_tasklist({"action": "create", "name": "Inbox"}))
    assert payload["tasklist"]["guid"] == "tl_1"


def test_feishu_task_tasklist_tasks_handler(monkeypatch):
    from tools.feishu.tasklist import _handle_tasklist

    monkeypatch.setattr(
        "tools.feishu.tasklist.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "task_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_tasklist({"action": "tasks", "tasklist_guid": "tl_1"}))
    assert payload["tasks"][0]["guid"] == "task_1"


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


def test_feishu_task_comment_create_handler(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    monkeypatch.setattr(
        "tools.feishu.task_comment.feishu_api_request",
        lambda *a, **kw: {"data": {"comment": {"id": "c_1", "content": "ok"}}},
    )
    payload = json.loads(_handle_task_comment({"action": "create", "task_guid": "task_1", "content": "ok"}))
    assert payload["comment"]["id"] == "c_1"


def test_feishu_task_comment_list_handler(monkeypatch):
    from tools.feishu.task_comment import _handle_task_comment

    monkeypatch.setattr(
        "tools.feishu.task_comment.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"id": "c_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_comment({"action": "list", "resource_id": "task_1"}))
    assert payload["comments"][0]["id"] == "c_1"


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


def test_feishu_task_subtask_list_handler(monkeypatch):
    from tools.feishu.task_subtask import _handle_task_subtask

    monkeypatch.setattr(
        "tools.feishu.task_subtask.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "sub_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_subtask({"action": "list", "task_guid": "task_1"}))
    assert payload["subtasks"][0]["guid"] == "sub_1"


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


def test_feishu_task_section_list_handler(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    monkeypatch.setattr(
        "tools.feishu.task_section.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "sec_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_section({"action": "list", "resource_type": "my_tasks"}))
    assert payload["sections"][0]["guid"] == "sec_1"


def test_feishu_task_section_tasks_handler(monkeypatch):
    from tools.feishu.task_section import _handle_task_section

    monkeypatch.setattr(
        "tools.feishu.task_section.feishu_api_request",
        lambda *a, **kw: {"data": {"items": [{"guid": "task_1"}], "has_more": False}},
    )
    payload = json.loads(_handle_task_section({"action": "tasks", "section_guid": "sec_1"}))
    assert payload["tasks"][0]["guid"] == "task_1"


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
    assert any(item[0] == "POST" and item[1].endswith("/children") for item in calls)


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


def test_feishu_oauth_uses_active_adapter(monkeypatch):
    from tools.feishu.oauth import _handle_feishu_oauth

    adapter = SimpleNamespace(
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

    try:
        payload = json.loads(_handle_feishu_oauth({"scopes": ["contact:user.base:readonly"]}))
        assert payload["status"] == "pending"
        assert payload["request_id"] == "fo_123"
        adapter.send_oauth_request_card.assert_awaited_once()
    finally:
        unregister_adapter(Platform.FEISHU, adapter)


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
    )
    adapter._update_interactive_card = AsyncMock()

    import asyncio

    result = asyncio.run(
        adapter.send_oauth_request_card(
            chat_id="oc_chat_1",
            scopes=["contact:user.base:readonly", "calendar:calendar.readonly"],
            reason="Need calendar too.",
            title="Auth",
            metadata={"thread_id": "omt_1"},
        )
    )

    assert result.success is True
    assert result.raw_response["request_id"] == "fo_existing"
    assert result.raw_response["merged"] is True
    state = adapter._pending_oauth_requests["fo_existing"]
    assert state.scopes == ["contact:user.base:readonly", "calendar:calendar.readonly"]
    adapter._update_interactive_card.assert_awaited_once()


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
