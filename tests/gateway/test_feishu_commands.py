"""Tests for Feishu-specific gateway slash commands."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, SendResult
from gateway.session import SessionSource


def _make_event(
    text="/feishu-doctor",
    platform=Platform.FEISHU,
    user_id="ou_user_1",
    chat_id="oc_chat_1",
    account_id=None,
):
    source = SessionSource(
        platform=platform,
        user_id=user_id,
        chat_id=chat_id,
        user_name="testuser",
        account_id=account_id,
    )
    return MessageEvent(text=text, source=source)


def _make_runner():
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner._voice_mode = {}
    runner._session_db = None
    runner._reasoning_config = None
    runner._provider_routing = {}
    runner._fallback_model = None
    runner._running_agents = {}
    runner.hooks = MagicMock()
    runner.hooks.emit = AsyncMock()
    runner.hooks.loaded_hooks = []
    return runner


@pytest.mark.asyncio
async def test_feishu_doctor_requires_feishu_platform():
    runner = _make_runner()
    result = await runner._handle_feishu_doctor_command(_make_event(platform=Platform.TELEGRAM))
    assert "only available inside a Feishu chat" in result


@pytest.mark.asyncio
async def test_feishu_unified_help_lists_subcommands():
    runner = _make_runner()
    result = await runner._handle_feishu_command(_make_event("/feishu"))
    assert "Feishu Commands" in result
    assert "/feishu start" in result
    assert "/feishu auth" in result


@pytest.mark.asyncio
async def test_feishu_unified_doctor_delegates(monkeypatch):
    runner = _make_runner()
    delegated = AsyncMock(return_value="doctor-result")
    runner._handle_feishu_doctor_command = delegated

    result = await runner._handle_feishu_command(_make_event("/feishu doctor"))

    assert result == "doctor-result"
    delegated.assert_awaited_once()


@pytest.mark.asyncio
async def test_feishu_unified_auth_delegates(monkeypatch):
    runner = _make_runner()
    delegated = AsyncMock(return_value="auth-result")
    runner._handle_feishu_auth_command = delegated

    result = await runner._handle_feishu_command(_make_event("/feishu auth status"))

    assert result == "auth-result"
    delegated.assert_awaited_once()
    forwarded_event = delegated.await_args.args[0]
    assert forwarded_event.text == "/feishu-auth status"


@pytest.mark.asyncio
async def test_feishu_unified_start_reports_success(monkeypatch):
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = SimpleNamespace()
    monkeypatch.setattr(
        "hermes_cli.doctor.collect_feishu_doctor_report",
        lambda **kwargs: {"items": [{"status": "ok", "label": "Feishu platform enabled", "detail": ""}], "issues": []},
    )

    result = await runner._handle_feishu_command(_make_event("/feishu start"))

    assert "Feishu Start Check Passed" in result


@pytest.mark.asyncio
async def test_feishu_unified_start_surfaces_warnings(monkeypatch):
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = SimpleNamespace()
    monkeypatch.setattr(
        "hermes_cli.doctor.collect_feishu_doctor_report",
        lambda **kwargs: {
            "items": [{"status": "warn", "label": "App self-manage scope missing", "detail": "cannot query app scopes"}],
            "issues": ["Grant application:application:self_manage"],
        },
    )

    result = await runner._handle_feishu_command(_make_event("/feishu start"))

    assert "Passed With Warnings" in result
    assert "App self-manage scope missing" in result
    assert "Grant application:application:self_manage" in result


@pytest.mark.asyncio
async def test_feishu_doctor_uses_shared_report(monkeypatch):
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = SimpleNamespace()
    monkeypatch.setattr(
        "hermes_cli.doctor.collect_feishu_doctor_report",
        lambda **kwargs: {
            "items": [
                {"status": "ok", "label": "Feishu platform enabled", "detail": ""},
                {"status": "warn", "label": "App self-manage scope missing", "detail": "cannot query app scopes"},
            ],
            "issues": ["Grant application:application:self_manage"],
        },
    )

    result = await runner._handle_feishu_doctor_command(_make_event("/feishu-doctor"))
    assert "Feishu Doctor" in result
    assert "Feishu platform enabled" in result
    assert "App self-manage scope missing" in result
    assert "Grant application:application:self_manage" in result


@pytest.mark.asyncio
async def test_feishu_doctor_forwards_account_id(monkeypatch):
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = SimpleNamespace()
    captured = {}

    def _collect(**kwargs):
        captured.update(kwargs)
        return {"items": [], "issues": []}

    monkeypatch.setattr("hermes_cli.doctor.collect_feishu_doctor_report", _collect)
    await runner._handle_feishu_doctor_command(_make_event("/feishu-doctor", account_id="feishu-cn"))
    assert captured["account_id"] == "feishu-cn"


@pytest.mark.asyncio
async def test_feishu_auth_status_reports_current_grants():
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = SimpleNamespace(
        get_authorization_status=lambda user_open_id, scopes=None: {
            "authorized": True,
            "granted_scopes": ["im:chat:read", "im:message:readonly"],
            "requested_scopes": list(scopes or []),
            "missing_scopes": [],
        },
        _normalize_scope_list=lambda scopes: list(dict.fromkeys(scopes)),
    )

    result = await runner._handle_feishu_auth_command(_make_event("/feishu-auth status"))
    assert "Feishu Authorization Status" in result
    assert "Authorized: yes" in result
    assert "Granted scopes: 2" in result


@pytest.mark.asyncio
async def test_feishu_auth_tool_request_uses_scope_mapping():
    adapter = SimpleNamespace(
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(success=True, message_id="msg_auth", raw_response={"request_id": "fo_1"})
        ),
        _normalize_scope_list=lambda scopes: list(dict.fromkeys(scopes)),
    )
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = adapter

    result = await runner._handle_feishu_auth_command(
        _make_event("/feishu-auth feishu_calendar_event delete")
    )
    assert "Feishu authorization requested" in result
    assert "`feishu_calendar_event`" in result
    assert "`delete`" in result
    adapter.send_oauth_request_card.assert_awaited_once()
    kwargs = adapter.send_oauth_request_card.await_args.kwargs
    assert kwargs["scopes"] == ["calendar:calendar.event:delete"]
    assert kwargs["metadata"]["tool_name"] == "feishu_calendar_event"
    assert kwargs["metadata"]["action"] == "delete"


@pytest.mark.asyncio
async def test_feishu_auth_tool_request_forwards_account_id():
    adapter = SimpleNamespace(
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(success=True, message_id="msg_auth", raw_response={"request_id": "fo_1"})
        ),
        _normalize_scope_list=lambda scopes: list(dict.fromkeys(scopes)),
    )
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = adapter

    await runner._handle_feishu_auth_command(
        _make_event("/feishu-auth feishu_calendar_event delete", account_id="feishu-cn")
    )
    kwargs = adapter.send_oauth_request_card.await_args.kwargs
    assert kwargs["metadata"]["account_id"] == "feishu-cn"


@pytest.mark.asyncio
async def test_feishu_auth_scope_request_accepts_explicit_scopes():
    adapter = SimpleNamespace(
        send_oauth_request_card=AsyncMock(
            return_value=SendResult(success=True, message_id="msg_auth", raw_response={"request_id": "fo_2"})
        ),
        _normalize_scope_list=lambda scopes: list(dict.fromkeys(scopes)),
    )
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = adapter

    result = await runner._handle_feishu_auth_command(
        _make_event("/feishu-auth scope im:chat:read,im:message:readonly")
    )
    assert "Feishu authorization requested" in result
    adapter.send_oauth_request_card.assert_awaited_once()
    kwargs = adapter.send_oauth_request_card.await_args.kwargs
    assert kwargs["scopes"] == ["im:chat:read", "im:message:readonly"]


@pytest.mark.asyncio
async def test_feishu_auth_revoke_uses_adapter_status():
    adapter = SimpleNamespace(
        revoke_authorization=lambda user_open_id, scopes=None: {
            "authorized": False,
            "granted_scopes": ["im:chat:read"],
        },
        _normalize_scope_list=lambda scopes: list(dict.fromkeys(scopes)),
    )
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = adapter

    result = await runner._handle_feishu_auth_command(_make_event("/feishu-auth revoke im:message:readonly"))
    assert "Feishu authorization revoked" in result
    assert "Remaining scopes: 1" in result


@pytest.mark.asyncio
async def test_feishu_auth_status_and_revoke_forward_account_id():
    captured = {"status": None, "revoke": None}

    def _status(user_open_id, scopes=None, account_id=None):
        captured["status"] = account_id
        return {
            "authorized": True,
            "granted_scopes": ["im:chat:read"],
            "requested_scopes": list(scopes or []),
            "missing_scopes": [],
        }

    def _revoke(user_open_id, scopes=None, account_id=None):
        captured["revoke"] = account_id
        return {
            "authorized": False,
            "granted_scopes": [],
        }

    adapter = SimpleNamespace(
        get_authorization_status=_status,
        revoke_authorization=_revoke,
        _normalize_scope_list=lambda scopes: list(dict.fromkeys(scopes)),
    )
    runner = _make_runner()
    runner.adapters[Platform.FEISHU] = adapter

    await runner._handle_feishu_auth_command(_make_event("/feishu-auth status", account_id="feishu-cn"))
    await runner._handle_feishu_auth_command(_make_event("/feishu-auth revoke", account_id="feishu-cn"))

    assert captured["status"] == "feishu-cn"
    assert captured["revoke"] == "feishu-cn"
