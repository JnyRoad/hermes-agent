from __future__ import annotations

from unittest.mock import MagicMock, patch

from hermes_cli import pairing


def test_cmd_approve_notifies_feishu_user(capsys):
    store = MagicMock()
    store.approve_code.return_value = {"user_id": "ou_feishu_user", "user_name": "Alice"}

    with patch("hermes_cli.pairing._notify_feishu_pairing_approval") as notify:
        pairing._cmd_approve(store, "feishu", "abcd1234")

    out = capsys.readouterr().out
    assert "Approved! User Alice (ou_feishu_user) on feishu can now use the bot" in out
    notify.assert_called_once_with("ou_feishu_user")


def test_cmd_approve_skips_non_feishu_notification(capsys):
    store = MagicMock()
    store.approve_code.return_value = {"user_id": "tg_user", "user_name": "Bob"}

    with patch("hermes_cli.pairing._notify_feishu_pairing_approval") as notify:
        pairing._cmd_approve(store, "telegram", "abcd1234")

    capsys.readouterr()
    notify.assert_not_called()


def test_cmd_approve_reports_feishu_notification_failure(capsys):
    store = MagicMock()
    store.approve_code.return_value = {"user_id": "ou_feishu_user", "user_name": "Alice"}

    with patch(
        "hermes_cli.pairing._notify_feishu_pairing_approval",
        side_effect=RuntimeError("boom"),
    ):
        pairing._cmd_approve(store, "feishu", "abcd1234")

    out = capsys.readouterr().out
    assert "Warning: pairing succeeded, but the Feishu approval notice could not be delivered." in out


def test_notify_feishu_pairing_approval_sends_open_id_message():
    with patch("tools.feishu.client.feishu_api_request") as api_request:
        pairing._notify_feishu_pairing_approval("ou_notice")

    api_request.assert_called_once()
    _, kwargs = api_request.call_args
    assert kwargs["params"] == {"receive_id_type": "open_id"}
    assert kwargs["json_body"]["receive_id"] == "ou_notice"
    assert kwargs["json_body"]["msg_type"] == "text"
