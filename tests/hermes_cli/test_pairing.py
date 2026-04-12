from __future__ import annotations

from unittest.mock import MagicMock, patch

from hermes_cli import pairing


def test_cmd_approve_notifies_feishu_user(capsys):
    store = MagicMock()
    store.approve_code.return_value = {
        "user_id": "ou_feishu_user",
        "user_name": "Alice",
        "account_id": "feishu-cn",
    }

    with patch("hermes_cli.pairing._notify_feishu_pairing_approval") as notify, patch(
        "hermes_cli.pairing._maybe_notify_feishu_onboarding",
        return_value=False,
    ) as onboarding:
        pairing._cmd_approve(store, "feishu", "abcd1234")

    out = capsys.readouterr().out
    assert "Approved! User Alice (ou_feishu_user) on feishu can now use the bot" in out
    notify.assert_called_once_with("ou_feishu_user", account_id="feishu-cn")
    onboarding.assert_called_once_with("ou_feishu_user", account_id="feishu-cn")


def test_cmd_approve_skips_non_feishu_notification(capsys):
    store = MagicMock()
    store.approve_code.return_value = {"user_id": "tg_user", "user_name": "Bob"}

    with patch("hermes_cli.pairing._notify_feishu_pairing_approval") as notify, patch(
        "hermes_cli.pairing._maybe_notify_feishu_onboarding"
    ) as onboarding:
        pairing._cmd_approve(store, "telegram", "abcd1234")

    capsys.readouterr()
    notify.assert_not_called()
    onboarding.assert_not_called()


def test_cmd_approve_reports_feishu_notification_failure(capsys):
    store = MagicMock()
    store.approve_code.return_value = {"user_id": "ou_feishu_user", "user_name": "Alice", "account_id": "feishu-cn"}

    with patch(
        "hermes_cli.pairing._notify_feishu_pairing_approval",
        side_effect=RuntimeError("boom"),
    ), patch("hermes_cli.pairing._maybe_notify_feishu_onboarding", return_value=False):
        pairing._cmd_approve(store, "feishu", "abcd1234")

    out = capsys.readouterr().out
    assert "Warning: pairing succeeded, but the Feishu approval notice could not be delivered." in out


def test_cmd_approve_reports_feishu_onboarding_delivery(capsys):
    store = MagicMock()
    store.approve_code.return_value = {"user_id": "ou_feishu_user", "user_name": "Alice", "account_id": "feishu-cn"}

    with patch("hermes_cli.pairing._notify_feishu_pairing_approval"), patch(
        "hermes_cli.pairing._maybe_notify_feishu_onboarding",
        return_value=True,
    ):
        pairing._cmd_approve(store, "feishu", "abcd1234")

    out = capsys.readouterr().out
    assert "Sent a Feishu onboarding message for the app owner." in out


def test_notify_feishu_pairing_approval_sends_open_id_message():
    with patch("tools.feishu.client.feishu_api_request") as api_request:
        pairing._notify_feishu_pairing_approval("ou_notice", account_id="feishu-cn")

    api_request.assert_called_once()
    _, kwargs = api_request.call_args
    assert kwargs["params"] == {"receive_id_type": "open_id"}
    assert kwargs["account_id"] == "feishu-cn"
    assert kwargs["json_body"]["receive_id"] == "ou_notice"
    assert kwargs["json_body"]["msg_type"] == "text"


def test_maybe_notify_feishu_onboarding_skips_non_owner():
    with patch("tools.feishu.client.get_app_info", return_value={"effective_owner_open_id": "ou_other"}) as get_app_info, patch(
        "tools.feishu.client.get_app_granted_scopes_by_token_type"
    ) as get_scopes, patch("tools.feishu.client.feishu_api_request") as api_request:
        result = pairing._maybe_notify_feishu_onboarding("ou_notice", account_id="feishu-cn")

    assert result is False
    get_app_info.assert_called_once_with(account_id="feishu-cn")
    get_scopes.assert_not_called()
    api_request.assert_not_called()


def test_maybe_notify_feishu_onboarding_sends_owner_message():
    with patch(
        "tools.feishu.client.get_app_info",
        return_value={"effective_owner_open_id": "ou_notice"},
    ) as get_app_info, patch(
        "tools.feishu.client.get_app_granted_scopes_by_token_type",
        return_value=[
            "calendar:calendar.event:read",
            "im:message.send_as_user",
            "task:task:read",
        ],
    ) as get_scopes, patch("tools.feishu.client.feishu_api_request") as api_request:
        result = pairing._maybe_notify_feishu_onboarding("ou_notice", account_id="feishu-cn")

    assert result is True
    get_app_info.assert_called_once_with(account_id="feishu-cn")
    get_scopes.assert_called_once_with("user", account_id="feishu-cn")
    api_request.assert_called_once()
    _, kwargs = api_request.call_args
    assert kwargs["account_id"] == "feishu-cn"
    assert kwargs["json_body"]["receive_id"] == "ou_notice"
    assert "/feishu auth batch" in kwargs["json_body"]["content"]
    assert "3 granted user scopes" in kwargs["json_body"]["content"]
    assert "(2 safe, 1 sensitive)" in kwargs["json_body"]["content"]
