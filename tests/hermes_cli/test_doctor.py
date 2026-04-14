"""Tests for hermes_cli.doctor."""

import os
import sys
import types
from argparse import Namespace
from types import SimpleNamespace

import pytest

import hermes_cli.doctor as doctor
import hermes_cli.gateway as gateway_cli
from hermes_cli import doctor as doctor_mod
from hermes_cli.doctor import _has_provider_env_config
from gateway.config import GatewayConfig, HomeChannel, Platform, PlatformConfig


class TestDoctorPlatformHints:
    def test_termux_package_hint(self, monkeypatch):
        monkeypatch.setenv("TERMUX_VERSION", "0.118.3")
        monkeypatch.setenv("PREFIX", "/data/data/com.termux/files/usr")
        assert doctor._is_termux() is True
        assert doctor._python_install_cmd() == "python -m pip install"
        assert doctor._system_package_install_cmd("ripgrep") == "pkg install ripgrep"

    def test_non_termux_package_hint_defaults_to_apt(self, monkeypatch):
        monkeypatch.delenv("TERMUX_VERSION", raising=False)
        monkeypatch.setenv("PREFIX", "/usr")
        monkeypatch.setattr(sys, "platform", "linux")
        assert doctor._is_termux() is False
        assert doctor._python_install_cmd() == "uv pip install"
        assert doctor._system_package_install_cmd("ripgrep") == "sudo apt install ripgrep"


class TestProviderEnvDetection:
    def test_detects_openai_api_key(self):
        content = "OPENAI_BASE_URL=http://localhost:1234/v1\nOPENAI_API_KEY=***"
        assert _has_provider_env_config(content)

    def test_detects_custom_endpoint_without_openrouter_key(self):
        content = "OPENAI_BASE_URL=http://localhost:8080/v1\n"
        assert _has_provider_env_config(content)

    def test_returns_false_when_no_provider_settings(self):
        content = "TERMINAL_ENV=local\n"
        assert not _has_provider_env_config(content)


class TestDoctorToolAvailabilityOverrides:
    def test_marks_honcho_available_when_configured(self, monkeypatch):
        monkeypatch.setattr(doctor, "_honcho_is_configured_for_doctor", lambda: True)

        available, unavailable = doctor._apply_doctor_tool_availability_overrides(
            [],
            [{"name": "honcho", "env_vars": [], "tools": ["query_user_context"]}],
        )

        assert available == ["honcho"]
        assert unavailable == []

    def test_leaves_honcho_unavailable_when_not_configured(self, monkeypatch):
        monkeypatch.setattr(doctor, "_honcho_is_configured_for_doctor", lambda: False)

        honcho_entry = {"name": "honcho", "env_vars": [], "tools": ["query_user_context"]}
        available, unavailable = doctor._apply_doctor_tool_availability_overrides(
            [],
            [honcho_entry],
        )

        assert available == []
        assert unavailable == [honcho_entry]


class TestHonchoDoctorConfigDetection:
    def test_reports_configured_when_enabled_with_api_key(self, monkeypatch):
        fake_config = SimpleNamespace(enabled=True, api_key="***")

        monkeypatch.setattr(
            "plugins.memory.honcho.client.HonchoClientConfig.from_global_config",
            lambda: fake_config,
        )

        assert doctor._honcho_is_configured_for_doctor()

    def test_reports_not_configured_without_api_key(self, monkeypatch):
        fake_config = SimpleNamespace(enabled=True, api_key="")

        monkeypatch.setattr(
            "plugins.memory.honcho.client.HonchoClientConfig.from_global_config",
            lambda: fake_config,
        )

        assert not doctor._honcho_is_configured_for_doctor()


class TestFeishuDoctorChecks:
    def test_warns_when_feishu_not_configured(self, monkeypatch, capsys):
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: GatewayConfig())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Feishu Integration" in out
        assert "Feishu integration not configured" in out
        assert issues == []

    def test_reports_websocket_configuration(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                        "unauthorized_dm_behavior": "pair",
                    },
                )
            }
        )
        cfg.platforms[Platform.FEISHU].home_channel = HomeChannel(
            platform=Platform.FEISHU,
            chat_id="oc_home",
            name="Home",
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Feishu platform enabled" in out
        assert "FEISHU_APP_ID configured" in out
        assert "Connection mode: websocket" in out
        assert "Feishu home channel configured" in out
        assert "lark-oapi SDK" in out
        assert issues == []

    def test_reports_multi_account_webhook_configuration(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                        "verification_token": "verify_primary",
                        "encrypt_key": "encrypt_primary",
                        "accounts": {
                            "feishu-cn": {
                                "app_id": "cli_cn",
                                "app_secret": "sec_cn",
                                "webhook_path": "/webhook/feishu-cn",
                            }
                        },
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Feishu accounts configured: 2" in out
        assert "Account `feishu-cn`" in out
        assert "Multi-account webhook routing enabled" in out
        assert issues == []

    def test_warns_when_multi_account_uses_websocket(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                        "accounts": {
                            "feishu-cn": {
                                "app_id": "cli_cn",
                                "app_secret": "sec_cn",
                            }
                        },
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Multi-account websocket routing enabled" in out
        assert not any("webhook mode for multi-account Feishu" in item for item in issues)

    def test_webhook_mode_warns_when_tokens_missing(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "lark",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Connection mode: webhook" in out
        assert "FEISHU_VERIFICATION_TOKEN missing" in out
        assert "FEISHU_ENCRYPT_KEY missing" in out
        assert any("FEISHU_VERIFICATION_TOKEN" in item for item in issues)

    def test_warns_when_feishu_platform_disabled(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=False,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Feishu platform disabled" in out
        assert "FEISHU_APP_ID configured" in out
        assert issues == []

    def test_warns_when_connection_mode_is_unknown(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "long_polling",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Unknown connection mode: long_polling" in out
        assert any("connection_mode" in item for item in issues)

    def test_warns_when_feishu_domain_is_unknown(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "example",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Unknown Feishu domain: example" in out
        assert issues == []

    def test_websocket_mode_reports_webhook_checks_skipped(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Webhook-only checks skipped in websocket mode" in out
        assert issues == []

    def test_warns_when_lark_oapi_sdk_missing(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.delitem(sys.modules, "lark_oapi", raising=False)

        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "lark_oapi":
                raise ImportError("missing lark_oapi")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import)

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "lark-oapi SDK not installed" in out
        assert any("lark-oapi" in item for item in issues)

    def test_reports_app_scope_status_when_query_succeeds(self, monkeypatch, capsys):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes",
            lambda: ["application:application:self_manage", "im:message:readonly"],
        )

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Feishu app scopes: 2 granted" in out
        assert "App self-manage scope available" in out

    def test_warns_when_app_scope_query_cannot_run(self, monkeypatch, capsys):
        from tools.feishu.client import FeishuAPIError

        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes",
            lambda: (_ for _ in ()).throw(
                FeishuAPIError(
                    code=99991672,
                    message="missing application:application:self_manage",
                    missing_scopes=["application:application:self_manage"],
                )
            ),
        )

        issues = []
        doctor._check_feishu_integration(issues)

        out = capsys.readouterr().out
        assert "Unable to query Feishu app scopes" in out
        assert any("application:application:self_manage" in item for item in issues)

    def test_collect_report_scopes_user_authorization_by_account(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr("tools.feishu.client.get_app_granted_scopes", lambda: [])

        captured = {}

        class _Adapter:
            def get_authorization_status(self, user_open_id, scopes=None, account_id=None):
                captured["account_id"] = account_id
                return {"authorized": True, "granted_scopes": ["im:chat:read"]}

        report = doctor.collect_feishu_doctor_report(
            user_open_id="ou_user",
            adapter=_Adapter(),
            account_id="feishu-cn",
        )
        assert captured["account_id"] == "feishu-cn"
        assert any("Current user authorization (feishu-cn): 1 granted" in item["label"] for item in report["items"])

    def test_collect_report_recommends_owner_batch_auth(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr("tools.feishu.client.get_app_granted_scopes", lambda: ["application:application:self_manage"])
        monkeypatch.setattr(
            "tools.feishu.client.get_app_info",
            lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes_by_token_type",
            lambda token_type, account_id=None: ["im:chat:read", "task:task:read"],
        )

        class _Adapter:
            def get_authorization_status(self, user_open_id, scopes=None, account_id=None):
                return {"authorized": False, "granted_scopes": ["im:chat:read"]}

        report = doctor.collect_feishu_doctor_report(
            user_open_id="ou_owner",
            adapter=_Adapter(),
            account_id="feishu-cn",
        )

        assert any(item["label"] == "Current user is Feishu app owner" for item in report["items"])
        assert any(item["label"] == "Owner batch authorization recommended" for item in report["items"])
        assert any("/feishu auth batch" in issue for issue in report["issues"])

    def test_collect_report_warns_when_offline_access_missing(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr("tools.feishu.client.get_app_granted_scopes", lambda: ["application:application:self_manage"])
        monkeypatch.setattr(
            "tools.feishu.client.get_app_info",
            lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes_by_token_type",
            lambda token_type, account_id=None: ["im:chat:read"] if token_type is None else ["im:chat:read"],
        )

        report = doctor.collect_feishu_doctor_report(user_open_id="ou_owner", account_id="feishu-cn")

        assert any(item["label"] == "Feishu OAuth prerequisite missing" for item in report["items"])
        assert any("offline_access" in issue for issue in report["issues"])

    def test_collect_report_warns_when_app_has_no_granted_user_scopes(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes",
            lambda: ["application:application:self_manage", "offline_access"],
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_info",
            lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes_by_token_type",
            lambda token_type, account_id=None: ["application:application:self_manage", "offline_access"]
            if token_type is None
            else [],
        )

        report = doctor.collect_feishu_doctor_report(user_open_id="ou_owner", account_id="feishu-cn")

        assert any(item["label"] == "Feishu user scopes not granted to app yet" for item in report["items"])
        assert any("Grant the required Feishu user scopes first" in issue for issue in report["issues"])

    def test_collect_report_warns_missing_required_app_scopes(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr("tools.feishu.client.get_app_granted_scopes", lambda: ["application:application:self_manage"])
        monkeypatch.setattr(
            "tools.feishu.client.get_app_info",
            lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes_by_token_type",
            lambda token_type, account_id=None: [],
        )

        report = doctor.collect_feishu_doctor_report(user_open_id="ou_owner", account_id="feishu-cn")

        assert any(item["label"].startswith("Feishu required app scopes missing:") for item in report["items"])
        assert any("Grant the missing Feishu app scopes" in issue for issue in report["issues"])

    def test_collect_report_includes_directory_diagnostics(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                        "directory": {
                            "include_config_users": True,
                            "include_config_groups": False,
                            "include_live_users": False,
                            "include_live_groups": True,
                            "live_limit": 12,
                            "live_page_size": 6,
                        },
                        "reply_mode": "card",
                        "streaming": False,
                        "block_streaming": True,
                        "block_streaming_coalesce_ms": 850,
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr("tools.feishu.client.get_app_granted_scopes", lambda: ["application:application:self_manage"])
        monkeypatch.setattr(
            "tools.feishu.client.get_app_info",
            lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes_by_token_type",
            lambda token_type, account_id=None: [],
        )
        monkeypatch.setattr(
            "gateway.channel_directory.load_directory",
            lambda: {
                "updated_at": "2026-01-01T00:00:00",
                "platforms": {
                    "feishu": [
                        {"id": "ou_a", "name": "Alice", "type": "dm", "source": "config", "account_id": "default"},
                        {"id": "oc_b", "name": "Backend", "type": "group", "source": "live", "account_id": "default"},
                        {"id": "feishu-cn::oc_c", "name": "Ops", "type": "group", "source": "live", "account_id": "feishu-cn"},
                    ]
                },
            },
        )

        report = doctor.collect_feishu_doctor_report(
            user_open_id="ou_owner",
            adapter=SimpleNamespace(
                _get_account_live_directory_settings=lambda account_id: {
                    "default": {
                        "include_config_users": True,
                        "include_config_groups": False,
                        "include_live_users": False,
                        "include_live_groups": True,
                        "live_limit": 12,
                        "live_page_size": 6,
                    },
                    "feishu-cn": {
                        "include_config_users": False,
                        "include_config_groups": True,
                        "include_live_users": True,
                        "include_live_groups": False,
                        "live_limit": 20,
                        "live_page_size": 10,
                    },
                }[account_id],
                search_channel_directory_entries=lambda *args, **kwargs: [],
                get_transport_account_status=lambda: [
                    {
                        "account_id": "default",
                        "connection_mode": "webhook",
                        "runtime_state": "connected",
                        "domain": "feishu",
                    },
                    {
                        "account_id": "feishu-cn",
                        "connection_mode": "webhook",
                        "runtime_state": "error",
                        "domain": "feishu",
                        "last_error": "ws worker failed",
                    },
                ],
            ),
            account_id="feishu-cn",
        )

        assert any(item["label"] == "Feishu directory settings" for item in report["items"])
        assert any(
            item["label"] == "Feishu directory settings"
            and "config_users=True config_groups=False users=False groups=True limit=12 page_size=6" in item["detail"]
            for item in report["items"]
        )
        assert any(
            item["label"] == "Feishu directory policy (feishu-cn)"
            and "config_users=false config_groups=true users=true groups=false limit=20 page_size=10" in item["detail"]
            for item in report["items"]
        )
        assert any(
            item["label"] == "Feishu reply and streaming settings"
            and "reply_mode=card streaming=false block_streaming=true coalesce_ms=850" in item["detail"]
            for item in report["items"]
        )
        assert any(
            item["label"] == "Feishu effective delivery mode"
            and "static card-style replies without streaming updates" in item["detail"]
            and "coalesced edits at 850ms" in item["detail"]
            for item in report["items"]
        )
        assert any(
            item["label"] == "Feishu runtime transport status"
            and "default=connected/webhook, feishu-cn=error/webhook" in item["detail"]
            for item in report["items"]
        )
        assert any(
            item["label"] == "Feishu runtime transport errors"
            and "feishu-cn=ws worker failed" in item["detail"]
            for item in report["items"]
        )
        assert any(item["label"] == "Feishu cached directory targets: 3" for item in report["items"])
        assert any(item["label"] == "Feishu cached directory accounts" for item in report["items"])
        assert any(item["label"] == "Feishu live directory search fallback available" for item in report["items"])

    def test_collect_report_marks_multi_account_websocket_as_enabled(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_primary",
                        "app_secret": "sec_primary",
                        "connection_mode": "websocket",
                        "domain": "feishu",
                        "accounts": {
                            "feishu-cn": {
                                "app_id": "cli_secondary",
                                "app_secret": "sec_secondary",
                                "connection_mode": "websocket",
                            }
                        },
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr("tools.feishu.client.get_app_granted_scopes", lambda: ["application:application:self_manage"])
        monkeypatch.setattr(
            "tools.feishu.client.get_app_info",
            lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes_by_token_type",
            lambda token_type, account_id=None: [],
        )
        monkeypatch.setattr(
            "gateway.channel_directory.load_directory",
            lambda: {"updated_at": "2026-01-01T00:00:00", "platforms": {"feishu": []}},
        )

        report = doctor.collect_feishu_doctor_report(user_open_id="ou_owner", account_id="feishu-cn")

        assert any(item["label"] == "Multi-account websocket routing enabled" for item in report["items"])
        assert not any(item["label"] == "Multi-account websocket support incomplete" for item in report["items"])
        assert not any("Use webhook mode for multi-account Feishu" in issue for issue in report["issues"])

    def test_collect_report_warns_when_directory_disabled_for_account(self, monkeypatch):
        cfg = GatewayConfig(
            platforms={
                Platform.FEISHU: PlatformConfig(
                    enabled=True,
                    extra={
                        "app_id": "cli_aid",
                        "app_secret": "cli_secret",
                        "connection_mode": "webhook",
                        "domain": "feishu",
                    },
                )
            }
        )
        monkeypatch.setattr("gateway.config.load_gateway_config", lambda: cfg)
        monkeypatch.setitem(sys.modules, "lark_oapi", types.SimpleNamespace())
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes",
            lambda: ["application:application:self_manage", "offline_access"],
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_info",
            lambda account_id=None: {"effective_owner_open_id": "ou_owner"},
        )
        monkeypatch.setattr(
            "tools.feishu.client.get_app_granted_scopes_by_token_type",
            lambda token_type, account_id=None: [],
        )
        monkeypatch.setattr(
            "gateway.channel_directory.load_directory",
            lambda: {"updated_at": "2026-01-01T00:00:00", "platforms": {"feishu": []}},
        )

        report = doctor.collect_feishu_doctor_report(
            user_open_id="ou_owner",
            adapter=SimpleNamespace(
                _get_account_live_directory_settings=lambda account_id: {
                    "include_config_users": False,
                    "include_config_groups": False,
                    "include_live_users": False,
                    "include_live_groups": False,
                    "live_limit": 50,
                    "live_page_size": 50,
                }
            ),
            account_id="feishu-cn",
        )

        assert any(item["label"] == "Feishu directory policy (feishu-cn)" for item in report["items"])
        assert any(item["label"] == "Feishu directory disabled for account (feishu-cn)" for item in report["items"])
        assert any("Enable at least one Feishu directory source for account `feishu-cn`" in issue for issue in report["issues"])


def test_run_doctor_sets_interactive_env_for_tool_checks(monkeypatch, tmp_path):
    """Doctor should present CLI-gated tools as available in CLI context."""
    project_root = tmp_path / "project"
    hermes_home = tmp_path / ".hermes"
    project_root.mkdir()
    hermes_home.mkdir()

    monkeypatch.setattr(doctor_mod, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(doctor_mod, "HERMES_HOME", hermes_home)
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)

    seen = {}

    def fake_check_tool_availability(*args, **kwargs):
        seen["interactive"] = os.getenv("HERMES_INTERACTIVE")
        raise SystemExit(0)

    fake_model_tools = types.SimpleNamespace(
        check_tool_availability=fake_check_tool_availability,
        TOOLSET_REQUIREMENTS={},
    )
    monkeypatch.setitem(sys.modules, "model_tools", fake_model_tools)

    with pytest.raises(SystemExit):
        doctor_mod.run_doctor(Namespace(fix=False))

    assert seen["interactive"] == "1"


def test_check_gateway_service_linger_warns_when_disabled(monkeypatch, tmp_path, capsys):
    unit_path = tmp_path / "hermes-gateway.service"
    unit_path.write_text("[Unit]\n")

    monkeypatch.setattr(gateway_cli, "is_linux", lambda: True)
    monkeypatch.setattr(gateway_cli, "get_systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(gateway_cli, "get_systemd_linger_status", lambda: (False, ""))

    issues = []
    doctor._check_gateway_service_linger(issues)

    out = capsys.readouterr().out
    assert "Gateway Service" in out
    assert "Systemd linger disabled" in out
    assert "loginctl enable-linger" in out
    assert issues == [
        "Enable linger for the gateway user service: sudo loginctl enable-linger $USER"
    ]


def test_check_gateway_service_linger_skips_when_service_not_installed(monkeypatch, tmp_path, capsys):
    unit_path = tmp_path / "missing.service"

    monkeypatch.setattr(gateway_cli, "is_linux", lambda: True)
    monkeypatch.setattr(gateway_cli, "get_systemd_unit_path", lambda: unit_path)

    issues = []
    doctor._check_gateway_service_linger(issues)

    out = capsys.readouterr().out
    assert out == ""
    assert issues == []


# ── Memory provider section (doctor should only check the *active* provider) ──


class TestDoctorMemoryProviderSection:
    """The ◆ Memory Provider section should respect memory.provider config."""

    def _make_hermes_home(self, tmp_path, provider=""):
        """Create a minimal HERMES_HOME with config.yaml."""
        home = tmp_path / ".hermes"
        home.mkdir(parents=True, exist_ok=True)
        import yaml
        config = {"memory": {"provider": provider}} if provider else {"memory": {}}
        (home / "config.yaml").write_text(yaml.dump(config))
        return home

    def _run_doctor_and_capture(self, monkeypatch, tmp_path, provider=""):
        """Run doctor and capture stdout."""
        home = self._make_hermes_home(tmp_path, provider)
        monkeypatch.setattr(doctor_mod, "HERMES_HOME", home)
        monkeypatch.setattr(doctor_mod, "PROJECT_ROOT", tmp_path / "project")
        monkeypatch.setattr(doctor_mod, "_DHH", str(home))
        (tmp_path / "project").mkdir(exist_ok=True)

        # Stub tool availability (returns empty) so doctor runs past it
        fake_model_tools = types.SimpleNamespace(
            check_tool_availability=lambda *a, **kw: ([], []),
            TOOLSET_REQUIREMENTS={},
        )
        monkeypatch.setitem(sys.modules, "model_tools", fake_model_tools)

        # Stub auth checks to avoid real API calls
        try:
            from hermes_cli import auth as _auth_mod
            monkeypatch.setattr(_auth_mod, "get_nous_auth_status", lambda: {})
            monkeypatch.setattr(_auth_mod, "get_codex_auth_status", lambda: {})
        except Exception:
            pass

        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            doctor_mod.run_doctor(Namespace(fix=False))
        return buf.getvalue()

    def test_no_provider_shows_builtin_ok(self, monkeypatch, tmp_path):
        out = self._run_doctor_and_capture(monkeypatch, tmp_path, provider="")
        assert "Memory Provider" in out
        assert "Built-in memory active" in out
        # Should NOT mention Honcho or Mem0 errors
        assert "Honcho API key" not in out
        assert "Mem0" not in out

    def test_honcho_provider_not_installed_shows_fail(self, monkeypatch, tmp_path):
        # Make honcho import fail
        monkeypatch.setitem(
            sys.modules, "plugins.memory.honcho.client", None
        )
        out = self._run_doctor_and_capture(monkeypatch, tmp_path, provider="honcho")
        assert "Memory Provider" in out
        # Should show failure since honcho is set but not importable
        assert "Built-in memory active" not in out

    def test_mem0_provider_not_installed_shows_fail(self, monkeypatch, tmp_path):
        # Make mem0 import fail
        monkeypatch.setitem(sys.modules, "plugins.memory.mem0", None)
        out = self._run_doctor_and_capture(monkeypatch, tmp_path, provider="mem0")
        assert "Memory Provider" in out
        assert "Built-in memory active" not in out


def test_run_doctor_termux_treats_docker_and_browser_warnings_as_expected(monkeypatch, tmp_path):
    helper = TestDoctorMemoryProviderSection()
    monkeypatch.setenv("TERMUX_VERSION", "0.118.3")
    monkeypatch.setenv("PREFIX", "/data/data/com.termux/files/usr")

    real_which = doctor_mod.shutil.which

    def fake_which(cmd):
        if cmd in {"docker", "node", "npm"}:
            return None
        return real_which(cmd)

    monkeypatch.setattr(doctor_mod.shutil, "which", fake_which)

    out = helper._run_doctor_and_capture(monkeypatch, tmp_path, provider="")

    assert "Docker backend is not available inside Termux" in out
    assert "Node.js not found (browser tools are optional in the tested Termux path)" in out
    assert "Install Node.js on Termux with: pkg install nodejs" in out
    assert "Termux browser setup:" in out
    assert "1) pkg install nodejs" in out
    assert "2) npm install -g agent-browser" in out
    assert "3) agent-browser install" in out
    assert "docker not found (optional)" not in out


def test_run_doctor_termux_does_not_mark_browser_available_without_agent_browser(monkeypatch, tmp_path):
    home = tmp_path / ".hermes"
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.yaml").write_text("memory: {}\n", encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)

    monkeypatch.setenv("TERMUX_VERSION", "0.118.3")
    monkeypatch.setenv("PREFIX", "/data/data/com.termux/files/usr")
    monkeypatch.setattr(doctor_mod, "HERMES_HOME", home)
    monkeypatch.setattr(doctor_mod, "PROJECT_ROOT", project)
    monkeypatch.setattr(doctor_mod, "_DHH", str(home))
    monkeypatch.setattr(doctor_mod.shutil, "which", lambda cmd: "/data/data/com.termux/files/usr/bin/node" if cmd in {"node", "npm"} else None)

    fake_model_tools = types.SimpleNamespace(
        check_tool_availability=lambda *a, **kw: (["terminal"], [{"name": "browser", "env_vars": [], "tools": ["browser_navigate"]}]),
        TOOLSET_REQUIREMENTS={
            "terminal": {"name": "terminal"},
            "browser": {"name": "browser"},
        },
    )
    monkeypatch.setitem(sys.modules, "model_tools", fake_model_tools)

    try:
        from hermes_cli import auth as _auth_mod
        monkeypatch.setattr(_auth_mod, "get_nous_auth_status", lambda: {})
        monkeypatch.setattr(_auth_mod, "get_codex_auth_status", lambda: {})
    except Exception:
        pass

    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        doctor_mod.run_doctor(Namespace(fix=False))
    out = buf.getvalue()

    assert "✓ browser" not in out
    assert "browser" in out
    assert "system dependency not met" in out
    assert "agent-browser is not installed (expected in the tested Termux path)" in out
    assert "npm install -g agent-browser && agent-browser install" in out
