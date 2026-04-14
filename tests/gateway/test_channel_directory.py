"""Tests for gateway/channel_directory.py — channel resolution and display."""

import json
import os
from pathlib import Path
from unittest.mock import patch

from gateway.config import Platform
from gateway.channel_directory import (
    build_channel_directory,
    explain_channel_name_resolution,
    resolve_channel_name,
    format_directory_for_display,
    load_directory,
    _build_from_sessions,
    DIRECTORY_PATH,
)


def _write_directory(tmp_path, platforms):
    """Helper to write a fake channel directory."""
    data = {"updated_at": "2026-01-01T00:00:00", "platforms": platforms}
    cache_file = tmp_path / "channel_directory.json"
    cache_file.write_text(json.dumps(data))
    return cache_file


class TestLoadDirectory:
    def test_missing_file(self, tmp_path):
        with patch("gateway.channel_directory.DIRECTORY_PATH", tmp_path / "nope.json"):
            result = load_directory()
        assert result["updated_at"] is None
        assert result["platforms"] == {}

    def test_valid_file(self, tmp_path):
        cache_file = _write_directory(tmp_path, {
            "telegram": [{"id": "123", "name": "John", "type": "dm"}]
        })
        with patch("gateway.channel_directory.DIRECTORY_PATH", cache_file):
            result = load_directory()
        assert result["platforms"]["telegram"][0]["name"] == "John"

    def test_corrupt_file(self, tmp_path):
        cache_file = tmp_path / "channel_directory.json"
        cache_file.write_text("{bad json")
        with patch("gateway.channel_directory.DIRECTORY_PATH", cache_file):
            result = load_directory()
        assert result["updated_at"] is None


class TestBuildChannelDirectoryWrites:
    def test_failed_write_preserves_previous_cache(self, tmp_path, monkeypatch):
        cache_file = _write_directory(tmp_path, {
            "telegram": [{"id": "123", "name": "Alice", "type": "dm"}]
        })
        previous = json.loads(cache_file.read_text())

        def broken_dump(data, fp, *args, **kwargs):
            fp.write('{"updated_at":')
            fp.flush()
            raise OSError("disk full")

        monkeypatch.setattr(json, "dump", broken_dump)

        with patch("gateway.channel_directory.DIRECTORY_PATH", cache_file):
            build_channel_directory({})
            result = load_directory()

        assert result == previous

    def test_feishu_uses_native_directory_builder(self, tmp_path):
        class _Adapter:
            def build_channel_directory_entries(self, include_live=True):
                assert include_live is True
                return [
                    {"id": "ou_live_1", "name": "Alice", "type": "dm", "source": "live"},
                    {"id": "feishu-cn::oc_chat_1", "name": "Hermes Group", "type": "group", "source": "live", "account_id": "feishu-cn"},
                ]

        with patch.dict(os.environ, {"HERMES_HOME": str(tmp_path)}), patch(
            "gateway.channel_directory.DIRECTORY_PATH",
            tmp_path / "channel_directory.json",
        ):
            result = build_channel_directory({Platform.FEISHU: _Adapter()})

        assert result["platforms"]["feishu"] == [
            {"id": "ou_live_1", "name": "Alice", "type": "dm", "source": "live"},
            {"id": "feishu-cn::oc_chat_1", "name": "Hermes Group", "type": "group", "source": "live", "account_id": "feishu-cn"},
        ]


class TestResolveChannelName:
    def _setup(self, tmp_path, platforms):
        cache_file = _write_directory(tmp_path, platforms)
        return patch("gateway.channel_directory.DIRECTORY_PATH", cache_file)

    def test_exact_match(self, tmp_path):
        platforms = {
            "discord": [
                {"id": "111", "name": "bot-home", "guild": "MyServer", "type": "channel"},
                {"id": "222", "name": "general", "guild": "MyServer", "type": "channel"},
            ]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("discord", "bot-home") == "111"
            assert resolve_channel_name("discord", "#bot-home") == "111"

    def test_case_insensitive(self, tmp_path):
        platforms = {
            "slack": [{"id": "C01", "name": "Engineering", "type": "channel"}]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("slack", "engineering") == "C01"
            assert resolve_channel_name("slack", "ENGINEERING") == "C01"

    def test_guild_qualified_match(self, tmp_path):
        platforms = {
            "discord": [
                {"id": "111", "name": "general", "guild": "ServerA", "type": "channel"},
                {"id": "222", "name": "general", "guild": "ServerB", "type": "channel"},
            ]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("discord", "ServerA/general") == "111"
            assert resolve_channel_name("discord", "ServerB/general") == "222"

    def test_prefix_match_unambiguous(self, tmp_path):
        platforms = {
            "slack": [
                {"id": "C01", "name": "engineering-backend", "type": "channel"},
                {"id": "C02", "name": "design-team", "type": "channel"},
            ]
        }
        with self._setup(tmp_path, platforms):
            # "engineering" prefix matches only one channel
            assert resolve_channel_name("slack", "engineering") == "C01"

    def test_prefix_match_ambiguous_returns_none(self, tmp_path):
        platforms = {
            "slack": [
                {"id": "C01", "name": "eng-backend", "type": "channel"},
                {"id": "C02", "name": "eng-frontend", "type": "channel"},
            ]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("slack", "eng") is None

    def test_explain_resolution_reports_ambiguous_candidates(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "oc_1", "name": "Backend Guild", "type": "group", "account_id": "default"},
                {"id": "feishu-cn::oc_2", "name": "Backend Ops", "type": "group", "account_id": "feishu-cn"},
            ]
        }
        with self._setup(tmp_path, platforms):
            result = explain_channel_name_resolution("feishu", "Backend")

        assert result["status"] == "ambiguous"
        assert result["resolved_id"] is None
        assert [item["label"] for item in result["suggestions"]] == [
            "Backend Guild (group)",
            "feishu-cn/Backend Ops (group)",
        ]

    def test_feishu_preferred_account_resolves_duplicate_name(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "oc_default", "name": "Backend", "type": "group", "account_id": "default"},
                {"id": "feishu-cn::oc_cn", "name": "Backend", "type": "group", "account_id": "feishu-cn"},
            ]
        }
        with self._setup(tmp_path, platforms):
            result = explain_channel_name_resolution("feishu", "Backend", preferred_account_id="feishu-cn")

        assert result["status"] == "resolved"
        assert result["resolved_id"] == "feishu-cn::oc_cn"
        assert result["preferred_account_id"] == "feishu-cn"

    def test_feishu_preferred_account_keeps_ambiguous_without_unique_match(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "feishu-cn::oc_1", "name": "Backend", "type": "group", "account_id": "feishu-cn"},
                {"id": "feishu-cn::oc_2", "name": "Backend", "type": "group", "account_id": "feishu-cn"},
                {"id": "oc_default", "name": "Backend", "type": "group", "account_id": "default"},
            ]
        }
        with self._setup(tmp_path, platforms):
            result = explain_channel_name_resolution("feishu", "Backend", preferred_account_id="feishu-cn")

        assert result["status"] == "ambiguous"
        assert result["resolved_id"] is None

    def test_feishu_ambiguity_suggestions_rank_preferred_account_first(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "oc_default", "name": "Backend", "type": "group", "account_id": "default", "source": "config"},
                {"id": "feishu-cn::oc_cn", "name": "Backend", "type": "group", "account_id": "feishu-cn", "source": "live"},
                {"id": "feishu-cn::oc_prefix", "name": "Backend Ops", "type": "group", "account_id": "feishu-cn", "source": "live"},
            ]
        }
        with self._setup(tmp_path, platforms):
            result = explain_channel_name_resolution("feishu", "Backend", preferred_account_id="feishu-cn")

        assert result["status"] == "resolved"
        assert result["resolved_id"] == "feishu-cn::oc_cn"

    def test_feishu_ambiguity_suggestions_include_reason_and_sorting(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "oc_default", "name": "Backend", "type": "group", "account_id": "default", "source": "config"},
                {"id": "feishu-cn::oc_1", "name": "Backend", "type": "group", "account_id": "feishu-cn", "source": "live"},
                {"id": "feishu-cn::oc_2", "name": "Backend", "type": "group", "account_id": "feishu-cn", "source": "live_search"},
            ]
        }
        with self._setup(tmp_path, platforms):
            result = explain_channel_name_resolution("feishu", "Backend", preferred_account_id="feishu-cn")

        assert result["status"] == "ambiguous"
        assert [item["id"] for item in result["suggestions"]] == [
            "feishu-cn::oc_1",
            "feishu-cn::oc_2",
            "oc_default",
        ]
        assert result["suggestions"][0]["reason"] == "exact name match, preferred account, live directory, group target"
        assert result["suggestions"][2]["reason"] == "exact name match, config-backed, group target"

    def test_feishu_group_targets_rank_ahead_of_dm_targets(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "ou_alice", "name": "Backend", "type": "dm", "account_id": "default", "source": "config"},
                {"id": "oc_backend", "name": "Backend", "type": "group", "account_id": "default", "source": "config"},
            ]
        }
        with self._setup(tmp_path, platforms):
            result = explain_channel_name_resolution("feishu", "Backend")

        assert result["status"] == "ambiguous"
        assert [item["id"] for item in result["suggestions"]] == ["oc_backend", "ou_alice"]
        assert result["suggestions"][0]["reason"] == "exact name match, config-backed, group target"
        assert result["suggestions"][1]["reason"] == "exact name match, config-backed, dm target"

    def test_feishu_preferred_target_ranks_first_in_ambiguity(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "oc_home", "name": "Backend", "type": "group", "account_id": "default", "source": "config"},
                {"id": "oc_other", "name": "Backend", "type": "group", "account_id": "default", "source": "config"},
            ]
        }
        with self._setup(tmp_path, platforms):
            result = explain_channel_name_resolution(
                "feishu",
                "Backend",
                preferred_target_ids=["oc_other"],
            )

        assert result["status"] == "ambiguous"
        assert [item["id"] for item in result["suggestions"]] == ["oc_other", "oc_home"]
        assert result["suggestions"][0]["reason"] == "exact name match, preferred target, config-backed, group target"

    def test_feishu_recent_session_ranks_ahead_of_non_recent_target(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "oc_recent", "name": "Backend", "type": "group", "account_id": "default", "source": "live"},
                {"id": "oc_other", "name": "Backend", "type": "group", "account_id": "default", "source": "live"},
            ]
        }
        with self._setup(tmp_path, platforms), patch(
            "gateway.channel_directory._load_recent_session_target_ids",
            return_value=["oc_recent", "oc_other"],
        ):
            result = explain_channel_name_resolution("feishu", "Backend")

        assert result["status"] == "ambiguous"
        assert [item["id"] for item in result["suggestions"]] == ["oc_recent", "oc_other"]
        assert result["suggestions"][0]["reason"] == "exact name match, recent session, live directory, group target"

    def test_feishu_recent_successful_send_ranks_ahead_of_recent_session(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "oc_sent", "name": "Backend", "type": "group", "account_id": "default", "source": "live"},
                {"id": "oc_recent", "name": "Backend", "type": "group", "account_id": "default", "source": "live"},
            ]
        }
        with self._setup(tmp_path, platforms), patch(
            "gateway.channel_directory._load_recent_successful_target_ids",
            return_value=["oc_sent"],
        ), patch(
            "gateway.channel_directory._load_recent_session_target_ids",
            return_value=["oc_recent"],
        ):
            result = explain_channel_name_resolution("feishu", "Backend")

        assert result["status"] == "ambiguous"
        assert [item["id"] for item in result["suggestions"]] == ["oc_sent", "oc_recent"]
        assert result["suggestions"][0]["reason"] == "exact name match, recent successful send, live directory, group target"

    def test_no_channels_returns_none(self, tmp_path):
        with self._setup(tmp_path, {}):
            assert resolve_channel_name("telegram", "someone") is None

    def test_no_match_returns_none(self, tmp_path):
        platforms = {
            "telegram": [{"id": "123", "name": "John", "type": "dm"}]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("telegram", "nonexistent") is None

    def test_topic_name_resolves_to_composite_id(self, tmp_path):
        platforms = {
            "telegram": [{"id": "-1001:17585", "name": "Coaching Chat / topic 17585", "type": "group"}]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("telegram", "Coaching Chat / topic 17585") == "-1001:17585"

    def test_display_label_with_type_suffix_resolves(self, tmp_path):
        platforms = {
            "telegram": [
                {"id": "123", "name": "Alice", "type": "dm"},
                {"id": "456", "name": "Dev Group", "type": "group"},
                {"id": "-1001:17585", "name": "Coaching Chat / topic 17585", "type": "group"},
            ]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("telegram", "Alice (dm)") == "123"
            assert resolve_channel_name("telegram", "Dev Group (group)") == "456"
            assert resolve_channel_name("telegram", "Coaching Chat / topic 17585 (group)") == "-1001:17585"

    def test_feishu_account_qualified_label_resolves(self, tmp_path):
        platforms = {
            "feishu": [
                {"id": "ou_alice", "name": "Alice", "type": "dm", "account_id": "default"},
                {"id": "feishu-cn::ou_bob", "name": "Bob", "type": "dm", "account_id": "feishu-cn"},
            ]
        }
        with self._setup(tmp_path, platforms):
            assert resolve_channel_name("feishu", "Alice (dm)") == "ou_alice"
            assert resolve_channel_name("feishu", "feishu-cn/Bob (dm)") == "feishu-cn::ou_bob"


class TestBuildFromSessions:
    def _write_sessions(self, tmp_path, sessions_data):
        """Write sessions.json at the path _build_from_sessions expects."""
        sessions_path = tmp_path / "sessions" / "sessions.json"
        sessions_path.parent.mkdir(parents=True)
        sessions_path.write_text(json.dumps(sessions_data))

    def test_builds_from_sessions_json(self, tmp_path):
        self._write_sessions(tmp_path, {
            "session_1": {
                "origin": {
                    "platform": "telegram",
                    "chat_id": "12345",
                    "chat_name": "Alice",
                },
                "chat_type": "dm",
            },
            "session_2": {
                "origin": {
                    "platform": "telegram",
                    "chat_id": "67890",
                    "user_name": "Bob",
                },
                "chat_type": "group",
            },
            "session_3": {
                "origin": {
                    "platform": "discord",
                    "chat_id": "99999",
                },
            },
        })

        with patch.dict(os.environ, {"HERMES_HOME": str(tmp_path)}):
            entries = _build_from_sessions("telegram")

        assert len(entries) == 2
        names = {e["name"] for e in entries}
        assert "Alice" in names
        assert "Bob" in names

    def test_missing_sessions_file(self, tmp_path):
        with patch.dict(os.environ, {"HERMES_HOME": str(tmp_path)}):
            entries = _build_from_sessions("telegram")
        assert entries == []

    def test_deduplication_by_chat_id(self, tmp_path):
        self._write_sessions(tmp_path, {
            "s1": {"origin": {"platform": "telegram", "chat_id": "123", "chat_name": "X"}},
            "s2": {"origin": {"platform": "telegram", "chat_id": "123", "chat_name": "X"}},
        })

        with patch.dict(os.environ, {"HERMES_HOME": str(tmp_path)}):
            entries = _build_from_sessions("telegram")

        assert len(entries) == 1

    def test_keeps_distinct_topics_with_same_chat_id(self, tmp_path):
        self._write_sessions(tmp_path, {
            "group_root": {
                "origin": {"platform": "telegram", "chat_id": "-1001", "chat_name": "Coaching Chat"},
                "chat_type": "group",
            },
            "topic_a": {
                "origin": {
                    "platform": "telegram",
                    "chat_id": "-1001",
                    "chat_name": "Coaching Chat",
                    "thread_id": "17585",
                },
                "chat_type": "group",
            },
            "topic_b": {
                "origin": {
                    "platform": "telegram",
                    "chat_id": "-1001",
                    "chat_name": "Coaching Chat",
                    "thread_id": "17587",
                },
                "chat_type": "group",
            },
        })

        with patch.dict(os.environ, {"HERMES_HOME": str(tmp_path)}):
            entries = _build_from_sessions("telegram")

        ids = {entry["id"] for entry in entries}
        names = {entry["name"] for entry in entries}
        assert ids == {"-1001", "-1001:17585", "-1001:17587"}
        assert "Coaching Chat" in names
        assert "Coaching Chat / topic 17585" in names
        assert "Coaching Chat / topic 17587" in names


class TestFormatDirectoryForDisplay:
    def test_empty_directory(self, tmp_path):
        with patch("gateway.channel_directory.DIRECTORY_PATH", tmp_path / "nope.json"):
            result = format_directory_for_display()
        assert "No messaging platforms" in result

    def test_telegram_display(self, tmp_path):
        cache_file = _write_directory(tmp_path, {
            "telegram": [
                {"id": "123", "name": "Alice", "type": "dm"},
                {"id": "456", "name": "Dev Group", "type": "group"},
                {"id": "-1001:17585", "name": "Coaching Chat / topic 17585", "type": "group"},
            ]
        })
        with patch("gateway.channel_directory.DIRECTORY_PATH", cache_file):
            result = format_directory_for_display()

        assert "Telegram:" in result
        assert "telegram:Alice" in result
        assert "telegram:Dev Group" in result
        assert "telegram:Coaching Chat / topic 17585" in result

    def test_discord_grouped_by_guild(self, tmp_path):
        cache_file = _write_directory(tmp_path, {
            "discord": [
                {"id": "1", "name": "general", "guild": "Server1", "type": "channel"},
                {"id": "2", "name": "bot-home", "guild": "Server1", "type": "channel"},
                {"id": "3", "name": "chat", "guild": "Server2", "type": "channel"},
            ]
        })
        with patch("gateway.channel_directory.DIRECTORY_PATH", cache_file):
            result = format_directory_for_display()

        assert "Discord (Server1):" in result
        assert "Discord (Server2):" in result
        assert "discord:#general" in result
