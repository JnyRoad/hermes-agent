"""
Channel directory -- cached map of reachable channels/contacts per platform.

Built on gateway startup, refreshed periodically (every 5 min), and saved to
~/.hermes/channel_directory.json.  The send_message tool reads this file for
action="list" and for resolving human-friendly channel names to numeric IDs.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from hermes_cli.config import get_hermes_home
from utils import atomic_json_write

logger = logging.getLogger(__name__)

DIRECTORY_PATH = get_hermes_home() / "channel_directory.json"

# 会话发现只适用于消息平台。这里显式维护平台清单，避免基础设施型平台
# 被隐式带入目录，也让新增平台的行为更清晰可审查。
_SESSION_DISCOVERY_PLATFORMS = frozenset({
    "discord",
    "email",
    "feishu",
    "homeassistant",
    "matrix",
    "signal",
    "slack",
    "telegram",
    "whatsapp",
})


def _normalize_channel_query(value: str) -> str:
    return value.lstrip("#").strip().lower()


def _channel_target_name(platform_name: str, channel: Dict[str, Any]) -> str:
    """Return the human-facing target label shown to users for a channel entry."""
    name = channel["name"]
    if platform_name == "feishu":
        account_id = str(channel.get("account_id", "") or "").strip()
        base = f"{name} ({channel['type']})" if channel.get("type") else name
        if account_id and account_id != "default":
            return f"{account_id}/{base}"
        return base
    if platform_name == "discord" and channel.get("guild"):
        return f"#{name}"
    if platform_name != "discord" and channel.get("type"):
        return f"{name} ({channel['type']})"
    return name


def _collect_channel_resolution_candidates(
    platform_name: str,
    channels: List[Dict[str, Any]],
    query: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Collect exact and prefix channel matches for diagnostics and resolution."""
    if not channels:
        return {"exact": [], "prefix": []}

    exact_matches: List[Dict[str, Any]] = []
    prefix_matches: List[Dict[str, Any]] = []
    seen_exact: set[str] = set()
    seen_prefix: set[str] = set()

    for ch in channels:
        channel_id = str(ch.get("id", "")).strip()
        if not channel_id:
            continue
        if _normalize_channel_query(ch["name"]) == query:
            if channel_id not in seen_exact:
                exact_matches.append(ch)
                seen_exact.add(channel_id)
        if _normalize_channel_query(_channel_target_name(platform_name, ch)) == query:
            if channel_id not in seen_exact:
                exact_matches.append(ch)
                seen_exact.add(channel_id)

    # Guild-qualified match for Discord ("GuildName/channel")
    if platform_name == "discord" and "/" in query:
        guild_part, ch_part = query.rsplit("/", 1)
        for ch in channels:
            channel_id = str(ch.get("id", "")).strip()
            if not channel_id:
                continue
            guild = ch.get("guild", "").strip().lower()
            if guild == guild_part and _normalize_channel_query(ch["name"]) == ch_part and channel_id not in seen_exact:
                exact_matches.append(ch)
                seen_exact.add(channel_id)

    for ch in channels:
        channel_id = str(ch.get("id", "")).strip()
        if not channel_id:
            continue
        if _normalize_channel_query(ch["name"]).startswith(query) and channel_id not in seen_prefix:
            prefix_matches.append(ch)
            seen_prefix.add(channel_id)
        display_query = _normalize_channel_query(_channel_target_name(platform_name, ch))
        if display_query.startswith(query) and channel_id not in seen_prefix:
            prefix_matches.append(ch)
            seen_prefix.add(channel_id)

    return {"exact": exact_matches, "prefix": prefix_matches}


def _select_resolved_channel_id(platform_name: str, channels: List[Dict[str, Any]], query: str) -> Optional[str]:
    """Resolve a normalized query against a channel list using display-aware matching."""
    candidates = _collect_channel_resolution_candidates(platform_name, channels, query)
    exact_matches = candidates["exact"]
    if len(exact_matches) == 1:
        return exact_matches[0]["id"]
    prefix_matches = candidates["prefix"]
    if not exact_matches and len(prefix_matches) == 1:
        return prefix_matches[0]["id"]
    return None


def _resolve_feishu_live_channel_name(query: str) -> Optional[str]:
    """Best-effort Feishu live lookup when the cached directory does not contain a match."""
    try:
        from gateway.config import Platform, load_gateway_config
        from gateway.platforms.feishu import FeishuAdapter
    except Exception:
        return None

    try:
        config = load_gateway_config()
    except Exception:
        return None

    platform_config = (config.platforms or {}).get(Platform.FEISHU)
    if not platform_config or not getattr(platform_config, "enabled", False):
        return None

    try:
        adapter = FeishuAdapter(platform_config)
        live_entries = adapter.search_channel_directory_entries(query, limit_per_account=10)
    except Exception:
        logger.debug("Channel directory: failed live Feishu search for %s", query, exc_info=True)
        return None
    return _select_resolved_channel_id("feishu", live_entries, query)


def explain_channel_name_resolution(platform_name: str, name: str) -> Dict[str, Any]:
    """Explain how a channel target would resolve, including ambiguity details."""
    query = _normalize_channel_query(name)
    directory = load_directory()
    cached_channels = directory.get("platforms", {}).get(platform_name, [])
    cached_candidates = _collect_channel_resolution_candidates(platform_name, cached_channels, query)
    resolved_id = _select_resolved_channel_id(platform_name, cached_channels, query)
    source = "cache" if resolved_id else None
    live_candidates: Dict[str, List[Dict[str, Any]]] | None = None
    live_channels: List[Dict[str, Any]] = []

    if platform_name == "feishu" and not resolved_id:
        try:
            from gateway.config import Platform, load_gateway_config
            from gateway.platforms.feishu import FeishuAdapter

            config = load_gateway_config()
            platform_config = (config.platforms or {}).get(Platform.FEISHU)
            if platform_config and getattr(platform_config, "enabled", False):
                adapter = FeishuAdapter(platform_config)
                live_channels = adapter.search_channel_directory_entries(query, limit_per_account=10)
                live_candidates = _collect_channel_resolution_candidates(platform_name, live_channels, query)
                resolved_id = _select_resolved_channel_id(platform_name, live_channels, query)
                if resolved_id:
                    source = "live_search"
        except Exception:
            logger.debug("Channel directory: failed to explain live Feishu search for %s", query, exc_info=True)

    candidates = cached_candidates if source == "cache" or live_candidates is None else live_candidates
    exact_matches = list(candidates.get("exact", []))
    prefix_matches = list(candidates.get("prefix", []))
    if exact_matches:
        candidate_entries = exact_matches
    elif prefix_matches:
        candidate_entries = prefix_matches
    else:
        candidate_entries = []
    suggestions = [
        {
            "id": item["id"],
            "label": _channel_target_name(platform_name, item),
            "source": item.get("source"),
            "account_id": item.get("account_id"),
        }
        for item in candidate_entries[:5]
        if isinstance(item, dict)
    ]
    status = "resolved" if resolved_id else ("ambiguous" if len(candidate_entries) > 1 else "not_found")
    return {
        "status": status,
        "resolved_id": resolved_id,
        "source": source,
        "suggestions": suggestions,
    }


def _session_entry_id(origin: Dict[str, Any]) -> Optional[str]:
    chat_id = origin.get("chat_id")
    if not chat_id:
        return None
    platform = str(origin.get("platform", "") or "").strip().lower()
    account_id = str(origin.get("account_id", "") or "").strip()
    thread_id = origin.get("thread_id")
    base_chat_id = str(chat_id)
    if platform == "feishu" and account_id and account_id != "default":
        base_chat_id = f"{account_id}::{base_chat_id}"
    if thread_id:
        return f"{base_chat_id}:{thread_id}"
    return base_chat_id


def _session_entry_name(origin: Dict[str, Any]) -> str:
    base_name = origin.get("chat_name") or origin.get("user_name") or str(origin.get("chat_id"))
    thread_id = origin.get("thread_id")
    if not thread_id:
        return base_name

    topic_label = origin.get("chat_topic") or f"topic {thread_id}"
    return f"{base_name} / {topic_label}"


# ---------------------------------------------------------------------------
# Build / refresh
# ---------------------------------------------------------------------------

def build_channel_directory(adapters: Dict[Any, Any]) -> Dict[str, Any]:
    """
    Build a channel directory from connected platform adapters and session data.

    Returns the directory dict and writes it to DIRECTORY_PATH.
    """
    from gateway.config import Platform

    platforms: Dict[str, List[Dict[str, str]]] = {}

    for platform, adapter in adapters.items():
        try:
            if platform == Platform.DISCORD:
                platforms["discord"] = _build_discord(adapter)
            elif platform == Platform.SLACK:
                platforms["slack"] = _build_slack(adapter)
            elif platform == Platform.FEISHU:
                platforms["feishu"] = _build_feishu(adapter)
        except Exception as e:
            logger.warning("Channel directory: failed to build %s: %s", platform.value, e)

    # Session discovery explicitly covers messaging platforms like "email"
    # that do not have a direct channel enumeration API.
    for plat in Platform:
        plat_name = plat.value
        if plat_name not in _SESSION_DISCOVERY_PLATFORMS or plat_name in platforms:
            continue
        platforms[plat_name] = _build_from_sessions(plat_name)

    directory = {
        "updated_at": datetime.now().isoformat(),
        "platforms": platforms,
    }

    try:
        atomic_json_write(DIRECTORY_PATH, directory)
    except Exception as e:
        logger.warning("Channel directory: failed to write: %s", e)

    return directory


def _build_discord(adapter) -> List[Dict[str, str]]:
    """Enumerate all text channels the Discord bot can see."""
    channels = []
    client = getattr(adapter, "_client", None)
    if not client:
        return channels

    try:
        import discord as _discord  # noqa: F401 — SDK presence check
    except ImportError:
        return channels

    for guild in client.guilds:
        for ch in guild.text_channels:
            channels.append({
                "id": str(ch.id),
                "name": ch.name,
                "guild": guild.name,
                "type": "channel",
            })
        # Also include DM-capable users we've interacted with is not
        # feasible via guild enumeration; those come from sessions.

    # Merge any DMs from session history
    channels.extend(_build_from_sessions("discord"))
    return channels


def _build_slack(adapter) -> List[Dict[str, str]]:
    """List Slack channels the bot has joined."""
    # Slack adapter may expose a web client
    client = getattr(adapter, "_app", None) or getattr(adapter, "_client", None)
    if not client:
        return _build_from_sessions("slack")

    try:
        from tools.send_message_tool import _send_slack  # noqa: F401
        # Use the Slack Web API directly if available
    except Exception:
        pass

    # Fallback to session data
    return _build_from_sessions("slack")


def _build_feishu(adapter) -> List[Dict[str, str]]:
    """Use Feishu's native directory when available, then merge session discovery."""
    entries: List[Dict[str, str]] = []
    if hasattr(adapter, "build_channel_directory_entries"):
        try:
            native_entries = adapter.build_channel_directory_entries(include_live=True)
            if isinstance(native_entries, list):
                entries.extend(item for item in native_entries if isinstance(item, dict))
        except Exception as exc:
            logger.warning("Channel directory: failed to build native feishu directory: %s", exc)

    session_entries = _build_from_sessions("feishu")
    seen_ids = {str(item.get("id", "")).strip() for item in entries if str(item.get("id", "")).strip()}
    for item in session_entries:
        entry_id = str(item.get("id", "")).strip()
        if not entry_id or entry_id in seen_ids:
            continue
        seen_ids.add(entry_id)
        entries.append(item)
    return entries


def _build_from_sessions(platform_name: str) -> List[Dict[str, str]]:
    """Pull known channels/contacts from sessions.json origin data."""
    sessions_path = get_hermes_home() / "sessions" / "sessions.json"
    if not sessions_path.exists():
        return []

    entries = []
    try:
        with open(sessions_path, encoding="utf-8") as f:
            data = json.load(f)

        seen_ids = set()
        for _key, session in data.items():
            origin = session.get("origin") or {}
            if origin.get("platform") != platform_name:
                continue
            entry_id = _session_entry_id(origin)
            if not entry_id or entry_id in seen_ids:
                continue
            seen_ids.add(entry_id)
            entries.append({
                "id": entry_id,
                "name": _session_entry_name(origin),
                "type": session.get("chat_type", "dm"),
                "thread_id": origin.get("thread_id"),
                "account_id": origin.get("account_id"),
            })
    except Exception as e:
        logger.debug("Channel directory: failed to read sessions for %s: %s", platform_name, e)

    return entries


# ---------------------------------------------------------------------------
# Read / resolve
# ---------------------------------------------------------------------------

def load_directory() -> Dict[str, Any]:
    """Load the cached channel directory from disk."""
    if not DIRECTORY_PATH.exists():
        return {"updated_at": None, "platforms": {}}
    try:
        with open(DIRECTORY_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"updated_at": None, "platforms": {}}


def resolve_channel_name(platform_name: str, name: str) -> Optional[str]:
    """
    Resolve a human-friendly channel name to a numeric ID.

    Matching strategy (case-insensitive, first match wins):
    - Discord: "bot-home", "#bot-home", "GuildName/bot-home"
    - Telegram: display name or group name
    - Slack: "engineering", "#engineering"
    """
    return explain_channel_name_resolution(platform_name, name).get("resolved_id")


def format_directory_for_display() -> str:
    """Format the channel directory as a human-readable list for the model."""
    directory = load_directory()
    platforms = directory.get("platforms", {})

    if not any(platforms.values()):
        return "No messaging platforms connected or no channels discovered yet."

    lines = ["Available messaging targets:\n"]

    for plat_name, channels in sorted(platforms.items()):
        if not channels:
            continue

        # Group Discord channels by guild
        if plat_name == "discord":
            guilds: Dict[str, List] = {}
            dms: List = []
            for ch in channels:
                guild = ch.get("guild")
                if guild:
                    guilds.setdefault(guild, []).append(ch)
                else:
                    dms.append(ch)

            for guild_name, guild_channels in sorted(guilds.items()):
                lines.append(f"Discord ({guild_name}):")
                for ch in sorted(guild_channels, key=lambda c: c["name"]):
                    lines.append(f"  discord:{_channel_target_name(plat_name, ch)}")
            if dms:
                lines.append("Discord (DMs):")
                for ch in dms:
                    lines.append(f"  discord:{_channel_target_name(plat_name, ch)}")
            lines.append("")
        else:
            lines.append(f"{plat_name.title()}:")
            for ch in channels:
                lines.append(f"  {plat_name}:{_channel_target_name(plat_name, ch)}")
            lines.append("")

    lines.append('Use these as the "target" parameter when sending.')
    lines.append('Bare platform name (e.g. "telegram") sends to home channel.')

    return "\n".join(lines)
