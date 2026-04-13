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


def _select_preferred_feishu_candidate(
    matches: List[Dict[str, Any]],
    preferred_account_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Select one unique match from the caller's preferred Feishu account."""
    if not preferred_account_id or not matches:
        return None

    normalized_account_id = str(preferred_account_id or "").strip() or "default"
    preferred_matches = [
        item
        for item in matches
        if str(item.get("account_id", "") or "default").strip() == normalized_account_id
    ]
    if len(preferred_matches) == 1:
        return preferred_matches[0]
    return None


def _rank_resolution_candidate(
    platform_name: str,
    channel: Dict[str, Any],
    *,
    match_type: str,
    preferred_account_id: Optional[str],
    preferred_target_ranks: Dict[str, int],
    recent_target_ranks: Dict[str, int],
) -> tuple:
    """Return a stable sort key for ambiguity suggestions.

    The ranking keeps exact matches ahead of prefixes, then prefers the current
    Feishu account, then cached/config-backed entries, and finally sorts by
    human-facing label for determinism.
    """
    normalized_match_type = "exact" if match_type == "exact" else "prefix"
    match_rank = 0 if normalized_match_type == "exact" else 1
    channel_id = str(channel.get("id", "")).strip()
    target_rank = preferred_target_ranks.get(channel_id, len(preferred_target_ranks) + 1)
    recent_rank = recent_target_ranks.get(channel_id, len(recent_target_ranks) + 1)
    normalized_account_id = str(channel.get("account_id", "") or "default").strip() or "default"
    preferred_account = str(preferred_account_id or "").strip() or ""
    account_rank = 0 if preferred_account and normalized_account_id == preferred_account else 1
    channel_type = str(channel.get("type", "") or "").strip().lower()
    type_rank = 0
    if platform_name == "feishu":
        # Feishu business tools most often route follow-ups back into group chats.
        # When an operator gives an unqualified name, surfacing group targets ahead
        # of DMs makes the ambiguity list more action-oriented without changing the
        # conservative uniqueness requirement.
        type_rank = {
            "group": 0,
            "dm": 1,
        }.get(channel_type, 2)
    source = str(channel.get("source", "") or "unknown").strip() or "unknown"
    source_rank = {
        "config": 0,
        "live": 1,
        "live_search": 2,
        "session": 3,
        "unknown": 4,
    }.get(source, 5)
    label = _channel_target_name(platform_name, channel).lower()
    return (match_rank, target_rank, recent_rank, account_rank, type_rank, source_rank, label, channel_id)


def _load_recent_session_target_ids(platform_name: str, *, limit: int = 10) -> List[str]:
    """Return recently active session target IDs for one platform."""
    sessions_path = get_hermes_home() / "sessions" / "sessions.json"
    if not sessions_path.exists():
        return []

    ranked_entries: List[tuple[datetime, str]] = []
    try:
        with open(sessions_path, encoding="utf-8") as f:
            data = json.load(f)
        for session in data.values():
            if not isinstance(session, dict):
                continue
            origin = session.get("origin") or {}
            if origin.get("platform") != platform_name:
                continue
            entry_id = _session_entry_id(origin)
            updated_at_raw = str(session.get("updated_at", "") or "").strip()
            if not entry_id or not updated_at_raw:
                continue
            try:
                ranked_entries.append((datetime.fromisoformat(updated_at_raw), entry_id))
            except ValueError:
                continue
    except Exception as exc:
        logger.debug("Channel directory: failed to load recent session targets for %s: %s", platform_name, exc)
        return []

    seen_ids: set[str] = set()
    recent_ids: List[str] = []
    for _, entry_id in sorted(ranked_entries, reverse=True):
        if entry_id in seen_ids:
            continue
        seen_ids.add(entry_id)
        recent_ids.append(entry_id)
        if len(recent_ids) >= limit:
            break
    return recent_ids


def _build_resolution_suggestions(
    platform_name: str,
    exact_matches: List[Dict[str, Any]],
    prefix_matches: List[Dict[str, Any]],
    *,
    preferred_account_id: Optional[str],
    preferred_target_ranks: Dict[str, int],
    recent_target_ranks: Dict[str, int],
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Build ranked ambiguity suggestions with ranking explanations."""
    ranked_candidates: List[tuple[tuple, Dict[str, Any], str]] = []
    seen_ids: set[str] = set()

    for match_type, candidates in (("exact", exact_matches), ("prefix", prefix_matches)):
        for item in candidates:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id", "")).strip()
            if not item_id or item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            ranked_candidates.append(
                (
                    _rank_resolution_candidate(
                        platform_name,
                        item,
                        match_type=match_type,
                        preferred_account_id=preferred_account_id,
                        preferred_target_ranks=preferred_target_ranks,
                        recent_target_ranks=recent_target_ranks,
                    ),
                    item,
                    match_type,
                )
            )

    suggestions: List[Dict[str, Any]] = []
    for _, item, match_type in sorted(ranked_candidates)[:limit]:
        account_id = str(item.get("account_id", "") or "default").strip() or "default"
        item_id = str(item.get("id", "")).strip()
        reason_parts = [f"{match_type} name match"]
        if item_id and item_id in preferred_target_ranks:
            reason_parts.append("preferred target")
        elif item_id and item_id in recent_target_ranks:
            reason_parts.append("recent session")
        if preferred_account_id and account_id == (str(preferred_account_id).strip() or "default"):
            reason_parts.append("preferred account")
        source = str(item.get("source", "") or "unknown").strip() or "unknown"
        candidate_type = str(item.get("type", "") or "").strip().lower()
        if source == "config":
            reason_parts.append("config-backed")
        elif source == "live":
            reason_parts.append("live directory")
        elif source == "live_search":
            reason_parts.append("live search")
        if platform_name == "feishu" and candidate_type in {"group", "dm"}:
            reason_parts.append(f"{candidate_type} target")
        suggestions.append(
            {
                "id": item["id"],
                "label": _channel_target_name(platform_name, item),
                "source": source,
                "account_id": account_id,
                "type": candidate_type,
                "match_type": match_type,
                "reason": ", ".join(reason_parts),
            }
        )
    return suggestions


def _select_resolved_channel(
    platform_name: str,
    channels: List[Dict[str, Any]],
    query: str,
    *,
    preferred_account_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve a normalized query against a channel list using display-aware matching."""
    candidates = _collect_channel_resolution_candidates(platform_name, channels, query)
    exact_matches = candidates["exact"]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if platform_name == "feishu":
        preferred_exact = _select_preferred_feishu_candidate(exact_matches, preferred_account_id)
        if preferred_exact is not None:
            return preferred_exact
    prefix_matches = candidates["prefix"]
    if not exact_matches and len(prefix_matches) == 1:
        return prefix_matches[0]
    if platform_name == "feishu" and not exact_matches:
        preferred_prefix = _select_preferred_feishu_candidate(prefix_matches, preferred_account_id)
        if preferred_prefix is not None:
            return preferred_prefix
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
    resolved = _select_resolved_channel("feishu", live_entries, query)
    return str(resolved.get("id", "")).strip() if resolved else None


def explain_channel_name_resolution(
    platform_name: str,
    name: str,
    *,
    preferred_account_id: Optional[str] = None,
    preferred_target_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Explain how a channel target would resolve, including ambiguity details."""
    query = _normalize_channel_query(name)
    directory = load_directory()
    cached_channels = directory.get("platforms", {}).get(platform_name, [])
    cached_candidates = _collect_channel_resolution_candidates(platform_name, cached_channels, query)
    resolved_entry = _select_resolved_channel(
        platform_name,
        cached_channels,
        query,
        preferred_account_id=preferred_account_id,
    )
    resolved_id = str(resolved_entry.get("id", "")).strip() if resolved_entry else None
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
                resolved_entry = _select_resolved_channel(
                    platform_name,
                    live_channels,
                    query,
                    preferred_account_id=preferred_account_id,
                )
                resolved_id = str(resolved_entry.get("id", "")).strip() if resolved_entry else None
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
    normalized_preferred_targets = [
        str(item or "").strip()
        for item in (preferred_target_ids or [])
        if str(item or "").strip()
    ]
    preferred_target_ranks = {
        target_id: index
        for index, target_id in enumerate(normalized_preferred_targets)
    }
    recent_target_ids = _load_recent_session_target_ids(platform_name) if platform_name == "feishu" else []
    recent_target_ranks = {
        target_id: index
        for index, target_id in enumerate(recent_target_ids)
    }
    suggestions = _build_resolution_suggestions(
        platform_name,
        exact_matches,
        prefix_matches if not exact_matches else [],
        preferred_account_id=preferred_account_id,
        preferred_target_ranks=preferred_target_ranks,
        recent_target_ranks=recent_target_ranks,
    )
    status = "resolved" if resolved_id else ("ambiguous" if len(candidate_entries) > 1 else "not_found")
    return {
        "status": status,
        "resolved_id": resolved_id,
        "source": source,
        "suggestions": suggestions,
        "preferred_account_id": preferred_account_id,
        "preferred_target_ids": normalized_preferred_targets,
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


def resolve_channel_name(
    platform_name: str,
    name: str,
    *,
    preferred_account_id: Optional[str] = None,
    preferred_target_ids: Optional[List[str]] = None,
) -> Optional[str]:
    """
    Resolve a human-friendly channel name to a numeric ID.

    Matching strategy (case-insensitive, first match wins):
    - Discord: "bot-home", "#bot-home", "GuildName/bot-home"
    - Telegram: display name or group name
    - Slack: "engineering", "#engineering"
    """
    return explain_channel_name_resolution(
        platform_name,
        name,
        preferred_account_id=preferred_account_id,
        preferred_target_ids=preferred_target_ids,
    ).get("resolved_id")


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
