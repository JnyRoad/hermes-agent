"""Send Message Tool -- cross-channel messaging via platform APIs.

Sends a message to a user or channel on any connected messaging platform
(Telegram, Discord, Slack). Supports listing available targets and resolving
human-friendly channel names to IDs. Works in both CLI and gateway contexts.
"""

import json
import logging
import os
import re
import ssl
import time
from datetime import datetime, timezone

from agent.redact import redact_sensitive_text
from hermes_cli.config import get_hermes_home
from utils import atomic_json_write

logger = logging.getLogger(__name__)

_TELEGRAM_TOPIC_TARGET_RE = re.compile(r"^\s*(-?\d+)(?::(\d+))?\s*$")
_FEISHU_TARGET_RE = re.compile(
    r"^\s*(?:(?P<account>[A-Za-z0-9._-]+)::)?(?P<target>(?:oc|ou|on|chat|open)_[\-A-Za-z0-9_]+)(?::(?P<thread>[-A-Za-z0-9_]+))?\s*$"
)
_WEIXIN_TARGET_RE = re.compile(r"^\s*((?:wxid|gh|v\d+|wm|wb)_[A-Za-z0-9_-]+|[A-Za-z0-9._-]+@chatroom|filehelper)\s*$")
# Discord snowflake IDs are numeric, same regex pattern as Telegram topic targets.
_NUMERIC_TOPIC_RE = _TELEGRAM_TOPIC_TARGET_RE
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".3gp"}
_AUDIO_EXTS = {".ogg", ".opus", ".mp3", ".wav", ".m4a"}
_VOICE_EXTS = {".ogg", ".opus"}
_URL_SECRET_QUERY_RE = re.compile(
    r"([?&](?:access_token|api[_-]?key|auth[_-]?token|token|signature|sig)=)([^&#\s]+)",
    re.IGNORECASE,
)
_GENERIC_SECRET_ASSIGN_RE = re.compile(
    r"\b(access_token|api[_-]?key|auth[_-]?token|signature|sig)\s*=\s*([^\s,;]+)",
    re.IGNORECASE,
)
_RECENT_SEND_TARGETS_PATH = get_hermes_home() / "recent_send_targets.json"
_RECENT_SEND_TARGET_LIMIT = 20


def _sanitize_error_text(text) -> str:
    """Redact secrets from error text before surfacing it to users/models."""
    redacted = redact_sensitive_text(text)
    redacted = _URL_SECRET_QUERY_RE.sub(lambda m: f"{m.group(1)}***", redacted)
    redacted = _GENERIC_SECRET_ASSIGN_RE.sub(lambda m: f"{m.group(1)}=***", redacted)
    return redacted


def _error(message: str) -> dict:
    """Build a standardized error payload with redacted content."""
    return {"error": _sanitize_error_text(message)}


SEND_MESSAGE_SCHEMA = {
    "name": "send_message",
    "description": (
        "Send a message to a connected messaging platform, or list available targets.\n\n"
        "IMPORTANT: When the user asks to send to a specific channel or person "
        "(not just a bare platform name), call send_message(action='list') FIRST to see "
        "available targets, then send to the correct one.\n"
        "If the user just says a platform name like 'send to telegram', send directly "
        "to the home channel without listing first."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["send", "list"],
                "description": "Action to perform. 'send' (default) sends a message. 'list' returns all available channels/contacts across connected platforms."
            },
            "target": {
                "type": "string",
                "description": "Delivery target. Format: 'platform' (uses home channel), 'platform:#channel-name', 'platform:chat_id', or 'platform:chat_id:thread_id' for Telegram topics and Discord threads. Feishu also supports account-qualified IDs like 'feishu:feishu-cn::oc_xxx'. Examples: 'telegram', 'telegram:-1001234567890:17585', 'discord:999888777:555444333', 'discord:#bot-home', 'slack:#engineering', 'signal:+155****4567', 'feishu:feishu-cn::oc_xxx'"
            },
            "message": {
                "type": "string",
                "description": "The message text to send"
            }
        },
        "required": []
    }
}


def send_message_tool(args, **kw):
    """Handle cross-channel send_message tool calls."""
    action = args.get("action", "send")

    if action == "list":
        return _handle_list()

    return _handle_send(args)


def _handle_list():
    """Return formatted list of available messaging targets."""
    try:
        from gateway.channel_directory import format_directory_for_display
        return json.dumps({"targets": format_directory_for_display()})
    except Exception as e:
        return json.dumps(_error(f"Failed to load channel directory: {e}"))


def _handle_send(args):
    """Send a message to a platform target."""
    target = args.get("target", "")
    message = args.get("message", "")
    if not target or not message:
        return tool_error("Both 'target' and 'message' are required when action='send'")

    parts = target.split(":", 1)
    platform_name = parts[0].strip().lower()
    target_ref = parts[1].strip() if len(parts) > 1 else None
    chat_id = None
    thread_id = None
    account_id = None
    config = None
    platform = None
    pconfig = None

    from gateway.config import Platform as GatewayPlatform

    platform_map = {
        "telegram": GatewayPlatform.TELEGRAM,
        "discord": GatewayPlatform.DISCORD,
        "slack": GatewayPlatform.SLACK,
        "whatsapp": GatewayPlatform.WHATSAPP,
        "signal": GatewayPlatform.SIGNAL,
        "bluebubbles": GatewayPlatform.BLUEBUBBLES,
        "matrix": GatewayPlatform.MATRIX,
        "mattermost": GatewayPlatform.MATTERMOST,
        "homeassistant": GatewayPlatform.HOMEASSISTANT,
        "dingtalk": GatewayPlatform.DINGTALK,
        "feishu": GatewayPlatform.FEISHU,
        "wecom": GatewayPlatform.WECOM,
        "weixin": GatewayPlatform.WEIXIN,
        "email": GatewayPlatform.EMAIL,
        "sms": GatewayPlatform.SMS,
    }
    platform = platform_map.get(platform_name)
    if not platform:
        avail = ", ".join(platform_map.keys())
        return tool_error(f"Unknown platform: {platform_name}. Available: {avail}")

    try:
        from gateway.config import load_gateway_config
        config = load_gateway_config()
    except Exception as e:
        return json.dumps(_error(f"Failed to load gateway config: {e}"))

    pconfig = config.platforms.get(platform)
    if not pconfig or not pconfig.enabled:
        return tool_error(f"Platform '{platform_name}' is not configured. Set up credentials in ~/.hermes/config.yaml or environment variables.")

    if target_ref:
        chat_id, thread_id, is_explicit = _parse_target_ref(platform_name, target_ref)
    else:
        is_explicit = False

    # Resolve human-friendly channel names to numeric IDs
    if target_ref and not is_explicit:
        try:
            from gateway.channel_directory import explain_channel_name_resolution

            preferred_account_id = _get_preferred_resolution_account_id(
                platform_name,
                config=config,
                platform_config=pconfig,
            )
            resolution = explain_channel_name_resolution(
                platform_name,
                target_ref,
                preferred_account_id=preferred_account_id,
                preferred_target_ids=_get_preferred_resolution_target_ids(
                    platform_name,
                    config=config,
                    platform_config=pconfig,
                ),
            )
            resolved = resolution.get("resolved_id")
            if resolved:
                chat_id, thread_id, _ = _parse_target_ref(platform_name, resolved)
            else:
                suggestions = resolution.get("suggestions") or []
                suggestion_text = _format_resolution_suggestions_for_error(suggestions)
                if resolution.get("status") == "ambiguous":
                    return json.dumps({
                        "error": f"Target '{target_ref}' is ambiguous on {platform_name}.{suggestion_text} Use a more specific target or an explicit channel ID."
                    })
                return json.dumps({
                    "error": f"Could not resolve '{target_ref}' on {platform_name}. "
                    f"Use send_message(action='list') to see available targets.{suggestion_text}"
                })
        except Exception:
            return json.dumps({
                "error": f"Could not resolve '{target_ref}' on {platform_name}. "
                f"Try using a numeric channel ID instead."
            })

    if platform_name == "feishu" and chat_id:
        chat_id, account_id = _split_feishu_account_target(chat_id)

    from tools.interrupt import is_interrupted
    if is_interrupted():
        return tool_error("Interrupted")

    from gateway.platforms.base import BasePlatformAdapter

    media_files, cleaned_message = BasePlatformAdapter.extract_media(message)
    mirror_text = cleaned_message.strip() or _describe_media_for_mirror(media_files)

    used_home_channel = False
    if not chat_id:
        home = config.get_home_channel(platform)
        if home:
            chat_id = home.chat_id
            used_home_channel = True
        else:
            return json.dumps({
                "error": f"No home channel set for {platform_name} to determine where to send the message. "
                f"Either specify a channel directly with '{platform_name}:CHANNEL_NAME', "
                f"or set a home channel via: hermes config set {platform_name.upper()}_HOME_CHANNEL <channel_id>"
            })

    duplicate_skip = _maybe_skip_cron_duplicate_send(platform_name, chat_id, thread_id)
    if duplicate_skip:
        return json.dumps(duplicate_skip)

    try:
        from model_tools import _run_async
        send_kwargs = {
            "thread_id": thread_id,
            "media_files": media_files,
        }
        if account_id:
            send_kwargs["account_id"] = account_id
        result = _run_async(
            _send_to_platform(
                platform,
                pconfig,
                chat_id,
                cleaned_message,
                **send_kwargs,
            )
        )
        if used_home_channel and isinstance(result, dict) and result.get("success"):
            result["note"] = f"Sent to {platform_name} home channel (chat_id: {chat_id})"
        if isinstance(result, dict) and result.get("success"):
            _record_recent_successful_send(
                platform_name,
                chat_id,
                thread_id=thread_id,
                account_id=account_id,
            )

        # Mirror the sent message into the target's gateway session
        if isinstance(result, dict) and result.get("success") and mirror_text:
            try:
                from gateway.mirror import mirror_to_session
                from gateway.session_context import get_session_env
                source_label = get_session_env("HERMES_SESSION_PLATFORM", "cli")
                if mirror_to_session(platform_name, chat_id, mirror_text, source_label=source_label, thread_id=thread_id):
                    result["mirrored"] = True
            except Exception:
                pass

        if isinstance(result, dict) and "error" in result:
            result["error"] = _sanitize_error_text(result["error"])
        return json.dumps(result)
    except Exception as e:
        return json.dumps(_error(f"Send failed: {e}"))


def _get_preferred_resolution_account_id(platform_name: str, *, config, platform_config) -> str | None:
    """Infer a safe account preference for human-readable target resolution.

    This is intentionally conservative. For Feishu, a tool call does not carry
    chat-local account context, so the best available hint is the configured
    home channel account. If no explicit account is encoded there, fall back to
    the primary default account. The resolver still only auto-selects when that
    preferred account yields one unique match.
    """
    if platform_name != "feishu" or config is None or platform_config is None:
        return None

    from gateway.config import Platform as GatewayPlatform

    home_channel = config.get_home_channel(GatewayPlatform.FEISHU)
    if home_channel and getattr(home_channel, "chat_id", None):
        _, preferred_account_id = _split_feishu_account_target(str(home_channel.chat_id))
        if preferred_account_id:
            return preferred_account_id
    return "default"


def _get_preferred_resolution_target_ids(platform_name: str, *, config, platform_config) -> list[str]:
    """Return explicit target IDs that should rank first during resolution."""
    if platform_name != "feishu" or config is None or platform_config is None:
        return []

    from gateway.config import Platform as GatewayPlatform

    preferred_target_ids: list[str] = []
    home_channel = config.get_home_channel(GatewayPlatform.FEISHU)
    if home_channel and getattr(home_channel, "chat_id", None):
        target_id = str(home_channel.chat_id or "").strip()
        if target_id:
            preferred_target_ids.append(target_id)

    for target_id in _load_recent_successful_target_ids(platform_name):
        if target_id not in preferred_target_ids:
            preferred_target_ids.append(target_id)
    return preferred_target_ids


def _normalize_recent_send_target_id(
    platform_name: str,
    chat_id: str,
    *,
    thread_id: str | None = None,
    account_id: str | None = None,
) -> str:
    """Build a stable directory target ID for recently successful sends."""
    normalized_chat_id = str(chat_id or "").strip()
    normalized_account_id = str(account_id or "").strip()
    normalized_thread_id = str(thread_id or "").strip()

    if platform_name == "feishu" and "::" in normalized_chat_id and not normalized_account_id:
        normalized_chat_id, normalized_account_id = _split_feishu_account_target(normalized_chat_id)
        normalized_account_id = normalized_account_id or ""

    if platform_name == "feishu" and normalized_account_id and normalized_account_id != "default":
        normalized_chat_id = f"{normalized_account_id}::{normalized_chat_id}"
    if normalized_thread_id:
        return f"{normalized_chat_id}:{normalized_thread_id}"
    return normalized_chat_id


def _load_recent_successful_target_ids(platform_name: str, *, limit: int = 10) -> list[str]:
    """Return the most recent successfully delivered targets for one platform."""
    if not _RECENT_SEND_TARGETS_PATH.exists():
        return []

    try:
        with open(_RECENT_SEND_TARGETS_PATH, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        logger.debug("send_message: failed to load recent target history: %s", exc)
        return []

    platform_entries = payload.get("platforms", {}).get(platform_name, [])
    if not isinstance(platform_entries, list):
        return []

    ranked_entries: list[tuple[datetime, str]] = []
    for item in platform_entries:
        if not isinstance(item, dict):
            continue
        target_id = str(item.get("id", "") or "").strip()
        updated_at_raw = str(item.get("updated_at", "") or "").strip()
        if not target_id or not updated_at_raw:
            continue
        try:
            ranked_entries.append((datetime.fromisoformat(updated_at_raw), target_id))
        except ValueError:
            continue

    seen_ids: set[str] = set()
    recent_ids: list[str] = []
    for _, target_id in sorted(ranked_entries, reverse=True):
        if target_id in seen_ids:
            continue
        seen_ids.add(target_id)
        recent_ids.append(target_id)
        if len(recent_ids) >= limit:
            break
    return recent_ids


def _record_recent_successful_send(
    platform_name: str,
    chat_id: str,
    *,
    thread_id: str | None = None,
    account_id: str | None = None,
) -> None:
    """Persist a stable recency signal for later channel-name resolution."""
    target_id = _normalize_recent_send_target_id(
        platform_name,
        chat_id,
        thread_id=thread_id,
        account_id=account_id,
    )
    if not target_id:
        return

    try:
        payload: dict[str, object] = {"platforms": {}}
        if _RECENT_SEND_TARGETS_PATH.exists():
            with open(_RECENT_SEND_TARGETS_PATH, encoding="utf-8") as f:
                existing_payload = json.load(f)
            if isinstance(existing_payload, dict):
                payload = existing_payload

        platforms = payload.setdefault("platforms", {})
        if not isinstance(platforms, dict):
            platforms = {}
            payload["platforms"] = platforms

        current_entries = platforms.get(platform_name, [])
        normalized_entries = [
            item
            for item in current_entries
            if isinstance(item, dict) and str(item.get("id", "") or "").strip() != target_id
        ]
        normalized_entries.append(
            {
                "id": target_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        normalized_entries.sort(key=lambda item: str(item.get("updated_at", "") or ""), reverse=True)
        platforms[platform_name] = normalized_entries[:_RECENT_SEND_TARGET_LIMIT]
        atomic_json_write(_RECENT_SEND_TARGETS_PATH, payload)
    except Exception as exc:
        logger.debug("send_message: failed to persist recent target history: %s", exc)


def _format_resolution_suggestions_for_error(suggestions: list[dict]) -> str:
    """Format ranked directory suggestions for tool-facing error messages."""
    if not suggestions:
        return ""

    parts: list[str] = []
    for item in suggestions:
        label = str(item.get("label", "") or "").strip()
        if not label:
            continue
        reason = str(item.get("reason", "") or "").strip()
        if reason:
            parts.append(f"{label} [{reason}]")
        else:
            parts.append(label)
    if not parts:
        return ""
    return f" Candidates: {', '.join(parts)}."


def _parse_target_ref(platform_name: str, target_ref: str):
    """Parse a tool target into chat_id/thread_id and whether it is explicit."""
    if platform_name == "telegram":
        match = _TELEGRAM_TOPIC_TARGET_RE.fullmatch(target_ref)
        if match:
            return match.group(1), match.group(2), True
    if platform_name == "feishu":
        match = _FEISHU_TARGET_RE.fullmatch(target_ref)
        if match:
            account_id = str(match.group("account") or "").strip()
            chat_id = str(match.group("target") or "").strip()
            thread_id = str(match.group("thread") or "").strip() or None
            encoded_chat_id = f"{account_id}::{chat_id}" if account_id else chat_id
            return encoded_chat_id, thread_id, True
    if platform_name == "discord":
        match = _NUMERIC_TOPIC_RE.fullmatch(target_ref)
        if match:
            return match.group(1), match.group(2), True
    if platform_name == "weixin":
        match = _WEIXIN_TARGET_RE.fullmatch(target_ref)
        if match:
            return match.group(1), None, True
    if target_ref.lstrip("-").isdigit():
        return target_ref, None, True
    return None, None, False


def _split_feishu_account_target(chat_id: str):
    """Split an account-qualified Feishu target into raw chat_id and account_id."""
    value = str(chat_id or "").strip()
    if "::" not in value:
        return value, None
    account_id, raw_chat_id = value.split("::", 1)
    account_id = account_id.strip()
    raw_chat_id = raw_chat_id.strip()
    if not account_id or not raw_chat_id:
        return value, None
    return raw_chat_id, account_id


def _describe_media_for_mirror(media_files):
    """Return a human-readable mirror summary when a message only contains media."""
    if not media_files:
        return ""
    if len(media_files) == 1:
        media_path, is_voice = media_files[0]
        ext = os.path.splitext(media_path)[1].lower()
        if is_voice and ext in _VOICE_EXTS:
            return "[Sent voice message]"
        if ext in _IMAGE_EXTS:
            return "[Sent image attachment]"
        if ext in _VIDEO_EXTS:
            return "[Sent video attachment]"
        if ext in _AUDIO_EXTS:
            return "[Sent audio attachment]"
        return "[Sent document attachment]"
    return f"[Sent {len(media_files)} media attachments]"


def _get_cron_auto_delivery_target():
    """Return the cron scheduler's auto-delivery target for the current run, if any."""
    platform = os.getenv("HERMES_CRON_AUTO_DELIVER_PLATFORM", "").strip().lower()
    chat_id = os.getenv("HERMES_CRON_AUTO_DELIVER_CHAT_ID", "").strip()
    if not platform or not chat_id:
        return None
    thread_id = os.getenv("HERMES_CRON_AUTO_DELIVER_THREAD_ID", "").strip() or None
    return {
        "platform": platform,
        "chat_id": chat_id,
        "thread_id": thread_id,
    }


def _maybe_skip_cron_duplicate_send(platform_name: str, chat_id: str, thread_id: str | None):
    """Skip redundant cron send_message calls when the scheduler will auto-deliver there."""
    auto_target = _get_cron_auto_delivery_target()
    if not auto_target:
        return None

    same_target = (
        auto_target["platform"] == platform_name
        and str(auto_target["chat_id"]) == str(chat_id)
        and auto_target.get("thread_id") == thread_id
    )
    if not same_target:
        return None

    target_label = f"{platform_name}:{chat_id}"
    if thread_id is not None:
        target_label += f":{thread_id}"

    return {
        "success": True,
        "skipped": True,
        "reason": "cron_auto_delivery_duplicate_target",
        "target": target_label,
        "note": (
            f"Skipped send_message to {target_label}. This cron job will already auto-deliver "
            "its final response to that same target. Put the intended user-facing content in "
            "your final response instead, or use a different target if you want an additional message."
        ),
    }


async def _send_to_platform(platform, pconfig, chat_id, message, thread_id=None, media_files=None, account_id=None):
    """Route a message to the appropriate platform sender.

    Long messages are automatically chunked to fit within platform limits
    using the same smart-splitting algorithm as the gateway adapters
    (preserves code-block boundaries, adds part indicators).
    """
    from gateway.config import Platform
    from gateway.platforms.base import BasePlatformAdapter
    from gateway.platforms.telegram import TelegramAdapter
    from gateway.platforms.discord import DiscordAdapter
    from gateway.platforms.slack import SlackAdapter

    # Feishu adapter import is optional (requires lark-oapi)
    try:
        from gateway.platforms.feishu import FeishuAdapter
        _feishu_available = True
    except ImportError:
        _feishu_available = False

    media_files = media_files or []

    if platform == Platform.SLACK and message:
        try:
            slack_adapter = SlackAdapter.__new__(SlackAdapter)
            message = slack_adapter.format_message(message)
        except Exception:
            logger.debug("Failed to apply Slack mrkdwn formatting in _send_to_platform", exc_info=True)

    # Platform message length limits (from adapter class attributes)
    _MAX_LENGTHS = {
        Platform.TELEGRAM: TelegramAdapter.MAX_MESSAGE_LENGTH,
        Platform.DISCORD: DiscordAdapter.MAX_MESSAGE_LENGTH,
        Platform.SLACK: SlackAdapter.MAX_MESSAGE_LENGTH,
    }
    if _feishu_available:
        _MAX_LENGTHS[Platform.FEISHU] = FeishuAdapter.MAX_MESSAGE_LENGTH

    # Smart-chunk the message to fit within platform limits.
    # For short messages or platforms without a known limit this is a no-op.
    max_len = _MAX_LENGTHS.get(platform)
    if max_len:
        chunks = BasePlatformAdapter.truncate_message(message, max_len)
    else:
        chunks = [message]

    # --- Telegram: special handling for media attachments ---
    if platform == Platform.TELEGRAM:
        last_result = None
        for i, chunk in enumerate(chunks):
            is_last = (i == len(chunks) - 1)
            result = await _send_telegram(
                pconfig.token,
                chat_id,
                chunk,
                media_files=media_files if is_last else [],
                thread_id=thread_id,
            )
            if isinstance(result, dict) and result.get("error"):
                return result
            last_result = result
        return last_result

    # --- Weixin: use the native one-shot adapter helper for text + media ---
    if platform == Platform.WEIXIN:
        return await _send_weixin(pconfig, chat_id, message, media_files=media_files)

    # --- Non-Telegram platforms ---
    if media_files and not message.strip():
        return {
            "error": (
                f"send_message MEDIA delivery is currently only supported for telegram; "
                f"target {platform.value} had only media attachments"
            )
        }
    warning = None
    if media_files:
        warning = (
            f"MEDIA attachments were omitted for {platform.value}; "
            "native send_message media delivery is currently only supported for telegram"
        )

    last_result = None
    for chunk in chunks:
        if platform == Platform.DISCORD:
            result = await _send_discord(pconfig.token, chat_id, chunk, thread_id=thread_id)
        elif platform == Platform.SLACK:
            result = await _send_slack(pconfig.token, chat_id, chunk)
        elif platform == Platform.WHATSAPP:
            result = await _send_whatsapp(pconfig.extra, chat_id, chunk)
        elif platform == Platform.SIGNAL:
            result = await _send_signal(pconfig.extra, chat_id, chunk)
        elif platform == Platform.EMAIL:
            result = await _send_email(pconfig.extra, chat_id, chunk)
        elif platform == Platform.SMS:
            result = await _send_sms(pconfig.api_key, chat_id, chunk)
        elif platform == Platform.MATTERMOST:
            result = await _send_mattermost(pconfig.token, pconfig.extra, chat_id, chunk)
        elif platform == Platform.MATRIX:
            result = await _send_matrix(pconfig.token, pconfig.extra, chat_id, chunk)
        elif platform == Platform.HOMEASSISTANT:
            result = await _send_homeassistant(pconfig.token, pconfig.extra, chat_id, chunk)
        elif platform == Platform.DINGTALK:
            result = await _send_dingtalk(pconfig.extra, chat_id, chunk)
        elif platform == Platform.FEISHU:
            result = await _send_feishu(pconfig, chat_id, chunk, thread_id=thread_id, account_id=account_id)
        elif platform == Platform.WECOM:
            result = await _send_wecom(pconfig.extra, chat_id, chunk)
        elif platform == Platform.BLUEBUBBLES:
            result = await _send_bluebubbles(pconfig.extra, chat_id, chunk)
        else:
            result = {"error": f"Direct sending not yet implemented for {platform.value}"}

        if isinstance(result, dict) and result.get("error"):
            return result
        last_result = result

    if warning and isinstance(last_result, dict) and last_result.get("success"):
        warnings = list(last_result.get("warnings", []))
        warnings.append(warning)
        last_result["warnings"] = warnings
    return last_result


async def _send_telegram(token, chat_id, message, media_files=None, thread_id=None):
    """Send via Telegram Bot API (one-shot, no polling needed).

    Applies markdown→MarkdownV2 formatting (same as the gateway adapter)
    so that bold, links, and headers render correctly.  If the message
    already contains HTML tags, it is sent with ``parse_mode='HTML'``
    instead, bypassing MarkdownV2 conversion.
    """
    try:
        from telegram import Bot
        from telegram.constants import ParseMode

        # Auto-detect HTML tags — if present, skip MarkdownV2 and send as HTML.
        # Inspired by github.com/ashaney — PR #1568.
        _has_html = bool(re.search(r'<[a-zA-Z/][^>]*>', message))

        if _has_html:
            formatted = message
            send_parse_mode = ParseMode.HTML
        else:
            # Reuse the gateway adapter's format_message for markdown→MarkdownV2
            try:
                from gateway.platforms.telegram import TelegramAdapter
                _adapter = TelegramAdapter.__new__(TelegramAdapter)
                formatted = _adapter.format_message(message)
            except Exception:
                # Fallback: send as-is if formatting unavailable
                formatted = message
            send_parse_mode = ParseMode.MARKDOWN_V2

        bot = Bot(token=token)
        int_chat_id = int(chat_id)
        media_files = media_files or []
        thread_kwargs = {}
        if thread_id is not None:
            thread_kwargs["message_thread_id"] = int(thread_id)

        last_msg = None
        warnings = []

        if formatted.strip():
            try:
                last_msg = await bot.send_message(
                    chat_id=int_chat_id, text=formatted,
                    parse_mode=send_parse_mode, **thread_kwargs
                )
            except Exception as md_error:
                # Parse failed, fall back to plain text
                if "parse" in str(md_error).lower() or "markdown" in str(md_error).lower() or "html" in str(md_error).lower():
                    logger.warning(
                        "Parse mode %s failed in _send_telegram, falling back to plain text: %s",
                        send_parse_mode,
                        _sanitize_error_text(md_error),
                    )
                    if not _has_html:
                        try:
                            from gateway.platforms.telegram import _strip_mdv2
                            plain = _strip_mdv2(formatted)
                        except Exception:
                            plain = message
                    else:
                        plain = message
                    last_msg = await bot.send_message(
                        chat_id=int_chat_id, text=plain,
                        parse_mode=None, **thread_kwargs
                    )
                else:
                    raise

        for media_path, is_voice in media_files:
            if not os.path.exists(media_path):
                warning = f"Media file not found, skipping: {media_path}"
                logger.warning(warning)
                warnings.append(warning)
                continue

            ext = os.path.splitext(media_path)[1].lower()
            try:
                with open(media_path, "rb") as f:
                    if ext in _IMAGE_EXTS:
                        last_msg = await bot.send_photo(
                            chat_id=int_chat_id, photo=f, **thread_kwargs
                        )
                    elif ext in _VIDEO_EXTS:
                        last_msg = await bot.send_video(
                            chat_id=int_chat_id, video=f, **thread_kwargs
                        )
                    elif ext in _VOICE_EXTS and is_voice:
                        last_msg = await bot.send_voice(
                            chat_id=int_chat_id, voice=f, **thread_kwargs
                        )
                    elif ext in _AUDIO_EXTS:
                        last_msg = await bot.send_audio(
                            chat_id=int_chat_id, audio=f, **thread_kwargs
                        )
                    else:
                        last_msg = await bot.send_document(
                            chat_id=int_chat_id, document=f, **thread_kwargs
                        )
            except Exception as e:
                warning = _sanitize_error_text(f"Failed to send media {media_path}: {e}")
                logger.error(warning)
                warnings.append(warning)

        if last_msg is None:
            error = "No deliverable text or media remained after processing MEDIA tags"
            if warnings:
                return {"error": error, "warnings": warnings}
            return {"error": error}

        result = {
            "success": True,
            "platform": "telegram",
            "chat_id": chat_id,
            "message_id": str(last_msg.message_id),
        }
        if warnings:
            result["warnings"] = warnings
        return result
    except ImportError:
        return {"error": "python-telegram-bot not installed. Run: pip install python-telegram-bot"}
    except Exception as e:
        return _error(f"Telegram send failed: {e}")


async def _send_discord(token, chat_id, message, thread_id=None):
    """Send a single message via Discord REST API (no websocket client needed).

    Chunking is handled by _send_to_platform() before this is called.

    When thread_id is provided, the message is sent directly to that thread
    via the /channels/{thread_id}/messages endpoint.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}
    try:
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        _proxy = resolve_proxy_url(platform_env_var="DISCORD_PROXY")
        _sess_kw, _req_kw = proxy_kwargs_for_aiohttp(_proxy)
        # Thread endpoint: Discord threads are channels; send directly to the thread ID.
        if thread_id:
            url = f"https://discord.com/api/v10/channels/{thread_id}/messages"
        else:
            url = f"https://discord.com/api/v10/channels/{chat_id}/messages"
        headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), **_sess_kw) as session:
            async with session.post(url, headers=headers, json={"content": message}, **_req_kw) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    return _error(f"Discord API error ({resp.status}): {body}")
                data = await resp.json()
        return {"success": True, "platform": "discord", "chat_id": chat_id, "message_id": data.get("id")}
    except Exception as e:
        return _error(f"Discord send failed: {e}")


async def _send_slack(token, chat_id, message):
    """Send via Slack Web API."""
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}
    try:
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        _proxy = resolve_proxy_url()
        _sess_kw, _req_kw = proxy_kwargs_for_aiohttp(_proxy)
        url = "https://slack.com/api/chat.postMessage"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), **_sess_kw) as session:
            payload = {"channel": chat_id, "text": message, "mrkdwn": True}
            async with session.post(url, headers=headers, json=payload, **_req_kw) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return {"success": True, "platform": "slack", "chat_id": chat_id, "message_id": data.get("ts")}
                return _error(f"Slack API error: {data.get('error', 'unknown')}")
    except Exception as e:
        return _error(f"Slack send failed: {e}")


async def _send_whatsapp(extra, chat_id, message):
    """Send via the local WhatsApp bridge HTTP API."""
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}
    try:
        bridge_port = extra.get("bridge_port", 3000)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://localhost:{bridge_port}/send",
                json={"chatId": chat_id, "message": message},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "success": True,
                        "platform": "whatsapp",
                        "chat_id": chat_id,
                        "message_id": data.get("messageId"),
                    }
                body = await resp.text()
                return _error(f"WhatsApp bridge error ({resp.status}): {body}")
    except Exception as e:
        return _error(f"WhatsApp send failed: {e}")


async def _send_signal(extra, chat_id, message):
    """Send via signal-cli JSON-RPC API."""
    try:
        import httpx
    except ImportError:
        return {"error": "httpx not installed"}
    try:
        http_url = extra.get("http_url", "http://127.0.0.1:8080").rstrip("/")
        account = extra.get("account", "")
        if not account:
            return {"error": "Signal account not configured"}

        params = {"account": account, "message": message}
        if chat_id.startswith("group:"):
            params["groupId"] = chat_id[6:]
        else:
            params["recipient"] = [chat_id]

        payload = {
            "jsonrpc": "2.0",
            "method": "send",
            "params": params,
            "id": f"send_{int(time.time() * 1000)}",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{http_url}/api/v1/rpc", json=payload)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                return _error(f"Signal RPC error: {data['error']}")
            return {"success": True, "platform": "signal", "chat_id": chat_id}
    except Exception as e:
        return _error(f"Signal send failed: {e}")


async def _send_email(extra, chat_id, message):
    """Send via SMTP (one-shot, no persistent connection needed)."""
    import smtplib
    from email.mime.text import MIMEText

    address = extra.get("address") or os.getenv("EMAIL_ADDRESS", "")
    password = os.getenv("EMAIL_PASSWORD", "")
    smtp_host = extra.get("smtp_host") or os.getenv("EMAIL_SMTP_HOST", "")
    try:
        smtp_port = int(os.getenv("EMAIL_SMTP_PORT", "587"))
    except (ValueError, TypeError):
        smtp_port = 587

    if not all([address, password, smtp_host]):
        return {"error": "Email not configured (EMAIL_ADDRESS, EMAIL_PASSWORD, EMAIL_SMTP_HOST required)"}

    try:
        msg = MIMEText(message, "plain", "utf-8")
        msg["From"] = address
        msg["To"] = chat_id
        msg["Subject"] = "Hermes Agent"

        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls(context=ssl.create_default_context())
        server.login(address, password)
        server.send_message(msg)
        server.quit()
        return {"success": True, "platform": "email", "chat_id": chat_id}
    except Exception as e:
        return _error(f"Email send failed: {e}")


async def _send_sms(auth_token, chat_id, message):
    """Send a single SMS via Twilio REST API.

    Uses HTTP Basic auth (Account SID : Auth Token) and form-encoded POST.
    Chunking is handled by _send_to_platform() before this is called.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    import base64

    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    from_number = os.getenv("TWILIO_PHONE_NUMBER", "")
    if not account_sid or not auth_token or not from_number:
        return {"error": "SMS not configured (TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER required)"}

    # Strip markdown — SMS renders it as literal characters
    message = re.sub(r"\*\*(.+?)\*\*", r"\1", message, flags=re.DOTALL)
    message = re.sub(r"\*(.+?)\*", r"\1", message, flags=re.DOTALL)
    message = re.sub(r"__(.+?)__", r"\1", message, flags=re.DOTALL)
    message = re.sub(r"_(.+?)_", r"\1", message, flags=re.DOTALL)
    message = re.sub(r"```[a-z]*\n?", "", message)
    message = re.sub(r"`(.+?)`", r"\1", message)
    message = re.sub(r"^#{1,6}\s+", "", message, flags=re.MULTILINE)
    message = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", message)
    message = re.sub(r"\n{3,}", "\n\n", message)
    message = message.strip()

    try:
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        _proxy = resolve_proxy_url()
        _sess_kw, _req_kw = proxy_kwargs_for_aiohttp(_proxy)
        creds = f"{account_sid}:{auth_token}"
        encoded = base64.b64encode(creds.encode("ascii")).decode("ascii")
        url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
        headers = {"Authorization": f"Basic {encoded}"}

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), **_sess_kw) as session:
            form_data = aiohttp.FormData()
            form_data.add_field("From", from_number)
            form_data.add_field("To", chat_id)
            form_data.add_field("Body", message)

            async with session.post(url, data=form_data, headers=headers, **_req_kw) as resp:
                body = await resp.json()
                if resp.status >= 400:
                    error_msg = body.get("message", str(body))
                    return _error(f"Twilio API error ({resp.status}): {error_msg}")
                msg_sid = body.get("sid", "")
                return {"success": True, "platform": "sms", "chat_id": chat_id, "message_id": msg_sid}
    except Exception as e:
        return _error(f"SMS send failed: {e}")


async def _send_mattermost(token, extra, chat_id, message):
    """Send via Mattermost REST API."""
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}
    try:
        base_url = (extra.get("url") or os.getenv("MATTERMOST_URL", "")).rstrip("/")
        token = token or os.getenv("MATTERMOST_TOKEN", "")
        if not base_url or not token:
            return {"error": "Mattermost not configured (MATTERMOST_URL, MATTERMOST_TOKEN required)"}
        url = f"{base_url}/api/v4/posts"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.post(url, headers=headers, json={"channel_id": chat_id, "message": message}) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    return _error(f"Mattermost API error ({resp.status}): {body}")
                data = await resp.json()
        return {"success": True, "platform": "mattermost", "chat_id": chat_id, "message_id": data.get("id")}
    except Exception as e:
        return _error(f"Mattermost send failed: {e}")


async def _send_matrix(token, extra, chat_id, message):
    """Send via Matrix Client-Server API.

    Converts markdown to HTML for rich rendering in Matrix clients.
    Falls back to plain text if the ``markdown`` library is not installed.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}
    try:
        homeserver = (extra.get("homeserver") or os.getenv("MATRIX_HOMESERVER", "")).rstrip("/")
        token = token or os.getenv("MATRIX_ACCESS_TOKEN", "")
        if not homeserver or not token:
            return {"error": "Matrix not configured (MATRIX_HOMESERVER, MATRIX_ACCESS_TOKEN required)"}
        txn_id = f"hermes_{int(time.time() * 1000)}_{os.urandom(4).hex()}"
        url = f"{homeserver}/_matrix/client/v3/rooms/{chat_id}/send/m.room.message/{txn_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Build message payload with optional HTML formatted_body.
        payload = {"msgtype": "m.text", "body": message}
        try:
            import markdown as _md
            html = _md.markdown(message, extensions=["fenced_code", "tables"])
            # Convert h1-h6 to bold for Element X compatibility.
            html = re.sub(r"<h[1-6]>(.*?)</h[1-6]>", r"<strong>\1</strong>", html)
            payload["format"] = "org.matrix.custom.html"
            payload["formatted_body"] = html
        except ImportError:
            pass

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.put(url, headers=headers, json=payload) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    return _error(f"Matrix API error ({resp.status}): {body}")
                data = await resp.json()
        return {"success": True, "platform": "matrix", "chat_id": chat_id, "message_id": data.get("event_id")}
    except Exception as e:
        return _error(f"Matrix send failed: {e}")


async def _send_homeassistant(token, extra, chat_id, message):
    """Send via Home Assistant notify service."""
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}
    try:
        hass_url = (extra.get("url") or os.getenv("HASS_URL", "")).rstrip("/")
        token = token or os.getenv("HASS_TOKEN", "")
        if not hass_url or not token:
            return {"error": "Home Assistant not configured (HASS_URL, HASS_TOKEN required)"}
        url = f"{hass_url}/api/services/notify/notify"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.post(url, headers=headers, json={"message": message, "target": chat_id}) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    return _error(f"Home Assistant API error ({resp.status}): {body}")
        return {"success": True, "platform": "homeassistant", "chat_id": chat_id}
    except Exception as e:
        return _error(f"Home Assistant send failed: {e}")


async def _send_dingtalk(extra, chat_id, message):
    """Send via DingTalk robot webhook.

    Note: The gateway's DingTalk adapter uses per-session webhook URLs from
    incoming messages (dingtalk-stream SDK).  For cross-platform send_message
    delivery we use a static robot webhook URL instead, which must be
    configured via ``DINGTALK_WEBHOOK_URL`` env var or ``webhook_url`` in the
    platform's extra config.
    """
    try:
        import httpx
    except ImportError:
        return {"error": "httpx not installed"}
    try:
        webhook_url = extra.get("webhook_url") or os.getenv("DINGTALK_WEBHOOK_URL", "")
        if not webhook_url:
            return {"error": "DingTalk not configured. Set DINGTALK_WEBHOOK_URL env var or webhook_url in dingtalk platform extra config."}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                webhook_url,
                json={"msgtype": "text", "text": {"content": message}},
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("errcode", 0) != 0:
                return _error(f"DingTalk API error: {data.get('errmsg', 'unknown')}")
        return {"success": True, "platform": "dingtalk", "chat_id": chat_id}
    except Exception as e:
        return _error(f"DingTalk send failed: {e}")


async def _send_wecom(extra, chat_id, message):
    """Send via WeCom using the adapter's WebSocket send pipeline."""
    try:
        from gateway.platforms.wecom import WeComAdapter, check_wecom_requirements
        if not check_wecom_requirements():
            return {"error": "WeCom requirements not met. Need aiohttp + WECOM_BOT_ID/SECRET."}
    except ImportError:
        return {"error": "WeCom adapter not available."}

    try:
        from gateway.config import PlatformConfig
        pconfig = PlatformConfig(extra=extra)
        adapter = WeComAdapter(pconfig)
        connected = await adapter.connect()
        if not connected:
            return _error(f"WeCom: failed to connect - {adapter.fatal_error_message or 'unknown error'}")
        try:
            result = await adapter.send(chat_id, message)
            if not result.success:
                return _error(f"WeCom send failed: {result.error}")
            return {"success": True, "platform": "wecom", "chat_id": chat_id, "message_id": result.message_id}
        finally:
            await adapter.disconnect()
    except Exception as e:
        return _error(f"WeCom send failed: {e}")


async def _send_weixin(pconfig, chat_id, message, media_files=None):
    """Send via Weixin iLink using the native adapter helper."""
    try:
        from gateway.platforms.weixin import check_weixin_requirements, send_weixin_direct
        if not check_weixin_requirements():
            return {"error": "Weixin requirements not met. Need aiohttp + cryptography."}
    except ImportError:
        return {"error": "Weixin adapter not available."}

    try:
        return await send_weixin_direct(
            extra=pconfig.extra,
            token=pconfig.token,
            chat_id=chat_id,
            message=message,
            media_files=media_files,
        )
    except Exception as e:
        return _error(f"Weixin send failed: {e}")


async def _send_bluebubbles(extra, chat_id, message):
    """Send via BlueBubbles iMessage server using the adapter's REST API."""
    try:
        from gateway.platforms.bluebubbles import BlueBubblesAdapter, check_bluebubbles_requirements
        if not check_bluebubbles_requirements():
            return {"error": "BlueBubbles requirements not met (need aiohttp + httpx)."}
    except ImportError:
        return {"error": "BlueBubbles adapter not available."}

    try:
        from gateway.config import PlatformConfig
        pconfig = PlatformConfig(extra=extra)
        adapter = BlueBubblesAdapter(pconfig)
        connected = await adapter.connect()
        if not connected:
            return _error("BlueBubbles: failed to connect to server")
        try:
            result = await adapter.send(chat_id, message)
            if not result.success:
                return _error(f"BlueBubbles send failed: {result.error}")
            return {"success": True, "platform": "bluebubbles", "chat_id": chat_id, "message_id": result.message_id}
        finally:
            await adapter.disconnect()
    except Exception as e:
        return _error(f"BlueBubbles send failed: {e}")


async def _send_feishu(pconfig, chat_id, message, media_files=None, thread_id=None, account_id=None):
    """Send via Feishu/Lark using the adapter's send pipeline."""
    try:
        from gateway.platforms.feishu import FeishuAdapter, FEISHU_AVAILABLE
        if not FEISHU_AVAILABLE:
            return {"error": "Feishu dependencies not installed. Run: pip install 'hermes-agent[feishu]'"}
        from gateway.platforms.feishu import FEISHU_DOMAIN, LARK_DOMAIN
    except ImportError:
        return {"error": "Feishu dependencies not installed. Run: pip install 'hermes-agent[feishu]'"}

    media_files = media_files or []

    try:
        adapter = FeishuAdapter(pconfig)
        adapter._clients_by_account = {}
        for feishu_account in adapter._accounts.values():
            if not feishu_account.enabled:
                continue
            domain = FEISHU_DOMAIN if feishu_account.domain_name != "lark" else LARK_DOMAIN
            if feishu_account.account_id == "default":
                client = adapter._build_lark_client(domain)
                adapter._client = client
            else:
                client = adapter._build_lark_client_for_account(feishu_account, domain)
            adapter._clients_by_account[feishu_account.account_id] = client
        if adapter._client is None:
            domain_name = getattr(adapter, "_domain_name", "feishu")
            domain = FEISHU_DOMAIN if domain_name != "lark" else LARK_DOMAIN
            adapter._client = adapter._build_lark_client(domain)
            adapter._clients_by_account.setdefault("default", adapter._client)
        metadata = {}
        if thread_id:
            metadata["thread_id"] = thread_id
        if account_id:
            metadata["account_id"] = account_id
        metadata = metadata or None

        last_result = None
        if message.strip():
            last_result = await adapter.send(chat_id, message, metadata=metadata)
            if not last_result.success:
                return _error(f"Feishu send failed: {last_result.error}")

        for media_path, is_voice in media_files:
            if not os.path.exists(media_path):
                return _error(f"Media file not found: {media_path}")

            ext = os.path.splitext(media_path)[1].lower()
            if ext in _IMAGE_EXTS:
                last_result = await adapter.send_image_file(chat_id, media_path, metadata=metadata)
            elif ext in _VIDEO_EXTS:
                last_result = await adapter.send_video(chat_id, media_path, metadata=metadata)
            elif ext in _VOICE_EXTS and is_voice:
                last_result = await adapter.send_voice(chat_id, media_path, metadata=metadata)
            elif ext in _AUDIO_EXTS:
                last_result = await adapter.send_voice(chat_id, media_path, metadata=metadata)
            else:
                last_result = await adapter.send_document(chat_id, media_path, metadata=metadata)

            if not last_result.success:
                return _error(f"Feishu media send failed: {last_result.error}")

        if last_result is None:
            return {"error": "No deliverable text or media remained after processing MEDIA tags"}

        return {
            "success": True,
            "platform": "feishu",
            "chat_id": chat_id,
            "message_id": last_result.message_id,
        }
    except Exception as e:
        return _error(f"Feishu send failed: {e}")


def _check_send_message():
    """Gate send_message on gateway running (always available on messaging platforms)."""
    from gateway.session_context import get_session_env
    platform = get_session_env("HERMES_SESSION_PLATFORM", "")
    if platform and platform != "local":
        return True
    try:
        from gateway.status import is_gateway_running
        return is_gateway_running()
    except Exception:
        return False


# --- Registry ---
from tools.registry import registry, tool_error

registry.register(
    name="send_message",
    toolset="messaging",
    schema=SEND_MESSAGE_SCHEMA,
    handler=send_message_tool,
    check_fn=_check_send_message,
    emoji="📨",
)
