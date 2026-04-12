"""
CLI commands for the DM pairing system.

Usage:
    hermes pairing list              # Show all pending + approved users
    hermes pairing approve <platform> <code>  # Approve a pairing code
    hermes pairing revoke <platform> <user_id> # Revoke user access
    hermes pairing clear-pending     # Clear all expired/pending codes
"""

import json
import logging


logger = logging.getLogger(__name__)


def _notify_feishu_pairing_approval(user_open_id: str, account_id: str | None = None) -> None:
    """向飞书用户发送配对通过通知，减少用户等待下一条消息才能感知授权成功的情况。"""
    from tools.feishu.client import feishu_api_request

    content = (
        "Your Hermes access has been approved.\n\n"
        "You can message the bot again now.\n"
        "If a tool later needs extra Feishu permissions, use /feishu-auth in chat."
    )
    feishu_api_request(
        "POST",
        "/open-apis/im/v1/messages",
        params={"receive_id_type": "open_id"},
        account_id=account_id,
        json_body={
            "receive_id": user_open_id,
            "msg_type": "text",
            "content": json.dumps({"text": content}, ensure_ascii=False),
        },
    )


def _maybe_notify_feishu_onboarding(user_open_id: str, account_id: str | None = None) -> bool:
    """当配对用户就是应用 owner 时，主动发送 onboarding 指引。"""
    from tools.feishu.client import (
        get_app_granted_scopes_by_token_type,
        get_app_info,
        feishu_api_request,
    )
    from tools.feishu.scopes import split_sensitive_scopes

    app_info = get_app_info(account_id=account_id)
    effective_owner_open_id = str(app_info.get("effective_owner_open_id", "") or "").strip()
    if not effective_owner_open_id or effective_owner_open_id != str(user_open_id or "").strip():
        return False

    granted_user_scopes = get_app_granted_scopes_by_token_type("user", account_id=account_id)
    safe_scopes, _sensitive_scopes = split_sensitive_scopes(granted_user_scopes)
    if not safe_scopes:
        return False

    command = "/feishu-auth scope " + ",".join(safe_scopes)
    content = (
        "You are the Feishu app owner.\n\n"
        "To finish Hermes onboarding, run this command in the current Feishu chat:\n"
        f"{command}\n\n"
        "This requests the app's currently granted user scopes and skips sensitive scopes."
    )
    feishu_api_request(
        "POST",
        "/open-apis/im/v1/messages",
        params={"receive_id_type": "open_id"},
        account_id=account_id,
        json_body={
            "receive_id": user_open_id,
            "msg_type": "text",
            "content": json.dumps({"text": content}, ensure_ascii=False),
        },
    )
    return True


def pairing_command(args):
    """Handle hermes pairing subcommands."""
    from gateway.pairing import PairingStore

    store = PairingStore()
    action = getattr(args, "pairing_action", None)

    if action == "list":
        _cmd_list(store)
    elif action == "approve":
        _cmd_approve(store, args.platform, args.code)
    elif action == "revoke":
        _cmd_revoke(store, args.platform, args.user_id)
    elif action == "clear-pending":
        _cmd_clear_pending(store)
    else:
        print("Usage: hermes pairing {list|approve|revoke|clear-pending}")
        print("Run 'hermes pairing --help' for details.")


def _cmd_list(store):
    """List all pending and approved users."""
    pending = store.list_pending()
    approved = store.list_approved()

    if not pending and not approved:
        print("No pairing data found. No one has tried to pair yet~")
        return

    if pending:
        print(f"\n  Pending Pairing Requests ({len(pending)}):")
        print(f"  {'Platform':<12} {'Code':<10} {'User ID':<20} {'Name':<20} {'Age'}")
        print(f"  {'--------':<12} {'----':<10} {'-------':<20} {'----':<20} {'---'}")
        for p in pending:
            print(
                f"  {p['platform']:<12} {p['code']:<10} {p['user_id']:<20} "
                f"{p.get('user_name', ''):<20} {p['age_minutes']}m ago"
            )
    else:
        print("\n  No pending pairing requests.")

    if approved:
        print(f"\n  Approved Users ({len(approved)}):")
        print(f"  {'Platform':<12} {'User ID':<20} {'Name':<20}")
        print(f"  {'--------':<12} {'-------':<20} {'----':<20}")
        for a in approved:
            print(f"  {a['platform']:<12} {a['user_id']:<20} {a.get('user_name', ''):<20}")
    else:
        print("\n  No approved users.")

    print()


def _cmd_approve(store, platform: str, code: str):
    """Approve a pairing code."""
    platform = platform.lower().strip()
    code = code.upper().strip()

    result = store.approve_code(platform, code)
    if result:
        uid = result["user_id"]
        name = result.get("user_name", "")
        account_id = str(result.get("account_id", "") or "").strip() or None
        display = f"{name} ({uid})" if name else uid
        print(f"\n  Approved! User {display} on {platform} can now use the bot~")
        print("  They'll be recognized automatically on their next message.\n")
        if platform == "feishu" and uid:
            try:
                _notify_feishu_pairing_approval(uid, account_id=account_id)
            except Exception as exc:
                logger.warning("Failed to notify Feishu pairing approval for %s: %s", uid, exc)
                print("  Warning: pairing succeeded, but the Feishu approval notice could not be delivered.\n")
            try:
                onboarding_sent = _maybe_notify_feishu_onboarding(uid, account_id=account_id)
                if onboarding_sent:
                    print("  Sent a Feishu onboarding message for the app owner.\n")
            except Exception as exc:
                logger.warning("Failed to notify Feishu onboarding for %s: %s", uid, exc)
                print("  Warning: pairing succeeded, but the Feishu onboarding guidance could not be delivered.\n")
    else:
        print(f"\n  Code '{code}' not found or expired for platform '{platform}'.")
        print("  Run 'hermes pairing list' to see pending codes.\n")


def _cmd_revoke(store, platform: str, user_id: str):
    """Revoke a user's access."""
    platform = platform.lower().strip()

    if store.revoke(platform, user_id):
        print(f"\n  Revoked access for user {user_id} on {platform}.\n")
    else:
        print(f"\n  User {user_id} not found in approved list for {platform}.\n")


def _cmd_clear_pending(store):
    """Clear all pending pairing codes."""
    count = store.clear_pending()
    if count:
        print(f"\n  Cleared {count} pending pairing request(s).\n")
    else:
        print("\n  No pending requests to clear.\n")
