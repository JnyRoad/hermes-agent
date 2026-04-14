"""飞书向用户提问工具。"""

from __future__ import annotations

import json
import logging
import asyncio

from tools.feishu.runtime import get_active_feishu_adapter, require_feishu_session
from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)


def _run_async(coro):
    """在同步工具 handler 中执行异步协程。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=30)
    return asyncio.run(coro)


def _check_feishu_runtime() -> bool:
    try:
        require_feishu_session()
        get_active_feishu_adapter()
        return True
    except Exception:
        return False


def _handle_ask_user_question(args: dict, **_kw) -> str:
    question = str(args.get("question", "")).strip()
    if not question:
        return tool_error("Missing required parameter: question")
    options = args.get("options") or []
    if not isinstance(options, list) or len(options) < 1:
        return tool_error("Parameter 'options' must be a non-empty array.")
    normalized_options = [str(item).strip() for item in options if str(item).strip()]
    if not normalized_options:
        return tool_error("Parameter 'options' must contain at least one non-empty option.")
    if len(normalized_options) > 5:
        return tool_error("Parameter 'options' supports at most 5 options.")

    try:
        adapter = get_active_feishu_adapter()
        session = require_feishu_session()
        header = str(args.get("header", "")).strip() or "Question from Hermes"
        note = str(args.get("note", "")).strip()
        result = _run_async(
            adapter.send_question_card(
                chat_id=session["chat_id"],
                question=question,
                options=normalized_options,
                header=header,
                note=note,
                metadata={"thread_id": session["thread_id"] or None},
            )
        )
        if not result.success:
            return tool_error(result.error or "Failed to send Feishu question card.")
        return json.dumps(
            {
                "status": "pending",
                "question_id": ((result.raw_response or {}) if isinstance(result.raw_response, dict) else {}).get("question_id"),
                "message_id": result.message_id,
                "chat_id": session["chat_id"],
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.error("feishu_ask_user_question error: %s", exc)
        return tool_error(f"Failed to ask user question: {exc}")


FEISHU_ASK_USER_QUESTION_SCHEMA = {
    "name": "feishu_ask_user_question",
    "description": "Ask the current Feishu user a multiple-choice question using an interactive card, then wait for their answer in the same chat.",
    "parameters": {
        "type": "object",
        "properties": {
            "header": {"type": "string", "description": "Short card title shown above the question."},
            "question": {"type": "string", "description": "The question shown to the user."},
            "note": {"type": "string", "description": "Optional supporting note shown under the question."},
            "options": {
                "type": "array",
                "description": "List of answer options. Supports 1 to 5 choices.",
                "items": {"type": "string"},
            },
        },
        "required": ["question", "options"],
    },
}

registry.register(
    name="feishu_ask_user_question",
    toolset="feishu",
    schema=FEISHU_ASK_USER_QUESTION_SCHEMA,
    handler=_handle_ask_user_question,
    check_fn=_check_feishu_runtime,
    emoji="🪽",
)
