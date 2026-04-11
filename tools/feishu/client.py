"""飞书开放平台 HTTP 客户端。

这里封装的是模型工具使用的租户级 API 调用，不依赖网关会话运行循环。
涉及交互回调、卡片点击和等待态推进的逻辑仍由飞书适配器负责。
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

import httpx

from tools.feishu.runtime import get_feishu_platform_extra

logger = logging.getLogger(__name__)

_TOKEN_CACHE: dict[str, tuple[str, float]] = {}


def _feishu_domain_name() -> str:
    extra = get_feishu_platform_extra()
    return str(extra.get("domain", "feishu")).strip().lower() or "feishu"


def get_feishu_base_url() -> str:
    """根据配置返回飞书开放平台基地址。"""
    if _feishu_domain_name() == "lark":
        return "https://open.larksuite.com"
    return "https://open.feishu.cn"


def get_feishu_credentials() -> tuple[str, str]:
    """读取 app_id / app_secret。"""
    extra = get_feishu_platform_extra()
    app_id = str(extra.get("app_id", "")).strip()
    app_secret = str(extra.get("app_secret", "")).strip()
    if not app_id or not app_secret:
        raise RuntimeError("Feishu app_id/app_secret is not configured.")
    return app_id, app_secret


def _cache_key() -> str:
    app_id, _ = get_feishu_credentials()
    return f"{_feishu_domain_name()}:{app_id}"


def get_tenant_access_token(force_refresh: bool = False) -> str:
    """获取并缓存 tenant_access_token。"""
    key = _cache_key()
    now = time.time()
    cached = _TOKEN_CACHE.get(key)
    if cached and not force_refresh and cached[1] > now + 30:
        return cached[0]

    app_id, app_secret = get_feishu_credentials()
    url = f"{get_feishu_base_url()}/open-apis/auth/v3/tenant_access_token/internal"
    response = httpx.post(
        url,
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != 0 or not payload.get("tenant_access_token"):
        raise RuntimeError(
            f"Failed to get tenant access token: code={payload.get('code')} msg={payload.get('msg')}"
        )
    token = str(payload["tenant_access_token"])
    expires_in = int(payload.get("expire", 7200))
    _TOKEN_CACHE[key] = (token, now + expires_in)
    return token


def feishu_api_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    user_access_token: Optional[str] = None,
) -> Dict[str, Any]:
    """调用飞书开放平台 API 并返回 JSON。"""
    token = user_access_token or get_tenant_access_token()
    url = f"{get_feishu_base_url()}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    response = httpx.request(
        method=method.upper(),
        url=url,
        params=params,
        json=json_body,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Feishu API returned a non-object response.")
    if data.get("code") not in (0, None):
        raise RuntimeError(
            f"Feishu API error: code={data.get('code')} msg={data.get('msg') or data.get('message')}"
        )
    return data


def feishu_api_request_bytes(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    user_access_token: Optional[str] = None,
) -> Tuple[bytes, Dict[str, str]]:
    """调用返回二进制内容的飞书 API。"""
    token = user_access_token or get_tenant_access_token()
    url = f"{get_feishu_base_url()}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
    }
    response = httpx.request(
        method=method.upper(),
        url=url,
        params=params,
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    return response.content, dict(response.headers)


def feishu_json(result: Dict[str, Any]) -> str:
    """输出标准 JSON 字符串。"""
    return json.dumps(result, ensure_ascii=False)
