"""事件日志的轻量敏感字段脱敏。

只处理常见凭据字段和明显的密钥文本模式；它不是完整的安全审计方案，
但能保证框架自身的事件日志不会原样落盘 API Key、Bearer Token 等。
"""
from __future__ import annotations

import re
from typing import Any

REDACTED = "***REDACTED***"

# 精确匹配规范化后的 key，避免误伤 token_usage、prompt_tokens 等统计字段。
_SENSITIVE_KEYS = {
    "api_key",
    "apikey",
    "api_key_id",
    "api_token",
    "secret",
    "client_secret",
    "clientsecret",
    "password",
    "passwd",
    "authorization",
    "access_token",
    "accesstoken",
    "refresh_token",
    "refreshtoken",
    "bearer_token",
    "bearertoken",
    "private_key",
    "privatekey",
    "token",
}

_SECRET_VALUE_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}\b", re.IGNORECASE),
)


def _normalise_key(key: str) -> str:
    return key.strip().lower().replace("-", "_")


def redact(value: Any) -> Any:
    """递归脱敏 dict/list 中的敏感字段和明显的密钥文本。"""
    if isinstance(value, dict):
        return {
            key: REDACTED
            if isinstance(key, str) and _normalise_key(key) in _SENSITIVE_KEYS
            else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        result = value
        for pattern in _SECRET_VALUE_PATTERNS:
            result = pattern.sub(REDACTED, result)
        return result
    return value
