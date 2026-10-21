"""推理后端抽象、Mock 后端与 OpenAI 兼容后端。"""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence

from .errors import ModelCallError, StructuredOutputError
from .schema import validate_instance


def _extract_json_text(content: str) -> str:
    """从模型输出中取出第一个合法 JSON 对象。

    兼容常见的 ```json ... ``` 围栏、前后解释文字，以及带 thinking 的模型输出。
    """
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            _, end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        return text[index : index + end]
    return text


@dataclass
class LLMResponse:
    content: str
    model_name: str
    structured: dict[str, Any] | None = None
    token_usage: dict[str, int] = field(default_factory=dict)
    raw: Any = None


class LLMBackend(Protocol):
    name: str

    async def chat(self, messages: list[dict[str, Any]], **kwargs: Any) -> LLMResponse: ...

    async def structured_chat(
        self,
        messages: list[dict[str, Any]],
        output_schema: dict[str, Any],
        **kwargs: Any,
    ) -> LLMResponse: ...


class MockLLM:
    """确定性 Mock 后端。

    ``responses`` 可以是一个列表（按调用顺序消费）、一个字典（直接作为结构化结果）
    或一个 callable(messages) -> Any。列表项支持：
    - dict：作为结构化结果返回；
    - str：作为文本返回，结构化调用时尝试 JSON 解析；
    - Exception：直接抛出；
    - ``{"__sleep__": seconds, ...}``：先 sleep，再返回其余字段。
    """

    name = "mock"

    def __init__(
        self,
        responses: Sequence[Any] | dict[str, Any] | Any | None = None,
        *,
        default: Any | None = None,
        model_name: str = "mock-model",
    ) -> None:
        if isinstance(responses, (list, tuple)):
            self._queue = list(responses)
            self._fixed = None
        elif callable(responses):
            self._queue = None
            self._fixed = responses
        else:
            self._queue = None
            self._fixed = responses
        self._default = default
        self.model_name = model_name
        self.calls: list[dict[str, Any]] = []

    def _next(self, messages: list[dict[str, Any]]) -> Any:
        self.calls.append({"messages": messages})
        if self._queue is not None:
            if self._queue:
                return self._queue.pop(0)
            if self._default is not None:
                return self._default
            raise ModelCallError("MockLLM has no more scripted responses")
        if callable(self._fixed):
            return self._fixed(messages)
        if self._fixed is not None:
            return self._fixed
        if self._default is not None:
            return self._default
        raise ModelCallError("MockLLM has no response configured")

    async def _materialize(self, messages: list[dict[str, Any]]) -> Any:
        item = self._next(messages)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, dict) and "__sleep__" in item:
            await asyncio.sleep(float(item["__sleep__"]))
            item = {k: v for k, v in item.items() if k != "__sleep__"}
            if set(item) == {"response"}:
                item = item["response"]
        return item

    async def chat(self, messages: list[dict[str, Any]], **kwargs: Any) -> LLMResponse:
        item = await self._materialize(messages)
        if isinstance(item, dict):
            content = json.dumps(item, ensure_ascii=False)
        else:
            content = str(item)
        return LLMResponse(
            content=content,
            model_name=self.model_name,
            token_usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        )

    async def structured_chat(
        self,
        messages: list[dict[str, Any]],
        output_schema: dict[str, Any],
        **kwargs: Any,
    ) -> LLMResponse:
        item = await self._materialize(messages)
        if isinstance(item, str):
            try:
                item = json.loads(_extract_json_text(item))
            except json.JSONDecodeError as exc:
                raise StructuredOutputError(
                    f"mock response is not valid JSON: {exc}",
                    details={"content": item},
                    cause=exc,
                ) from exc
        if not isinstance(item, dict):
            raise StructuredOutputError(
                "mock structured response must be a JSON object",
                details={"response": item},
            )
        validate_instance(
            output_schema,
            item,
            error_cls=StructuredOutputError,
            context="mock structured output",
        )
        return LLMResponse(
            content=json.dumps(item, ensure_ascii=False),
            model_name=self.model_name,
            structured=item,
            token_usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        )


class OpenAICompatBackend:
    """通过 OpenAI 兼容接口访问真实模型。

    适配器本身不重试；超时由客户端执行，重试由运行时统一管理。
    """

    name = "openai-compat"

    def __init__(
        self,
        *,
        model: str | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        supports_json_schema: bool = True,
    ) -> None:
        try:
            from openai import AsyncOpenAI
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ModelCallError(
                "openai package is not installed; install with `uv sync --extra openai`",
                cause=exc,
            ) from exc

        self.model = model or os.getenv("KKK_LLM_MODEL") or os.getenv("OPENAI_MODEL")
        self.api_key = api_key or os.getenv("KKK_LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.base_url = base_url or os.getenv("KKK_LLM_BASE_URL") or os.getenv("OPENAI_BASE_URL")
        self.timeout = timeout or float(os.getenv("KKK_LLM_TIMEOUT", "60"))
        self.supports_json_schema = supports_json_schema
        if not self.model:
            raise ModelCallError("model name is required for OpenAICompatBackend")
        self._client = AsyncOpenAI(
            api_key=self.api_key or "not-needed",
            base_url=self.base_url,
            timeout=self.timeout,
        )

    @staticmethod
    def _messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [dict(message) for message in messages]

    async def chat(self, messages: list[dict[str, Any]], **kwargs: Any) -> LLMResponse:
        try:
            response = await self._client.chat.completions.create(
                model=kwargs.get("model", self.model),
                messages=self._messages(messages),
                temperature=kwargs.get("temperature", 0.0),
                max_tokens=kwargs.get("max_tokens"),
            )
        except Exception as exc:
            raise ModelCallError(
                f"chat completion failed: {exc}",
                details={"model": self.model},
                cause=exc,
            ) from exc
        content = response.choices[0].message.content or ""
        return LLMResponse(
            content=content,
            model_name=response.model,
            token_usage=self._usage(response),
            raw=response,
        )

    async def structured_chat(
        self,
        messages: list[dict[str, Any]],
        output_schema: dict[str, Any],
        **kwargs: Any,
    ) -> LLMResponse:
        response_format: dict[str, Any]
        if self.supports_json_schema:
            response_format = {
                "type": "json_schema",
                "json_schema": {"name": "task_output", "schema": output_schema, "strict": True},
            }
        else:
            response_format = {"type": "json_object"}

        try:
            response = await self._client.chat.completions.create(
                model=kwargs.get("model", self.model),
                messages=self._messages(messages),
                temperature=kwargs.get("temperature", 0.0),
                max_tokens=kwargs.get("max_tokens"),
                response_format=response_format,
            )
        except Exception as exc:
            # 只对“后端不支持 json_schema”做能力回退；网络/鉴权错误不在这里重试，
            # 交给统一运行时按 retry_policy 处理。
            if response_format["type"] == "json_schema" and self._is_unsupported_response_format(exc):
                try:
                    response = await self._client.chat.completions.create(
                        model=kwargs.get("model", self.model),
                        messages=self._messages(messages),
                        temperature=kwargs.get("temperature", 0.0),
                        max_tokens=kwargs.get("max_tokens"),
                        response_format={"type": "json_object"},
                    )
                except Exception as fallback_exc:
                    raise ModelCallError(
                        f"structured chat failed: {fallback_exc}",
                        details={"model": self.model},
                        cause=fallback_exc,
                    ) from fallback_exc
            else:
                raise ModelCallError(
                    f"structured chat failed: {exc}",
                    details={"model": self.model},
                    cause=exc,
                ) from exc

        content = response.choices[0].message.content or ""
        json_text = _extract_json_text(content)
        try:
            structured = json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise StructuredOutputError(
                f"model returned invalid JSON: {exc}",
                details={"content": content},
                cause=exc,
            ) from exc
        if not isinstance(structured, dict):
            raise StructuredOutputError(
                "model structured output must be a JSON object",
                details={"content": content},
            )
        validate_instance(
            output_schema,
            structured,
            error_cls=StructuredOutputError,
            context="model structured output",
        )
        return LLMResponse(
            content=content,
            model_name=response.model,
            structured=structured,
            token_usage=self._usage(response),
            raw=response,
        )

    @staticmethod
    def _is_unsupported_response_format(exc: Exception) -> bool:
        status = getattr(exc, "status_code", None)
        text = str(exc).lower()
        return status == 400 or "response_format" in text or "json_schema" in text

    @staticmethod
    def _usage(response: Any) -> dict[str, int]:
        usage = getattr(response, "usage", None)
        if usage is None:
            return {}
        return {
            "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
            "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
            "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
        }
