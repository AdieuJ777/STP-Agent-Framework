"""运行期执行上下文。"""
from __future__ import annotations

import hashlib
import inspect
import json
from typing import Any, Callable
from uuid import uuid4

from .errors import (
    FrameworkError,
    MaxModelCallsExceededError,
    MaxStepsExceededError,
    ModelCallError,
    OutputValidationError,
    TaskCancelledError,
)
from .events import EventBus
from .llm import LLMBackend
from .models import EventType, RunError, TaskRun, TaskSpec
from .plan import PlanNode, TaskIR
from .refs import resolve
from .schema import validate_instance
from .tokens import CancellationToken, PauseToken
from .tools import ToolCallContext, ToolRegistry


def hash_value(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class ExecutionContext:
    def __init__(
        self,
        *,
        run: TaskRun,
        spec: TaskSpec,
        plan: TaskIR,
        input_data: dict[str, Any],
        bus: EventBus,
        backend: LLMBackend,
        tools: ToolRegistry,
        cancellation: CancellationToken | None = None,
        pause: PauseToken | None = None,
    ) -> None:
        self.run = run
        self.spec = spec
        self.plan = plan
        self.input = input_data
        self.state: dict[str, Any] = {}
        self.bus = bus
        self.backend = backend
        self.tools = tools
        self.cancellation = cancellation or CancellationToken()
        self.pause = pause or PauseToken()
        self.step_index = -1
        self.step_count = 0

    # --- 基础 -----------------------------------------------------------
    def next_step_index(self) -> int:
        self.step_index += 1
        return self.step_index

    def emit(
        self,
        event_type: EventType,
        payload: dict[str, Any] | None = None,
        *,
        step_index: int | None = None,
        attempt: int = 0,
    ) -> None:
        self.bus.emit(
            event_type,
            step_index=self.step_index if step_index is None else step_index,
            attempt=attempt,
            payload=payload or {},
        )

    def check_cancelled(self) -> None:
        if self.cancellation.cancelled:
            raise TaskCancelledError("task was cancelled", details={"run_id": self.run.run_id})

    def check_step_limit(self) -> None:
        if self.step_count > self.spec.max_steps:
            raise MaxStepsExceededError(
                f"max_steps={self.spec.max_steps} exceeded",
                details={"max_steps": self.spec.max_steps},
            )

    def check_model_call_limit(self) -> None:
        if self.run.model_call_count >= self.spec.max_model_calls:
            raise MaxModelCallsExceededError(
                f"max_model_calls={self.spec.max_model_calls} exceeded",
                details={"max_model_calls": self.spec.max_model_calls},
            )

    def resolve(self, value: Any) -> Any:
        return resolve(value, self.input, self.state)

    def state_view(self) -> dict[str, Any]:
        """条件函数看到的视图；state 覆盖同名 input。"""
        return {**self.input, **self.state}

    def mark_failed(self, error: FrameworkError) -> None:
        self.run.error = RunError(
            code=error.code,
            message=str(error),
            retryable=error.retryable,
            details=error.details,
        )

    # --- 外部调用 -------------------------------------------------------
    async def call_tool(
        self,
        *,
        tool_name: str,
        input_data: dict[str, Any],
        step_index: int,
        attempt: int,
    ) -> dict[str, Any]:
        call_id = uuid4().hex
        context = ToolCallContext(
            run_id=self.run.run_id,
            step_index=step_index,
            attempt=attempt,
            call_id=call_id,
            idempotency_key=f"{self.run.run_id}:{call_id}",
            cancellation=self.cancellation,
            emit=lambda event_type, payload: self.emit(
                event_type, payload, step_index=step_index, attempt=attempt
            ),
        )
        return await self.tools.execute(tool_name, input_data, context)

    async def call_llm(
        self,
        *,
        messages: list[dict[str, Any]],
        output_schema: dict[str, Any] | None,
        step_index: int,
        attempt: int,
    ) -> Any:
        self.check_model_call_limit()
        self.run.model_call_count += 1
        self.emit(
            EventType.LLM_CALL_START,
            {
                "backend": getattr(self.backend, "name", type(self.backend).__name__),
                "model": getattr(self.backend, "model", None),
                "message_count": len(messages),
                "structured": output_schema is not None,
            },
            step_index=step_index,
            attempt=attempt,
        )
        try:
            if output_schema is not None:
                response = await self.backend.structured_chat(messages, output_schema)
                result = response.structured
            else:
                response = await self.backend.chat(messages)
                result = response.content
        except FrameworkError:
            raise
        except Exception as exc:
            raise ModelCallError(
                f"model call failed: {exc}",
                details={"backend": getattr(self.backend, "name", "unknown")},
                cause=exc,
            ) from exc

        self.emit(
            EventType.LLM_CALL_END,
            {
                "model_name": response.model_name,
                "token_usage": response.token_usage,
                "output": result,
            },
            step_index=step_index,
            attempt=attempt,
        )
        return result

    async def call_function(
        self,
        *,
        func: Callable[..., Any],
        input_data: dict[str, Any],
        output_schema: dict[str, Any] | None,
    ) -> Any:
        result = func(input_data)
        if inspect.isawaitable(result):
            result = await result
        if output_schema is not None:
            validate_instance(
                output_schema,
                result,
                error_cls=OutputValidationError,
                context=f"function {getattr(func, '__name__', 'anonymous')} output",
            )
        return result
