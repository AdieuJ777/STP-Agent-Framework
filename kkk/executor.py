"""统一执行器：两种前端生成的 TaskIR 都由这里执行。"""
from __future__ import annotations

import asyncio
import inspect
import json
from collections import defaultdict, deque
from typing import Any
from uuid import uuid4

from .context import ExecutionContext, hash_value
from .errors import (
    BranchConditionError,
    BusinessRuleViolationError,
    ContractValidationError,
    FrameworkError,
    MaxStepsExceededError,
    NonIdempotentRetryWarning,
    OutputValidationError,
    PlanValidationError,
    StepTimeoutError,
    TaskCancelledError,
    TaskTimeoutError,
    TerminationConditionError,
)
from .events import EventBus, EventSink
from .llm import LLMBackend, MockLLM
from .models import EventType, TaskRun, TaskSpec, TaskStatus
from .plan import PlanNode, TaskIR
from .schema import validate_instance
from .tokens import CancellationToken
from .tools import ToolRegistry

DEFAULT_STEP_TIMEOUT = 60.0


class TaskRuntime:
    def __init__(
        self,
        backend: LLMBackend | None = None,
        tools: ToolRegistry | None = None,
        event_store: EventSink | None = None,
    ) -> None:
        self.backend = backend or MockLLM(default={})
        self.tools = tools or ToolRegistry()
        self.event_store = event_store
        self.last_bus: EventBus | None = None

    @property
    def last_events(self):
        return self.last_bus.events if self.last_bus is not None else []

    async def run(
        self,
        spec: TaskSpec,
        plan: TaskIR,
        input_data: dict[str, Any],
        *,
        run_id: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> TaskRun:
        from .validation import validate_plan

        validate_plan(spec, plan).raise_if_errors()
        if str(spec.protocol_version).split(".", 1)[0] != "1":
            raise PlanValidationError(
                f"unsupported protocol major version {spec.protocol_version!r}"
            )

        run = TaskRun(
            run_id=run_id or uuid4().hex,
            task_spec_id=spec.task_id,
            input=input_data,
        )
        sinks: list[EventSink] = [self.event_store] if self.event_store is not None else []
        bus = EventBus(run.run_id, sinks)
        self.last_bus = bus
        ctx = ExecutionContext(
            run=run,
            spec=spec,
            plan=plan,
            input_data=input_data,
            bus=bus,
            backend=self.backend,
            tools=self.tools,
            cancellation=cancellation,
        )

        run.transition_to(TaskStatus.RUNNING)

        if ctx.cancellation.cancelled:
            cancelled = TaskCancelledError(
                "task was cancelled before start",
                details={"run_id": run.run_id},
            )
            ctx.mark_failed(cancelled)
            run.transition_to(TaskStatus.CANCELLED)
            bus.emit(
                EventType.TASK_CANCELLED,
                payload={"error_code": cancelled.code, "message": str(cancelled)},
            )
            return run

        bus.emit(
            EventType.TASK_STARTED,
            payload={"task_id": spec.task_id, "task_name": spec.name, "run_id": run.run_id},
        )

        try:
            validate_instance(
                spec.input_schema,
                input_data,
                error_cls=ContractValidationError,
                context="task input",
            )
        except FrameworkError as exc:
            ctx.mark_failed(exc)
            run.transition_to(TaskStatus.FAILED)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
            return run

        try:
            await asyncio.wait_for(self._execute_graph(ctx), timeout=spec.timeout_seconds)
            if run.output is None:
                raise FrameworkError("plan finished without producing output")
            run.transition_to(TaskStatus.SUCCEEDED)
            bus.emit(
                EventType.TASK_COMPLETED,
                payload={"output": run.output, "output_hash": hash_value(run.output)},
            )
        except TaskCancelledError as exc:
            ctx.mark_failed(exc)
            self._close_inflight_tools(bus, exc.code, "task cancelled")
            run.transition_to(TaskStatus.CANCELLED)
            bus.emit(
                EventType.TASK_CANCELLED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except NonIdempotentRetryWarning as exc:
            ctx.mark_failed(exc)
            run.transition_to(TaskStatus.FAILED)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except FrameworkError as exc:
            ctx.mark_failed(exc)
            run.transition_to(TaskStatus.FAILED)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except asyncio.TimeoutError:
            exc = TaskTimeoutError(
                f"task timed out after {spec.timeout_seconds}s",
                details={"timeout_seconds": spec.timeout_seconds},
            )
            ctx.mark_failed(exc)
            self._close_inflight_tools(bus, exc.code, "task timed out")
            run.transition_to(TaskStatus.FAILED)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except Exception as exc:  # pragma: no cover - 兜底
            error = FrameworkError(f"unexpected runtime error: {exc}", cause=exc)
            ctx.mark_failed(error)
            run.transition_to(TaskStatus.FAILED)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": error.code, "message": str(error)},
            )
        return run

    @staticmethod
    def _close_inflight_tools(bus: EventBus, error_code: str, reason: str) -> None:
        """任务被取消/超时时，为没有收到 END 的工具调用补一条 UNKNOWN 结果。"""
        started: dict[str, tuple[int, int, dict[str, Any]]] = {}
        closed: set[str] = set()
        for event in bus.events:
            if event.event_type == EventType.TOOL_CALL_START:
                call_id = event.payload.get("call_id")
                if call_id:
                    started[call_id] = (event.step_index, event.attempt, event.payload)
            elif event.event_type == EventType.TOOL_CALL_END:
                call_id = event.payload.get("call_id")
                if call_id:
                    closed.add(call_id)
        for call_id, (step_index, attempt, payload) in started.items():
            if call_id in closed:
                continue
            bus.emit(
                EventType.TOOL_CALL_END,
                step_index=step_index,
                attempt=attempt,
                payload={
                    "tool_name": payload.get("tool_name"),
                    "call_id": call_id,
                    "status": "UNKNOWN",
                    "error_code": error_code,
                    "message": reason,
                },
            )

    # ------------------------------------------------------------------
    # 图执行
    # ------------------------------------------------------------------
    async def _execute_graph(self, ctx: ExecutionContext) -> None:
        plan = ctx.plan
        incoming = plan.incoming()
        resolved: dict[str, set[tuple[str, str]]] = defaultdict(set)
        active: dict[str, set[tuple[str, str]]] = defaultdict(set)
        processed: set[str] = set()
        queue: deque[str] = deque([plan.entry])

        start_edge = ("__start__", "__start__")
        resolved[plan.entry].add(start_edge)
        active[plan.entry].add(start_edge)

        def ready(node_id: str) -> bool:
            return all(edge in resolved[node_id] for edge in incoming.get(node_id, []))

        def enqueue_if_ready(node_id: str) -> None:
            if node_id not in processed and ready(node_id):
                queue.append(node_id)

        while queue:
            node_id = queue.popleft()
            if node_id in processed:
                continue
            if not ready(node_id):
                continue
            processed.add(node_id)
            ctx.check_cancelled()

            node = plan.nodes[node_id]
            if not active[node_id]:
                for target, label in plan.outgoing(node_id):
                    resolved[target].add((node_id, label))
                    enqueue_if_ready(target)
                continue

            selected: str | None = None
            if node.kind == "step":
                await self._execute_step(node, ctx)
                selected = "next"
            elif node.kind == "branch":
                selected = await self._execute_branch(node, ctx)
            elif node.kind == "join":
                selected = "next"
            elif node.kind == "end":
                await self._execute_end(node, ctx)
            else:  # pragma: no cover
                raise PlanValidationError(f"unknown node kind {node.kind!r}")

            for target, label in plan.outgoing(node_id):
                resolved[target].add((node_id, label))
                if selected is not None and label == selected:
                    active[target].add((node_id, label))
                enqueue_if_ready(target)

    # ------------------------------------------------------------------
    # 节点执行
    # ------------------------------------------------------------------
    async def _execute_step(self, node: PlanNode, ctx: ExecutionContext) -> Any:
        ctx.step_count += 1
        ctx.check_step_limit()
        step_index = ctx.next_step_index()
        max_attempts = ctx.spec.retry_policy.max_attempts
        attempt = 0

        while True:
            attempt += 1
            ctx.run.attempt_count = attempt
            ctx.emit(
                EventType.STEP_START,
                {"node_id": node.id, "name": node.name, "kind": "step", "attempt": attempt},
                step_index=step_index,
                attempt=attempt,
            )
            try:
                result = await asyncio.wait_for(
                    self._perform_action(node, ctx, step_index, attempt),
                    timeout=self._step_timeout(node),
                )
                if node.output_key:
                    ctx.state[node.output_key] = result
                ctx.emit(
                    EventType.STEP_END,
                    {
                        "node_id": node.id,
                        "output_key": node.output_key,
                        "output": result,
                        "output_hash": hash_value(result),
                    },
                    step_index=step_index,
                    attempt=attempt,
                )
                return result
            except TaskCancelledError:
                raise
            except asyncio.TimeoutError as exc:
                error = StepTimeoutError(
                    f"step {node.name!r} timed out after {self._step_timeout(node)}s",
                    details={"node_id": node.id, "timeout": True},
                    cause=exc,
                )
                ctx.emit(
                    EventType.ERROR_OCCURRED,
                    {
                        "node_id": node.id,
                        "error_code": error.code,
                        "retryable": error.retryable,
                        "attempt": attempt,
                        "message": str(error),
                        "details": error.details,
                    },
                    step_index=step_index,
                    attempt=attempt,
                )
                if not self._can_retry(node, ctx, attempt, max_attempts, error):
                    raise error
                await self._schedule_retry(ctx, step_index, attempt, error)
            except FrameworkError as exc:
                ctx.emit(
                    EventType.ERROR_OCCURRED,
                    {
                        "node_id": node.id,
                        "error_code": exc.code,
                        "retryable": exc.retryable,
                        "attempt": attempt,
                        "message": str(exc),
                        "details": exc.details,
                    },
                    step_index=step_index,
                    attempt=attempt,
                )
                if not self._can_retry(node, ctx, attempt, max_attempts, exc):
                    raise
                await self._schedule_retry(ctx, step_index, attempt, exc)
            except Exception as exc:  # pragma: no cover - 兜底
                error = FrameworkError(
                    f"step {node.name!r} failed unexpectedly: {exc}",
                    details={"node_id": node.id},
                    cause=exc,
                )
                ctx.emit(
                    EventType.ERROR_OCCURRED,
                    {"node_id": node.id, "error_code": error.code, "message": str(error)},
                    step_index=step_index,
                    attempt=attempt,
                )
                raise error from exc

    def _can_retry(
        self,
        node: PlanNode,
        ctx: ExecutionContext,
        attempt: int,
        max_attempts: int,
        error: FrameworkError,
    ) -> bool:
        if attempt >= max_attempts or not error.retryable:
            return False
        if self._unsafe_retry(node, ctx):
            raise NonIdempotentRetryWarning(
                f"step {node.name!r} is a non-idempotent side-effecting tool; refusing to retry",
                details={"node_id": node.id, "original_error": error.code},
                cause=error,
            )
        return True

    async def _schedule_retry(
        self,
        ctx: ExecutionContext,
        step_index: int,
        attempt: int,
        error: FrameworkError,
    ) -> None:
        policy = ctx.spec.retry_policy
        delay = min(
            policy.initial_delay_seconds * (policy.backoff_factor ** (attempt - 1)),
            policy.max_delay_seconds,
        )
        ctx.emit(
            EventType.RETRY_SCHEDULED,
            {
                "attempt": attempt,
                "next_attempt": attempt + 1,
                "delay_seconds": delay,
                "error_code": error.code,
            },
            step_index=step_index,
            attempt=attempt,
        )
        ctx.run.transition_to(TaskStatus.RETRYING)
        await asyncio.sleep(delay)
        ctx.run.transition_to(TaskStatus.RUNNING)

    async def _perform_action(
        self,
        node: PlanNode,
        ctx: ExecutionContext,
        step_index: int,
        attempt: int,
    ) -> Any:
        action = node.action
        if action is None:
            raise PlanValidationError(f"step {node.name!r} has no action")
        input_data = ctx.resolve(node.input_mapping)

        if action.kind == "tool":
            if not action.tool_name:
                raise PlanValidationError(f"step {node.name!r} has no tool_name")
            result = await ctx.call_tool(
                tool_name=action.tool_name,
                input_data=input_data,
                step_index=step_index,
                attempt=attempt,
            )
            if node.output_schema is not None:
                validate_instance(
                    node.output_schema,
                    result,
                    error_cls=OutputValidationError,
                    context=f"step {node.name!r} output",
                )
            return result

        if action.kind == "function":
            if action.func is None:
                raise PlanValidationError(f"step {node.name!r} has no function")
            return await ctx.call_function(
                func=action.func,
                input_data=input_data,
                output_schema=node.output_schema,
            )

        if action.kind == "llm":
            if not action.llm_messages:
                raise PlanValidationError(f"step {node.name!r} has no messages")
            messages = ctx.resolve(action.llm_messages)
            return await ctx.call_llm(
                messages=messages,
                output_schema=action.llm_output_schema,
                step_index=step_index,
                attempt=attempt,
            )

        raise PlanValidationError(f"unsupported action kind {action.kind!r}")

    async def _execute_branch(self, node: PlanNode, ctx: ExecutionContext) -> str:
        ctx.step_count += 1
        ctx.check_step_limit()
        step_index = ctx.next_step_index()
        if node.condition is None:
            raise BranchConditionError(f"branch {node.name!r} has no condition")
        ctx.emit(
            EventType.STEP_START,
            {"node_id": node.id, "name": node.name, "kind": "branch"},
            step_index=step_index,
        )
        try:
            label = node.condition.fn(ctx.state_view())
            if inspect.isawaitable(label):
                label = await label
        except Exception as exc:
            raise BranchConditionError(
                f"branch {node.name!r} condition failed: {exc}",
                details={"node_id": node.id},
                cause=exc,
            ) from exc
        if not isinstance(label, str):
            raise BranchConditionError(
                f"branch {node.name!r} condition must return str, got {type(label).__name__}",
                details={"node_id": node.id},
            )
        if label not in node.routes:
            if node.default_route is None:
                raise BranchConditionError(
                    f"branch {node.name!r} got unknown route {label!r} and has no default",
                    details={"node_id": node.id, "label": label},
                )
            label = node.default_route
        ctx.emit(
            EventType.STEP_END,
            {"node_id": node.id, "selected_route": label},
            step_index=step_index,
        )
        return label

    async def _execute_end(self, node: PlanNode, ctx: ExecutionContext) -> None:
        output = ctx.resolve(node.output_mapping)
        if not isinstance(output, dict):
            raise OutputValidationError(
                f"end {node.name!r} output_mapping must resolve to an object",
                details={"output": output},
            )
        validate_instance(
            ctx.spec.output_schema,
            output,
            error_cls=OutputValidationError,
            context="task output",
        )
        for validator in ctx.spec.validators:
            try:
                result = validator.func(output)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                raise BusinessRuleViolationError(
                    f"business validator {validator.name!r} failed: {exc}",
                    details={"validator": validator.name},
                    cause=exc,
                ) from exc
        self._check_termination(ctx.spec, output)
        ctx.run.output = output

    @staticmethod
    def _check_termination(spec: TaskSpec, output: dict[str, Any]) -> None:
        condition = spec.termination_condition
        if condition.type != "output_match" or condition.value is None:
            return
        expected = condition.value
        if isinstance(expected, str):
            haystack = json.dumps(output, ensure_ascii=False)
            if expected not in haystack:
                raise TerminationConditionError(
                    f"termination output_match {expected!r} was not found",
                    details={"expected": expected, "output": output},
                )
        elif isinstance(expected, dict):
            for key, value in expected.items():
                if output.get(key) != value:
                    raise TerminationConditionError(
                        f"termination output_match failed at {key!r}",
                        details={"expected": expected, "output": output},
                    )

    @staticmethod
    def _step_timeout(node: PlanNode) -> float:
        return node.timeout_seconds or DEFAULT_STEP_TIMEOUT

    @staticmethod
    def _unsafe_retry(node: PlanNode, ctx: ExecutionContext) -> bool:
        if node.action is None or node.action.kind != "tool" or not node.action.tool_name:
            return False
        try:
            tool = ctx.tools.get(node.action.tool_name)
        except FrameworkError:
            return False
        return bool(tool.spec.side_effect and not tool.spec.idempotent)
