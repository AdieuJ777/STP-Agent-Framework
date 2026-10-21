"""统一执行器：两种前端生成的 TaskIR 都由这里执行。"""
from __future__ import annotations

import asyncio
import inspect
import json
import time
from collections import defaultdict, deque
from typing import Any
from uuid import uuid4

from .checkpoint import (
    CHECKPOINT_VERSION,
    Checkpoint,
    CheckpointStore,
    plan_hash,
    spec_hash,
)
from .context import ExecutionContext, hash_value
from .errors import (
    BranchConditionError,
    BusinessRuleViolationError,
    CheckpointError,
    CheckpointIncompatibleError,
    CheckpointNotFoundError,
    ContractValidationError,
    EventLogError,
    FrameworkError,
    MaxStepsExceededError,
    NonIdempotentRetryWarning,
    OutputValidationError,
    PlanValidationError,
    ResumeNotSupportedError,
    StepTimeoutError,
    TaskCancelledError,
    TaskTimeoutError,
    TerminationConditionError,
)
from .events import EventBus, EventSink
from .llm import LLMBackend, MockLLM
from .models import EventType, RunError, TaskRun, TaskSpec, TaskStatus
from .plan import PlanNode, TaskIR
from .schema import validate_instance
from .tokens import CancellationToken
from .tools import ToolRegistry

# 这些错误发生时，待执行步骤中的非幂等副作用工具可能已经执行过，
# 因此不能自动恢复。
_RISKY_ERROR_CODES = {
    "E_TOOL_CALL",
    "E_STEP_TIMEOUT",
    "E_TASK_TIMEOUT",
    "E_CONTRACT_OUTPUT",
    "E_MODEL_CALL",
    "E_STRUCTURED_OUTPUT",
    "W_NON_IDEMPOTENT",
    "E_FRAMEWORK",
}

DEFAULT_STEP_TIMEOUT = 60.0


class TaskRuntime:
    def __init__(
        self,
        backend: LLMBackend | None = None,
        tools: ToolRegistry | None = None,
        event_store: EventSink | None = None,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        self.backend = backend or MockLLM(default={})
        self.tools = tools or ToolRegistry()
        self.event_store = event_store
        self.checkpoint_store = checkpoint_store
        self.last_bus: EventBus | None = None

    @property
    def last_events(self):
        return self.last_bus.events if self.last_bus is not None else []

    async def run(
        self,
        spec: TaskSpec,
        plan: TaskIR,
        input_data: dict[str, Any] | None = None,
        *,
        run_id: str | None = None,
        cancellation: CancellationToken | None = None,
        resume_from: str | Checkpoint | None = None,
    ) -> TaskRun:
        from .validation import validate_plan

        validate_plan(spec, plan).raise_if_errors()
        if str(spec.protocol_version).split(".", 1)[0] != "1":
            raise PlanValidationError(
                f"unsupported protocol major version {spec.protocol_version!r}"
            )

        checkpoint = self._resolve_resume_checkpoint(resume_from, spec, plan)
        if checkpoint is not None:
            if input_data is None:
                input_data = checkpoint.input
            elif input_data != checkpoint.input:
                raise CheckpointIncompatibleError(
                    "resume input does not match the checkpoint input",
                    details={"checkpoint_id": checkpoint.checkpoint_id},
                )
        if input_data is None:
            raise ValueError("input_data is required when not resuming from a checkpoint")

        elapsed_before_resume = (
            checkpoint.elapsed_seconds if checkpoint is not None else 0.0
        )
        remaining_timeout = max(0.0, spec.timeout_seconds - elapsed_before_resume)

        run = TaskRun(
            run_id=run_id or uuid4().hex,
            task_spec_id=spec.task_id,
            input=input_data,
            resumed_from_run_id=checkpoint.run_id if checkpoint is not None else None,
            model_call_count=checkpoint.model_call_count if checkpoint is not None else 0,
            attempt_count=checkpoint.attempt_count if checkpoint is not None else 0,
            last_checkpoint_id=checkpoint.checkpoint_id if checkpoint is not None else None,
        )
        sinks: list[EventSink] = [self.event_store] if self.event_store is not None else []
        bus = EventBus(run.run_id, sinks, task_id=spec.task_id)
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
        if checkpoint is not None:
            ctx.apply_snapshot(checkpoint.to_snapshot())

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
            payload={
                "task_id": spec.task_id,
                "task_name": spec.name,
                "run_id": run.run_id,
                "resumed_from_run_id": run.resumed_from_run_id,
            },
        )

        if checkpoint is not None:
            bus.emit(
                EventType.CHECKPOINT_LOADED,
                payload={
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "sequence": checkpoint.sequence,
                    "resumed_from_run_id": checkpoint.run_id,
                    "completed_steps": list(checkpoint.completed_steps),
                    "next_step_id": checkpoint.next_step_id,
                },
            )
            bus.emit(
                EventType.TASK_RESUMED,
                payload={
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "resumed_from_run_id": checkpoint.run_id,
                    "next_step_id": checkpoint.next_step_id,
                    "completed_steps": list(checkpoint.completed_steps),
                },
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
            if remaining_timeout <= 0:
                raise TaskTimeoutError(
                    "task timeout budget exhausted before resume could start",
                    details={
                        "timeout_seconds": spec.timeout_seconds,
                        "elapsed_seconds": elapsed_before_resume,
                    },
                )
            await asyncio.wait_for(self._execute_graph(ctx), timeout=remaining_timeout)
            if run.output is None:
                raise FrameworkError("plan finished without producing output")
            # 事件 sink 失败不能被静默忽略：任务仍然处于 RUNNING，
            # 可以合法地转为 FAILED，而不会“已经成功但日志失败”。
            if bus.sink_errors:
                raise EventLogError(
                    "event sink failed before task completion",
                    details={"errors": list(bus.sink_errors)},
                )
            bus.emit(
                EventType.TASK_COMPLETED,
                payload={"output": run.output, "output_hash": hash_value(run.output)},
            )
            if bus.sink_errors:
                raise EventLogError(
                    "event sink failed while writing TASK_COMPLETED",
                    details={"errors": list(bus.sink_errors)},
                )
            run.transition_to(TaskStatus.SUCCEEDED)
        except TaskCancelledError as exc:
            ctx.mark_failed(exc)
            self._close_inflight_tools(ctx, exc.code, "task cancelled")
            run.transition_to(TaskStatus.CANCELLED)
            self._save_failure_checkpoint(ctx, exc)
            bus.emit(
                EventType.TASK_CANCELLED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except NonIdempotentRetryWarning as exc:
            ctx.mark_failed(exc)
            run.transition_to(TaskStatus.FAILED)
            self._save_failure_checkpoint(ctx, exc)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except FrameworkError as exc:
            ctx.mark_failed(exc)
            run.transition_to(TaskStatus.FAILED)
            self._save_failure_checkpoint(ctx, exc)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except asyncio.TimeoutError:
            exc = TaskTimeoutError(
                f"task timed out after a remaining budget of {remaining_timeout}s",
                details={
                    "timeout_seconds": spec.timeout_seconds,
                    "elapsed_seconds": elapsed_before_resume,
                    "remaining_seconds": remaining_timeout,
                },
            )
            ctx.mark_failed(exc)
            self._close_inflight_tools(ctx, exc.code, "task timed out")
            run.transition_to(TaskStatus.FAILED)
            self._save_failure_checkpoint(ctx, exc)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": exc.code, "message": str(exc), "details": exc.details},
            )
        except Exception as exc:  # pragma: no cover - 兜底
            error = FrameworkError(f"unexpected runtime error: {exc}", cause=exc)
            ctx.mark_failed(error)
            self._close_inflight_tools(ctx, error.code, "unexpected runtime error")
            run.transition_to(TaskStatus.FAILED)
            self._save_failure_checkpoint(ctx, error)
            bus.emit(
                EventType.TASK_FAILED,
                payload={"error_code": error.code, "message": str(error)},
            )
        return run

    def _resolve_resume_checkpoint(
        self,
        resume_from: str | Checkpoint | None,
        spec: TaskSpec,
        plan: TaskIR,
    ) -> Checkpoint | None:
        if resume_from is None:
            return None
        if isinstance(resume_from, Checkpoint):
            checkpoint = resume_from
        else:
            if self.checkpoint_store is None:
                raise CheckpointError(
                    "resume requires a configured checkpoint_store",
                    details={"checkpoint_id": str(resume_from)},
                )
            checkpoint = self.checkpoint_store.load(str(resume_from))

        if checkpoint.checkpoint_version != CHECKPOINT_VERSION:
            raise CheckpointIncompatibleError(
                f"unsupported checkpoint_version {checkpoint.checkpoint_version}",
                details={"expected": CHECKPOINT_VERSION},
            )
        if checkpoint.task_id != spec.task_id:
            raise CheckpointIncompatibleError(
                f"checkpoint task_id {checkpoint.task_id!r} does not match {spec.task_id!r}",
                details={"checkpoint_id": checkpoint.checkpoint_id},
            )
        checkpoint_major = str(checkpoint.protocol_version).split(".", 1)[0]
        spec_major = str(spec.protocol_version).split(".", 1)[0]
        if checkpoint_major != spec_major:
            raise CheckpointIncompatibleError(
                f"checkpoint protocol major {checkpoint_major!r} does not match {spec_major!r}",
                details={"checkpoint_id": checkpoint.checkpoint_id},
            )
        if checkpoint.plan_hash != plan_hash(plan):
            raise CheckpointIncompatibleError(
                "checkpoint plan_hash does not match the current plan",
                details={"checkpoint_id": checkpoint.checkpoint_id},
            )
        if checkpoint.spec_hash != spec_hash(spec):
            raise CheckpointIncompatibleError(
                "checkpoint spec_hash does not match the current TaskSpec",
                details={"checkpoint_id": checkpoint.checkpoint_id},
            )
        if not checkpoint.resume_safe:
            raise ResumeNotSupportedError(
                f"checkpoint is not safely resumable: {checkpoint.resume_block_reason}",
                details={
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "reason": checkpoint.resume_block_reason,
                },
            )
        return checkpoint

    async def resume(
        self,
        spec: TaskSpec,
        plan: TaskIR,
        checkpoint_id: str | None = None,
        *,
        run_id: str | None = None,
        input_data: dict[str, Any] | None = None,
        cancellation: CancellationToken | None = None,
    ) -> TaskRun:
        """从检查点恢复执行；``run_id`` 用于加载该次运行的最近检查点。"""
        event_run_id = run_id or checkpoint_id or uuid4().hex
        try:
            if self.checkpoint_store is None:
                raise CheckpointError("resume requires a configured checkpoint_store")
            if checkpoint_id is None:
                if run_id is None:
                    raise CheckpointError("checkpoint_id or run_id is required to resume")
                checkpoint = self.checkpoint_store.load_latest(run_id, spec.task_id)
                if checkpoint is None:
                    raise CheckpointNotFoundError(
                        f"no checkpoint found for run_id {run_id!r}",
                        details={"run_id": run_id, "task_id": spec.task_id},
                    )
            else:
                checkpoint = self.checkpoint_store.load(checkpoint_id)
            event_run_id = checkpoint.run_id
            return await self.run(
                spec,
                plan,
                input_data,
                cancellation=cancellation,
                resume_from=checkpoint,
            )
        except FrameworkError as exc:
            self._emit_recovery_failure(spec, event_run_id, checkpoint_id, exc)
            raise

    def _emit_recovery_failure(
        self,
        spec: TaskSpec,
        run_id: str,
        checkpoint_id: str | None,
        error: FrameworkError,
    ) -> None:
        """恢复预检查失败时也留下一条可查询的事件。

        此时还没有真正创建执行 run，因此事件只记录失败原因和检查点标识。
        """
        sinks: list[EventSink] = [self.event_store] if self.event_store is not None else []
        bus = EventBus(run_id, sinks, task_id=spec.task_id)
        self.last_bus = bus
        try:
            bus.emit(
                EventType.RECOVERY_FAILED,
                payload={
                    "task_id": spec.task_id,
                    "checkpoint_id": checkpoint_id,
                    "error_code": error.code,
                    "message": str(error),
                    "details": error.details,
                },
            )
        except Exception:  # pragma: no cover - 事件 sink 不应掩盖原始恢复错误
            pass

    @staticmethod
    def _close_inflight_tools(
        ctx: ExecutionContext,
        error_code: str,
        reason: str,
    ) -> list[dict[str, Any]]:
        """任务被取消/超时时，为没有收到 END 的工具调用补一条 UNKNOWN 结果。"""
        started: dict[str, tuple[int, int, dict[str, Any], str | None]] = {}
        closed: set[str] = set()
        for event in ctx.bus.events:
            if event.event_type == EventType.TOOL_CALL_START:
                call_id = event.payload.get("call_id")
                if call_id:
                    started[call_id] = (
                        event.step_index,
                        event.attempt,
                        event.payload,
                        event.step_id,
                    )
            elif event.event_type == EventType.TOOL_CALL_END:
                call_id = event.payload.get("call_id")
                if call_id:
                    closed.add(call_id)

        unknown: list[dict[str, Any]] = []
        for call_id, (step_index, attempt, payload, step_id) in started.items():
            if call_id in closed:
                continue
            unknown.append(
                {
                    "tool_name": payload.get("tool_name"),
                    "call_id": call_id,
                    "step_index": step_index,
                    "attempt": attempt,
                    "status": "UNKNOWN",
                }
            )
            ctx.bus.emit(
                EventType.TOOL_CALL_END,
                step_index=step_index,
                step_id=step_id,
                attempt=attempt,
                payload={
                    "tool_name": payload.get("tool_name"),
                    "call_id": call_id,
                    "status": "UNKNOWN",
                    "error_code": error_code,
                    "message": reason,
                },
            )
        if unknown:
            ctx.unknown_tool_calls.extend(unknown)
        return unknown

    def _save_checkpoint(
        self,
        ctx: ExecutionContext,
        *,
        step_index: int = -1,
        attempt: int = 0,
    ) -> str | None:
        """在步骤成功后保存检查点；保存失败不重试该步骤。"""
        if self.checkpoint_store is None:
            return None
        try:
            snapshot = ctx.export_snapshot()
            plan_hash_value = plan_hash(ctx.plan)
            spec_hash_value = spec_hash(ctx.spec)
            checkpoint = Checkpoint(
                run_id=ctx.run.run_id,
                task_id=ctx.spec.task_id,
                task_name=ctx.spec.name,
                task_version=spec_hash_value[:16],
                protocol_version=ctx.spec.protocol_version,
                status=ctx.run.status.value,
                plan_hash=plan_hash_value,
                spec_hash=spec_hash_value,
                input=snapshot["input"],
                state=snapshot["state"],
                step_results=snapshot["step_results"],
                completed_steps=snapshot["completed_steps"],
                branch_routes=snapshot["branch_routes"],
                step_count=snapshot["step_count"],
                model_call_count=snapshot["model_call_count"],
                attempt_count=snapshot["attempt_count"],
                next_step_id=snapshot["next_step_id"],
                elapsed_seconds=snapshot["elapsed_seconds"],
            )
            checkpoint_id = self.checkpoint_store.save(checkpoint)
        except FrameworkError as exc:
            ctx.emit(
                EventType.ERROR_OCCURRED,
                {
                    "error_code": exc.code,
                    "retryable": False,
                    "attempt": attempt,
                    "message": str(exc),
                    "details": exc.details,
                },
                step_index=step_index,
                attempt=attempt,
            )
            raise
        except Exception as exc:
            error = CheckpointError(
                f"failed to build checkpoint: {exc}",
                details={"run_id": ctx.run.run_id},
                cause=exc,
            )
            ctx.emit(
                EventType.ERROR_OCCURRED,
                {
                    "error_code": error.code,
                    "retryable": False,
                    "attempt": attempt,
                    "message": str(error),
                },
                step_index=step_index,
                attempt=attempt,
            )
            raise error from exc

        ctx.run.last_checkpoint_id = checkpoint_id
        ctx.emit(
            EventType.CHECKPOINT_SAVED,
            {
                "checkpoint_id": checkpoint_id,
                "sequence": checkpoint.sequence,
                "completed_steps": list(checkpoint.completed_steps),
                "next_step_id": checkpoint.next_step_id,
            },
            step_index=step_index,
            attempt=attempt,
        )
        return checkpoint_id

    def _save_failure_checkpoint(
        self,
        ctx: ExecutionContext,
        error: FrameworkError,
    ) -> str | None:
        """保存失败现场。恢复安全性在这里判定，失败不掩盖原始错误。"""
        if self.checkpoint_store is None:
            return None
        resume_safe, reason = self._resume_safety(ctx, error)
        try:
            snapshot = ctx.export_snapshot()
            plan_hash_value = plan_hash(ctx.plan)
            spec_hash_value = spec_hash(ctx.spec)
            checkpoint = Checkpoint(
                run_id=ctx.run.run_id,
                task_id=ctx.spec.task_id,
                task_name=ctx.spec.name,
                task_version=spec_hash_value[:16],
                protocol_version=ctx.spec.protocol_version,
                status=ctx.run.status.value,
                plan_hash=plan_hash_value,
                spec_hash=spec_hash_value,
                input=snapshot["input"],
                state=snapshot["state"],
                step_results=snapshot["step_results"],
                completed_steps=snapshot["completed_steps"],
                branch_routes=snapshot["branch_routes"],
                step_count=snapshot["step_count"],
                model_call_count=snapshot["model_call_count"],
                attempt_count=snapshot["attempt_count"],
                next_step_id=snapshot["next_step_id"],
                elapsed_seconds=snapshot["elapsed_seconds"],
                error=ctx.run.error
                or RunError(code=error.code, message=str(error), details=error.details),
                unknown_tool_calls=list(ctx.unknown_tool_calls),
                resume_safe=resume_safe,
                resume_block_reason=reason,
            )
            checkpoint_id = self.checkpoint_store.save(checkpoint)
        except Exception as exc:
            # 检查点保存失败不应覆盖真正的任务错误。
            error_code = getattr(exc, "code", "E_CHECKPOINT_IO")
            try:
                ctx.bus.emit(
                    EventType.ERROR_OCCURRED,
                    payload={
                        "error_code": error_code,
                        "retryable": False,
                        "message": f"failed to save failure checkpoint: {exc}",
                    },
                    step_index=ctx.step_index,
                    attempt=ctx.run.attempt_count,
                )
            except Exception:  # pragma: no cover - 事件总线本身不应失败
                pass
            return None

        ctx.run.last_checkpoint_id = checkpoint_id
        ctx.bus.emit(
            EventType.CHECKPOINT_SAVED,
            payload={
                "checkpoint_id": checkpoint_id,
                "sequence": checkpoint.sequence,
                "completed_steps": list(checkpoint.completed_steps),
                "next_step_id": checkpoint.next_step_id,
                "resume_safe": resume_safe,
                "resume_block_reason": reason,
            },
            step_index=ctx.step_index,
            attempt=ctx.run.attempt_count,
        )
        return checkpoint_id

    @staticmethod
    def _resume_safety(
        ctx: ExecutionContext,
        error: FrameworkError,
    ) -> tuple[bool, str | None]:
        if error.code == "E_CANCELLED":
            return False, "task was cancelled; cancelled runs are not resumable"
        if error.code == "E_RESUME_UNSUPPORTED":
            return False, "the plan is not safely replayable; automatic resume was refused"

        # 步骤已经执行成功、但检查点写入失败时，从更早的检查点恢复可能
        # 重复该步骤；如果它是非幂等副作用工具，必须拒绝自动恢复。
        if error.code in {"E_CHECKPOINT_IO", "E_STATE_SERIALIZATION"}:
            last_id = ctx.completed_steps[-1] if ctx.completed_steps else None
            if last_id is not None and last_id in ctx.plan.nodes:
                last = ctx.plan.nodes[last_id]
                if (
                    last.kind == "step"
                    and last.action is not None
                    and last.action.kind == "tool"
                    and last.action.tool_name
                ):
                    try:
                        last_tool = ctx.tools.get(last.action.tool_name)
                    except FrameworkError:
                        last_tool = None
                    if (
                        last_tool is not None
                        and last_tool.spec.side_effect
                        and not last_tool.spec.idempotent
                    ):
                        return (
                            False,
                            "checkpoint write failed after a non-idempotent tool step; "
                            "resuming from an earlier checkpoint could repeat the side effect",
                        )

        for call in ctx.unknown_tool_calls:
            tool_name = str(call.get("tool_name"))
            try:
                tool = ctx.tools.get(tool_name)
            except FrameworkError:
                return False, f"tool {tool_name!r} has an UNKNOWN result and is not registered"
            if tool.spec.side_effect and not tool.spec.idempotent:
                return (
                    False,
                    f"tool {tool_name!r} is non-idempotent and its result is UNKNOWN",
                )

        pending_id = ctx.next_step_id
        if pending_id is not None and pending_id in ctx.plan.nodes:
            pending = ctx.plan.nodes[pending_id]
            if (
                pending.kind == "step"
                and pending.action is not None
                and pending.action.kind == "tool"
                and pending.action.tool_name
                and error.code in _RISKY_ERROR_CODES
            ):
                try:
                    tool = ctx.tools.get(pending.action.tool_name)
                except FrameworkError:
                    tool = None
                if tool is not None and tool.spec.side_effect and not tool.spec.idempotent:
                    return (
                        False,
                        f"pending tool {tool.spec.tool_name!r} is non-idempotent "
                        f"and may already have executed ({error.code})",
                    )
        return True, None

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

            node = plan.nodes[node_id]
            if not active[node_id]:
                for target, label in plan.outgoing(node_id):
                    resolved[target].add((node_id, label))
                    enqueue_if_ready(target)
                continue

            ctx.current_node_id = node_id
            # 已完成的节点只做跳过；不要把 next_step_id 从“下一个待执行节点”
            # 覆盖回当前节点。未执行的节点则先记为待执行，取消/失败时可定位。
            if not ctx.is_completed(node_id) and node.kind in ("step", "branch", "end"):
                ctx.next_step_id = node_id
            ctx.check_cancelled()

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
        if ctx.is_completed(node.id):
            step_index = ctx.next_step_index()
            if not node.output_key or node.output_key not in ctx.state:
                raise CheckpointIncompatibleError(
                    f"completed step {node.id!r} has no output_key "
                    f"{node.output_key!r} in restored state",
                    details={"node_id": node.id, "output_key": node.output_key},
                )
            result = ctx.state[node.output_key]
            ctx.next_step_id = node.next
            ctx.emit(
                EventType.STEP_SKIPPED,
                {
                    "node_id": node.id,
                    "name": node.name,
                    "kind": "step",
                    "output_key": node.output_key,
                    "input_hash": hash_value(ctx.resolve(node.input_mapping)),
                    "output_hash": hash_value(result),
                    "attempt": 0,
                },
                step_index=step_index,
            )
            return result

        ctx.next_step_id = node.id
        ctx.ensure_step_capacity()
        ctx.step_count += 1
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
            started = time.perf_counter()
            try:
                action_input = ctx.resolve(node.input_mapping)
                result = await asyncio.wait_for(
                    self._perform_action(node, ctx, step_index, attempt, action_input),
                    timeout=self._step_timeout(node),
                )
            except TaskCancelledError:
                raise
            except asyncio.TimeoutError as exc:
                duration = (time.perf_counter() - started) * 1000
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
                    duration_ms=duration,
                )
                # 步骤超时会取消在途工具调用；即使工具不支持取消，也要把
                # 没有 END 的调用补记为 UNKNOWN，供恢复安全性判断使用。
                self._close_inflight_tools(ctx, error.code, "step timed out")
                if not self._can_retry(node, ctx, attempt, max_attempts, error):
                    raise error
                await self._schedule_retry(ctx, step_index, attempt, error)
            except FrameworkError as exc:
                duration = (time.perf_counter() - started) * 1000
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
                    duration_ms=duration,
                )
                if not self._can_retry(node, ctx, attempt, max_attempts, exc):
                    raise
                await self._schedule_retry(ctx, step_index, attempt, exc)
            except Exception as exc:  # pragma: no cover - 兜底
                duration = (time.perf_counter() - started) * 1000
                error = FrameworkError(
                    f"step {node.name!r} failed unexpectedly: {exc}",
                    details={"node_id": node.id},
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
                    },
                    step_index=step_index,
                    attempt=attempt,
                    duration_ms=duration,
                )
                raise error from exc
            else:
                duration = (time.perf_counter() - started) * 1000
                if node.output_key:
                    ctx.state[node.output_key] = result
                ctx.emit(
                    EventType.STEP_END,
                    {
                        "node_id": node.id,
                        "output_key": node.output_key,
                        "input_hash": hash_value(action_input),
                        "output": result,
                        "output_hash": hash_value(result),
                    },
                    step_index=step_index,
                    attempt=attempt,
                    duration_ms=duration,
                )
                ctx.mark_completed(node.id, node.next)
                # 检查点写入放在重试 try/except 之外：写失败不能触发步骤重试。
                self._save_checkpoint(ctx, step_index=step_index, attempt=attempt)
                return result

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
        input_data: dict[str, Any],
    ) -> Any:
        action = node.action
        if action is None:
            raise PlanValidationError(f"step {node.name!r} has no action")

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

    async def _evaluate_branch(self, node: PlanNode, ctx: ExecutionContext) -> str:
        if node.condition is None:
            raise BranchConditionError(f"branch {node.name!r} has no condition")
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
        return label

    async def _execute_branch(self, node: PlanNode, ctx: ExecutionContext) -> str:
        if ctx.is_completed(node.id):
            step_index = ctx.next_step_index()
            previous = ctx.branch_routes.get(node.id)
            if previous is None:
                raise CheckpointIncompatibleError(
                    f"completed branch {node.id!r} has no recorded route",
                    details={"node_id": node.id},
                )
            label = await self._evaluate_branch(node, ctx)
            if label != previous:
                raise ResumeNotSupportedError(
                    f"branch {node.name!r} selected {label!r} on resume "
                    f"but selected {previous!r} during the original run",
                    details={
                        "node_id": node.id,
                        "original_route": previous,
                        "resume_route": label,
                    },
                )
            ctx.next_step_id = node.routes[label]
            ctx.emit(
                EventType.STEP_SKIPPED,
                {
                    "node_id": node.id,
                    "name": node.name,
                    "kind": "branch",
                    "selected_route": label,
                    "attempt": 0,
                },
                step_index=step_index,
            )
            return label

        ctx.next_step_id = node.id
        ctx.ensure_step_capacity()
        ctx.step_count += 1
        step_index = ctx.next_step_index()
        ctx.emit(
            EventType.STEP_START,
            {"node_id": node.id, "name": node.name, "kind": "branch"},
            step_index=step_index,
        )
        started = time.perf_counter()
        try:
            label = await self._evaluate_branch(node, ctx)
        except FrameworkError as exc:
            ctx.emit(
                EventType.ERROR_OCCURRED,
                {
                    "node_id": node.id,
                    "error_code": exc.code,
                    "retryable": exc.retryable,
                    "attempt": 0,
                    "message": str(exc),
                    "details": exc.details,
                },
                step_index=step_index,
                duration_ms=(time.perf_counter() - started) * 1000,
            )
            raise

        ctx.branch_routes[node.id] = label
        ctx.next_step_id = node.routes[label]
        ctx.emit(
            EventType.STEP_END,
            {"node_id": node.id, "selected_route": label},
            step_index=step_index,
            duration_ms=(time.perf_counter() - started) * 1000,
        )
        ctx.mark_completed(node.id, ctx.next_step_id)
        self._save_checkpoint(ctx, step_index=step_index, attempt=0)
        return label

    async def _execute_end(self, node: PlanNode, ctx: ExecutionContext) -> None:
        ctx.next_step_id = node.id
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
        ctx.next_step_id = None

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
