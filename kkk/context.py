"""运行期执行上下文。"""
from __future__ import annotations

import hashlib
import inspect
import json
import time
from typing import Any, Callable
from uuid import uuid4

from .errors import (
    CheckpointIncompatibleError,
    FrameworkError,
    MaxModelCallsExceededError,
    MaxStepsExceededError,
    ModelCallError,
    OutputValidationError,
    StateSerializationError,
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


def ensure_json_safe(value: Any, *, path: str = "value") -> None:
    """确认值可以做无损 JSON 往返。

    检查点只保存可序列化的任务数据；函数、客户端、协程、set、tuple、
    非字符串键等都应尽早报错，而不是静默丢字段。
    """
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise StateSerializationError(
            f"{path} is not JSON-serializable: {exc}",
            details={"path": path},
            cause=exc,
        ) from exc
    decoded = json.loads(encoded)
    if decoded != value:
        raise StateSerializationError(
            f"{path} is not JSON round-trip safe; use JSON-compatible values "
            "(avoid tuples, sets, non-string dict keys and custom objects)",
            details={"path": path},
        )


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
        # P1：恢复所需的最小执行状态。completed_steps 只记录真正执行过的
        # step / branch 节点，join / end 属于结构节点，不参与跳过判定。
        self.completed_steps: list[str] = []
        self.completed_step_set: set[str] = set()
        self.branch_routes: dict[str, str] = {}
        self.next_step_id: str | None = None
        self.current_node_id: str | None = None
        # 恢复来源信息（仅用于诊断；新 run 的状态仍然是 RUNNING）。
        self.source_run_id: str | None = None
        self.source_status: str | None = None
        # 恢复后仍要遵守任务总超时预算；恢复前的执行耗时从这里续接。
        self.run_started_at = time.monotonic()
        self.elapsed_before_resume = 0.0
        # 超时/取消时没有得到结果的工具调用；失败检查点会据此拒绝不安全恢复。
        self.unknown_tool_calls: list[dict[str, Any]] = []

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
        step_id: str | None = None,
        attempt: int = 0,
        duration_ms: float | None = None,
    ) -> None:
        resolved_index = self.step_index if step_index is None else step_index
        resolved_step_id = step_id
        if resolved_step_id is None and step_index is not None and step_index >= 0:
            resolved_step_id = self.current_node_id
        self.bus.emit(
            event_type,
            step_index=resolved_index,
            step_id=resolved_step_id,
            attempt=attempt,
            payload=payload or {},
            duration_ms=duration_ms,
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

    def ensure_step_capacity(self) -> None:
        """在递增 step_count 之前检查容量。

        这样“因超限而未执行”的节点不会被写进检查点，恢复时不会
        把一次未发生的执行误当成已经发生的执行。
        """
        if self.step_count >= self.spec.max_steps:
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

    # --- 快照 / 恢复 -----------------------------------------------------
    def is_completed(self, node_id: str) -> bool:
        return node_id in self.completed_step_set

    def mark_completed(self, node_id: str, next_step_id: str | None) -> None:
        if node_id not in self.completed_step_set:
            self.completed_steps.append(node_id)
            self.completed_step_set.add(node_id)
        self.next_step_id = next_step_id

    def export_snapshot(self) -> dict[str, Any]:
        """导出可 JSON 往返的执行状态，不包含后端、总线、取消信号等运行时对象。"""
        ensure_json_safe(self.state, path="state")
        ensure_json_safe(self.input, path="input")

        # step_results 是 state 的派生只读视图，不是第二份事实来源。
        step_results: dict[str, Any] = {}
        for node_id in self.completed_steps:
            node = self.plan.nodes.get(node_id)
            if (
                node is not None
                and node.kind == "step"
                and node.output_key
                and node.output_key in self.state
            ):
                step_results[node_id] = self.state[node.output_key]

        return {
            "task_id": self.spec.task_id,
            "run_id": self.run.run_id,
            "status": self.run.status.value,
            "protocol_version": self.spec.protocol_version,
            "input": self.input,
            "state": self.state,
            "step_results": step_results,
            "completed_steps": list(self.completed_steps),
            "branch_routes": dict(self.branch_routes),
            "step_count": self.step_count,
            "model_call_count": self.run.model_call_count,
            "attempt_count": self.run.attempt_count,
            "next_step_id": self.next_step_id,
            "elapsed_seconds": self.elapsed_before_resume
            + (time.monotonic() - self.run_started_at),
        }

    def apply_snapshot(self, snapshot: dict[str, Any]) -> None:
        """校验并恢复快照；任何不兼容都显式失败，不静默跳过。"""
        required = (
            "task_id",
            "protocol_version",
            "input",
            "state",
            "completed_steps",
            "step_count",
            "model_call_count",
        )
        missing = [key for key in required if key not in snapshot]
        if missing:
            raise CheckpointIncompatibleError(
                f"snapshot is missing required fields: {missing}",
                details={"missing": missing},
            )
        if snapshot["task_id"] != self.spec.task_id:
            raise CheckpointIncompatibleError(
                f"snapshot task_id {snapshot['task_id']!r} does not match {self.spec.task_id!r}",
                details={"snapshot_task_id": snapshot["task_id"], "task_id": self.spec.task_id},
            )
        snapshot_major = str(snapshot["protocol_version"]).split(".", 1)[0]
        spec_major = str(self.spec.protocol_version).split(".", 1)[0]
        if snapshot_major != spec_major:
            raise CheckpointIncompatibleError(
                f"snapshot protocol major {snapshot_major!r} does not match {spec_major!r}",
                details={"snapshot": snapshot["protocol_version"], "spec": self.spec.protocol_version},
            )
        if snapshot["input"] != self.input:
            raise CheckpointIncompatibleError(
                "snapshot input does not match the input supplied for this run",
                details={"snapshot_input": snapshot["input"], "input": self.input},
            )

        source_run_id = snapshot.get("run_id")
        if source_run_id is not None and not isinstance(source_run_id, str):
            raise CheckpointIncompatibleError("snapshot run_id must be a string")
        source_status = snapshot.get("status")
        if source_status is not None and not isinstance(source_status, str):
            raise CheckpointIncompatibleError("snapshot status must be a string")

        provided_step_results = snapshot.get("step_results")
        if provided_step_results is not None and not isinstance(provided_step_results, dict):
            raise CheckpointIncompatibleError("snapshot step_results must be an object")
        elapsed_seconds = snapshot.get("elapsed_seconds", 0.0)
        if (
            not isinstance(elapsed_seconds, (int, float))
            or isinstance(elapsed_seconds, bool)
            or elapsed_seconds < 0
        ):
            raise CheckpointIncompatibleError(
                "snapshot elapsed_seconds must be a non-negative number"
            )

        state = snapshot["state"]
        if not isinstance(state, dict):
            raise CheckpointIncompatibleError("snapshot state must be an object")
        ensure_json_safe(state, path="state")
        ensure_json_safe(snapshot["input"], path="input")

        completed = snapshot["completed_steps"]
        if not isinstance(completed, list) or not all(isinstance(item, str) for item in completed):
            raise CheckpointIncompatibleError("snapshot completed_steps must be a list[str]")
        if len(completed) != len(set(completed)):
            raise CheckpointIncompatibleError("snapshot completed_steps contains duplicates")
        for node_id in completed:
            node = self.plan.nodes.get(node_id)
            if node is None or node.kind not in ("step", "branch"):
                raise CheckpointIncompatibleError(
                    f"snapshot completed_steps contains unknown or non-executable node {node_id!r}",
                    details={"node_id": node_id},
                )
            if node.kind == "step" and (
                not node.output_key or node.output_key not in state
            ):
                raise CheckpointIncompatibleError(
                    f"completed step {node_id!r} has no output_key "
                    f"{node.output_key!r} in restored state",
                    details={"node_id": node_id, "output_key": node.output_key},
                )

        routes = snapshot.get("branch_routes", {})
        if not isinstance(routes, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in routes.items()
        ):
            raise CheckpointIncompatibleError("snapshot branch_routes must be a dict[str, str]")
        for branch_id, label in routes.items():
            node = self.plan.nodes.get(branch_id)
            if node is None or node.kind != "branch":
                raise CheckpointIncompatibleError(
                    f"snapshot branch_routes references non-branch node {branch_id!r}",
                    details={"node_id": branch_id},
                )
            if label not in node.routes:
                raise CheckpointIncompatibleError(
                    f"snapshot branch_routes has unknown route {label!r} for {branch_id!r}",
                    details={"node_id": branch_id, "label": label},
                )
        orphan_routes = [branch_id for branch_id in routes if branch_id not in completed]
        if orphan_routes:
            raise CheckpointIncompatibleError(
                f"snapshot branch_routes contains branches not in completed_steps: "
                f"{orphan_routes}",
                details={"branches": orphan_routes},
            )
        missing_routes = [
            node_id
            for node_id in completed
            if self.plan.nodes[node_id].kind == "branch" and node_id not in routes
        ]
        if missing_routes:
            raise CheckpointIncompatibleError(
                f"completed branches have no recorded route: {missing_routes}",
                details={"branches": missing_routes},
            )

        derived_step_results: dict[str, Any] = {}
        for node_id in completed:
            node = self.plan.nodes[node_id]
            if node.kind == "step" and node.output_key:
                derived_step_results[node_id] = state[node.output_key]
        if (
            provided_step_results is not None
            and provided_step_results != derived_step_results
        ):
            raise CheckpointIncompatibleError(
                "snapshot step_results does not match state",
                details={"step_results": provided_step_results},
            )

        step_count = snapshot["step_count"]
        model_call_count = snapshot["model_call_count"]
        attempt_count = snapshot.get("attempt_count", 0)
        if not isinstance(step_count, int) or step_count < 0:
            raise CheckpointIncompatibleError("snapshot step_count must be a non-negative int")
        if not isinstance(model_call_count, int) or model_call_count < 0:
            raise CheckpointIncompatibleError("snapshot model_call_count must be a non-negative int")
        if not isinstance(attempt_count, int) or attempt_count < 0:
            raise CheckpointIncompatibleError("snapshot attempt_count must be a non-negative int")

        next_step_id = snapshot.get("next_step_id")
        if next_step_id is not None and next_step_id not in self.plan.nodes:
            raise CheckpointIncompatibleError(
                f"snapshot next_step_id {next_step_id!r} does not exist in the plan",
                details={"next_step_id": next_step_id},
            )
        if next_step_id is not None and next_step_id in completed:
            raise CheckpointIncompatibleError(
                "snapshot next_step_id points to an already completed step",
                details={"next_step_id": next_step_id},
            )

        self._validate_completed_path(completed, routes, next_step_id)

        # 深拷贝，避免调用方后续修改快照影响运行中的 context。
        self.state = json.loads(json.dumps(state, ensure_ascii=False))
        self.completed_steps = list(completed)
        self.completed_step_set = set(completed)
        self.branch_routes = dict(routes)
        self.step_count = step_count
        self.run.model_call_count = model_call_count
        self.run.attempt_count = attempt_count
        self.next_step_id = next_step_id
        self.source_run_id = source_run_id
        self.source_status = source_status
        self.elapsed_before_resume = float(elapsed_seconds)
        self.run_started_at = time.monotonic()

    def _validate_completed_path(
        self,
        completed: list[str],
        routes: dict[str, str],
        next_step_id: str | None,
    ) -> None:
        """按记录的 branch_routes 重放结构，确认已完成节点在同一路径上。

        这里只做结构校验，不执行条件函数；分支条件在恢复时仍会重新求值，
        并由 Executor 对比原始路由。
        """
        reachable: set[str] = set()
        stack = [self.plan.entry]
        while stack:
            node_id = stack.pop()
            if node_id in reachable:
                continue
            reachable.add(node_id)
            node = self.plan.nodes[node_id]
            if node.kind in ("step", "join"):
                if node.next:
                    stack.append(node.next)
            elif node.kind == "branch":
                label = routes.get(node.id)
                if label is not None and label in node.routes:
                    stack.append(node.routes[label])
            # end 节点没有后继

        unreachable = [node_id for node_id in completed if node_id not in reachable]
        if unreachable:
            raise CheckpointIncompatibleError(
                f"completed_steps contains nodes not reachable on the recorded path: "
                f"{unreachable}",
                details={"nodes": unreachable},
            )
        if next_step_id is not None and next_step_id not in reachable:
            raise CheckpointIncompatibleError(
                f"next_step_id {next_step_id!r} is not reachable on the recorded path",
                details={"next_step_id": next_step_id},
            )

    @classmethod
    def from_snapshot(
        cls,
        snapshot: dict[str, Any],
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
    ) -> "ExecutionContext":
        ctx = cls(
            run=run,
            spec=spec,
            plan=plan,
            input_data=input_data,
            bus=bus,
            backend=backend,
            tools=tools,
            cancellation=cancellation,
            pause=pause,
        )
        ctx.apply_snapshot(snapshot)
        return ctx

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
            emit=lambda event_type, payload, **kwargs: self.emit(
                event_type,
                payload,
                step_index=step_index,
                attempt=attempt,
                **kwargs,
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
        started = time.perf_counter()
        try:
            if output_schema is not None:
                response = await self.backend.structured_chat(messages, output_schema)
                result = response.structured
            else:
                response = await self.backend.chat(messages)
                result = response.content
        except FrameworkError as exc:
            self.emit(
                EventType.LLM_CALL_END,
                {"status": "error", "error_code": exc.code, "message": str(exc)},
                step_index=step_index,
                attempt=attempt,
                duration_ms=(time.perf_counter() - started) * 1000,
            )
            raise
        except Exception as exc:
            error = ModelCallError(
                f"model call failed: {exc}",
                details={"backend": getattr(self.backend, "name", "unknown")},
                cause=exc,
            )
            self.emit(
                EventType.LLM_CALL_END,
                {"status": "error", "error_code": error.code, "message": str(error)},
                step_index=step_index,
                attempt=attempt,
                duration_ms=(time.perf_counter() - started) * 1000,
            )
            raise error from exc

        self.emit(
            EventType.LLM_CALL_END,
            {
                "model_name": response.model_name,
                "token_usage": response.token_usage,
                "output": result,
            },
            step_index=step_index,
            attempt=attempt,
            duration_ms=(time.perf_counter() - started) * 1000,
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
