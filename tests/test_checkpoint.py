"""P1：检查点、快照、恢复与诊断字段的测试。"""
from __future__ import annotations

import asyncio
import json
import random

import pytest

from kkk import (
    Checkpoint,
    CheckpointIOError,
    CheckpointIncompatibleError,
    CheckpointInvalidError,
    CheckpointNotFoundError,
    EventType,
    FrameworkError,
    GraphBuilder,
    GraphTask,
    InMemoryEventStore,
    JsonCheckpointStore,
    JsonlEventStore,
    MockLLM,
    ResumeNotSupportedError,
    RetryPolicy,
    StateSerializationError,
    TaskRun,
    TaskRuntime,
    TaskSpec,
    TaskStatus,
    Tool,
    ToolExecutionError,
    ToolRegistry,
    ToolSpec,
    task,
)
from kkk.context import ExecutionContext
from kkk.diff import compare_runs
from kkk.events import EventBus
from kkk.refs import Ref


def make_spec(**overrides):
    values = dict(
        task_id="TSK-P1",
        name="p1",
        input_schema={
            "type": "object",
            "properties": {"x": {"type": "integer"}},
            "required": ["x"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        },
        retry_policy=RetryPolicy(
            max_attempts=1,
            backoff_factor=1.0,
            initial_delay_seconds=0.001,
        ),
        max_steps=20,
        timeout_seconds=5.0,
    )
    values.update(overrides)
    return TaskSpec(**values)


def make_linear_script(spec, calls, *, fail_first_b=True):
    def step_a(_data):
        calls["a"] += 1
        return {"value": 1}

    def step_b(_data):
        calls["b"] += 1
        if fail_first_b and calls["b"] == 1:
            raise FrameworkError("temporary failure")
        return {"value": 2}

    def step_c(_data):
        calls["c"] += 1
        return {"value": 3}

    @task(spec)
    def build(ctx):
        ctx.step("a", func=step_a, input={}, output_key="a")
        ctx.step("b", func=step_b, input={}, output_key="b")
        ctx.step("c", func=step_c, input={}, output_key="c")
        ctx.end({"value": ctx.state["c"]["value"]})

    return build


def make_linear_graph(spec, calls, *, fail_first_b=True):
    def step_a(_data):
        calls["a"] += 1
        return {"value": 1}

    def step_b(_data):
        calls["b"] += 1
        if fail_first_b and calls["b"] == 1:
            raise FrameworkError("temporary failure")
        return {"value": 2}

    def step_c(_data):
        calls["c"] += 1
        return {"value": 3}

    builder = GraphBuilder(spec)
    a = builder.add_step("a", func=step_a, input={}, output_key="a")
    b = builder.add_step("b", func=step_b, input={}, output_key="b")
    c = builder.add_step("c", func=step_c, input={}, output_key="c")
    end = builder.add_end("end", {"value": Ref("state", "c", ("value",))})
    builder.set_entry(a)
    builder.add_edge(a, b)
    builder.add_edge(b, c)
    builder.add_edge(c, end)
    return GraphTask(spec, builder)


def make_branch_script(spec, calls, route_holder):
    def classify(_data):
        calls["classify"] += 1
        return {"amount": 1200}

    def high(_data):
        calls["high"] += 1
        if calls["high"] == 1:
            raise FrameworkError("approval service unavailable")
        return {"status": "approved"}

    def low(_data):
        calls["low"] += 1
        return {"status": "recorded"}

    @task(spec)
    def build(ctx):
        ctx.step("classify", func=classify, input={}, output_key="expense")

        def high_case(c):
            c.step("high", func=high, input={}, output_key="result")

        def low_case(c):
            c.step("low", func=low, input={}, output_key="result")

        ctx.branch(
            "route",
            condition=lambda state: route_holder["route"],
            cases={"high": high_case, "low": low_case},
            default="low",
        )
        ctx.end({"status": ctx.state["result"]["status"]})

    return build


class _DangerousTool(Tool):
    spec = ToolSpec(
        tool_name="danger",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        output_schema={"type": "object"},
        side_effect=True,
        idempotent=False,
    )

    async def execute(self, input_data, context):
        raise ToolExecutionError("external service state is unknown")


class _SlowTool(Tool):
    spec = ToolSpec(
        tool_name="slow",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}},
        timeout_seconds=5.0,
    )

    async def execute(self, input_data, context):
        await asyncio.sleep(0.2)
        return {"ok": True}


class _FailingStore:
    def save(self, checkpoint):
        raise CheckpointIOError("disk full")

    def load(self, checkpoint_id):
        raise NotImplementedError

    def load_latest(self, run_id, task_id):
        raise NotImplementedError

    def list(self, run_id):
        raise NotImplementedError


class _FailOnCompletedSink:
    def __init__(self):
        self._events = []

    def append(self, event):
        if event.event_type == EventType.TASK_COMPLETED:
            raise RuntimeError("event sink is down")
        self._events.append(event)

    def events(self, run_id=None):
        if run_id is None:
            return list(self._events)
        return [event for event in self._events if event.run_id == run_id]

    def close(self):
        return None


class _FailOnEverySink:
    def __init__(self):
        self._events = []

    def append(self, event):
        self._events.append(event)
        raise RuntimeError("event sink is down")

    def events(self, run_id=None):
        if run_id is None:
            return list(self._events)
        return [event for event in self._events if event.run_id == run_id]

    def close(self):
        return None


class _EchoTool(Tool):
    spec = ToolSpec(
        tool_name="echo",
        input_schema={
            "type": "object",
            "properties": {
                "api_key": {"type": "string"},
                "value": {"type": "integer"},
            },
            "required": ["value"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
    )

    async def execute(self, input_data, context):
        return dict(input_data)


def _simple_checkpoint(**overrides):
    values = dict(
        run_id="run-1",
        task_id="TSK-P1",
        plan_hash="plan-hash",
        spec_hash="spec-hash",
        input={"x": 1},
        state={"a": {"value": 1}},
        completed_steps=["step_1"],
        next_step_id="step_2",
    )
    values.update(overrides)
    return Checkpoint(**values)


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------
def test_json_checkpoint_store_roundtrip_and_latest(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    first = _simple_checkpoint()
    first_id = store.save(first)

    assert first.checkpoint_id == first_id
    loaded = store.load(first_id)
    assert loaded.state == {"a": {"value": 1}}
    assert loaded.completed_steps == ["step_1"]
    assert loaded.next_step_id == "step_2"
    assert store.load_latest("run-1", "TSK-P1").checkpoint_id == first_id
    assert store.load_latest("run-1", "TSK-OTHER") is None

    second = _simple_checkpoint(state={"a": {"value": 2}})
    second_id = store.save(second)
    assert second_id != first_id
    assert store.load_latest("run-1", "TSK-P1").checkpoint_id == second_id
    assert [item.checkpoint_id for item in store.list("run-1")] == [first_id, second_id]


def test_checkpoint_not_found(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    with pytest.raises(CheckpointNotFoundError):
        store.load("run-1-000999")


def test_corrupted_checkpoint_is_rejected(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    checkpoint_id = store.save(_simple_checkpoint())
    path = tmp_path / "run-1" / "000001.json"
    path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(CheckpointInvalidError):
        store.load(checkpoint_id)
    with pytest.raises(CheckpointInvalidError):
        store.load_latest("run-1", "TSK-P1")


def test_unsupported_checkpoint_version_is_rejected(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    checkpoint_id = store.save(_simple_checkpoint())
    path = tmp_path / "run-1" / "000001.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["checkpoint_version"] = 99
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(CheckpointIncompatibleError):
        store.load(checkpoint_id)


# ---------------------------------------------------------------------------
# 快照
# ---------------------------------------------------------------------------
def test_snapshot_roundtrip_and_non_json_state():
    spec = make_spec()
    task_obj = make_linear_script(spec, {"a": 0, "b": 0, "c": 0}, fail_first_b=False)
    plan = task_obj.build()
    first_step = next(node for node in plan.nodes.values() if node.kind == "step")

    run = TaskRun(task_spec_id=spec.task_id, input={"x": 1})
    ctx = ExecutionContext(
        run=run,
        spec=spec,
        plan=plan,
        input_data={"x": 1},
        bus=EventBus(run.run_id),
        backend=MockLLM(default={}),
        tools=ToolRegistry(),
    )
    ctx.state = {"a": {"value": 1}}
    ctx.step_count = 1
    ctx.mark_completed(first_step.id, first_step.next)
    run.model_call_count = 2
    snapshot = ctx.export_snapshot()

    run2 = TaskRun(task_spec_id=spec.task_id, input={"x": 1})
    ctx2 = ExecutionContext(
        run=run2,
        spec=spec,
        plan=plan,
        input_data={"x": 1},
        bus=EventBus(run2.run_id),
        backend=MockLLM(default={}),
        tools=ToolRegistry(),
    )
    ctx2.apply_snapshot(snapshot)

    assert ctx2.state == {"a": {"value": 1}}
    assert ctx2.completed_steps == [first_step.id]
    assert ctx2.step_count == 1
    assert ctx2.run.model_call_count == 2
    assert ctx2.next_step_id == first_step.next

    ctx.state = {"bad": (1, 2)}
    with pytest.raises(StateSerializationError):
        ctx.export_snapshot()


def test_snapshot_rejects_unknown_completed_node():
    spec = make_spec()
    task_obj = make_linear_script(spec, {"a": 0, "b": 0, "c": 0}, fail_first_b=False)
    plan = task_obj.build()
    run = TaskRun(task_spec_id=spec.task_id, input={"x": 1})
    ctx = ExecutionContext(
        run=run,
        spec=spec,
        plan=plan,
        input_data={"x": 1},
        bus=EventBus(run.run_id),
        backend=MockLLM(default={}),
        tools=ToolRegistry(),
    )
    snapshot = ctx.export_snapshot()
    snapshot["completed_steps"] = ["does-not-exist"]

    with pytest.raises(CheckpointIncompatibleError):
        ctx.apply_snapshot(snapshot)


# ---------------------------------------------------------------------------
# 恢复
# ---------------------------------------------------------------------------
def test_resume_skips_completed_steps(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    calls = {"a": 0, "b": 0, "c": 0}
    spec = make_spec()
    task_obj = make_linear_script(spec, calls)
    runtime = TaskRuntime(checkpoint_store=store)

    first = asyncio.run(task_obj.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED
    assert first.error.code == "E_FRAMEWORK"

    latest = store.load_latest(first.run_id, spec.task_id)
    assert latest is not None
    assert latest.completed_steps and len(latest.completed_steps) == 1
    assert latest.resume_safe is True
    assert latest.run_id == first.run_id
    assert latest.status == "FAILED"
    assert latest.task_version
    assert latest.to_snapshot()["run_id"] == first.run_id
    assert latest.to_snapshot()["status"] == "FAILED"

    resumed = asyncio.run(task_obj.resume(run_id=first.run_id, runtime=runtime))
    assert resumed.status == TaskStatus.SUCCEEDED
    assert resumed.resumed_from_run_id == first.run_id
    assert resumed.output == {"value": 3}
    assert calls == {"a": 1, "b": 2, "c": 1}

    event_types = [event.event_type for event in runtime.last_events]
    assert EventType.CHECKPOINT_LOADED in event_types
    assert EventType.TASK_RESUMED in event_types
    assert EventType.STEP_SKIPPED in event_types


def test_branch_resume_and_route_mismatch(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    calls = {"classify": 0, "high": 0, "low": 0}
    route_holder = {"route": "high"}
    spec = make_spec(
        output_schema={
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
            "additionalProperties": False,
        }
    )
    task_obj = make_branch_script(spec, calls, route_holder)
    runtime = TaskRuntime(checkpoint_store=store)

    first = asyncio.run(task_obj.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED
    latest = store.load_latest(first.run_id, spec.task_id)
    assert latest is not None
    assert latest.branch_routes
    assert "high" in latest.branch_routes.values()

    resumed = asyncio.run(task_obj.resume(run_id=first.run_id, runtime=runtime))
    assert resumed.status == TaskStatus.SUCCEEDED
    assert resumed.output == {"status": "approved"}
    assert calls == {"classify": 1, "high": 2, "low": 0}

    # 条件函数在恢复时给出不同路由：必须拒绝继续，而不是走错分支。
    route_holder["route"] = "low"
    mismatched = asyncio.run(task_obj.resume(run_id=first.run_id, runtime=runtime))
    assert mismatched.status == TaskStatus.FAILED
    assert mismatched.error is not None
    assert mismatched.error.code == "E_RESUME_UNSUPPORTED"


def test_non_idempotent_unknown_result_blocks_resume(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    spec = make_spec(
        output_schema={
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
            "additionalProperties": False,
        },
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.001),
    )

    @task(spec)
    def build(ctx):
        ctx.step("danger", tool="danger", input={}, output_key="result")
        ctx.end({"status": "ok"})

    runtime = TaskRuntime(
        tools=ToolRegistry([_DangerousTool()]),
        checkpoint_store=store,
    )
    first = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED
    assert first.error.code == "W_NON_IDEMPOTENT"

    latest = store.load_latest(first.run_id, spec.task_id)
    assert latest is not None
    assert latest.resume_safe is False
    assert "non-idempotent" in (latest.resume_block_reason or "")

    with pytest.raises(ResumeNotSupportedError):
        asyncio.run(build.resume(run_id=first.run_id, runtime=runtime))


def test_resume_preserves_model_call_counter(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    calls = {"fail": 0}
    spec = make_spec(max_model_calls=1)

    def fail_once(_data):
        calls["fail"] += 1
        if calls["fail"] == 1:
            raise FrameworkError("temporary")
        return {"value": 10}

    @task(spec)
    def build(ctx):
        ctx.step(
            "first_llm",
            llm="first",
            output_schema={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
                "additionalProperties": False,
            },
            output_key="first",
        )
        ctx.step("fail", func=fail_once, input={}, output_key="middle")
        ctx.step(
            "second_llm",
            llm="second",
            output_schema={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
                "additionalProperties": False,
            },
            output_key="second",
        )
        ctx.end({"value": ctx.state["second"]["value"]})

    runtime = TaskRuntime(
        backend=MockLLM([{"value": 1}, {"value": 2}]),
        checkpoint_store=store,
    )
    first = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED
    latest = store.load_latest(first.run_id, spec.task_id)
    assert latest is not None and latest.model_call_count == 1

    resumed = asyncio.run(build.resume(run_id=first.run_id, runtime=runtime))
    assert resumed.status == TaskStatus.FAILED
    assert resumed.error is not None
    assert resumed.error.code == "E_LIMIT_EXCEEDED"


def test_checkpoint_save_failure_does_not_retry_step(tmp_path):
    calls = {"step": 0}
    spec = make_spec()

    def only_step(_data):
        calls["step"] += 1
        return {"value": 1}

    @task(spec)
    def build(ctx):
        ctx.step("only", func=only_step, input={}, output_key="only")
        ctx.end({"value": ctx.state["only"]["value"]})

    runtime = TaskRuntime(checkpoint_store=_FailingStore())
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None
    assert run.error.code == "E_CHECKPOINT_IO"
    assert calls["step"] == 1
    assert any(
        event.event_type == EventType.ERROR_OCCURRED
        and event.payload.get("error_code") == "E_CHECKPOINT_IO"
        for event in runtime.last_events
    )


def test_resume_rejects_mismatched_input(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    calls = {"a": 0, "b": 0, "c": 0}
    spec = make_spec()
    task_obj = make_linear_script(spec, calls)
    runtime = TaskRuntime(checkpoint_store=store)

    first = asyncio.run(task_obj.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED

    with pytest.raises(CheckpointIncompatibleError):
        asyncio.run(
            runtime.resume(
                spec,
                task_obj.build(),
                run_id=first.run_id,
                input_data={"x": 2},
            )
        )


def test_resume_rejects_changed_plan(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    calls = {"a": 0, "b": 0, "c": 0}
    spec = make_spec()
    task_obj = make_linear_script(spec, calls)
    runtime = TaskRuntime(checkpoint_store=store)

    first = asyncio.run(task_obj.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED

    @task(spec)
    def other_build(ctx):
        ctx.step("a", func=lambda _data: {"value": 1}, input={}, output_key="a")
        ctx.end({"value": ctx.state["a"]["value"]})

    with pytest.raises(CheckpointIncompatibleError):
        asyncio.run(
            runtime.resume(
                spec,
                other_build.build(),
                run_id=first.run_id,
            )
        )


def test_resume_rejects_changed_spec(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    calls = {"a": 0, "b": 0, "c": 0}
    spec = make_spec()
    task_obj = make_linear_script(spec, calls)
    runtime = TaskRuntime(checkpoint_store=store)

    first = asyncio.run(task_obj.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED

    changed_spec = make_spec(max_steps=30)
    with pytest.raises(CheckpointIncompatibleError):
        asyncio.run(
            runtime.resume(
                changed_spec,
                task_obj.build(),
                run_id=first.run_id,
            )
        )


def test_script_and_graph_resume_are_equivalent(tmp_path):
    script_calls = {"a": 0, "b": 0, "c": 0}
    graph_calls = {"a": 0, "b": 0, "c": 0}
    spec = make_spec()
    script_task = make_linear_script(spec, script_calls)
    graph_task = make_linear_graph(spec, graph_calls)

    script_runtime = TaskRuntime(checkpoint_store=JsonCheckpointStore(tmp_path / "script"))
    graph_runtime = TaskRuntime(checkpoint_store=JsonCheckpointStore(tmp_path / "graph"))

    script_first = asyncio.run(script_task.run({"x": 1}, runtime=script_runtime))
    graph_first = asyncio.run(graph_task.run({"x": 1}, runtime=graph_runtime))
    assert script_first.status == graph_first.status == TaskStatus.FAILED

    script_resumed = asyncio.run(
        script_task.resume(run_id=script_first.run_id, runtime=script_runtime)
    )
    graph_resumed = asyncio.run(
        graph_task.resume(run_id=graph_first.run_id, runtime=graph_runtime)
    )

    report = compare_runs(
        script_resumed,
        graph_resumed,
        left_events=script_runtime.last_events,
        right_events=graph_runtime.last_events,
        strict_fields=["value"],
    )
    assert report.equivalent, report.differences
    assert script_calls == graph_calls == {"a": 1, "b": 2, "c": 1}


# ---------------------------------------------------------------------------
# 诊断
# ---------------------------------------------------------------------------
def test_step_timeout_closes_inflight_tool_as_unknown():
    spec = make_spec(
        output_schema={
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
            "additionalProperties": False,
        }
    )

    @task(spec)
    def build(ctx):
        ctx.step(
            "slow",
            tool="slow",
            input={},
            output_key="result",
            timeout_seconds=0.01,
        )
        ctx.end({"ok": ctx.state["result"]["ok"]})

    runtime = TaskRuntime(tools=ToolRegistry([_SlowTool()]))
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error.code == "E_STEP_TIMEOUT"

    tool_end_events = [
        event
        for event in runtime.last_events
        if event.event_type == EventType.TOOL_CALL_END
    ]
    assert any(event.payload.get("status") == "UNKNOWN" for event in tool_end_events)


def test_event_diagnostics_fields():
    spec = make_spec(
        output_schema={
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        }
    )

    @task(spec)
    def build(ctx):
        ctx.step("a", func=lambda _data: {"value": 1}, input={}, output_key="a")
        ctx.end({"value": ctx.state["a"]["value"]})

    runtime = TaskRuntime()
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED

    assert all(event.event_schema_version == 1 for event in runtime.last_events)
    step_end = next(
        event for event in runtime.last_events if event.event_type == EventType.STEP_END
    )
    assert step_end.duration_ms is not None
    assert step_end.duration_ms >= 0
    assert "input_hash" in step_end.payload
    assert "output_hash" in step_end.payload


def test_events_carry_task_and_step_ids():
    spec = make_spec()

    @task(spec)
    def build(ctx):
        ctx.step("a", func=lambda _data: {"value": 1}, input={}, output_key="a")
        ctx.end({"value": ctx.state["a"]["value"]})

    runtime = TaskRuntime()
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED

    assert all(event.task_id == spec.task_id for event in runtime.last_events)
    task_started = next(
        event for event in runtime.last_events if event.event_type == EventType.TASK_STARTED
    )
    assert task_started.step_id is None
    step_events = [
        event
        for event in runtime.last_events
        if event.event_type in (EventType.STEP_START, EventType.STEP_END)
    ]
    assert step_events
    assert all(event.step_id for event in step_events)


def test_snapshot_next_step_must_not_be_completed():
    spec = make_spec()
    task_obj = make_linear_script(spec, {"a": 0, "b": 0, "c": 0}, fail_first_b=False)
    plan = task_obj.build()
    first_step = next(node for node in plan.nodes.values() if node.kind == "step")

    run = TaskRun(task_spec_id=spec.task_id, input={"x": 1})
    ctx = ExecutionContext(
        run=run,
        spec=spec,
        plan=plan,
        input_data={"x": 1},
        bus=EventBus(run.run_id, task_id=spec.task_id),
        backend=MockLLM(default={}),
        tools=ToolRegistry(),
    )
    ctx.mark_completed(first_step.id, first_step.next)
    snapshot = ctx.export_snapshot()
    snapshot["next_step_id"] = first_step.id

    with pytest.raises(CheckpointIncompatibleError):
        ctx.apply_snapshot(snapshot)


def test_at005_first_two_succeed_third_fails_then_resume(tmp_path):
    calls = {"a": 0, "b": 0, "c": 0}
    spec = make_spec()

    def step_a(_data):
        calls["a"] += 1
        return {"value": 1}

    def step_b(_data):
        calls["b"] += 1
        return {"value": 2}

    def step_c(_data):
        calls["c"] += 1
        if calls["c"] == 1:
            raise FrameworkError("third step failed")
        return {"value": 3}

    @task(spec)
    def build(ctx):
        ctx.step("a", func=step_a, input={}, output_key="a")
        ctx.step("b", func=step_b, input={}, output_key="b")
        ctx.step("c", func=step_c, input={}, output_key="c")
        ctx.end({"value": ctx.state["c"]["value"]})

    runtime = TaskRuntime(checkpoint_store=JsonCheckpointStore(tmp_path))
    first = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED

    resumed = asyncio.run(build.resume(run_id=first.run_id, runtime=runtime))
    assert resumed.status == TaskStatus.SUCCEEDED
    assert calls == {"a": 1, "b": 1, "c": 2}


def test_task_completed_sink_failure_does_not_leave_succeeded_status():
    spec = make_spec()

    @task(spec)
    def build(ctx):
        ctx.step("a", func=lambda _data: {"value": 1}, input={}, output_key="a")
        ctx.end({"value": ctx.state["a"]["value"]})

    sink = _FailOnCompletedSink()
    runtime = TaskRuntime(event_store=sink)
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))

    # 完成事件写失败时，任务不能被静默标记为 SUCCEEDED。
    assert run.status == TaskStatus.FAILED
    assert run.error is not None
    assert run.error.code == "E_EVENT_LOG"


def test_resume_failure_emits_recovery_event(tmp_path):
    event_store = InMemoryEventStore()
    spec = make_spec()
    task_obj = make_linear_script(spec, {"a": 0, "b": 0, "c": 0})
    runtime = TaskRuntime(
        checkpoint_store=JsonCheckpointStore(tmp_path),
        event_store=event_store,
    )

    with pytest.raises(CheckpointNotFoundError):
        asyncio.run(
            runtime.resume(
                spec,
                task_obj.build(),
                run_id="missing-run",
            )
        )

    events = event_store.events("missing-run")
    assert any(event.event_type == EventType.RECOVERY_FAILED for event in events)
    assert events[0].task_id == spec.task_id


def test_event_payload_redacts_sensitive_fields():
    spec = make_spec()

    def secret_step(_data):
        return {
            "api_key": "sk-test-secret-1234567890",
            "nested": {"password": "p@ss"},
            "value": 1,
        }

    @task(spec)
    def build(ctx):
        ctx.step("secret", func=secret_step, input={}, output_key="secret")
        ctx.end({"value": ctx.state["secret"]["value"]})

    runtime = TaskRuntime()
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED

    dumped = json.dumps(
        [event.model_dump(mode="json") for event in runtime.last_events],
        ensure_ascii=False,
    )
    assert "sk-test-secret" not in dumped
    assert "p@ss" not in dumped

    step_end = next(
        event for event in runtime.last_events if event.event_type == EventType.STEP_END
    )
    assert step_end.payload["output"]["api_key"] == "***REDACTED***"
    assert step_end.payload["output"]["nested"]["password"] == "***REDACTED***"
    assert step_end.payload["output"]["value"] == 1


def test_event_sink_failure_always_marks_failed():
    spec = make_spec()

    @task(spec)
    def build(ctx):
        ctx.step("a", func=lambda _data: {"value": 1}, input={}, output_key="a")
        ctx.end({"value": ctx.state["a"]["value"]})

    runtime = TaskRuntime(event_store=_FailOnEverySink())
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None
    assert run.error.code == "E_EVENT_LOG"


def test_checkpoint_step_results_and_content_hash(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    spec = make_spec()
    task_obj = make_linear_script(spec, {"a": 0, "b": 0, "c": 0}, fail_first_b=False)
    runtime = TaskRuntime(checkpoint_store=store)

    run = asyncio.run(task_obj.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED
    latest = store.load_latest(run.run_id, spec.task_id)
    assert latest is not None
    assert latest.step_results
    assert latest.content_hash
    assert latest.elapsed_seconds >= 0
    assert set(latest.step_results) == set(latest.completed_steps)

    # 合法 JSON 内篡改 state：内容哈希必须拒绝。
    path = tmp_path / run.run_id / f"{latest.sequence:06d}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["state"]["a"]["value"] = 999
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(CheckpointInvalidError):
        store.load(latest.checkpoint_id)


def test_snapshot_path_consistency_rejects_unselected_branch(tmp_path):
    store = JsonCheckpointStore(tmp_path)
    # 预置 high=1，让 high 步骤第一次调用就成功，生成成功检查点。
    calls = {"classify": 0, "high": 1, "low": 0}
    route_holder = {"route": "high"}
    spec = make_spec(
        output_schema={
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
            "additionalProperties": False,
        }
    )
    task_obj = make_branch_script(spec, calls, route_holder)
    runtime = TaskRuntime(checkpoint_store=store)
    run = asyncio.run(task_obj.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED

    latest = store.load_latest(run.run_id, spec.task_id)
    assert latest is not None
    plan = task_obj.build()
    branch = next(node for node in plan.nodes.values() if node.kind == "branch")
    snapshot = latest.to_snapshot()
    snapshot["branch_routes"][branch.id] = "low"

    ctx = ExecutionContext(
        run=TaskRun(task_spec_id=spec.task_id, input={"x": 1}),
        spec=spec,
        plan=plan,
        input_data={"x": 1},
        bus=EventBus("path-check", task_id=spec.task_id),
        backend=MockLLM(default={}),
        tools=ToolRegistry(),
    )
    with pytest.raises(CheckpointIncompatibleError):
        ctx.apply_snapshot(snapshot)


def test_resume_respects_total_timeout_budget(tmp_path):
    calls = {"a": 0, "b": 0}
    spec = make_spec(timeout_seconds=1.5)

    async def first_step(_data):
        calls["a"] += 1
        await asyncio.sleep(1.0)
        return {"value": 1}

    async def second_step(_data):
        calls["b"] += 1
        if calls["b"] == 1:
            raise FrameworkError("temporary failure after first step")
        await asyncio.sleep(0.8)
        return {"value": 2}

    @task(spec)
    def build(ctx):
        ctx.step("a", func=first_step, input={}, output_key="a")
        ctx.step("b", func=second_step, input={}, output_key="b")
        ctx.end({"value": ctx.state["b"]["value"]})

    store = JsonCheckpointStore(tmp_path)
    runtime = TaskRuntime(checkpoint_store=store)
    first = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert first.status == TaskStatus.FAILED
    latest = store.load_latest(first.run_id, spec.task_id)
    assert latest is not None
    assert latest.elapsed_seconds >= 0.5

    resumed = asyncio.run(build.resume(run_id=first.run_id, runtime=runtime))
    assert resumed.status == TaskStatus.FAILED
    assert resumed.error is not None
    assert resumed.error.code == "E_TASK_TIMEOUT"


def test_tool_events_redact_sensitive_input_and_output(tmp_path):
    spec = make_spec()

    @task(spec)
    def build(ctx):
        ctx.step(
            "echo",
            tool="echo",
            input={"api_key": "sk-tool-secret-123456789", "value": 1},
            output_key="echo",
        )
        ctx.end({"value": ctx.state["echo"]["value"]})

    event_path = tmp_path / "events.jsonl"
    runtime = TaskRuntime(
        tools=ToolRegistry([_EchoTool()]),
        event_store=JsonlEventStore(event_path),
    )
    run = asyncio.run(build.run({"x": 1}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED

    text = event_path.read_text(encoding="utf-8")
    assert "sk-tool-secret" not in text
    tool_start = next(
        event for event in runtime.last_events if event.event_type == EventType.TOOL_CALL_START
    )
    assert tool_start.payload["input"]["api_key"] == "***REDACTED***"
    assert tool_start.payload["input"]["value"] == 1


def _build_random_graph(spec, seed, counts, *, fail_name: str | None = None):
    rng = random.Random(seed)
    builder = GraphBuilder(spec)
    value_counter = {"n": 0}
    executed_steps: list[str] = []

    def add_step(name):
        value = value_counter["n"]
        value_counter["n"] += 1

        def fn(_data, _value=value, _failure=fail_name):
            counts[name] = counts.get(name, 0) + 1
            if _failure is not None and name == _failure and counts[name] == 1:
                raise FrameworkError("injected random DAG failure")
            return {"value": _value}

        return builder.add_step(name, func=fn, input={}, output_key=name)

    previous = None
    segment_count = rng.randint(1, 4)
    for index in range(segment_count):
        if rng.random() < 0.45:
            name = f"s{index}"
            node = add_step(name)
            executed_steps.append(name)
            if previous is None:
                builder.set_entry(node)
            else:
                builder.add_edge(previous, node)
            previous = node
            continue

        choice = rng.choice(["left", "right"])
        branch = builder.add_branch(
            f"b{index}",
            condition=lambda _state, selected=choice: selected,
            default="right",
        )
        join = builder.add_join(f"j{index}")
        for side in ("left", "right"):
            case_count = rng.randint(0, 2)
            entry = None
            tail = None
            for position in range(case_count):
                name = f"{side}_{index}_{position}"
                node = add_step(name)
                if side == choice:
                    executed_steps.append(name)
                if tail is None:
                    entry = node
                else:
                    builder.add_edge(tail, node)
                tail = node
            if entry is None:
                builder.add_route(branch, side, join)
            else:
                builder.add_route(branch, side, entry)
                builder.add_edge(tail, join)
        if previous is None:
            builder.set_entry(branch)
        else:
            builder.add_edge(previous, branch)
        previous = join

    tail = add_step("tail")
    executed_steps.append("tail")
    builder.add_edge(previous, tail)
    end = builder.add_end("end", {"value": Ref("state", "tail", ("value",))})
    builder.add_edge(tail, end)
    if fail_name is None:
        fail_name = rng.choice(executed_steps)
    return GraphTask(spec, builder), fail_name


def test_random_dag_resume_preserves_calls_and_output(tmp_path):
    for seed in range(1, 17):
        reference_counts = {}
        interrupted_counts = {}
        spec = make_spec(max_steps=50, timeout_seconds=10.0)

        reference_task, fail_name = _build_random_graph(
            spec, seed, reference_counts
        )
        interrupted_task, _ = _build_random_graph(
            spec, seed, interrupted_counts, fail_name=fail_name
        )

        reference_run = asyncio.run(
            reference_task.run({"x": 1}, runtime=TaskRuntime())
        )
        assert reference_run.status == TaskStatus.SUCCEEDED

        store = JsonCheckpointStore(tmp_path / f"seed-{seed}")
        runtime = TaskRuntime(checkpoint_store=store)
        first = asyncio.run(interrupted_task.run({"x": 1}, runtime=runtime))
        assert first.status == TaskStatus.FAILED

        resumed = asyncio.run(
            interrupted_task.resume(run_id=first.run_id, runtime=runtime)
        )
        assert resumed.status == TaskStatus.SUCCEEDED
        assert resumed.output == reference_run.output

        for name, reference_count in reference_counts.items():
            expected = reference_count + (1 if name == fail_name else 0)
            assert interrupted_counts.get(name, 0) == expected, (
                seed,
                fail_name,
                name,
                reference_counts,
                interrupted_counts,
            )
        assert set(interrupted_counts) == set(reference_counts)
