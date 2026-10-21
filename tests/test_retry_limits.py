import asyncio

from kkk import (
    EventType,
    FrameworkError,
    MockLLM,
    ModelCallError,
    RetryPolicy,
    TaskRuntime,
    TaskSpec,
    TaskStatus,
    Tool,
    ToolExecutionError,
    ToolRegistry,
    ToolSpec,
    task,
)


def _llm_spec(**kwargs):
    return TaskSpec(
        task_id="TSK-RETRY",
        name="retry",
        input_schema={"type": "object"},
        output_schema={
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        },
        retry_policy=RetryPolicy(max_attempts=2, backoff_factor=1.0, initial_delay_seconds=0.001),
        **kwargs,
    )


def _llm_task(spec):
    @task(spec)
    def build(ctx):
        ctx.step(
            "extract",
            llm="value",
            output_schema={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
                "additionalProperties": False,
            },
            output_key="data",
        )
        ctx.end({"value": ctx.state["data"]["value"]})

    return build


def test_retry_then_success():
    runtime = TaskRuntime(backend=MockLLM([ModelCallError("temporary"), {"value": 7}]))
    run = asyncio.run(_llm_task(_llm_spec()).run({}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED
    assert run.output == {"value": 7}
    assert run.attempt_count == 2
    assert sum(1 for e in runtime.last_events if e.event_type == EventType.RETRY_SCHEDULED) == 1


def test_retry_exhausted():
    runtime = TaskRuntime(backend=MockLLM([ModelCallError("a"), ModelCallError("b")]))
    run = asyncio.run(_llm_task(_llm_spec()).run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_MODEL_CALL"
    assert sum(1 for e in runtime.last_events if e.event_type == EventType.RETRY_SCHEDULED) == 1


def test_non_retryable_error_is_not_retried():
    runtime = TaskRuntime(backend=MockLLM([FrameworkError("permanent"), {"value": 1}]))
    run = asyncio.run(_llm_task(_llm_spec()).run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert not any(e.event_type == EventType.RETRY_SCHEDULED for e in runtime.last_events)


class _DangerousTool(Tool):
    spec = ToolSpec(
        tool_name="danger",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        output_schema={"type": "object"},
        side_effect=True,
        idempotent=False,
    )

    async def execute(self, input_data, context):
        raise ToolExecutionError("boom")


def test_non_idempotent_side_effect_is_not_retried():
    spec = TaskSpec(
        task_id="TSK-DANGER",
        name="danger",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        retry_policy=RetryPolicy(max_attempts=3, initial_delay_seconds=0.001),
    )

    @task(spec)
    def build(ctx):
        ctx.step("danger", tool="danger", input={}, output_key="v")
        ctx.end({"ok": True})

    runtime = TaskRuntime(tools=ToolRegistry([_DangerousTool()]))
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "W_NON_IDEMPOTENT"
    assert not any(e.event_type == EventType.RETRY_SCHEDULED for e in runtime.last_events)


def test_runtime_max_steps():
    spec = TaskSpec(
        task_id="TSK-STEPS",
        name="steps",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        max_steps=1,
    )

    @task(spec)
    def build(ctx):
        ctx.step("a", func=lambda data: {"ok": True}, input={}, output_key="a")
        ctx.branch("b", condition=lambda state: "yes", cases={"yes": lambda c: None, "no": lambda c: None}, default="no")
        ctx.end({"ok": ctx.state["a"]["ok"]})

    runtime = TaskRuntime()
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_LIMIT_EXCEEDED"
    assert any(e.payload.get("error_code") == "E_LIMIT_EXCEEDED" for e in runtime.last_events)


def test_max_model_calls():
    spec = TaskSpec(
        task_id="TSK-MODEL-LIMIT",
        name="model-limit",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"v": {"type": "string"}}, "required": ["v"]},
        max_model_calls=1,
    )

    @task(spec)
    def build(ctx):
        ctx.step("one", llm="a", output_key="a")
        ctx.step("two", llm="b", output_key="b")
        ctx.end({"v": ctx.state["b"]})

    runtime = TaskRuntime(backend=MockLLM(["x", "y"]))
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_LIMIT_EXCEEDED"


def test_step_timeout():
    spec = TaskSpec(
        task_id="TSK-STEP-TIMEOUT",
        name="step-timeout",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        retry_policy=RetryPolicy(max_attempts=1),
    )

    async def slow(data):
        await asyncio.sleep(0.2)
        return {"ok": True}

    @task(spec)
    def build(ctx):
        ctx.step("slow", func=slow, input={}, output_key="v", timeout_seconds=0.01)
        ctx.end({"ok": ctx.state["v"]["ok"]})

    runtime = TaskRuntime()
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_STEP_TIMEOUT"
    assert any(e.event_type == EventType.ERROR_OCCURRED for e in runtime.last_events)


def test_task_timeout():
    spec = TaskSpec(
        task_id="TSK-TASK-TIMEOUT",
        name="task-timeout",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        timeout_seconds=0.05,
        retry_policy=RetryPolicy(max_attempts=1),
    )

    async def slow(data):
        await asyncio.sleep(0.2)
        return {"ok": True}

    @task(spec)
    def build(ctx):
        ctx.step("slow", func=slow, input={}, output_key="v")
        ctx.end({"ok": ctx.state["v"]["ok"]})

    run = asyncio.run(build.run({}, runtime=TaskRuntime()))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_TASK_TIMEOUT"
