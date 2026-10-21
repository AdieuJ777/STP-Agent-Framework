import asyncio

from kkk import CancellationToken, EventType, TaskRuntime, TaskSpec, TaskStatus, task


def test_cancel_before_start():
    spec = TaskSpec(
        task_id="TSK-CANCEL",
        name="cancel",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
    )

    @task(spec)
    def build(ctx):
        ctx.step("noop", func=lambda data: {"ok": True}, input={}, output_key="v")
        ctx.end({"ok": ctx.state["v"]["ok"]})

    token = CancellationToken()
    token.cancel()
    runtime = TaskRuntime()
    run = asyncio.run(build.run({}, runtime=runtime, cancellation=token))
    assert run.status == TaskStatus.CANCELLED
    assert run.error is not None and run.error.code == "E_CANCELLED"
    assert [e.event_type for e in runtime.last_events] == [EventType.TASK_CANCELLED]


def test_cancel_between_steps():
    spec = TaskSpec(
        task_id="TSK-CANCEL2",
        name="cancel2",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
    )
    token = CancellationToken()

    def cancel_after_first(data):
        token.cancel()
        return {"ok": True}

    @task(spec)
    def build(ctx):
        ctx.step("first", func=cancel_after_first, input={}, output_key="first")
        ctx.step("second", func=lambda data: {"ok": True}, input={}, output_key="second")
        ctx.end({"ok": ctx.state["second"]["ok"]})

    runtime = TaskRuntime()
    run = asyncio.run(build.run({}, runtime=runtime, cancellation=token))
    assert run.status == TaskStatus.CANCELLED
    assert run.error is not None and run.error.code == "E_CANCELLED"
    started = [
        e.payload.get("node_id")
        for e in runtime.last_events
        if e.event_type == EventType.STEP_START
    ]
    assert len(started) == 1
    assert any(e.event_type == EventType.TASK_CANCELLED for e in runtime.last_events)
