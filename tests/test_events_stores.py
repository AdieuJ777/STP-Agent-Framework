import asyncio

from kkk import (
    EventType,
    JsonlEventStore,
    MockLLM,
    ModelCallError,
    RetryPolicy,
    SqliteEventStore,
    TaskRuntime,
    TaskSpec,
    TaskStatus,
    task,
)


def _build_task():
    spec = TaskSpec(
        task_id="TSK-EVENTS",
        name="events",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]},
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.001),
    )

    @task(spec)
    def build(ctx):
        ctx.step(
            "extract",
            llm="value",
            output_schema={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
            },
            output_key="data",
        )
        ctx.end({"value": ctx.state["data"]["value"]})

    return build


def test_jsonl_store_roundtrip(tmp_path):
    path = tmp_path / "events.jsonl"
    store = JsonlEventStore(path)
    runtime = TaskRuntime(backend=MockLLM([{"value": 3}]), event_store=store)
    run = asyncio.run(_build_task().run({}, runtime=runtime))
    store.close()

    assert run.status == TaskStatus.SUCCEEDED
    events = store.events(run.run_id)
    assert events
    assert [e.seq for e in events] == list(range(1, len(events) + 1))
    assert events[-1].event_type == EventType.TASK_COMPLETED


def test_sqlite_store_roundtrip(tmp_path):
    path = tmp_path / "events.db"
    store = SqliteEventStore(path)
    runtime = TaskRuntime(backend=MockLLM([{"value": 4}]), event_store=store)
    run = asyncio.run(_build_task().run({}, runtime=runtime))
    events = store.events(run.run_id)
    store.close()

    assert run.status == TaskStatus.SUCCEEDED
    assert events
    assert events[-1].event_type == EventType.TASK_COMPLETED


def test_error_event_has_standard_fields():
    runtime = TaskRuntime(backend=MockLLM([ModelCallError("bad"), ModelCallError("bad")]))
    run = asyncio.run(_build_task().run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    errors = [e for e in runtime.last_events if e.event_type == EventType.ERROR_OCCURRED]
    assert errors
    payload = errors[0].payload
    assert payload["error_code"] == "E_MODEL_CALL"
    assert payload["retryable"] is True
    assert payload["attempt"] == 1
