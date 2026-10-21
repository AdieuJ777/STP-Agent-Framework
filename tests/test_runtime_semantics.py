import asyncio

from kkk import (
    EventType,
    TaskRuntime,
    TaskSpec,
    TaskStatus,
    TerminationCondition,
    Tool,
    ToolRegistry,
    ToolSpec,
    task,
)
from kkk.models import ValidatorRef


class _SlowTool(Tool):
    spec = ToolSpec(
        tool_name="slow_tool",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        timeout_seconds=5.0,
    )

    async def execute(self, input_data, context):
        await asyncio.sleep(0.2)
        return {"ok": True}


def test_task_timeout_marks_inflight_tool_unknown():
    spec = TaskSpec(
        task_id="TSK-UNKNOWN",
        name="unknown",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        timeout_seconds=0.05,
    )

    @task(spec)
    def build(ctx):
        ctx.step("slow", tool="slow_tool", input={}, output_key="v")
        ctx.end({"ok": ctx.state["v"]["ok"]})

    runtime = TaskRuntime(tools=ToolRegistry([_SlowTool()]))
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_TASK_TIMEOUT"
    ends = [e for e in runtime.last_events if e.event_type == EventType.TOOL_CALL_END]
    assert ends
    assert ends[-1].payload["status"] == "UNKNOWN"
    assert ends[-1].payload["error_code"] == "E_TASK_TIMEOUT"


def test_business_validator_failure():
    def reject(output):
        raise ValueError("budget rule violated")

    spec = TaskSpec(
        task_id="TSK-VALIDATOR",
        name="validator",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        validators=[ValidatorRef(name="budget", func=reject)],
    )

    @task(spec)
    def build(ctx):
        ctx.step("ok", func=lambda data: {"ok": True}, input={}, output_key="v")
        ctx.end({"ok": ctx.state["v"]["ok"]})

    run = asyncio.run(build.run({}, runtime=TaskRuntime()))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_BUSINESS_RULE"


def test_output_match_termination_condition():
    spec = TaskSpec(
        task_id="TSK-TERM",
        name="termination",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]},
        termination_condition=TerminationCondition(type="output_match", value="ok"),
    )

    @task(spec)
    def build(ctx):
        ctx.step("ok", func=lambda data: {"status": "ok"}, input={}, output_key="v")
        ctx.end({"status": ctx.state["v"]["status"]})

    run = asyncio.run(build.run({}, runtime=TaskRuntime()))
    assert run.status == TaskStatus.SUCCEEDED


def test_output_match_termination_failure():
    spec = TaskSpec(
        task_id="TSK-TERM2",
        name="termination2",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]},
        termination_condition=TerminationCondition(type="output_match", value="missing"),
    )

    @task(spec)
    def build(ctx):
        ctx.step("ok", func=lambda data: {"status": "ok"}, input={}, output_key="v")
        ctx.end({"status": ctx.state["v"]["status"]})

    run = asyncio.run(build.run({}, runtime=TaskRuntime()))
    assert run.status == TaskStatus.FAILED
    assert run.error is not None and run.error.code == "E_TERMINATION"
