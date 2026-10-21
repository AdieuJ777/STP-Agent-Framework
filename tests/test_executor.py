import asyncio

from kkk import (
    CalculatorTool,
    EventType,
    GraphBuilder,
    GraphTask,
    MockLLM,
    TaskRuntime,
    TaskSpec,
    TaskStatus,
    ToolRegistry,
    task,
)
from kkk.refs import Ref


def test_linear_tool_task():
    spec = TaskSpec(
        task_id="TSK-LINEAR",
        name="linear",
        input_schema={"type": "object"},
        output_schema={
            "type": "object",
            "properties": {"sum": {"type": "number"}},
            "required": ["sum"],
            "additionalProperties": False,
        },
    )

    @task(spec)
    def build(ctx):
        ctx.step("add", tool="calculator", input={"expression": "2 + 3"}, output_key="sum_raw")
        ctx.end({"sum": ctx.state["sum_raw"]["result"]})

    runtime = TaskRuntime(tools=ToolRegistry([CalculatorTool()]))
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED
    assert run.output == {"sum": 5}

    event_types = [event.event_type for event in runtime.last_events]
    assert EventType.TASK_STARTED in event_types
    assert EventType.TOOL_CALL_START in event_types
    assert EventType.TOOL_CALL_END in event_types
    assert EventType.TASK_COMPLETED in event_types
    assert [event.seq for event in runtime.last_events] == list(
        range(1, len(runtime.last_events) + 1)
    )


def test_llm_structured_task_and_event_payloads():
    spec = TaskSpec(
        task_id="TSK-LLM",
        name="llm",
        input_schema={"type": "object"},
        output_schema={
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        },
    )

    @task(spec)
    def build(ctx):
        ctx.step(
            "extract",
            llm="return a value",
            output_schema={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
                "additionalProperties": False,
            },
            output_key="data",
        )
        ctx.end({"value": ctx.state["data"]["value"]})

    runtime = TaskRuntime(backend=MockLLM([{"value": 7}]))
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED
    assert run.output == {"value": 7}

    llm_end = [e for e in runtime.last_events if e.event_type == EventType.LLM_CALL_END]
    assert llm_end
    assert llm_end[0].payload["model_name"] == "mock-model"
    assert "token_usage" in llm_end[0].payload

    step_end = [e for e in runtime.last_events if e.event_type == EventType.STEP_END]
    assert step_end
    assert "output_hash" in step_end[-1].payload


def test_branch_script_task():
    spec = TaskSpec(
        task_id="TSK-BRANCH",
        name="branch",
        input_schema={"type": "object"},
        output_schema={
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
            "additionalProperties": False,
        },
    )

    @task(spec)
    def build(ctx):
        ctx.step("classify", func=lambda data: {"amount": 1500}, input={}, output_key="expense")

        def high(c):
            c.step("high", func=lambda data: {"status": "high"}, input={}, output_key="result")

        def low(c):
            c.step("low", func=lambda data: {"status": "low"}, input={}, output_key="result")

        ctx.branch(
            "route",
            condition=lambda state: "high" if state["expense"]["amount"] > 1000 else "low",
            cases={"high": high, "low": low},
            default="low",
        )
        ctx.end({"status": ctx.state["result"]["status"]})

    runtime = TaskRuntime()
    run = asyncio.run(build.run({}, runtime=runtime))
    assert run.status == TaskStatus.SUCCEEDED
    assert run.output == {"status": "high"}
    assert [e.event_type for e in runtime.last_events].count(EventType.TASK_COMPLETED) == 1


def test_branch_graph_task():
    spec = TaskSpec(
        task_id="TSK-GRAPH",
        name="graph",
        input_schema={"type": "object"},
        output_schema={
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
            "additionalProperties": False,
        },
    )
    builder = GraphBuilder(spec)
    classify = builder.add_step("classify", func=lambda data: {"amount": 1500}, input={}, output_key="expense")
    route = builder.add_branch(
        "route",
        condition=lambda state: "high" if state["expense"]["amount"] > 1000 else "low",
        default="low",
    )
    high = builder.add_step("high", func=lambda data: {"status": "high"}, input={}, output_key="result")
    low = builder.add_step("low", func=lambda data: {"status": "low"}, input={}, output_key="result")
    join = builder.add_join("join")
    end = builder.add_end("end", {"status": Ref("state", "result", ("status",))})
    builder.set_entry(classify)
    builder.add_edge(classify, route)
    builder.add_route(route, "high", high)
    builder.add_route(route, "low", low)
    builder.add_edge(high, join)
    builder.add_edge(low, join)
    builder.add_edge(join, end)

    graph_task = GraphTask(spec, builder)
    run = asyncio.run(graph_task.run({}, runtime=TaskRuntime()))
    assert run.status == TaskStatus.SUCCEEDED
    assert run.output == {"status": "high"}
