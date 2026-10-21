import asyncio

from kkk import GraphBuilder, GraphTask, TaskRuntime, TaskSpec, TaskStatus, task
from kkk.diff import compare_runs
from kkk.refs import Ref


def _spec():
    return TaskSpec(
        task_id="TSK-EQ",
        name="equivalence",
        input_schema={"type": "object"},
        output_schema={
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
            "additionalProperties": False,
        },
    )


def _build_script(spec):
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

    return build


def _build_graph(spec):
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
    return GraphTask(spec, builder)


def test_script_and_graph_are_equivalent():
    spec = _spec()
    script_runtime = TaskRuntime()
    script_run = asyncio.run(_build_script(spec).run({}, runtime=script_runtime))

    graph_runtime = TaskRuntime()
    graph_run = asyncio.run(_build_graph(spec).run({}, runtime=graph_runtime))

    assert script_run.status == TaskStatus.SUCCEEDED
    assert graph_run.status == TaskStatus.SUCCEEDED
    report = compare_runs(
        script_run,
        graph_run,
        left_events=script_runtime.last_events,
        right_events=graph_runtime.last_events,
        strict_fields=["status"],
    )
    assert report.equivalent, report.differences


def test_diff_detects_output_difference():
    spec = _spec()
    left = asyncio.run(_build_script(spec).run({}, runtime=TaskRuntime()))
    right = asyncio.run(_build_script(spec).run({}, runtime=TaskRuntime()))
    right.output = {"status": "low"}
    report = compare_runs(left, right, strict_fields=["status"])
    assert not report.equivalent
