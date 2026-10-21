from kkk import PlanNode, StepAction, TaskSpec, task
from kkk.plan import PlanBuilder
from kkk.refs import Ref
from kkk.validation import validate_plan


def _spec(**kwargs):
    base = dict(
        task_id="TSK-PLAN",
        name="plan",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
    )
    base.update(kwargs)
    return TaskSpec(**base)


def _func(data):
    return {"ok": True}


def _step(node_id, output_key):
    return PlanNode(
        id=node_id,
        kind="step",
        name=node_id,
        action=StepAction(kind="function", func=_func),
        output_key=output_key,
    )


def _end(node_id="end", output_mapping=None):
    return PlanNode(id=node_id, kind="end", name=node_id, output_mapping=output_mapping or {})


def test_duplicate_output_key_same_path_is_error():
    builder = PlanBuilder("TSK-PLAN")
    builder.add_node(_step("a", "x"))
    builder.add_node(_step("b", "x"))
    builder.add_node(_end())
    builder.set_entry("a")
    builder.link("a", "b")
    builder.link("b", "end")
    report = validate_plan(_spec(), builder.build())
    assert not report.ok
    assert any("duplicate output_key" in error for error in report.errors)


def test_duplicate_output_key_in_exclusive_branches_is_allowed():
    spec = _spec(
        output_schema={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]}
    )

    @task(spec)
    def build(ctx):
        ctx.step("classify", func=lambda data: {"amount": 1}, input={}, output_key="expense")

        def high(c):
            c.step("high", func=lambda data: {"status": "high"}, input={}, output_key="result")

        def low(c):
            c.step("low", func=lambda data: {"status": "low"}, input={}, output_key="result")

        ctx.branch("route", condition=lambda state: "high", cases={"high": high, "low": low}, default="low")
        ctx.end({"status": ctx.state["result"]["status"]})

    report = validate_plan(spec, build.build())
    assert report.ok, report.errors


def test_dangling_reference_is_error():
    builder = PlanBuilder("TSK-PLAN")
    builder.add_node(_step("a", "x"))
    builder.add_node(_end())
    builder.set_entry("a")
    builder.nodes["a"].next = "missing"
    report = validate_plan(_spec(), builder.build())
    assert not report.ok


def test_cycle_is_error():
    builder = PlanBuilder("TSK-PLAN")
    builder.add_node(_step("a", "x"))
    builder.add_node(_step("b", "y"))
    builder.add_node(_end())
    builder.set_entry("a")
    builder.link("a", "b")
    builder.link("b", "a")
    report = validate_plan(_spec(), builder.build())
    assert not report.ok
    assert any("cycle" in error for error in report.errors)


def test_unreachable_node_is_error():
    builder = PlanBuilder("TSK-PLAN")
    builder.add_node(_step("a", "x"))
    builder.add_node(_step("b", "y"))
    builder.add_node(_end())
    builder.set_entry("a")
    builder.link("a", "end")
    builder.link("b", "end")
    report = validate_plan(_spec(), builder.build())
    assert not report.ok
    assert any("unreachable" in error for error in report.errors)


def test_branch_without_default_is_error():
    builder = PlanBuilder("TSK-PLAN")
    branch = PlanNode(
        id="b",
        kind="branch",
        name="b",
        condition=__import__("kkk.plan", fromlist=["ConditionSpec"]).ConditionSpec(
            name="b", fn=lambda state: "yes"
        ),
        routes={"yes": "end"},
        default_route=None,
    )
    builder.add_node(branch)
    builder.add_node(_end())
    builder.set_entry("b")
    report = validate_plan(_spec(), builder.build())
    assert not report.ok
    assert any("default_route" in error for error in report.errors)


def test_output_mapping_missing_required_is_error():
    builder = PlanBuilder("TSK-PLAN")
    builder.add_node(_step("a", "x"))
    builder.add_node(_end(output_mapping={}))
    builder.set_entry("a")
    builder.link("a", "end")
    spec = _spec(
        output_schema={"type": "object", "properties": {"x": {"type": "boolean"}}, "required": ["x"]}
    )
    report = validate_plan(spec, builder.build())
    assert not report.ok
    assert any("missing required" in error for error in report.errors)


def test_max_steps_smaller_than_shortest_is_error():
    builder = PlanBuilder("TSK-PLAN")
    builder.add_node(_step("a", "x"))
    builder.add_node(_step("b", "y"))
    builder.add_node(_end())
    builder.set_entry("a")
    builder.link("a", "b")
    builder.link("b", "end")
    report = validate_plan(_spec(max_steps=1), builder.build())
    assert not report.ok
    assert any("shortest path" in error for error in report.errors)


def test_unknown_step_reference_is_error():
    builder = PlanBuilder("TSK-PLAN")
    builder.add_node(
        PlanNode(
            id="a",
            kind="step",
            name="a",
            action=StepAction(kind="function", func=_func),
            output_key="x",
            input_mapping={"missing": Ref("step", "nope")},
        )
    )
    builder.add_node(_end(output_mapping={"x": Ref("state", "x")}))
    builder.set_entry("a")
    builder.link("a", "end")
    report = validate_plan(_spec(), builder.build())
    assert not report.ok
    assert any("unknown step output" in error for error in report.errors)
