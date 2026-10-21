"""执行计划的静态校验。

目标不是做完整的类型系统，而是在执行前拦住结构性错误：
悬空引用、成环、不可达节点、分支缺省、output_key 重复、输出映射缺字段等。
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .errors import PlanValidationError
from .models import TaskSpec
from .plan import PlanNode, TaskIR
from .refs import Ref
from .schema import check_schema, schema_properties


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def raise_if_errors(self) -> None:
        if self.errors:
            raise PlanValidationError(
                "plan validation failed: " + "; ".join(self.errors),
                details={"errors": self.errors, "warnings": self.warnings},
            )


def _walk_refs(value: Any):
    if isinstance(value, Ref):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_refs(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk_refs(item)


def _major(version: str) -> str:
    return str(version).split(".", 1)[0]


def _reachable(plan: TaskIR) -> set[str]:
    seen: set[str] = set()
    queue = deque([plan.entry])
    while queue:
        node_id = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        for target, _ in plan.outgoing(node_id):
            if target not in seen:
                queue.append(target)
    return seen


def _has_cycle(plan: TaskIR) -> bool:
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {node_id: WHITE for node_id in plan.nodes}

    def visit(node_id: str) -> bool:
        color[node_id] = GRAY
        for target, _ in plan.outgoing(node_id):
            if color[target] == GRAY:
                return True
            if color[target] == WHITE and visit(target):
                return True
        color[node_id] = BLACK
        return False

    return any(color[node_id] == WHITE and visit(node_id) for node_id in plan.nodes)


def _shortest_step_count(plan: TaskIR) -> int | None:
    """从 entry 到任一 end 的最短路径上经过的 step 节点数。"""
    queue = deque([(plan.entry, 0)])
    seen: set[str] = set()
    while queue:
        node_id, count = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        node = plan.nodes[node_id]
        next_count = count + (1 if node.kind == "step" else 0)
        if node.kind == "end":
            return next_count
        for target, _ in plan.outgoing(node_id):
            queue.append((target, next_count))
    return None


def _path_constraints(plan: TaskIR) -> dict[str, list[dict[str, str]]]:
    """枚举到每个节点的路径条件：经过某个 branch 时选择了哪个 label。

    只用于判断两个步骤是否可能在同一次运行中同时执行。P0 的图很小，
    路径枚举足够；如果分支数量很大，可以后续替换成符号化分析。
    """
    result: dict[str, list[dict[str, str]]] = {node_id: [] for node_id in plan.nodes}

    def visit(node_id: str, constraints: dict[str, str]) -> None:
        if constraints in result[node_id]:
            return
        result[node_id].append(dict(constraints))
        node = plan.nodes[node_id]
        for target, label in plan.outgoing(node_id):
            next_constraints = dict(constraints)
            if node.kind == "branch":
                next_constraints[node.id] = label
            visit(target, next_constraints)

    visit(plan.entry, {})
    return result


def _constraints_compatible(a: dict[str, str], b: dict[str, str]) -> bool:
    return all(a[key] == b[key] for key in a.keys() & b.keys())


def _check_node(plan: TaskIR, node: PlanNode, report: ValidationReport) -> None:
    if node.kind == "step":
        if node.action is None:
            report.errors.append(f"step {node.id!r} has no action")
        if not node.output_key:
            report.errors.append(f"step {node.id!r} has no output_key")
        if node.action is not None:
            if node.action.kind == "tool" and not node.action.tool_name:
                report.errors.append(f"step {node.id!r} uses tool action without tool_name")
            if node.action.kind == "function" and node.action.func is None:
                report.errors.append(f"step {node.id!r} uses function action without func")
            if node.action.kind == "llm" and not node.action.llm_messages:
                report.errors.append(f"step {node.id!r} uses llm action without messages")
            if node.action.kind == "llm" and node.action.llm_output_schema:
                try:
                    check_schema(node.action.llm_output_schema, context=f"step {node.id!r}")
                except PlanValidationError as exc:
                    report.errors.append(str(exc.message))
        if node.next is None:
            report.errors.append(f"step {node.id!r} has no successor; add an end node")
    elif node.kind == "branch":
        if node.condition is None:
            report.errors.append(f"branch {node.id!r} has no condition")
        if not node.routes:
            report.errors.append(f"branch {node.id!r} has no routes")
        if node.default_route is None:
            report.errors.append(f"branch {node.id!r} has no default_route")
        elif node.default_route not in node.routes:
            report.errors.append(
                f"branch {node.id!r} default_route {node.default_route!r} is not in routes"
            )
    elif node.kind == "join":
        if node.next is None:
            report.errors.append(f"join {node.id!r} has no successor")
    elif node.kind == "end":
        if not node.output_mapping:
            report.warnings.append(f"end {node.id!r} has an empty output_mapping")


def validate_plan(spec: TaskSpec, plan: TaskIR) -> ValidationReport:
    report = ValidationReport()

    if plan.task_id != spec.task_id:
        report.errors.append(
            f"plan task_id {plan.task_id!r} does not match spec task_id {spec.task_id!r}"
        )
    if _major(plan.protocol_version) != _major(spec.protocol_version):
        report.errors.append(
            f"protocol major version mismatch: plan={plan.protocol_version!r}, spec={spec.protocol_version!r}"
        )

    try:
        check_schema(spec.input_schema, context="TaskSpec.input_schema")
        check_schema(spec.output_schema, context="TaskSpec.output_schema")
    except PlanValidationError as exc:
        report.errors.append(exc.message)

    if plan.entry not in plan.nodes:
        report.errors.append(f"entry node {plan.entry!r} does not exist")
        return report

    for node_id, node in plan.nodes.items():
        if node_id != node.id:
            report.errors.append(f"node key {node_id!r} does not match node.id {node.id!r}")
        _check_node(plan, node, report)

    try:
        incoming = plan.incoming()
    except PlanValidationError as exc:
        report.errors.append(exc.message)
        return report

    # 输出键唯一性：互斥分支中的同名 output_key 允许复用（运行期只会写一次）
    output_keys: dict[str, list[str]] = {}
    for node in plan.nodes.values():
        if node.kind == "step" and node.output_key:
            output_keys.setdefault(node.output_key, []).append(node.id)
    constraints = _path_constraints(plan)
    for key, node_ids in output_keys.items():
        for i, left in enumerate(node_ids):
            for right in node_ids[i + 1 :]:
                compatible = any(
                    _constraints_compatible(a, b)
                    for a in constraints.get(left, [])
                    for b in constraints.get(right, [])
                )
                if compatible:
                    report.errors.append(
                        f"duplicate output_key {key!r} in {left!r} and {right!r} "
                        "on paths that can execute together"
                    )

    # 引用检查
    input_props = schema_properties(spec.input_schema)
    for node in plan.nodes.values():
        for ref in _walk_refs(node.input_mapping):
            if ref.source == "step" and ref.key not in output_keys:
                report.errors.append(
                    f"step {node.id!r} references unknown step output {ref.key!r}"
                )
            if ref.source == "state" and ref.key not in output_keys:
                report.errors.append(
                    f"step {node.id!r} references unknown state key {ref.key!r}"
                )
            if ref.source == "input" and input_props and ref.key not in input_props:
                report.warnings.append(
                    f"step {node.id!r} references input key {ref.key!r} not declared in input_schema"
                )

    # 可达性 / 环 / 终止
    reachable = _reachable(plan)
    for node_id in plan.nodes:
        if node_id not in reachable:
            report.errors.append(f"node {node_id!r} is unreachable from entry")

    if _has_cycle(plan):
        report.errors.append("plan contains a cycle; the prototype supports DAGs only")

    if not any(node.kind == "end" for node in plan.nodes.values()):
        report.errors.append("plan has no end node")

    shortest = _shortest_step_count(plan)
    if shortest is not None and shortest > spec.max_steps:
        report.errors.append(
            f"max_steps={spec.max_steps} is smaller than the shortest path ({shortest} steps)"
        )

    # 输出映射覆盖必填字段（尽力而为）
    required = spec.output_schema.get("required", [])
    if isinstance(required, list):
        for end in (node for node in plan.nodes.values() if node.kind == "end"):
            missing = [key for key in required if key not in end.output_mapping]
            if missing:
                report.errors.append(
                    f"end {end.id!r} output_mapping is missing required fields: {missing}"
                )

    # 分支条件在构建期不可判定，只做存在性检查；这里不执行 condition。
    return report
