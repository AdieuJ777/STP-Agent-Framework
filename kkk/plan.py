"""统一执行计划（轻量 TaskIR）。

节点类型：
- ``step``：真正执行工具 / LLM / 函数的节点；
- ``branch``：按运行期条件选择唯一后继；
- ``join``：分支汇合的结构节点，不执行任何业务动作；
- ``end``：产出最终输出并结束任务。

第一版只支持有向无环的“顺序 + 分支 + 汇合”结构；循环与并行留给 P2。
"""
from __future__ import annotations

import re
from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field

from .errors import PlanValidationError


class StepAction(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    kind: Literal["tool", "llm", "function"]
    tool_name: str | None = None
    func: Callable[..., Any] | None = Field(default=None, exclude=True)
    func_name: str | None = None
    llm_messages: list[dict[str, Any]] | None = None
    llm_output_schema: dict[str, Any] | None = None


class ConditionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    name: str
    fn: Callable[[dict[str, Any]], str] = Field(exclude=True)


class PlanNode(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    id: str
    kind: Literal["step", "branch", "join", "end"]
    name: str

    # step
    action: StepAction | None = None
    output_key: str | None = None
    input_mapping: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] | None = None
    timeout_seconds: float | None = None
    next: str | None = None

    # branch
    condition: ConditionSpec | None = None
    routes: dict[str, str] = Field(default_factory=dict)
    default_route: str | None = None

    # end
    output_mapping: dict[str, Any] = Field(default_factory=dict)


class TaskIR(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    task_id: str
    protocol_version: str
    entry: str
    nodes: dict[str, PlanNode] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """可序列化视图；callable 字段已被 Field(exclude=True) 排除。"""
        return self.model_dump(mode="json", exclude_none=True)

    def outgoing(self, node_id: str) -> list[tuple[str, str]]:
        node = self.nodes[node_id]
        if node.kind in ("step", "join"):
            return [(node.next, "next")] if node.next else []
        if node.kind == "branch":
            return [(target, label) for label, target in node.routes.items()]
        return []

    def incoming(self) -> dict[str, list[tuple[str, str]]]:
        result: dict[str, list[tuple[str, str]]] = {node_id: [] for node_id in self.nodes}
        for node_id in self.nodes:
            for target, label in self.outgoing(node_id):
                if target not in result:
                    raise PlanValidationError(
                        f"node {node_id!r} references missing node {target!r}",
                        details={"source": node_id, "target": target},
                    )
                result[target].append((node_id, label))
        return result


class PlanBuilder:
    """底层计划构造器，脚本 API 和图 API 都复用它。"""

    def __init__(self, task_id: str, protocol_version: str = "1.0") -> None:
        self.task_id = task_id
        self.protocol_version = protocol_version
        self.nodes: dict[str, PlanNode] = {}
        self.entry: str | None = None
        self._counter = 0

    def _new_id(self, prefix: str) -> str:
        self._counter += 1
        safe = re.sub(r"[^a-zA-Z0-9_]+", "_", prefix).strip("_") or "node"
        node_id = f"{safe}_{self._counter}"
        while node_id in self.nodes:
            self._counter += 1
            node_id = f"{safe}_{self._counter}"
        return node_id

    def add_node(self, node: PlanNode) -> PlanNode:
        if node.id in self.nodes:
            raise PlanValidationError(f"duplicate node id {node.id!r}")
        self.nodes[node.id] = node
        return node

    def set_entry(self, node_id: str) -> None:
        if node_id not in self.nodes:
            raise PlanValidationError(f"entry node {node_id!r} does not exist")
        self.entry = node_id

    def link(self, source_id: str, target_id: str) -> None:
        source = self.nodes[source_id]
        if source.kind not in ("step", "join"):
            raise PlanValidationError(
                f"node {source_id!r} of kind {source.kind!r} cannot use link(); use add_route()"
            )
        if target_id not in self.nodes:
            raise PlanValidationError(f"target node {target_id!r} does not exist")
        source.next = target_id

    def add_route(self, branch_id: str, label: str, target_id: str) -> None:
        branch = self.nodes[branch_id]
        if branch.kind != "branch":
            raise PlanValidationError(f"node {branch_id!r} is not a branch")
        if target_id not in self.nodes:
            raise PlanValidationError(f"route target {target_id!r} does not exist")
        branch.routes[label] = target_id

    def build(self) -> TaskIR:
        if self.entry is None:
            raise PlanValidationError("plan has no entry node")
        if not any(node.kind == "end" for node in self.nodes.values()):
            raise PlanValidationError("plan has no end node")
        return TaskIR(
            task_id=self.task_id,
            protocol_version=self.protocol_version,
            entry=self.entry,
            nodes=self.nodes,
        )
