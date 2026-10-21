"""P1 检查点：可序列化的执行快照与 JSON 存储。

设计边界：
- 只保存任务数据（输入、状态、已完成节点、计数器、恢复位置），
  不保存 ExecutionContext / 后端 / 工具 / 协程等运行时对象；
- 写入采用临时文件 + fsync + 原子替换，避免把半截文件当成有效检查点；
- 读取时校验文件格式、checkpoint_version 和关键标识，失败必须显式报错。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .errors import (
    CheckpointIOError,
    CheckpointIncompatibleError,
    CheckpointInvalidError,
    CheckpointNotFoundError,
)
from .models import RunError, TaskSpec, utcnow
from .plan import TaskIR

CHECKPOINT_VERSION = 1


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _checkpoint_content_hash(payload: dict[str, Any]) -> str:
    clone = dict(payload)
    clone["content_hash"] = ""
    return hashlib.sha256(_canonical_json(clone).encode("utf-8")).hexdigest()


def plan_hash(plan: TaskIR) -> str:
    """执行计划的结构哈希。

    注意：callable 字段被 ``TaskIR.to_dict()`` 排除，因此这是结构哈希，
    不是函数体级别的代码指纹。恢复前仍应保证代码版本一致。
    """
    return hashlib.sha256(_canonical_json(plan.to_dict()).encode("utf-8")).hexdigest()


def spec_hash(spec: TaskSpec) -> str:
    """TaskSpec 的规范化哈希（验证器 callable 不参与）。"""
    return hashlib.sha256(
        _canonical_json(spec.model_dump(mode="json")).encode("utf-8")
    ).hexdigest()


class Checkpoint(BaseModel):
    """一次步骤边界上的完整恢复点。"""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    checkpoint_version: int = CHECKPOINT_VERSION
    checkpoint_id: str = ""
    sequence: int = 0

    run_id: str
    task_id: str
    task_name: str = ""
    task_version: str = ""
    protocol_version: str = "1.0"
    status: str = "RUNNING"
    plan_hash: str
    spec_hash: str

    input: dict[str, Any]
    state: dict[str, Any] = Field(default_factory=dict)
    # 派生只读视图：state 仍是唯一事实来源；旧检查点可以为 None。
    step_results: dict[str, Any] | None = None
    completed_steps: list[str] = Field(default_factory=list)
    branch_routes: dict[str, str] = Field(default_factory=dict)

    step_count: int = 0
    model_call_count: int = 0
    attempt_count: int = 0
    next_step_id: str | None = None
    elapsed_seconds: float = 0.0

    error: RunError | None = None
    unknown_tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    resume_safe: bool = True
    resume_block_reason: str | None = None
    content_hash: str = ""

    created_at: datetime = Field(default_factory=utcnow)

    def to_snapshot(self) -> dict[str, Any]:
        """转换为 ExecutionContext.apply_snapshot() 接受的形状。"""
        return {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "status": self.status,
            "protocol_version": self.protocol_version,
            "input": self.input,
            "state": self.state,
            "step_results": dict(self.step_results)
            if self.step_results is not None
            else None,
            "completed_steps": list(self.completed_steps),
            "branch_routes": dict(self.branch_routes),
            "step_count": self.step_count,
            "model_call_count": self.model_call_count,
            "attempt_count": self.attempt_count,
            "next_step_id": self.next_step_id,
            "elapsed_seconds": self.elapsed_seconds,
        }


class CheckpointStore(Protocol):
    def save(self, checkpoint: Checkpoint) -> str: ...

    def load(self, checkpoint_id: str) -> Checkpoint: ...

    def load_latest(self, run_id: str, task_id: str) -> Checkpoint | None: ...

    def list(self, run_id: str) -> list[Checkpoint]: ...


class JsonCheckpointStore:
    """按 ``<base_dir>/<safe_run_id>/<seq>.json`` 保存检查点。"""

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    @staticmethod
    def _safe_component(value: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
        return safe or "run"

    def _run_dir(self, run_id: str) -> Path:
        return self.base_dir / self._safe_component(run_id)

    def _path(self, checkpoint_id: str) -> Path:
        run_part, sep, seq_part = checkpoint_id.rpartition("-")
        if not sep or not seq_part.isdigit():
            raise CheckpointInvalidError(
                f"invalid checkpoint_id {checkpoint_id!r}",
                details={"checkpoint_id": checkpoint_id},
            )
        return self.base_dir / self._safe_component(run_part) / f"{int(seq_part):06d}.json"

    @staticmethod
    def _next_sequence(run_dir: Path) -> int:
        max_sequence = 0
        for path in run_dir.glob("*.json"):
            try:
                max_sequence = max(max_sequence, int(path.stem))
            except ValueError:
                continue
        return max_sequence + 1

    def save(self, checkpoint: Checkpoint) -> str:
        run_dir = self._run_dir(checkpoint.run_id)
        try:
            run_dir.mkdir(parents=True, exist_ok=True)
            sequence = self._next_sequence(run_dir)
            checkpoint_id = f"{self._safe_component(checkpoint.run_id)}-{sequence:06d}"
            checkpoint.sequence = sequence
            checkpoint.checkpoint_id = checkpoint_id
            checkpoint.content_hash = ""

            payload = checkpoint.model_dump(mode="json")
            checkpoint.content_hash = _checkpoint_content_hash(payload)
            payload = checkpoint.model_dump(mode="json")
            text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)

            fd, temp_name = tempfile.mkstemp(
                prefix=".tmp-", suffix=".json", dir=str(run_dir)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(text)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, run_dir / f"{sequence:06d}.json")
            except Exception:
                try:
                    os.unlink(temp_name)
                except OSError:
                    pass
                raise
        except CheckpointError:
            raise
        except Exception as exc:
            raise CheckpointIOError(
                f"failed to save checkpoint: {exc}",
                details={"run_id": checkpoint.run_id},
                cause=exc,
            ) from exc
        return checkpoint_id

    def _read(self, path: Path) -> Checkpoint:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CheckpointIOError(
                f"failed to read checkpoint file {path}: {exc}",
                details={"path": str(path)},
                cause=exc,
            ) from exc

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CheckpointInvalidError(
                f"checkpoint file {path} is not valid JSON: {exc}",
                details={"path": str(path)},
                cause=exc,
            ) from exc

        try:
            checkpoint = Checkpoint.model_validate(payload)
        except ValidationError as exc:
            raise CheckpointInvalidError(
                f"checkpoint file {path} does not match the checkpoint schema",
                details={"path": str(path), "errors": exc.errors()},
                cause=exc,
            ) from exc

        if checkpoint.checkpoint_version != CHECKPOINT_VERSION:
            raise CheckpointIncompatibleError(
                f"unsupported checkpoint_version {checkpoint.checkpoint_version}; "
                f"expected {CHECKPOINT_VERSION}",
                details={
                    "path": str(path),
                    "checkpoint_version": checkpoint.checkpoint_version,
                },
            )
        if checkpoint.content_hash:
            payload = checkpoint.model_dump(mode="json")
            computed = _checkpoint_content_hash(payload)
            if computed != checkpoint.content_hash:
                raise CheckpointInvalidError(
                    f"checkpoint content hash mismatch for {path}",
                    details={"path": str(path)},
                )
        return checkpoint

    def load(self, checkpoint_id: str) -> Checkpoint:
        path = self._path(checkpoint_id)
        if not path.exists():
            raise CheckpointNotFoundError(
                f"checkpoint {checkpoint_id!r} was not found",
                details={"checkpoint_id": checkpoint_id, "path": str(path)},
            )
        checkpoint = self._read(path)
        if checkpoint.checkpoint_id and checkpoint.checkpoint_id != checkpoint_id:
            raise CheckpointInvalidError(
                f"checkpoint file {path} contains checkpoint_id "
                f"{checkpoint.checkpoint_id!r}, expected {checkpoint_id!r}",
                details={"path": str(path), "expected": checkpoint_id},
            )
        return checkpoint

    def load_latest(self, run_id: str, task_id: str) -> Checkpoint | None:
        run_dir = self._run_dir(run_id)
        if not run_dir.exists():
            return None
        files = sorted(
            (path for path in run_dir.glob("*.json") if path.stem.isdigit()),
            key=lambda path: int(path.stem),
            reverse=True,
        )
        for path in files:
            checkpoint = self._read(path)
            if checkpoint.run_id == run_id and checkpoint.task_id == task_id:
                return checkpoint
        return None

    def list(self, run_id: str) -> list[Checkpoint]:
        run_dir = self._run_dir(run_id)
        if not run_dir.exists():
            return []
        files = sorted(
            (path for path in run_dir.glob("*.json") if path.stem.isdigit()),
            key=lambda path: int(path.stem),
        )
        return [self._read(path) for path in files]
