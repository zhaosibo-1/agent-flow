"""执行状态与持久化：断点续跑的基础。

运行状态要能完整地写出去、再原样读回来 —— 这不是加分项，
是工作流引擎的基本盘。理由：人工审批会让一次运行**挂起几分钟到几天**，
进程重启、发版、机器换掉都发生在挂起期间。
不能恢复的状态等于「审批之后从头再跑」，
对下游幂等性差的任务（发邮件、扣款）这是事故。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

#: 节点级状态
NODE_PENDING = "pending"
NODE_RUNNING = "running"
#: 审批挂起。刻意**不复用 running**：
#: 跑着的节点和"卡在等一个人点按钮"的节点是两回事 ——
#: 复用会让监控看板把「等审批等了三天」显示成「执行了三天」，
#: 而这正是审批流最需要被看见的那个指标。
NODE_WAITING = "waiting"
NODE_SUCCEEDED = "succeeded"
NODE_FAILED = "failed"
NODE_SKIPPED = "skipped"      # 条件分支没选中它
NODE_COMPENSATED = "compensated"  # 已执行补偿

#: 运行级状态
RUN_PENDING = "pending"
RUN_RUNNING = "running"
RUN_WAITING = "waiting_approval"
RUN_SUCCEEDED = "succeeded"
RUN_FAILED = "failed"
RUN_CANCELLED = "cancelled"

#: 对外暴露的状态全集（/api/options 与前端图例共用）。
#: 集中定义而不是让各处手写字面量列表 —— 加一个新状态时
#: 只改一处，否则前端图例迟早和后端对不上。
NODE_STATUSES: tuple[str, ...] = (
    NODE_PENDING, NODE_RUNNING, NODE_WAITING, NODE_SUCCEEDED,
    NODE_FAILED, NODE_SKIPPED, NODE_COMPENSATED,
)
RUN_STATUSES: tuple[str, ...] = (
    RUN_PENDING, RUN_RUNNING, RUN_WAITING,
    RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELLED,
)

#: 合法的运行级状态转移。显式列出而不是随手 if ——
#: 非法转移（成功之后又变 running）是并发 bug 的温床，
#: 集合里查不到就直接拒绝，比"看起来还行"诚实得多。
LEGAL_RUN_TRANSITIONS: dict[str, set[str]] = {
    RUN_PENDING: {RUN_RUNNING, RUN_CANCELLED},
    RUN_RUNNING: {RUN_WAITING, RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELLED},
    RUN_WAITING: {RUN_RUNNING, RUN_CANCELLED},
    RUN_FAILED: set(),      # 终态。要重跑就起新的 run（带断点续跑）。
    #: succeeded → cancelled 是**有意保留**的唯一终态出口：
    #: 「这笔业务做完了，但别做了」= 取消并回滚，是补偿（saga）存在的意义。
    #: 没有这条出口，误操作后只能手工逆操作，而引擎明明知道每一步做了什么。
    RUN_SUCCEEDED: {RUN_CANCELLED},
    RUN_CANCELLED: set(),
}


@dataclass
class NodeState:
    """一个节点的执行记录。"""

    node_id: str
    status: str = NODE_PENDING
    attempts: int = 0
    #: 最后一次的输出（成功）或错误（失败）
    output: Any = None
    error: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    #: 由哪个分支到达（条件节点走的哪个分支）
    via_branch: str | None = None

    @property
    def duration_ms(self) -> float | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return (self.finished_at - self.started_at) * 1000.0

    def to_wire(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "status": self.status,
            "attempts": self.attempts,
            "output": self.output,
            "error": self.error,
            "duration_ms": None if self.duration_ms is None
            else round(self.duration_ms, 2),
            "via_branch": self.via_branch,
        }


@dataclass
class RunState:
    """一次运行的完整状态。可序列化、可恢复。"""

    run_id: str
    workflow_name: str
    workflow_version: str
    status: str = RUN_PENDING
    #: 节点 id → 状态
    nodes: dict[str, NodeState] = field(default_factory=dict)
    #: 上下文：上游输出按节点 id 存进来，供下游表达式引用
    context: dict[str, Any] = field(default_factory=dict)
    #: 事件日志（追加式）。状态每变一次记一条，这是排障的唯一凭据。
    events: list[dict[str, Any]] = field(default_factory=list)
    #: 挂起中的审批请求 {node_id, token}
    pending_approval: dict[str, Any] | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    # -- 转移 ------------------------------------------------------------

    def transition(self, new_status: str) -> None:
        legal = LEGAL_RUN_TRANSITIONS.get(self.status, set())
        if new_status not in legal:
            raise RuntimeError(
                f"非法状态转移 {self.status!r} → {new_status!r}；"
                f"合法目标：{sorted(legal) or '（终态，无）'}"
            )
        self.record("run", self.status, new_status)
        self.status = new_status
        self.updated_at = time.time()

    def node_state(self, node_id: str) -> NodeState:
        if node_id not in self.nodes:
            self.nodes[node_id] = NodeState(node_id=node_id)
        return self.nodes[node_id]

    def record(self, kind: str, old: Any, new: Any, **extra: Any) -> None:
        """追加一条事件。

        事件里带时间戳与转移前后值 —— "节点 X 为什么失败"
        这类问题只有事件日志能回答，输出和 error 都不够。
        """
        self.events.append(
            {"at": round(time.time(), 4), "kind": kind,
             "from": old, "to": new, **extra}
        )
        self.updated_at = time.time()

    # -- 派生查询 ---------------------------------------------------------

    def completed_ids(self) -> list[str]:
        return [nid for nid, ns in self.nodes.items()
                if ns.status == NODE_SUCCEEDED]

    def skipped_ids(self) -> list[str]:
        return [nid for nid, ns in self.nodes.items()
                if ns.status == NODE_SKIPPED]

    def pending_ids(self) -> list[str]:
        return [nid for nid, ns in self.nodes.items()
                if ns.status == NODE_PENDING]

    # -- 序列化 -----------------------------------------------------------

    def to_json(self) -> str:
        return json.dumps(self._to_dict(), ensure_ascii=False)

    def _to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_name": self.workflow_name,
            "workflow_version": self.workflow_version,
            "status": self.status,
            "nodes": {nid: ns.__dict__ for nid, ns in self.nodes.items()},
            "context": self.context,
            "events": self.events,
            "pending_approval": self.pending_approval,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_json(cls, raw: str | dict[str, Any]) -> "RunState":
        data = json.loads(raw) if isinstance(raw, str) else dict(raw)
        nodes = {
            nid: NodeState(**node_data)
            for nid, node_data in data.pop("nodes").items()
        }
        state = cls(**data)
        state.nodes = nodes
        return state

    def summary(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for ns in self.nodes.values():
            counts[ns.status] = counts.get(ns.status, 0) + 1
        return {
            "run_id": self.run_id,
            "workflow": f"{self.workflow_name}@{self.workflow_version}",
            "status": self.status,
            "node_counts": counts,
            "created_at": round(self.created_at, 3),
            "updated_at": round(self.updated_at, 3),
            "pending_approval": self.pending_approval,
        }
