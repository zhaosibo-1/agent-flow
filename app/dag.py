"""工作流的静态结构：DAG 定义、校验、拓扑排序。

这一层是**纯静态**的 —— 不执行任何东西，只回答三个问题：

1. 这张图合法吗？（引用的节点存在、无环、每个节点的输入都接得上）
2. 合法的执行顺序是什么？（拓扑序）
3. 从 A 到 B 有哪些路径？（前端画图、补偿排序都用）

把"结构"从"执行"里拆出来的理由很实际：执行时的错误
（"节点 3 的输入接不到上游输出"）几乎全是结构错误，
在定义阶段就能拦截，根本不用等到运行到一半才炸。
运行到一半才炸的工作流系统是最难调试的一类 ——
它的失败依赖状态，而状态转瞬即逝。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class WorkflowError(ValueError):
    """工作流定义不合法。"""


#: 所有合法的节点类型。执行器按这个分发。
NODE_TYPES: tuple[str, ...] = (
    "task",        # 执行一个动作（表达式 / HTTP / 任意可调用）
    "condition",   # 求值一个谓词，选一个分支
    "approval",    # 人工审批，挂起等待
    "parallel",    # 并行组（本实现按拓扑序展开，记录分组用于展示）
)


@dataclass
class NodeDef:
    """一个节点。``id`` 在工作流内唯一。"""

    id: str
    type: str
    #: 分支名 → 下游节点 id 列表。仅 condition 使用；
    #: 其他类型把全部下游写进 ``""`` 这个默认键。
    next: dict[str, list[str]] = field(default_factory=dict)
    #: 节点参数（表达式、重试配置等），执行器解释
    params: dict[str, Any] = field(default_factory=dict)
    #: 失败时的补偿节点 id（saga 模式）。为空表示无需补偿。
    compensation: str | None = None
    #: 失败后的重试次数（0 = 不重试）
    retries: int = 0
    #: 展示用
    label: str = ""

    def __post_init__(self) -> None:
        if self.type not in NODE_TYPES:
            raise WorkflowError(
                f"未知节点类型 {self.type!r}；合法值：{'、'.join(NODE_TYPES)}"
            )
        if not self.id:
            raise WorkflowError("节点 id 不能为空")
        # next 的键统一成 str，避免调用方传 int 键造成分支匹配不上
        self.next = {str(k): list(v) for k, v in self.next.items()}

    def successors(self, branch: str = "") -> list[str]:
        """给定分支名，返回下游节点。

        非条件节点只有默认分支；条件节点必须给分支名，
        否则等于"所有分支都走"，这是条件分支最常见的误用。
        """
        if self.type == "condition" and branch == "":
            raise WorkflowError(
                f"条件节点 {self.id!r} 必须指定分支名才能取下游"
            )
        return list(self.next.get(branch, []))


@dataclass
class WorkflowDef:
    """一张 DAG。"""

    name: str
    nodes: dict[str, NodeDef]
    #: 入口节点（可以有多个，都会被调度）
    entry: list[str] = field(default_factory=list)
    version: str = "1"

    def __post_init__(self) -> None:
        self.entry = list(self.entry)
        if not self.nodes:
            raise WorkflowError("工作流至少要有一个节点")

    # -- 查询 -----------------------------------------------------------

    def node(self, node_id: str) -> NodeDef:
        try:
            return self.nodes[node_id]
        except KeyError:
            raise WorkflowError(f"节点 {node_id!r} 不存在于工作流 {self.name!r}") from None

    def all_edges(self) -> list[tuple[str, str, str]]:
        """``[(from, branch, to)]``，前端画图与补偿排序共用。"""
        edges: list[tuple[str, str, str]] = []
        for node in self.nodes.values():
            for branch, targets in node.next.items():
                for target in targets:
                    edges.append((node.id, branch, target))
        return edges

    # -- 校验与排序 ------------------------------------------------------

    def validate(self) -> list[str]:
        """返回所有问题（不只是第一个）。

        报一个错就停，用户要改一轮才见得到下一个错；
        一次性报全，定义一张 20 节点的图只需要调试一轮。
        """
        problems: list[str] = []

        if not self.entry:
            problems.append("没有指定入口节点")
        else:
            for entry_id in self.entry:
                if entry_id not in self.nodes:
                    problems.append(f"入口 {entry_id!r} 不是已定义的节点")

        edges = self.all_edges()
        for src, _branch, dst in edges:
            if dst not in self.nodes:
                problems.append(f"{src!r} 的下游 {dst!r} 未定义")

        # 补偿节点必须存在，且不能反过来成为主流程的一环
        for node in self.nodes.values():
            if node.compensation is not None:
                if node.compensation not in self.nodes:
                    problems.append(
                        f"{node.id!r} 的补偿节点 {node.compensation!r} 未定义"
                    )
                elif node.compensation in [t for s, _b, t in edges]:
                    problems.append(
                        f"{node.id!r} 的补偿节点 {node.compensation!r} 同时出现在"
                        "主流程里 —— 补偿是失败路径，不该被正常调度"
                    )
                elif node.compensation in self.entry:
                    # 入口节点入度为 0，调度器会把入度为 0 的节点直接执行 ——
                    # 于是补偿动作会在每次正常运行时跑一遍。
                    problems.append(
                        f"{node.id!r} 的补偿节点 {node.compensation!r} 被指定为"
                        "入口 —— 补偿只能由失败/取消路径调度"
                    )

        cycle = self.find_cycle()
        if cycle:
            problems.append("存在环：" + " → ".join(cycle + [cycle[0]]))

        # 不可达检查**排除补偿节点**：补偿由失败路径调度，
        # 本来就不从入口可达 —— 这是它的定义，不是缺陷。
        # 曾经这里不排除，导致「带补偿的合法工作流」永远注册不上：
        # 三条校验互相矛盾（补偿必须存在、不能进主流程、又必须可达）。
        compensation_ids = {n.compensation for n in self.nodes.values()
                            if n.compensation is not None}
        unreachable = self.unreachable_nodes() - compensation_ids
        if unreachable:
            problems.append(
                "从入口不可达的节点：" + "、".join(sorted(unreachable))
            )
        return problems

    def find_cycle(self) -> list[str] | None:
        """用 DFS 三色标记找环，返回环上的节点序列。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {nid: WHITE for nid in self.nodes}
        path: list[str] = []

        def visit(node_id: str) -> list[str] | None:
            color[node_id] = GRAY
            path.append(node_id)
            for _src, _b, dst in self.all_edges():
                if _src != node_id:
                    continue
                if dst not in self.nodes:
                    continue
                if color[dst] == GRAY:
                    # 回到灰色的节点：环。从 path 里截取这一段。
                    start = path.index(dst)
                    return path[start:]
                if color[dst] == WHITE:
                    found = visit(dst)
                    if found:
                        return found
            path.pop()
            color[node_id] = BLACK
            return None

        for node_id in self.nodes:
            if color[node_id] == WHITE:
                found = visit(node_id)
                if found:
                    return found
        return None

    def topological_order(self) -> list[str]:
        """Kahn 算法。平局按 id 排序 —— **结果必须可复现**。

        两个没有依赖关系的节点，先跑谁都是对的；
        但如果顺序不稳定，同一张图两次运行的事件日志就会不同，
        调试与测试都会变成玄学。
        """
        indegree: dict[str, int] = {nid: 0 for nid in self.nodes}
        for _src, _b, dst in self.all_edges():
            indegree[dst] += 1

        ready = sorted(nid for nid, deg in indegree.items() if deg == 0)
        order: list[str] = []
        while ready:
            current = ready.pop(0)
            order.append(current)
            for _s, _b, dst in self.all_edges():
                if _s != current:
                    continue
                indegree[dst] -= 1
                if indegree[dst] == 0:
                    ready.append(dst)
                    ready.sort()
        if len(order) != len(self.nodes):
            remaining = sorted(set(self.nodes) - set(order))
            raise WorkflowError(
                "图里有环，无法拓扑排序；剩余节点：" + "、".join(remaining)
            )
        return order

    def unreachable_nodes(self) -> set[str]:
        """从入口 BFS 不可达的节点。

        不可达通常意味着「某条分支忘接了」—— 节点写好了但没人指向它。
        运行时它只是安静地不被执行，等价于一个静默的功能缺失。
        """
        if not self.entry:
            return set(self.nodes)
        seen: set[str] = set()
        frontier = [e for e in self.entry if e in self.nodes]
        while frontier:
            current = frontier.pop()
            if current in seen:
                continue
            seen.add(current)
            for src, _b, dst in self.all_edges():
                if src == current and dst not in seen:
                    frontier.append(dst)
        return set(self.nodes) - seen

    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "nodes": [
                {
                    "id": n.id,
                    "type": n.type,
                    "label": n.label or n.id,
                    "next": n.next,
                    "retries": n.retries,
                    "compensation": n.compensation,
                    "params": n.params,
                }
                for n in self.nodes.values()
            ],
            "entry": self.entry,
            "edges": [
                {"from": s, "branch": b, "to": t} for s, b, t in self.all_edges()
            ],
            "topological_order": self.topological_order(),
        }
