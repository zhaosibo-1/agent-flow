"""构造工作流定义的小工具。

用函数而不是 fixture：测试需要**现场拼图**（不同拓扑、不同分支），
函数比 fixture 灵活，也让每个测试文件自包含。
"""

from __future__ import annotations

from app.dag import NodeDef, WorkflowDef


def wf(name: str, *nodes: NodeDef, entry: str | None = None) -> WorkflowDef:
    """把一串节点拼成工作流，入口默认取第一个节点。"""
    return WorkflowDef(
        name=name,
        nodes={n.id: n for n in nodes},
        entry=[entry] if entry else ([nodes[0].id] if nodes else []),
    )


def task(node_id: str, **kw) -> NodeDef:
    """线性 task 节点：下游统一写进空分支。

    ``next`` 接受 str / list / dict 三种写法，前两种都归一到 {"" : [...]}。
    """
    nxt = kw.pop("next", None)
    if nxt is None:
        next_map: dict = {}
    elif isinstance(nxt, str):
        next_map = {"": [nxt]}
    elif isinstance(nxt, list):
        next_map = {"": list(nxt)}
    else:
        next_map = nxt
    return NodeDef(id=node_id, type="task", next=next_map, **kw)


def condition(node_id: str, expr: str, true_next: list[str],
              false_next: list[str], **kw) -> NodeDef:
    return NodeDef(
        id=node_id, type="condition",
        next={"true": true_next, "false": false_next},
        params={"expr": expr}, **kw,
    )


def approval(node_id: str, next_ids: list[str], **kw) -> NodeDef:
    return NodeDef(
        id=node_id, type="approval",
        next={"approved": next_ids}, **kw,
    )
