"""DAG 静态结构的测试。

这一层的错误如果放到运行时才暴露，就是「跑到第 7 个节点才炸」的
经典调试场景。所有结构性问题必须在定义阶段拦截。
"""

from __future__ import annotations

import pytest

from app.dag import NODE_TYPES, NodeDef, WorkflowDef, WorkflowError
from tests.helpers import condition, task, wf


class TestNodeDef:
    def test_unknown_type_rejected(self):
        with pytest.raises(WorkflowError, match="未知节点类型"):
            NodeDef(id="a", type="quantum")

    def test_all_builtin_types_valid(self):
        for t in NODE_TYPES:
            NodeDef(id="a", type=t)

    def test_empty_id_rejected(self):
        with pytest.raises(WorkflowError, match="id"):
            NodeDef(id="", type="task")

    def test_int_keys_normalised_to_str(self):
        """分支键统一成字符串 —— 调用方传 int 键会导致匹配不上。"""
        node = NodeDef(id="c", type="condition",
                       next={1: ["a"], 0: ["b"]})
        assert set(node.next) == {"1", "0"}

    def test_successors_default_branch(self):
        node = task("a", next="b")
        assert node.successors() == ["b"]

    def test_condition_requires_branch_name(self):
        """条件节点不带分支名取下游 = 「所有分支都走」，
        这是对条件语义最常见的误用，必须显式拒绝。"""
        node = condition("c", "input.x > 1", ["t"], ["f"])
        with pytest.raises(WorkflowError, match="分支名"):
            node.successors()
        assert node.successors("true") == ["t"]
        assert node.successors("false") == ["f"]

    def test_task_can_use_empty_branch(self):
        node = task("a", next="b")
        assert node.successors("") == ["b"]


class TestValidation:
    def test_missing_entry(self):
        problems = WorkflowDef(name="w", nodes={"a": task("a")}, entry=[]).validate()
        assert any("入口" in p for p in problems)

    def test_unknown_node_type(self):
        with pytest.raises(WorkflowError, match="未知节点类型"):
            NodeDef(id="x", type="ghost")

    def test_dangling_reference(self):
        w = wf("w", task("a", next="ghost"))
        problems = w.validate()
        assert any("ghost" in p for p in problems)

    def test_cycle_detected(self):
        a = task("a", next="b")
        b = task("b", next="a")
        w = wf("w", a, b)
        problems = w.validate()
        assert any("环" in p for p in problems)

    def test_self_cycle(self):
        a = task("a", next="a")
        w = wf("w", a)
        assert any("环" in p for p in w.validate())

    def test_unreachable_node_reported(self):
        """不可达节点 = 忘接的分支，静默缺失比报错更危险。"""
        a = task("a")
        orphan = task("orphan")
        w = wf("w", a, orphan)
        problems = w.validate()
        assert any("不可达" in p for p in problems)

    def test_compensation_must_exist(self):
        a = task("a", compensation="ghost")
        w = wf("w", a)
        assert any("补偿" in p for p in w.validate())

    def test_compensation_not_in_main_flow(self):
        """补偿节点同时出现在主流程 = 失败路径被正常调度。"""
        main = task("main", next="comp", compensation="comp")
        comp = task("comp")
        w = wf("w", main, comp)
        assert any("补偿" in p and "主流程" in p for p in w.validate())

    def test_valid_workflow_has_no_problems(self):
        a = task("a", next="b")
        b = task("b")
        w = wf("w", a, b)
        assert w.validate() == []

    def test_reports_all_problems_at_once(self):
        """报一个错就停，用户要改一轮才见下一个。"""
        a = task("a", next="ghost1", compensation="ghost2")
        orphan = task("orphan")
        w = wf("w", a, orphan)
        problems = w.validate()
        assert len(problems) >= 3


class TestTopologicalOrder:
    def test_linear_chain(self):
        w = wf("w", task("a", next="b"), task("b", next="c"), task("c"))
        assert w.topological_order() == ["a", "b", "c"]

    def test_diamond(self):
        a = task("a", next=["b", "c"])
        b = task("b", next="d")
        c = task("c", next="d")
        d = task("d")
        w = wf("w", a, b, c, d)
        order = w.topological_order()
        assert order.index("a") < order.index("b") < order.index("d")
        assert order.index("a") < order.index("c") < order.index("d")

    def test_deterministic_on_ties(self):
        """无依赖的节点顺序必须可复现 —— 否则事件日志每次都不同。"""
        nodes = [task(n) for n in "xyzw"]
        w = wf("w", *nodes)
        assert w.topological_order() == ["w", "x", "y", "z"]

    def test_cycle_raises(self):
        a = task("a", next="b")
        b = task("b", next="a")
        w = wf("w", a, b)
        with pytest.raises(WorkflowError, match="环"):
            w.topological_order()


class TestFindCycle:
    def test_no_cycle_returns_none(self):
        w = wf("w", task("a", next="b"), task("b"))
        assert w.find_cycle() is None

    def test_cycle_returns_path(self):
        a = task("a", next="b")
        b = task("b", next="c")
        c = task("c", next="a")
        w = wf("w", a, b, c)
        cycle = w.find_cycle()
        assert cycle is not None
        assert set(cycle) == {"a", "b", "c"}
        # 环要首尾相接：路径末端的下游就是路径开头
        assert c.successors() == ["a"]

    def test_cycle_in_subgraph_only(self):
        """主链没环但子图有环，也要找得出来。"""
        a = task("a", next="b")
        b = task("b")
        x = task("x", next="y")
        y = task("y", next="x")
        w = WorkflowDef(name="w", nodes={"a": a, "b": b, "x": x, "y": y},
                        entry=["a"])
        cycle = w.find_cycle()
        assert cycle is not None and set(cycle) == {"x", "y"}


class TestUnreachable:
    def test_from_entry(self):
        a = task("a", next="b")
        b = task("b")
        orphan = task("orphan")
        w = wf("w", a, b, orphan)
        assert w.unreachable_nodes() == {"orphan"}

    def test_reachable_through_condition_true_branch(self):
        """条件分支的 false 侧虽然这次不走，但**结构上**可达。"""
        c = condition("c", "true", ["t"], ["f"])
        t = task("t")
        f = task("f")
        w = wf("w", c, t, f)
        assert w.unreachable_nodes() == set()

    def test_no_entry_means_all_unreachable(self):
        w = WorkflowDef(name="w", nodes={"a": task("a")}, entry=[])
        assert w.unreachable_nodes() == {"a"}


class TestDescribe:
    def test_describe_roundtrip_fields(self):
        a = task("a", next="b")
        b = task("b")
        w = wf("w", a, b)
        d = w.describe()
        assert d["name"] == "w"
        assert len(d["nodes"]) == 2
        assert d["topological_order"] == ["a", "b"]
        assert d["edges"] == [{"from": "a", "branch": "", "to": "b"}]
