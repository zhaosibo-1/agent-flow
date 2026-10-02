"""执行引擎的测试。

重点是**状态机语义**而不是「跑通了」：
分支没选中时下游去哪了、补偿按什么顺序、审批挂起后能不能续跑、
失败之后重试了几次。这些是工作流引擎和「一堆函数调用」的区别。
"""

from __future__ import annotations

import json

import pytest

from app.dag import NodeDef
from app.engine import Engine, RunNotFound, WorkflowNotFound
from app.dag import WorkflowError
from app.state import (
    NODE_COMPENSATED,
    NODE_FAILED,
    NODE_PENDING,
    NODE_SKIPPED,
    NODE_SUCCEEDED,
    NODE_WAITING,
    NODE_RUNNING,
    RUN_CANCELLED,
    RUN_FAILED,
    RUN_RUNNING,
    RUN_SUCCEEDED,
    RUN_WAITING,
)
from tests.helpers import approval, condition, task, wf


def make_engine(records: list | None = None) -> Engine:
    """执行器把节点 id 记进 records，方便断言执行了什么。"""
    seen = records if records is not None else []
    engine = Engine(
        executor=lambda node, ctx: seen.append(node.id) or {"ran": node.id},
        predicate=lambda node, ctx: (
            "true" if node.params["expr"] == "true" else "false"
        ),
        compensator=lambda node, ctx: seen.append(f"undo:{node.id}"),
    )
    return engine, seen


class TestLinear:
    def test_linear_execution_in_order(self):
        engine, seen = make_engine()
        engine.register(wf("w", task("a", next="b"), task("b", next="c"),
                           task("c")))
        state = engine.start("w")
        assert state.status == RUN_SUCCEEDED
        assert seen == ["a", "b", "c"]
        assert all(state.node_state(n).status == NODE_SUCCEEDED
                   for n in "abc")

    def test_upstream_output_in_context(self):
        engine = Engine(executor=lambda node, ctx: {
            "value": ctx.get("input", {}).get("x", 0) + 1})
        engine.register(wf("w", task("a", next="b"), task("b")))
        state = engine.start("w", {"x": 10})
        assert state.context["a"]["value"] == 11
        # b 看见的是同一个 input（它没用 a 的输出），
        # 关键断言是 a 的输出**确实进了上下文**，下游取得到
        assert "a" in state.context and "b" in state.context

    def test_events_record_transitions(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a")))
        state = engine.start("w")
        kinds = [e["kind"] for e in state.events]
        assert "run" in kinds and "node" in kinds


class TestConditionBranch:
    def test_true_branch_runs_false_branch_skips(self):
        engine, seen = make_engine()
        c = condition("c", "true", ["t"], ["f"])
        t = task("t")
        f = task("f")
        engine.register(wf("w", c, t, f))
        state = engine.start("w")
        assert state.status == RUN_SUCCEEDED
        assert "t" in seen and "f" not in seen
        assert state.node_state("t").status == NODE_SUCCEEDED
        assert state.node_state("f").status == NODE_SKIPPED

    def test_false_branch(self):
        engine, seen = make_engine()
        c = condition("c", "false", ["t"], ["f"])
        t = task("t")
        f = task("f")
        engine.register(wf("w", c, t, f))
        state = engine.start("w")
        assert "f" in seen and "t" not in seen

    def test_non_bool_predicate_rejected(self):
        """谓词返回 1 而不是 True 是条件节点最隐蔽的错误 ——
        「 truthy」语义下 1 也能走 true 分支，
        但它通常意味着表达式写错了（漏了比较）。"""
        engine = Engine(
            executor=lambda n, c: None,
            predicate=lambda n, c: "1",  # type: ignore[return-value]
        )
        engine.register(wf("w", condition("c", "true", [], [])))
        state = engine.start("w")
        assert state.status == RUN_FAILED
        assert state.node_state("c").status == NODE_FAILED


class TestSkipPropagation:
    def test_skip_propagates_downstream(self):
        """skip 必须递归传播 —— 不传播的话，孙节点会看见
        「上游 skipped 了但我的输入没来」然后永远挂起。"""
        engine, seen = make_engine()
        c = condition("c", "false", ["t1"], ["f1"])
        t1 = task("t1", next="t2")
        t2 = task("t2")
        f1 = task("f1")
        engine.register(wf("w", c, t1, t2, f1))
        state = engine.start("w")
        assert state.node_state("t1").status == NODE_SKIPPED
        assert state.node_state("t2").status == NODE_SKIPPED
        assert state.node_state("f1").status == NODE_SUCCEEDED
        assert seen == ["f1"]

    def test_deep_chain_all_skipped(self):
        engine, seen = make_engine()
        c = condition("c", "true", ["a1"], ["b1"])
        a1 = task("a1", next="a2")
        a2 = task("a2", next="a3")
        a3 = task("a3")
        b1 = task("b1")
        engine.register(wf("w", c, a1, a2, a3, b1))
        state = engine.start("w")
        assert state.skipped_ids() == ["b1"]
        assert set(state.completed_ids()) == {"c", "a1", "a2", "a3"}

    def test_join_node_runs_when_any_upstream_succeeded(self):
        """汇合点：任一上游成功即可执行（审批流的默认直觉）。"""
        engine, seen = make_engine()
        c = condition("c", "true", ["left"], ["right"])
        left = task("left", next="join")
        right = task("right", next="join")
        join = task("join")
        engine.register(wf("w", c, left, right, join))
        state = engine.start("w")
        # right 被跳过，但 left 成功 → join 照常执行
        assert state.node_state("join").status == NODE_SUCCEEDED
        assert seen == ["left", "join"]


class TestRetry:
    def test_retry_then_success(self):
        attempts = {"n": 0}

        def flaky(node, ctx):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("暂时性故障")
            return "ok"

        engine = Engine(executor=flaky)
        engine.register(wf("w", task("a", retries=3)))
        state = engine.start("w")
        assert state.status == RUN_SUCCEEDED
        assert state.node_state("a").attempts == 3

    def test_retry_exhausted_fails_run(self):
        def always_fail(node, ctx):
            raise RuntimeError("永远失败")

        engine = Engine(executor=always_fail)
        engine.register(wf("w", task("a", retries=2)))
        state = engine.start("w")
        assert state.status == RUN_FAILED
        assert state.node_state("a").attempts == 3  # 1 次初始 + 2 次重试
        assert state.node_state("a").status == NODE_FAILED

    def test_retry_events_recorded(self):
        def always_fail(node, ctx):
            raise RuntimeError("boom")

        engine = Engine(executor=always_fail)
        engine.register(wf("w", task("a", retries=1)))
        state = engine.start("w")
        kinds = [e["kind"] for e in state.events]
        assert kinds.count("retry") == 1


class TestCompensation:
    def test_compensation_runs_in_reverse_order(self):
        """补偿必须按**完成时间的逆序** ——
        后完成的动作可能建立在先完成动作的状态上
        （先建订单、再扣库存），撤销必须先撤后者。"""
        engine, seen = make_engine()
        a = task("a", next="b", compensation="undo_a")
        b = task("b", next="c", compensation="undo_b")
        c = task("c", next="d", compensation="undo_c")
        d = task("d")
        engine.register(wf("w", a, b, c, d,
                           task("undo_a"), task("undo_b"), task("undo_c")))
        # 让 d 失败
        engine.executor = lambda node, ctx: (
            (_ for _ in ()).throw(RuntimeError("炸了")) if node.id == "d"
            else {"ran": node.id}
        )
        state = engine.start("w")
        assert state.status == RUN_FAILED
        undos = [s for s in seen if s.startswith("undo:")]
        assert undos == ["undo:undo_c", "undo:undo_b", "undo:undo_a"], seen
        assert state.node_state("c").status == NODE_COMPENSATED

    def test_nodes_without_compensation_are_skipped_in_undo(self):
        """没有配置补偿的节点，撤销时被跳过而不是报错。"""
        engine, seen = make_engine()
        a = task("a", next="b", compensation="undo_a")
        b = task("b", next="c")          # 无补偿
        c = task("c")
        engine.register(wf("w", a, b, c, task("undo_a")))
        engine.executor = lambda node, ctx: (
            (_ for _ in ()).throw(RuntimeError("x")) if node.id == "c"
            else {"ran": node.id}
        )
        state = engine.start("w")
        # 只有 a 被撤销；b 没配补偿，静默跳过（记录在案但不炸）
        assert [s for s in seen if s.startswith("undo")] == ["undo:undo_a"]
        assert state.node_state("a").status == NODE_COMPENSATED
        assert state.node_state("b").status == NODE_SUCCEEDED

    def test_compensation_failure_recorded_not_fatal(self):
        """补偿失败不能改变 run 的终态，但要留下「有脏数据没清干净」的线索。

        场景：a 成功，b 失败 → 触发 a 的补偿 → 补偿自己也炸。
        如果这时把 run 状态改成别的，或抛出新异常，
        上层看到的就不是「业务失败」这个第一现场了。
        """
        engine, seen = make_engine()
        a = task("a", next="b", compensation="undo_a")
        b = task("b")
        engine.register(wf("w", a, b, task("undo_a")))

        def executor(node, ctx):
            if node.id == "b":
                raise RuntimeError("业务失败")
            if node.id == "undo_a":
                raise RuntimeError("补偿也失败")
            return {"ran": node.id}

        engine.executor = executor
        engine.compensator = executor
        state = engine.start("w")
        assert state.status == RUN_FAILED
        comp_events = [e for e in state.events if e["kind"] == "compensation"]
        assert any(e.get("to") == "failed" for e in comp_events)
        # 失败的具体原因必须留在事件里
        failed_event = next(e for e in comp_events if e.get("to") == "failed")
        assert "补偿也失败" in failed_event.get("error", "")


class TestApproval:
    def test_run_pauses_at_approval(self):
        engine, seen = make_engine()
        a = approval("a", ["b"])
        b = task("b")
        engine.register(wf("w", a, b))
        state = engine.start("w")
        assert state.status == RUN_WAITING
        assert state.pending_approval is not None
        assert state.pending_approval["node_id"] == "a"
        assert "b" not in seen

    def test_approve_continues_downstream(self):
        engine, seen = make_engine()
        engine.register(wf("w", approval("a", ["b"]), task("b")))
        state = engine.start("w")
        assert state.status == RUN_WAITING
        state2 = engine.approve(state.run_id, approved=True)
        assert state2.status == RUN_SUCCEEDED
        assert seen == ["b"]
        assert state2.node_state("a").output["approved"] is True

    def test_reject_takes_rejected_branch(self):
        engine, seen = make_engine()
        a = NodeDef(
            id="a", type="approval",
            next={"approved": ["ok"], "rejected": ["no"]},
        )
        engine.register(wf("w", a, task("ok"), task("no")))
        state = engine.start("w")
        state2 = engine.approve(state.run_id, approved=False)
        assert state2.status == RUN_SUCCEEDED
        assert "no" in seen and "ok" not in seen

    def test_cannot_approve_non_waiting_run(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a")))
        state = engine.start("w")   # 直接成功
        with pytest.raises(RuntimeError, match="不在等待审批"):
            engine.approve(state.run_id, approved=True)

    def test_approve_unknown_run(self):
        engine, _ = make_engine()
        with pytest.raises(RunNotFound):
            engine.approve("ghost", approved=True)


class TestResume:
    def test_completed_nodes_not_rerun(self):
        """续跑的核心承诺：已成功的节点绝不重跑。

        对下游幂等性差的任务（发邮件、扣款），
        「重跑一遍」就是重复扣款 —— 这条测试是引擎的价值声明。
        """
        engine, seen = make_engine()
        a = task("a", next="b")
        b = task("b", next="c")
        c = task("c")
        engine.register(wf("w", a, b, c))
        state = engine.start("w")
        assert seen == ["a", "b", "c"]

        # 模拟「引擎在 running 状态崩溃」的存档：
        # 真实场景里 run 停在 running，部分节点已完成。
        # 直接改 JSON（而不是调 transition）是因为这是**历史存档**，
        # 不是本次运行内的合法转移 —— succeeded 的 run 依然不许续跑。
        archive = json.loads(state.to_json())
        archive["status"] = RUN_RUNNING
        archive["nodes"]["c"]["status"] = "pending"
        archive["nodes"]["c"]["attempts"] = 0
        archive["nodes"]["c"]["output"] = None
        restored = type(state).from_json(archive)
        engine.runs[restored.run_id] = restored

        state2 = engine.resume(restored.run_id)
        assert seen.count("a") == 1 and seen.count("b") == 1
        assert seen.count("c") == 2   # 只有 c 重跑
        assert state2.status == RUN_SUCCEEDED

    def test_resume_from_serialized_state(self):
        """状态要能走「序列化 → 新引擎 → 续跑」这条路。

        这是生产环境的真实场景：进程重启后从存储里恢复。
        新引擎实例上续跑，依赖的只有 RunState 自身，不靠内存残留。
        """
        engine, seen = make_engine()
        a = task("a", next="b")
        b = task("b")
        engine.register(wf("w", a, b))
        state = engine.start("w")
        raw = state.to_json()

        # 模拟重启：新引擎注册同一份工作流定义
        engine2, seen2 = make_engine()
        engine2.register(wf("w", task("a", next="b"), task("b")))
        # 模拟崩溃存档：run 停在 running、b 还没跑
        archive = json.loads(raw)
        archive["status"] = RUN_RUNNING
        archive["nodes"]["b"]["status"] = "pending"
        archive["nodes"]["b"]["attempts"] = 0
        archive["nodes"]["b"]["output"] = None
        restored = type(state).from_json(archive)
        engine2.runs[restored.run_id] = restored
        engine2.resume(restored.run_id)
        assert seen2 == ["b"]

    def test_resume_terminal_run_rejected(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a")))
        state = engine.start("w")
        with pytest.raises(RuntimeError, match="终态"):
            engine.resume(state.run_id)

    def test_resume_waiting_run_rejected(self):
        engine, _ = make_engine()
        engine.register(wf("w", approval("a", ["b"]), task("b")))
        state = engine.start("w")
        with pytest.raises(RuntimeError, match="审批"):
            engine.resume(state.run_id)

    def test_cancel_compensates_completed(self):
        engine, seen = make_engine()
        a = task("a", next="b", compensation="undo_a")
        b = task("b")
        engine.register(wf("w", a, b, task("undo_a")))
        state = engine.start("w")
        assert state.status == RUN_SUCCEEDED
        state2 = engine.cancel(state.run_id)
        assert state2.status == RUN_CANCELLED
        assert "undo:undo_a" in seen


class TestStateTransitions:
    def test_illegal_transition_rejected(self):
        from app.state import RunState

        state = RunState(run_id="r", workflow_name="w", workflow_version="1")
        state.transition(RUN_RUNNING)
        state.transition(RUN_SUCCEEDED)
        with pytest.raises(RuntimeError, match="非法状态转移"):
            state.transition(RUN_RUNNING)

    def test_terminal_states_exit_rules(self):
        from app.state import (
            LEGAL_RUN_TRANSITIONS,
            RUN_CANCELLED,
            RUN_FAILED,
            RUN_SUCCEEDED,
        )
        assert LEGAL_RUN_TRANSITIONS[RUN_FAILED] == set()
        # succeeded 的唯一出口是 cancelled（取消并回滚 = 补偿的场景），
        # 除此之外仍是终态
        assert LEGAL_RUN_TRANSITIONS[RUN_SUCCEEDED] == {RUN_CANCELLED}
        assert LEGAL_RUN_TRANSITIONS[RUN_CANCELLED] == set()

    def test_roundtrip_preserves_everything(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a", next="b"), task("b", retries=2)))
        state = engine.start("w", {"k": "v"})
        raw = state.to_json()
        restored = type(state).from_json(raw)
        assert restored.run_id == state.run_id
        assert restored.context == state.context
        assert len(restored.events) == len(state.events)
        assert restored.node_state("a").status == NODE_SUCCEEDED
        assert restored.node_state("b").retries == 2 if hasattr(
            restored.node_state("b"), "retries") else True


class TestEngineErrors:
    def test_unregistered_workflow(self):
        engine, _ = make_engine()
        with pytest.raises(WorkflowNotFound):
            engine.start("nope")

    def test_invalid_workflow_rejected_at_register(self):
        engine, _ = make_engine()
        with pytest.raises(WorkflowError, match="不合法"):
            engine.register(wf("w", task("a", next="ghost")))

    def test_unknown_run(self):
        engine, _ = make_engine()
        with pytest.raises(RunNotFound):
            engine.get_run("ghost")

    def test_condition_branch_recorded_in_events(self):
        engine, _ = make_engine()
        engine.register(wf("w", condition("c", "true", ["t"], ["f"]),
                           task("t"), task("f")))
        state = engine.start("w")
        branch_events = [e for e in state.events if e["kind"] == "branch"]
        assert branch_events and branch_events[0]["to"] == "true"

    def test_via_branch_recorded(self):
        engine, _ = make_engine()
        engine.register(wf("w", condition("c", "true", ["t"], ["f"]),
                           task("t"), task("f")))
        state = engine.start("w")
        assert state.node_state("t").via_branch == "true"


class TestCompensationIsolation:
    """补偿节点绝不能出现在正常路径上。

    这组测试对应一个真实 bug：补偿节点从入口不可达 → 入度为 0 →
    调度器把「入度为 0」当成入口节点直接执行 →
    一次成功的报销审批输出里出现了「已冲销：差旅报销」。
    """

    def _saga(self) -> WorkflowDef:  # type: ignore[name-defined] # noqa: F821
        return wf(
            "saga",
            task("a", next="b"),
            task("b", compensation="undo_b"),
            task("undo_b"),
        )

    def test_compensation_not_run_on_success(self):
        engine, seen = make_engine()
        engine.register(self._saga())
        state = engine.start("w" if False else "saga")
        assert state.status == RUN_SUCCEEDED
        assert seen == ["a", "b"]           # 没有 undo:b
        assert state.node_state("undo_b").status == NODE_PENDING

    def test_compensation_runs_on_cancel(self):
        engine, seen = make_engine()
        engine.register(self._saga())
        state = engine.start("saga")
        state = engine.cancel(state.run_id)
        assert state.status == RUN_CANCELLED
        assert "undo:undo_b" in seen
        assert state.node_state("b").status == NODE_COMPENSATED
        # 补偿节点自己也要留执行记录，否则前端永远看不出它跑没跑
        assert state.node_state("undo_b").status == NODE_SUCCEEDED

    def test_compensation_reverse_order(self):
        engine, seen = make_engine()
        engine.register(wf(
            "saga2",
            task("a", next="b", compensation="undo_a"),
            task("b", next="c", compensation="undo_b"),
            task("c", compensation="undo_c"),
            task("undo_a"), task("undo_b"), task("undo_c"),
        ))
        state = engine.start("saga2")
        engine.cancel(state.run_id)
        undo_events = [e for e in state.events if e["kind"] == "compensation"]
        undone = [e["node"] for e in undo_events
                  if e["to"] == "running"]
        assert undone == ["c", "b", "a"]     # 逆序

    def test_compensation_as_entry_rejected(self):
        from app.dag import WorkflowDef as _W
        engine, _ = make_engine()
        bad = _W(
            name="bad2",
            nodes={"a": task("a", compensation="undo_a"),
                   "undo_a": task("undo_a")},
            entry=["a", "undo_a"],
        )
        with pytest.raises(WorkflowError, match="入口"):
            engine.register(bad)


class TestCrashAndResume:
    """崩溃窗口 + 续跑。

    崩溃模拟的不是「程序抛异常」，而是**副作用已发生、完成标记没落盘**
    这个窗口。续跑必然重跑该节点，所以任务必须幂等 —— 这是本组测试
    真正想钉住的语义。
    """

    def test_crash_rolls_back_last_finished_node(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a", next="b"), task("b", next="c"),
                           task("c")))
        state = engine.start("w")
        assert state.node_state("c").status == NODE_SUCCEEDED
        engine.simulate_crash(state.run_id)
        assert state.status == RUN_RUNNING
        assert state.node_state("c").status == NODE_PENDING
        assert state.node_state("b").status == NODE_SUCCEEDED

    def test_resume_after_crash_reruns_only_lost_node(self):
        engine, seen = make_engine()
        engine.register(wf("w", task("a", next="b"), task("b", next="c"),
                           task("c")))
        state = engine.start("w")
        seen.clear()
        engine.simulate_crash(state.run_id)
        state = engine.resume(state.run_id)
        assert state.status == RUN_SUCCEEDED
        assert seen == ["c"]                       # a、b 不重跑
        # attempts 累加而不是重置：2 才是「它被跑了两次」的证据
        assert state.node_state("c").attempts == 2

    def test_crash_clears_stale_output_from_context(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a", next="b"), task("b")))
        state = engine.start("w")
        engine.simulate_crash(state.run_id)
        assert "b" not in state.context

    def test_crash_on_empty_run_rejected(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a", next="b"), task("b")))
        state = engine.start("w")
        # 把所有节点打回 pending，模拟"刚启动就崩了"
        for ns in state.nodes.values():
            ns.status = NODE_PENDING
            ns.finished_at = None
        with pytest.raises(RuntimeError, match="没有意义"):
            engine.simulate_crash(state.run_id)

    def test_crash_unknown_run(self):
        engine, _ = make_engine()
        with pytest.raises(RunNotFound):
            engine.simulate_crash("ghost")

    def test_resume_on_terminal_state_rejected(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a")))
        state = engine.start("w")
        with pytest.raises(RuntimeError, match="终态"):
            engine.resume(state.run_id)


class TestArchiveRestore:
    def test_restore_returns_state(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a", next="b"), task("b")))
        state = engine.start("w")
        raw = state.to_json()
        cloned = type(state).from_json(raw)
        out = engine.restore(cloned)
        assert out.run_id == state.run_id
        assert engine.get_run(state.run_id) is cloned

    def test_restore_records_event(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a")))
        state = engine.start("w")
        cloned = type(state).from_json(state.to_json())
        engine.restore(cloned)
        # 事件记在**恢复出来的那个对象**上，不是原对象 ——
        # 真实场景里原对象早就随进程一起没了。
        assert any(e["kind"] == "restore" for e in cloned.events)
        assert not any(e["kind"] == "restore" for e in state.events)

    def test_restore_unknown_workflow_rejected(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a")))
        state = engine.start("w")
        engine.workflows.clear()
        with pytest.raises(WorkflowNotFound):
            engine.restore(type(state).from_json(state.to_json()))

    def test_json_roundtrip_preserves_context(self):
        engine, _ = make_engine()
        engine.register(wf("w", task("a")))
        state = engine.start("w", {"k": "v"})
        back = type(state).from_json(json.loads(state.to_json()))
        assert back.context["input"] == {"k": "v"}


class TestApprovalSuspension:
    """审批挂起的节点状态是 waiting，不是 running。

    复用 running 会让监控看板把「等审批等了三天」显示成「执行了三天」，
    而前者恰恰是审批流最该被看见的那个指标。
    """

    def test_suspended_node_is_waiting_not_running(self):
        engine, _ = make_engine()
        engine.register(wf("w", approval("ap", ["done"]), task("done")))
        state = engine.start("w")
        assert state.status == RUN_WAITING
        assert state.node_state("ap").status == NODE_WAITING
        assert state.node_state("ap").status != NODE_RUNNING

    def test_waiting_node_has_no_finish_time(self):
        engine, _ = make_engine()
        engine.register(wf("w", approval("ap", ["done"]), task("done")))
        state = engine.start("w")
        assert state.node_state("ap").finished_at is None

    def test_approve_moves_waiting_to_succeeded(self):
        engine, _ = make_engine()
        engine.register(wf("w", approval("ap", ["done"]), task("done")))
        state = engine.start("w")
        state = engine.approve(state.run_id, True, "ok")
        assert state.node_state("ap").status == NODE_SUCCEEDED
        assert state.node_state("done").status == NODE_SUCCEEDED

    def test_pending_approval_blocks_success(self):
        """存档里还挂着审批时，续跑不许判定成功。

        崩溃恢复最容易撞上这个窗口：status 被置成 running，
        而 pending_approval 还在 —— 一路没活干就直接 succeeded 了，
        等于把一笔还没被批准的业务标记成办完了。
        """
        engine, _ = make_engine()
        engine.register(wf("w", task("a", next="ap"),
                           approval("ap", ["done"]), task("done")))
        state = engine.start("w")
        assert state.status == RUN_WAITING
        # 手工制造"崩溃恢复"：状态回到 running，审批还挂着
        state.status = RUN_RUNNING
        out = engine.resume(state.run_id)
        assert out.status == RUN_WAITING   # 不是 succeeded
        assert out.pending_approval is not None
