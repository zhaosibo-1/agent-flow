"""执行引擎：调度、条件分支、跳过传播、重试、补偿、审批、断点续跑。

这个引擎刻意保持**同步、单线程**。并发执行是工作流系统
最常见的过度设计： DAG 的价值在于把依赖关系写清楚，
而依赖关系的价值在于让执行顺序确定 ——
一旦真并发，事件日志的顺序就不再确定，排障成本指数级上升。
需要吞吐量的是数据管道，不是审批流。

设计上最重要的一个决定：**跳过（skipped）是显式状态，不是"没执行"**。

条件分支没选中的下游，必须被标记为 skipped 并**递归传播**给它的下游。
如果不传播，下游节点会看见「上游 skipped 了，但我的输入还没来」，
然后要么永远挂起、要么误判失败。传播规则是：

* 下游的**所有**上游里有任意一个 succeeded → 正常执行；
* 所有上游都 skipped → 自己也 skipped；
* 混合（一部分 succeeded 一部分 skipped）→ 取决于 join 策略，
  本实现默认"任一成功即可"，这是审批流里最符合直觉的语义。
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable

from .dag import NODE_TYPES, NodeDef, WorkflowDef, WorkflowError
from .state import (
    NODE_FAILED,
    NODE_PENDING,
    NODE_RUNNING,
    NODE_SKIPPED,
    NODE_SUCCEEDED,
    NODE_COMPENSATED,
    NODE_WAITING,
    RUN_CANCELLED,
    RUN_FAILED,
    RUN_PENDING,
    RUN_RUNNING,
    RUN_SUCCEEDED,
    RUN_WAITING,
    RunState,
)


class ApprovalRequired(Exception):
    """审批节点触发挂起。引擎内部用它做控制流，不对外抛。"""


class WorkflowNotFound(KeyError):
    pass


class RunNotFound(KeyError):
    pass


#: 任务执行器：``(node, ctx) -> 输出``。ctx 里带上游输出。
Executor = Callable[[NodeDef, dict[str, Any]], Any]

#: 谓词：``(node, ctx) -> 分支名``
Predicate = Callable[[NodeDef, dict[str, Any]], str]


def default_executor(node: NodeDef, ctx: dict[str, Any]) -> Any:
    """内置执行器：算术/字符串表达式。

    刻意**不用 eval**。表达式语言只支持「取上下文 + 字面量 + 四则运算」，
    用 ast 白名单实现。eval 的能力边界取决于调用方传进来的字符串，
    而工作流的节点参数几乎总是来自外部输入（API 请求体、配置文件）。
    """
    from .safe_expr import safe_eval

    expr = node.params.get("expr")
    if expr is None:
        # 无表达式的 task 节点：把上游原样透传（占位/透传节点）
        return {"node": node.id, "upstream": ctx}
    return safe_eval(str(expr), ctx)


def default_predicate(node: NodeDef, ctx: dict[str, Any]) -> str:
    from .safe_expr import safe_eval

    result = safe_eval(str(node.params.get("expr", "true")), ctx)
    if not isinstance(result, bool):
        raise RuntimeError(
            f"条件节点 {node.id!r} 的谓词必须返回布尔值，得到 {type(result).__name__}"
        )
    return "true" if result else "false"


class Engine:
    """同步 DAG 执行器。"""

    def __init__(
        self,
        executor: Executor | None = None,
        predicate: Predicate | None = None,
        #: 补偿执行器：``(node, ctx) -> None``。
        #: saga 模式的关键：**补偿按完成的逆序执行**，
        #: 因为后完成的动作可能依赖先完成动作留下的状态。
        compensator: Executor | None = None,
    ) -> None:
        self.executor = executor or default_executor
        self.predicate = predicate or default_predicate
        self.compensator = compensator or self.executor
        #: workflow_name → WorkflowDef
        self.workflows: dict[str, WorkflowDef] = {}
        #: run_id → RunState
        self.runs: dict[str, RunState] = {}

    # -- 注册与查找 -------------------------------------------------------

    def register(self, workflow: WorkflowDef) -> list[str]:
        """注册前先做完整校验，返回问题列表（为空表示成功）。"""
        problems = workflow.validate()
        if problems:
            raise WorkflowError(
                "工作流定义不合法：" + "；".join(problems)
            )
        self.workflows[workflow.name] = workflow
        return []

    def get_workflow(self, name: str, version: str | None = None) -> WorkflowDef:
        wf = self.workflows.get(name)
        if wf is None or (version is not None and wf.version != version):
            raise WorkflowNotFound(f"工作流 {name}@{version or '*'} 未注册")
        return wf

    # -- 启动 -------------------------------------------------------------

    def start(self, workflow_name: str, payload: dict[str, Any] | None = None,
              run_id: str | None = None) -> RunState:
        wf = self.get_workflow(workflow_name)
        state = RunState(
            run_id=run_id or uuid.uuid4().hex[:12],
            workflow_name=wf.name,
            workflow_version=wf.version,
        )
        state.transition(RUN_RUNNING)
        state.context = {"input": payload or {}}
        self.runs[state.run_id] = state
        self._drive(wf, state)
        return state

    # -- 断点续跑 ----------------------------------------------------------

    def resume(self, run_id: str) -> RunState:
        """从持久化状态继续执行。

        已 succeeded 的节点**不会重跑** —— 这是续跑的全部意义。
        判断依据是节点级状态，不是"从头再跑一遍但跳过某些"：
        只要状态序列化是完整的，跳过的粒度就自动正确。
        """
        state = self.runs.get(run_id)
        if state is None:
            raise RunNotFound(f"运行 {run_id!r} 不存在")
        wf = self.get_workflow(state.workflow_name, state.workflow_version)

        if state.status in (RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELLED):
            raise RuntimeError(f"运行 {run_id} 已是终态 {state.status}，不能续跑")

        if state.status == RUN_WAITING:
            raise RuntimeError(
                f"运行 {run_id} 正在等审批（节点 {state.pending_approval}），"
                "先调用 approve / reject"
            )

        # 存档通常停在 running（引擎崩溃时的状态），此时不需要转移；
        # pending 才需要正式进入 running。
        # 曾经这里无条件 transition，running → running 直接炸 ——
        # 恰好在最常见的续跑场景（崩溃恢复）上。
        if state.status == RUN_PENDING:
            state.transition(RUN_RUNNING)
        state.record("resume", "archived", state.status)
        self._drive(wf, state)
        return state

    # -- 审批 --------------------------------------------------------------

    def approve(self, run_id: str, approved: bool,
                comment: str = "") -> RunState:
        state = self.runs.get(run_id)
        if state is None:
            raise RunNotFound(f"运行 {run_id!r} 不存在")
        wf = self.get_workflow(state.workflow_name, state.workflow_version)

        if state.status != RUN_WAITING:
            raise RuntimeError(
                f"运行 {run_id} 不在等待审批状态（当前 {state.status}）"
            )
        pending = state.pending_approval or {}
        node_id = str(pending.get("node_id", ""))
        branch = "approved" if approved else "rejected"

        state.record("approval", "pending", branch, node=node_id, comment=comment)
        state.pending_approval = None
        ns = state.node_state(node_id)
        ns.status = NODE_SUCCEEDED
        ns.output = {"approved": approved, "comment": comment}
        ns.finished_at = time.time()
        state.context[node_id] = ns.output

        state.transition(RUN_RUNNING)
        self._drive(wf, state)
        return state

    def restore(self, state: RunState) -> RunState:
        """把一个从存档里读回来的运行状态放回引擎。

        只做两件事：校验它引用的工作流还在，然后登记。
        **不自动续跑** —— 恢复和继续是两件事，分开才可以在
        「刚启动、还没准备好接客」的时候只恢复不执行。
        """
        self.get_workflow(state.workflow_name, state.workflow_version)
        self.runs[state.run_id] = state
        state.record("restore", "archived", state.status)
        return state

    def simulate_crash(self, run_id: str) -> RunState:
        """模拟进程崩溃：把**最后一个已完成的节点**退回 pending。

        这不是为了演示编造的场景，它对应的是分布式系统里最经典的窗口：

            节点执行完了 → 副作用已经发生 →
            「标记完成」还没落盘 → 进程没了

        存档里的这个节点是 pending，但它的副作用已经出去了。
        续跑只能**重跑**它 —— 这就是为什么工作流里的任务必须幂等。
        不幂等的任务（发邮件、扣款）在这个窗口里必然重复一次，
        靠引擎是救不回来的，只能靠任务侧的去重键。

        顺带说明为什么崩溃不调用 ``transition``：崩溃按定义就不遵守
        状态机 —— 状态机的作用是拦住"程序写错了"，拦不住"进程没了"。
        """
        state = self.runs.get(run_id)
        if state is None:
            raise RunNotFound(f"运行 {run_id!r} 不存在")

        finished = [ns for ns in state.nodes.values()
                    if ns.status == NODE_SUCCEEDED and ns.finished_at is not None]
        if not finished:
            raise RuntimeError(
                f"运行 {run_id} 还没有任何节点完成，模拟崩溃没有意义"
            )
        victim = max(finished, key=lambda ns: ns.finished_at or 0.0)
        victim.status = NODE_PENDING
        victim.finished_at = None
        victim.output = None
        victim.error = None
        # attempts 不清零 —— 续跑后它会变成 2，这才是证据
        # 上下文里的输出也必须一起撤销，否则下游会读到"已经丢失"的值
        state.context.pop(victim.node_id, None)
        state.status = RUN_RUNNING
        state.record("crash", NODE_SUCCEEDED, "lost", node=victim.node_id)
        return state

    def cancel(self, run_id: str) -> RunState:
        state = self.runs.get(run_id)
        if state is None:
            raise RunNotFound(f"运行 {run_id!r} 不存在")
        state.transition(RUN_CANCELLED)
        # 已成功节点要不要补偿？要 —— 取消不是遗忘，是「这笔业务别做了」。
        self._run_compensations(self.get_workflow(
            state.workflow_name, state.workflow_version), state)
        return state

    # -- 主循环 -------------------------------------------------------------

    def _drive(self, wf: WorkflowDef, state: RunState) -> None:
        """调度循环：反复找可执行节点，直到没有可执行的为止。

        「可执行」= pending 且所有上游都已就绪（succeeded 或 skipped）。
        每执行完一批重新扫描，因为条件分支会改变下游的可达性。
        """
        order = wf.topological_order()
        # 补偿节点**永远不进主调度循环**。
        # 它从入口不可达（校验规则强制），于是入度为 0，
        # 而入度为 0 在主循环里的含义是"这是入口节点，直接执行" ——
        # 于是补偿动作在**成功路径**上也被跑了一遍。
        # 实测：一次成功的报销审批，输出里出现了「已冲销：差旅报销」。
        # 补偿只能由 _run_compensations 在失败/取消路径上调用。
        compensation_ids = {n.compensation for n in wf.nodes.values()
                            if n.compensation is not None}
        entry_ids = set(wf.entry)

        while True:
            progressed = False
            waiting = False

            for node_id in order:
                if node_id in compensation_ids:
                    continue
                ns = state.node_state(node_id)
                if ns.status != NODE_PENDING:
                    continue

                node = wf.node(node_id)
                if state.status == RUN_WAITING and state.pending_approval:
                    # 有挂起的审批：不再调度新节点（保持挂起语义）
                    waiting = True
                    continue

                effective = self._effective_upstream(wf, node_id, state)

                if NODE_RUNNING in effective:
                    waiting = True
                    continue
                if NODE_FAILED in effective:
                    continue  # 失败路径由补偿逻辑处理
                if not effective:
                    # 入度为 0：必须是入口节点才行。
                    # 校验保证了「非入口且入度为 0」只剩补偿节点一种情况，
                    # 但这里仍然显式再挡一次 —— 调度器不该依赖
                    # "上游校验肯定做对了"这种假设，尤其是当违反它的后果
                    # 是"业务动作被无故执行一遍"的时候。
                    if node_id not in entry_ids:
                        continue
                    self._run_node(wf, state, node)
                    progressed = True
                    if state.status in (RUN_FAILED, RUN_WAITING):
                        # 与下方成功分支同样的原因：入口失败后
                        # 不能继续把其他孤立节点当成入口调度起来。
                        if state.status == RUN_FAILED:
                            self._run_compensations(wf, state)
                        return
                    continue
                if all(s == NODE_SKIPPED for s in effective):
                    self._skip_node(wf, state, node)
                    progressed = True
                    continue
                if NODE_SUCCEEDED in effective:
                    # 记录"经由哪个分支到达"：条件/审批节点的入边分支名。
                    # 没有它，事件日志回答不了「这条路径是怎么走进来的」。
                    via = self._arrived_via(wf, node_id, state)
                    self._run_node(wf, state, node, via_branch=via)
                    progressed = True
                    if state.status in (RUN_FAILED, RUN_WAITING):
                        # 失败/挂起后**立即停止调度**。
                        # 曾经这里继续跑 for 循环的下一个节点 ——
                        # 一个 pending 的补偿节点会被当成"无上游的入口"
                        # 执行起来，业务动作在失败之后照样发生。
                        if state.status == RUN_FAILED:
                            self._run_compensations(wf, state)
                        return
                    continue
                # 上游还在 pending（比如另一条更长的路径没跑完）
                waiting = True

            if state.status == RUN_WAITING:
                return
            if state.status == RUN_FAILED:
                self._run_compensations(wf, state)
                return
            if not progressed and not waiting:
                break
            if not progressed and waiting:
                break

        if state.status == RUN_RUNNING:
            # 还有挂起的审批没处理，就不许判定成功。
            # 崩溃恢复最容易撞上这个窗口：存档里 pending_approval 还在，
            # 而 status 被置成了 running，于是「一路没活干」就直接 succeeded
            # —— 一次还没被批准的业务被标记成办完了。
            if state.pending_approval is not None:
                state.transition(RUN_WAITING)
            else:
                state.transition(RUN_SUCCEEDED)

    def _upstream_states(self, wf: WorkflowDef, node_id: str,
                         state: RunState) -> list[Any]:
        """node_id 的所有直接上游的节点状态（去重）。"""
        sources: list[str] = []
        for src, _b, dst in wf.all_edges():
            if dst == node_id and src not in sources:
                sources.append(src)
        return [state.node_state(s) for s in sources]

    def _arrived_via(self, wf: WorkflowDef, node_id: str,
                     state: RunState) -> str | None:
        """找到把它调度起来的那条入边的分支名。"""
        for src, branch, dst in wf.all_edges():
            if dst != node_id:
                continue
            src_state = state.node_state(src)
            if src_state.status != NODE_SUCCEEDED:
                continue
            src_node = wf.nodes[src]
            if src_node.type == "condition":
                selected = (state.context.get(src) or {}).get("branch")
                if selected == branch:
                    return branch
            elif src_node.type == "approval":
                output = state.context.get(src) or {}
                selected = "approved" if output.get("approved") else "rejected"
                if selected == branch:
                    return branch
        return None

    def _effective_upstream(self, wf: WorkflowDef, node_id: str,
                            state: RunState) -> list[str]:
        """节点所有入边的**有效**上游状态。

        关键规则：**有分支选择的节点**（condition、approval）的
        未选中分支视为 skipped。

        没有这条规则会发生什么：condition 求值完是 succeeded，
        它的两个分支的「上游都 succeeded」→ 两个分支**都会执行**。
        条件分支就成了摆设 —— 这正是本引擎第一版的真实 bug，
        由 ``test_true_branch_runs_false_branch_skips`` 抓出来的。
        """
        statuses: list[str] = []
        for src, branch, dst in wf.all_edges():
            if dst != node_id:
                continue
            src_state = state.node_state(src)
            if src_state.status == NODE_SUCCEEDED:
                src_node = wf.nodes[src]
                selected: str | None = None
                if src_node.type == "condition":
                    selected = (state.context.get(src) or {}).get("branch")
                elif src_node.type == "approval":
                    output = state.context.get(src) or {}
                    selected = ("approved" if output.get("approved")
                                else "rejected")
                if selected is not None and branch != selected:
                    statuses.append(NODE_SKIPPED)
                    continue
            statuses.append(src_state.status)
        return statuses

    # -- 单节点执行 ---------------------------------------------------------

    def _run_node(self, wf: WorkflowDef, state: RunState,
                  node: NodeDef, via_branch: str | None = None) -> None:
        ns = state.node_state(node.id)

        # 重试循环。attempts 从 1 开始计数 ——
        # "试了 0 次"这种日志没有任何信息量。
        max_attempts = node.retries + 1
        for attempt in range(1, max_attempts + 1):
            # 累加而不是赋值：崩溃续跑时节点会被重新调度，
            # 赋成 attempt 会把"这是第 2 次跑它"抹成 1，
            # 而这恰恰是判断「有没有重复执行副作用」的唯一线索。
            ns.attempts += 1
            ns.status = NODE_RUNNING
            ns.started_at = time.time()
            ns.via_branch = via_branch
            state.record("node", NODE_PENDING, NODE_RUNNING,
                         node=node.id, attempt=attempt)
            try:
                output = self._invoke(node, state)
                ns.status = NODE_SUCCEEDED
                ns.finished_at = time.time()
                ns.output = output
                state.context[node.id] = output
                state.record("node", NODE_RUNNING, NODE_SUCCEEDED, node=node.id)
                return
            except ApprovalRequired:
                # 审批节点：挂起。两件事都不能做：
                # * 不能改回 pending —— 续跑时它会再挂起一次，死循环；
                # * 不能留在 running —— 它会让监控把「等审批等了三天」
                #   显示成「执行了三天」，而前者正是审批流最该被看见的指标。
                ns.status = NODE_WAITING
                ns.finished_at = None
                state.pending_approval = {
                    "node_id": node.id,
                    "token": uuid.uuid4().hex[:8],
                    "requested_at": round(time.time(), 3),
                }
                state.transition(RUN_WAITING)
                state.record("approval", "requested", "waiting", node=node.id)
                return
            except Exception as exc:  # noqa: BLE001
                ns.error = f"{type(exc).__name__}: {exc}"
                state.record("node", NODE_RUNNING, "error",
                             node=node.id, attempt=attempt, error=ns.error)
                if attempt < max_attempts:
                    state.record("retry", attempt - 1, attempt, node=node.id)
                    continue

        ns.status = NODE_FAILED
        ns.finished_at = time.time()
        state.record("node", NODE_RUNNING, NODE_FAILED, node=node.id,
                     error=ns.error)
        state.transition(RUN_FAILED)

    def _invoke(self, node: NodeDef, state: RunState) -> Any:
        """按类型分发执行。"""
        if node.type == "task":
            return self.executor(node, dict(state.context))
        if node.type == "condition":
            branch = self.predicate(node, dict(state.context))
            if branch not in ("true", "false"):
                # 谓词返回别的值（比如 truthy 的 "1"）通常意味着表达式写错了
                # —— 漏了比较。静默按 "true" 处理会掩盖这个错误。
                raise RuntimeError(
                    f"条件节点 {node.id!r} 的谓词必须返回 true/false，"
                    f"得到 {branch!r}"
                )
            state.record("branch", "evaluated", branch, node=node.id)
            # 条件节点本身"执行成功"，它的产出是走的分支名。
            state.context[node.id] = {"branch": branch}
            return {"branch": branch}
        if node.type == "approval":
            # 走到这说明是第一次执行 —— 抛出挂起信号
            raise ApprovalRequired(node.id)
        if node.type == "parallel":
            # 本实现按拓扑序执行；这个节点类型的意义是把"这些节点是并列的"
            # 写进结构，前端按它画泳道。它自己透传上游。
            return {"node": node.id, "parallel_group": True}
        raise WorkflowError(
            f"节点类型 {node.type!r} 不可执行；合法类型：{'、'.join(NODE_TYPES)}"
        )

    # -- 跳过与传播 ----------------------------------------------------------

    def _skip_node(self, wf: WorkflowDef, state: RunState, node: NodeDef) -> None:
        ns = state.node_state(node.id)
        ns.status = NODE_SKIPPED
        ns.finished_at = time.time()
        state.record("node", NODE_PENDING, NODE_SKIPPED, node=node.id)

    def propagate_skips(self, wf: WorkflowDef, state: RunState) -> None:
        """把 skip 沿 DAG 传播。

        拆成独立方法而不是塞进主循环：传播只在**条件分支生效之后**才有意义，
        独立出来才可以在单测里构造「只测传播」的用例。
        """
        order = wf.topological_order()
        changed = True
        while changed:
            changed = False
            for node_id in order:
                ns = state.node_state(node_id)
                if ns.status != NODE_PENDING:
                    continue
                effective = self._effective_upstream(wf, node_id, state)
                if effective and all(s == NODE_SKIPPED for s in effective):
                    ns.status = NODE_SKIPPED
                    state.record("node", NODE_PENDING, NODE_SKIPPED, node=node_id)
                    changed = True

    # -- 补偿（saga） ---------------------------------------------------------

    def _run_compensations(self, wf: WorkflowDef, state: RunState) -> None:
        """按完成时间**逆序**执行补偿。

        为什么逆序：后完成的动作可能建立在先完成动作的状态上
        （先创建订单、再扣库存），撤销必须先撤销后者。
        顺序补偿会试图撤销一个已经不存在的状态。
        """
        done = [state.nodes[nid] for nid in state.completed_ids()]
        done.sort(key=lambda ns: ns.finished_at or 0.0, reverse=True)

        for ns in done:
            node = wf.nodes.get(ns.node_id)
            if node is None or node.compensation is None:
                continue
            comp_node = wf.node(node.compensation)
            state.record("compensation", "pending", "running", node=ns.node_id)
            try:
                output = self.compensator(comp_node, dict(state.context))
                ns.status = NODE_COMPENSATED
                # 补偿节点自己也留一条执行记录，否则前端图上
                # 「冲销入账」永远是灰的，看不出它跑没跑。
                comp_ns = state.node_state(comp_node.id)
                comp_ns.attempts += 1
                comp_ns.status = NODE_SUCCEEDED
                comp_ns.output = output
                comp_ns.finished_at = time.time()
                state.record("compensation", "running", "done", node=ns.node_id)
            except Exception as exc:  # noqa: BLE001
                # 补偿失败不能让整次运行从 failed 变成别的 ——
                # 但必须记下来，这是"有脏数据没清干净"的唯一线索。
                state.record("compensation", "running", "failed",
                             node=ns.node_id,
                             error=f"{type(exc).__name__}: {exc}")

    # -- 查询 ----------------------------------------------------------------

    def get_run(self, run_id: str) -> RunState:
        run = self.runs.get(run_id)
        if run is None:
            raise RunNotFound(f"运行 {run_id!r} 不存在")
        return run

    def list_runs(self) -> list[dict[str, Any]]:
        return [r.summary() for r in self.runs.values()]
