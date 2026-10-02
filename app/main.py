"""HTTP 层。唯一允许第三方依赖的模块（CI 门禁强制）。"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import __version__
from .dag import NODE_TYPES, NodeDef, WorkflowDef, WorkflowError
from .engine import Engine, RunNotFound, WorkflowNotFound
from .state import NODE_STATUSES, RUN_STATUSES

STARTED_AT = time.time()


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------


class NodePayload(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    type: str = "task"
    next: dict[str, list[str]] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    compensation: str | None = None
    retries: int = Field(default=0, ge=0, le=10)
    label: str = ""


class WorkflowPayload(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    version: str = "1"
    entry: list[str] = Field(default_factory=list)
    nodes: list[NodePayload]

    def to_def(self) -> WorkflowDef:
        return WorkflowDef(
            name=self.name,
            version=self.version,
            entry=self.entry,
            nodes={
                n.id: NodeDef(
                    id=n.id, type=n.type, next=n.next, params=n.params,
                    compensation=n.compensation, retries=n.retries, label=n.label,
                )
                for n in self.nodes
            },
        )


class StartPayload(BaseModel):
    workflow: str
    payload: dict[str, Any] = Field(default_factory=dict)


class ApprovePayload(BaseModel):
    approved: bool
    comment: str = ""


class ArchivePayload(BaseModel):
    json: str = Field(min_length=2, description="RunState.to_json() 的产出")


# ---------------------------------------------------------------------------
# 演示工作流：让「打开页面就能看到东西」成立
# ---------------------------------------------------------------------------

#: 节点 id 用 ASCII、label 用中文。原因很实际：表达式里会写
#: ``submit.amount`` 这种「上游输出.字段」的引用，而中文标识符虽然
#: 语法上合法，一旦落到日志、shell、URL 里就会变成编码问题。
#: 展示层归展示层，引用层归引用层。
_DEMO = WorkflowPayload(
    name="报销审批",
    version="1",
    entry=["submit"],
    nodes=[
        NodePayload(
            id="submit", type="task", label="提交报销单",
            params={"expr": "{'amount': input.amount, 'title': input.title}"},
            next={"out": ["check"]},
        ),
        NodePayload(
            id="check", type="condition", label="金额 > 1000?",
            params={"expr": "input.amount > 1000"},
            next={"true": ["manager"], "false": ["auto"]},
        ),
        NodePayload(
            id="manager", type="approval", label="主管审批",
            next={"approved": ["finance"], "rejected": ["reject"]},
        ),
        NodePayload(
            id="finance", type="task", label="财务复核",
            # 引用上游输出：submit 的输出是 {'amount': ...}
            params={"expr": "submit.amount * 0.95"},
            compensation="rollback",
        ),
        NodePayload(
            id="auto", type="task", label="小额自动通过",
            params={"expr": "'自动通过：' + (input.title or '未命名')"},
        ),
        NodePayload(
            id="reject", type="task", label="驳回通知",
            params={"expr": "'已驳回：' + (input.title or '未命名')"},
        ),
        # 补偿节点：只由失败/取消路径调度，不出现在主流程里
        # （校验规则强制这一点，否则注册会被拒）
        NodePayload(
            id="rollback", type="task", label="冲销入账",
            params={"expr": "'已冲销：' + (input.title or '未命名')"},
        ),
    ],
)


def _seed_demo(eng: Engine) -> None:
    """注册内置工作流。**故意不吞异常**。

    早先这里写的是 ``except WorkflowError: pass``，理由是「热重载时
    可能已注册过」。结果某次改坏了演示定义（漏了一条边），校验失败被
    静默吞掉，页面上工作流数一直是 0，却没有任何报错 ——
    「静默失败」比「启动即崩」难查得多，因为它没有现场。
    现在：注册失败就是启动失败，直接抛。
    """
    eng.register(_DEMO.to_def())


engine = Engine()
WEB_DIR = "web"


@asynccontextmanager
async def lifespan(_: FastAPI):
    _seed_demo(engine)
    yield


app = FastAPI(
    title="agent-flow",
    version=__version__,
    description="工作流 DAG 编排引擎：条件分支、补偿、人工审批、断点续跑",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# 序列化：给前端的形状
# ---------------------------------------------------------------------------


def _run_to_wire(state: Any) -> dict[str, Any]:
    """运行状态的线上表示。

    刻意**不**直接把 ``NodeState.__dict__`` 丢出去：``__dict__`` 里
    没有 ``duration_ms``（它是 property），前端拿到的是 undefined，
    表格里会渲染出 "undefined ms"。线上表示必须显式构造，
    数据类的内部字段布局不是 API 契约。
    """
    data = state._to_dict()
    data["nodes"] = {nid: ns.to_wire() for nid, ns in state.nodes.items()}
    data["node_counts"] = _count_statuses(state)
    return data


def _count_statuses(state: Any) -> dict[str, int]:
    counts = {s: 0 for s in NODE_STATUSES}
    for ns in state.nodes.values():
        counts[ns.status] = counts.get(ns.status, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# 元信息
# ---------------------------------------------------------------------------


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "version": __version__,
        "workflows": len(engine.workflows),
        "runs": len(engine.runs),
        "uptime_seconds": round(time.time() - STARTED_AT, 1),
    }


@app.get("/api/options")
def options() -> dict[str, Any]:
    return {
        "node_types": list(NODE_TYPES),
        "node_statuses": list(NODE_STATUSES),
        "run_states": list(RUN_STATUSES),
        "expression_language": (
            "白名单 AST：字面量 / 名字 / 四则与比较 / 与或非（短路）/ 三元 / "
            "dict 一层取属性。函数调用、下标、导入一律拒绝。"
        ),
    }


# ---------------------------------------------------------------------------
# 工作流定义
# ---------------------------------------------------------------------------


@app.post("/api/workflow")
def register_workflow(body: WorkflowPayload) -> dict[str, Any]:
    try:
        wf = body.to_def()
        engine.register(wf)
    except WorkflowError as exc:
        # 定义不合法是客户端错误 → 422，并把**全部**问题一次性带回。
        # 只报第一个错，用户要改一轮才能见到下一个。
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"registered": wf.name, "version": wf.version,
            "definition": wf.describe()}


@app.get("/api/workflow")
def list_workflows() -> dict[str, Any]:
    return {
        "items": [wf.describe() for wf in engine.workflows.values()],
        "count": len(engine.workflows),
    }


@app.get("/api/workflow/{name}")
def get_workflow(name: str) -> dict[str, Any]:
    try:
        wf = engine.get_workflow(name)
    except WorkflowNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return wf.describe()


# ---------------------------------------------------------------------------
# 运行
# ---------------------------------------------------------------------------


@app.post("/api/run")
def start_run(body: StartPayload) -> dict[str, Any]:
    try:
        state = engine.start(body.workflow, body.payload)
    except WorkflowNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except WorkflowError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _run_to_wire(state)


@app.get("/api/run")
def list_runs() -> dict[str, Any]:
    return {"items": engine.list_runs(), "count": len(engine.runs)}


@app.get("/api/run/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    try:
        state = engine.get_run(run_id)
    except RunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _run_to_wire(state)


@app.post("/api/run/{run_id}/approve")
def approve_run(run_id: str, body: ApprovePayload) -> dict[str, Any]:
    try:
        state = engine.approve(run_id, body.approved, body.comment)
    except RunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _run_to_wire(state)


@app.post("/api/run/{run_id}/resume")
def resume_run(run_id: str) -> dict[str, Any]:
    try:
        state = engine.resume(run_id)
    except RunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _run_to_wire(state)


@app.post("/api/run/{run_id}/cancel")
def cancel_run(run_id: str) -> dict[str, Any]:
    try:
        state = engine.cancel(run_id)
    except RunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _run_to_wire(state)


@app.post("/api/run/{run_id}/crash")
def crash_run(run_id: str) -> dict[str, Any]:
    """模拟进程崩溃，制造一个「需要续跑」的状态。"""
    try:
        state = engine.simulate_crash(run_id)
    except RunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _run_to_wire(state)


# ---------------------------------------------------------------------------
# 存档：跨进程续跑的载体
# ---------------------------------------------------------------------------


@app.get("/api/run/{run_id}/archive")
def archive_run(run_id: str) -> dict[str, Any]:
    try:
        state = engine.get_run(run_id)
    except RunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    raw = state.to_json()
    return {"run_id": run_id, "bytes": len(raw.encode("utf-8")), "json": raw}


@app.post("/api/run/restore")
def restore_run(body: ArchivePayload) -> dict[str, Any]:
    from .state import RunState

    try:
        state = RunState.from_json(body.json)
        engine.restore(state)
    except (ValueError, TypeError, KeyError) as exc:
        raise HTTPException(
            status_code=422, detail=f"存档无法解析：{exc}"
        ) from exc
    except WorkflowNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"restored": state.run_id, "run": _run_to_wire(state)}


# ---------------------------------------------------------------------------
# 静态页面
# ---------------------------------------------------------------------------


@app.get("/")
def index_page() -> FileResponse:
    return FileResponse(f"{WEB_DIR}/index.html")


app.mount("/", StaticFiles(directory=WEB_DIR), name="web")
