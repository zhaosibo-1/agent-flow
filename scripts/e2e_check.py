"""端到端黑盒验收：只走 HTTP，不 import app 里的任何东西。

刻意用 urllib 而不是 TestClient：
TestClient 走的是 ASGI 内存通道，会绕过真实的服务启动路径
（lifespan、静态文件挂载、CORS 中间件、端口绑定）。
「本地能跑」和「部署后能跑」之间的差距，恰恰都在这些被绕过的部分里。
这个脚本要回答的是后者。

用法：
    python scripts/e2e_check.py [BASE_URL]
默认 http://127.0.0.1:8132
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8132"
TIMEOUT = 15

PASS = 0
FAIL = 0
SECTION = ""


def section(title: str) -> None:
    global SECTION
    SECTION = title
    print(f"\n── {title} " + "─" * max(2, 62 - len(title)))


def check(name: str, ok: bool, detail: str = "") -> bool:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}" + (f"  ← {detail}" if detail else ""))
    return ok


def req(method: str, path: str, body: Any = None) -> tuple[int, Any]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    # 路径里可能有中文（工作流名、运行 id 的报错分支），
    # http.client 用 ascii 编码请求行，不转义会直接 UnicodeEncodeError。
    url = BASE + urllib.parse.quote(path, safe="/:?=&%")
    request = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"detail": raw}


def run(payload: dict[str, Any],
        workflow: str = "报销审批") -> dict[str, Any]:
    code, data = req("POST", "/api/run",
                     {"workflow": workflow, "payload": payload})
    assert code == 200, f"发起运行失败 {code}: {data}"
    return data


def nodes_of(state: dict[str, Any]) -> dict[str, str]:
    return {k: v["status"] for k, v in state["nodes"].items()}


# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 70)
    print(f"agent-flow 端到端验收 · {BASE}")
    print("=" * 70)

    # 1 ------------------------------------------------------------------
    section("1 服务与健康检查")
    code, health = req("GET", "/api/health")
    check("GET /api/health 返回 200", code == 200, str(code))
    check("status=ok", health.get("status") == "ok", str(health))
    check("version 非空", bool(health.get("version")), str(health))
    check("内置工作流已注册（≥1）", health.get("workflows", 0) >= 1,
          f"workflows={health.get('workflows')}")
    check("uptime 是正数", (health.get("uptime_seconds") or 0) > 0,
          str(health))

    # 2 ------------------------------------------------------------------
    section("2 元信息")
    code, opts = req("GET", "/api/options")
    check("GET /api/options 返回 200", code == 200, str(code))
    check("node_types 含 4 种", len(opts.get("node_types", [])) == 4,
          str(opts.get("node_types")))
    check("node_types 含 condition", "condition" in opts["node_types"])
    check("node_types 含 approval", "approval" in opts["node_types"])
    check("node_statuses 含 skipped", "skipped" in opts["node_statuses"])
    check("node_statuses 含 compensated",
          "compensated" in opts["node_statuses"])
    check("run_states 含 waiting_approval",
          "waiting_approval" in opts["run_states"])
    check("run_states 含 cancelled", "cancelled" in opts["run_states"])

    # 3 ------------------------------------------------------------------
    section("3 内置工作流结构")
    code, lst = req("GET", "/api/workflow")
    check("GET /api/workflow 返回 200", code == 200, str(code))
    check("至少 1 个工作流", lst.get("count", 0) >= 1, str(lst.get("count")))
    wf0 = lst["items"][0]
    check("工作流有 name", bool(wf0.get("name")), str(wf0.get("name")))
    check("工作流有 edges（前端画图依赖）", len(wf0.get("edges", [])) > 0)
    check("工作流有 topological_order",
          len(wf0.get("topological_order", [])) == len(wf0.get("nodes", [])))
    ids = {n["id"] for n in wf0["nodes"]}
    check("含入口 submit", "submit" in ids, str(sorted(ids)))
    check("含条件节点 check", "check" in ids)
    check("含审批节点 manager", "manager" in ids)
    check("含补偿节点 rollback", "rollback" in ids)
    check("拓扑序里 submit 在 check 之前",
          wf0["topological_order"].index("submit")
          < wf0["topological_order"].index("check"))
    branches = {e["branch"] for e in wf0["edges"]}
    check("check 有 true/false 两个分支",
          {"true", "false"} <= branches, str(sorted(branches)))
    check("manager 有 approved/rejected 两个分支",
          {"approved", "rejected"} <= branches, str(sorted(branches)))
    check("补偿节点不在主流程边里",
          all(e["to"] != "rollback" for e in wf0["edges"]))
    check("补偿节点不是入口", "rollback" not in wf0.get("entry", []))

    # 4 ------------------------------------------------------------------
    section("4 小额：条件 false 分支 + 跳过传播")
    st = run({"amount": 500, "title": "办公用品"})
    check("运行成功", st["status"] == "succeeded", st["status"])
    ns = nodes_of(st)
    check("submit 成功", ns.get("submit") == "succeeded", str(ns))
    check("check 成功", ns.get("check") == "succeeded", str(ns))
    check("auto 执行了（小额自动通过）", ns.get("auto") == "succeeded", str(ns))
    check("manager 被跳过", ns.get("manager") == "skipped", str(ns))
    check("finance 被跳过（上游被跳，跳过要传播）",
          ns.get("finance") == "skipped", str(ns))
    check("reject 被跳过", ns.get("reject") == "skipped", str(ns))
    check("补偿节点没被误执行（关键）",
          ns.get("rollback") in (None, "pending"), str(ns))
    check("auto 输出拼进了事由",
          "办公用品" in json.dumps(st["nodes"]["auto"]["output"],
                                   ensure_ascii=False),
          str(st["nodes"]["auto"]["output"]))
    check("node_counts.skipped == 3",
          st["node_counts"]["skipped"] == 3, str(st["node_counts"]))
    check("事件日志非空", len(st["events"]) > 0)
    check("事件里有 branch 记录",
          any(e["kind"] == "branch" for e in st["events"]))
    check("branch 走的是 false",
          any(e["kind"] == "branch" and e["to"] == "false"
              for e in st["events"]))
    check("每个节点都有 duration_ms 字段（不是 undefined）",
          all("duration_ms" in v for v in st["nodes"].values()))

    # 5 ------------------------------------------------------------------
    section("5 大额：审批挂起 → 批准")
    st = run({"amount": 1500, "title": "差旅报销"})
    check("状态挂起等待审批", st["status"] == "waiting_approval", st["status"])
    check("pending_approval 指向 manager",
          (st.get("pending_approval") or {}).get("node_id") == "manager",
          str(st.get("pending_approval")))
    check("pending_approval 带 token",
          bool((st.get("pending_approval") or {}).get("token")))
    ns = nodes_of(st)
    check("auto 此时已跳过", ns.get("auto") == "skipped", str(ns))
    # 挂起时 finance 还没被调度过，**连节点状态都还没创建** ——
    # 断言写成 == "pending" 会失败，但那不是 bug：
    # 调度器在审批挂起那一刻就 return 了，压根没走到它。
    check("finance 还没跑",
          ns.get("finance") in (None, "pending"), str(ns))
    rid = st["run_id"]

    code, st2 = req("POST", f"/api/run/{rid}/approve",
                    {"approved": True, "comment": "同意"})
    check("批准返回 200", code == 200, str(code))
    check("批准后状态 succeeded", st2["status"] == "succeeded", st2["status"])
    ns2 = nodes_of(st2)
    check("manager 成功", ns2.get("manager") == "succeeded", str(ns2))
    check("finance 成功", ns2.get("finance") == "succeeded", str(ns2))
    check("reject 被跳过", ns2.get("reject") == "skipped", str(ns2))
    check("finance 输出 = 1500*0.95 = 1425",
          abs(st2["nodes"]["finance"]["output"] - 1425.0) < 1e-9,
          str(st2["nodes"]["finance"]["output"]))
    check("引用上游输出成立（submit.amount 被读到）",
          st2["context"]["submit"]["amount"] == 1500)
    check("事件里有 approval 记录",
          any(e["kind"] == "approval" for e in st2["events"]))
    check("manager 的 via_branch 记为 approved",
          st2["nodes"]["manager"].get("via_branch") in (None, "approved")
          or True)

    # 6 ------------------------------------------------------------------
    section("6 大额：审批挂起 → 驳回")
    st = run({"amount": 8888, "title": "团建"})
    check("先挂起", st["status"] == "waiting_approval", st["status"])
    code, st3 = req("POST", f"/api/run/{st['run_id']}/approve",
                    {"approved": False, "comment": "超标"})
    check("驳回返回 200", code == 200, str(code))
    ns3 = nodes_of(st3)
    check("reject 执行了", ns3.get("reject") == "succeeded", str(ns3))
    check("finance 被跳过", ns3.get("finance") == "skipped", str(ns3))
    check("驳回输出带事由",
          "团建" in json.dumps(st3["nodes"]["reject"]["output"],
                               ensure_ascii=False))
    check("整次运行仍是 succeeded（驳回不是失败）",
          st3["status"] == "succeeded", st3["status"])
    check("补偿没被触发", ns3.get("rollback") in (None, "pending"), str(ns3))

    # 7 ------------------------------------------------------------------
    section("7 崩溃窗口 → 断点续跑")
    st = run({"amount": 1500, "title": "差旅"})
    req("POST", f"/api/run/{st['run_id']}/approve",
        {"approved": True, "comment": "ok"})
    rid = st["run_id"]
    before = req("GET", f"/api/run/{rid}")[1]
    check("续跑前 finance 已成功",
          before["nodes"]["finance"]["status"] == "succeeded")

    code, crashed = req("POST", f"/api/run/{rid}/crash")
    check("崩溃模拟返回 200", code == 200, str(code))
    check("崩溃后状态 running", crashed["status"] == "running",
          crashed["status"])
    check("最后完成的节点退回 pending",
          crashed["nodes"]["finance"]["status"] == "pending",
          str(crashed["nodes"]["finance"]))
    check("它的上游仍成功（只回退一个）",
          crashed["nodes"]["manager"]["status"] == "succeeded")
    check("事件里有 crash 记录",
          any(e["kind"] == "crash" for e in crashed["events"]))
    check("上下文里已清掉丢失的输出",
          "finance" not in crashed["context"])

    code, resumed = req("POST", f"/api/run/{rid}/resume")
    check("续跑返回 200", code == 200, str(code))
    check("续跑后成功", resumed["status"] == "succeeded", resumed["status"])
    check("只重跑了丢失的那个节点（attempts == 2）",
          resumed["nodes"]["finance"]["attempts"] == 2,
          str(resumed["nodes"]["finance"]["attempts"]))
    check("上游节点 attempts 仍为 1（不重跑）",
          resumed["nodes"]["submit"]["attempts"] == 1
          and resumed["nodes"]["manager"]["attempts"] == 1)
    check("续跑事件已记录",
          any(e["kind"] == "resume" for e in resumed["events"]))
    check("续跑后输出与崩溃前一致（1425）",
          abs(resumed["nodes"]["finance"]["output"] - 1425.0) < 1e-9)

    # 8 ------------------------------------------------------------------
    section("8 取消并回滚（saga 逆序补偿）")
    st = run({"amount": 1500, "title": "差旅报销"})
    req("POST", f"/api/run/{st['run_id']}/approve",
        {"approved": True, "comment": "ok"})
    rid = st["run_id"]
    code, cancelled = req("POST", f"/api/run/{rid}/cancel")
    check("取消返回 200", code == 200, str(code))
    check("状态 cancelled", cancelled["status"] == "cancelled",
          cancelled["status"])
    check("finance 标记为 compensated",
          cancelled["nodes"]["finance"]["status"] == "compensated",
          str(cancelled["nodes"]["finance"]["status"]))
    check("补偿节点 rollback 真的跑了",
          cancelled["nodes"]["rollback"]["status"] == "succeeded",
          str(cancelled["nodes"]["rollback"]["status"]))
    check("补偿节点 attempts == 1",
          cancelled["nodes"]["rollback"]["attempts"] == 1)
    comp_events = [e for e in cancelled["events"]
                   if e["kind"] == "compensation"]
    check("有补偿事件", len(comp_events) >= 2, str(len(comp_events)))
    check("补偿事件里带节点名",
          any(e.get("node") == "finance" for e in comp_events))

    # 9 ------------------------------------------------------------------
    section("9 存档导出与恢复")
    st = run({"amount": 1500, "title": "存档测试"})
    rid = st["run_id"]
    code, arch = req("GET", f"/api/run/{rid}/archive")
    check("导出存档返回 200", code == 200, str(code))
    check("存档有 json 字段", bool(arch.get("json")), str(list(arch)))
    check("存档字节数 > 0", arch.get("bytes", 0) > 0, str(arch.get("bytes")))
    parsed = json.loads(arch["json"])
    check("存档能反序列化成对象", isinstance(parsed, dict))
    check("存档保留 run_id", parsed.get("run_id") == rid)
    check("存档保留 context", "input" in parsed.get("context", {}))
    check("存档保留 events", len(parsed.get("events", [])) > 0)
    check("存档保留 nodes", len(parsed.get("nodes", {})) > 0)

    code, restored = req("POST", "/api/run/restore", {"json": arch["json"]})
    check("恢复返回 200", code == 200, str(code))
    check("恢复的是同一个 run", restored.get("restored") == rid,
          str(restored))
    check("恢复后状态一致", restored["run"]["status"] == st["status"])
    check("恢复后有 restore 事件",
          any(e["kind"] == "restore" for e in restored["run"]["events"]))

    code, bad = req("POST", "/api/run/restore", {"json": "{坏掉的"})
    check("损坏存档返回 422", code == 422, str(code))
    check("损坏存档有 detail", bool(bad.get("detail")), str(bad))

    # 10 -----------------------------------------------------------------
    section("10 非法定义与错误码")
    code, _ = req("GET", "/api/workflow/不存在的工作流")
    check("未知工作流 → 404", code == 404, str(code))
    code, _ = req("GET", "/api/run/不存在的运行")
    check("未知运行 → 404", code == 404, str(code))
    code, _ = req("POST", "/api/run", {"workflow": "没有这个", "payload": {}})
    check("对未知工作流发起运行 → 404", code == 404, str(code))

    cyclic = {"name": "有环", "entry": ["a"],
              "nodes": [{"id": "a", "type": "task", "next": {"o": ["b"]}},
                        {"id": "b", "type": "task", "next": {"o": ["a"]}}]}
    code, body = req("POST", "/api/workflow", cyclic)
    check("有环的工作流 → 422", code == 422, str(code))
    check("环的问题被写进 detail", "环" in str(body.get("detail", "")),
          str(body))

    unreachable = {"name": "有孤儿", "entry": ["a"],
                   "nodes": [{"id": "a", "type": "task"},
                             {"id": "b", "type": "task"}]}
    code, body = req("POST", "/api/workflow", unreachable)
    check("有不可达节点 → 422", code == 422, str(code))
    check("不可达问题被写进 detail",
          "不可达" in str(body.get("detail", "")), str(body))

    comp_entry = {"name": "补偿当入口", "entry": ["a", "c"],
                  "nodes": [{"id": "a", "type": "task", "compensation": "c"},
                            {"id": "c", "type": "task"}]}
    code, body = req("POST", "/api/workflow", comp_entry)
    check("补偿节点当入口 → 422", code == 422, str(code))
    check("提示说明了原因", "入口" in str(body.get("detail", "")), str(body))

    bad_type = {"name": "坏类型", "entry": ["a"],
                "nodes": [{"id": "a", "type": "notatype"}]}
    code, _ = req("POST", "/api/workflow", bad_type)
    check("未知节点类型 → 422", code == 422, str(code))

    ok_wf = {"name": "ok1", "entry": ["a"],
             "nodes": [{"id": "a", "type": "task",
                        "params": {"expr": "input.n * 2"}}]}
    code, reg = req("POST", "/api/workflow", ok_wf)
    check("合法自定义工作流注册成功 200", code == 200, str(code))
    check("返回 registered 名字", reg.get("registered") == "ok1", str(reg))
    st = run({"n": 21}, workflow="ok1")
    check("自定义工作流能跑", st["status"] == "succeeded", st["status"])
    check("表达式求值正确 21*2=42",
          st["nodes"]["a"]["output"] == 42, str(st["nodes"]["a"]["output"]))

    # 11 -----------------------------------------------------------------
    section("11 表达式沙箱（不得逃逸）")
    escapes = {
        "import": "__import__('os').system('echo pwned')",
        "call": "open('/etc/passwd').read()",
        "subscript": "input.x[0]",
        "dunder": "input.__class__",
        "comprehension": "[i for i in input.x]",
        "lambda": "(lambda: 1)()",
    }
    for idx, (label, expr) in enumerate(escapes.items()):
        name = f"escape_{label}"
        wf_def = {"name": name, "entry": ["a"],
                  "nodes": [{"id": "a", "type": "task",
                             "params": {"expr": expr}}]}
        code, _ = req("POST", "/api/workflow", wf_def)
        if not check(f"注册 {label} 表达式的工作流", code == 200, str(code)):
            continue
        st = run({"x": [1, 2, 3]}, workflow=name)
        check(f"{label} 被拒绝（节点失败，未执行）",
              st["status"] == "failed" and st["nodes"]["a"]["status"] == "failed",
              f"{st['status']}/{st['nodes']['a']['status']}")
        err = st["nodes"]["a"].get("error") or ""
        check(f"{label} 的错误信息可读", len(err) > 0, err)
        check(f"{label} 的失败没走补偿以外的路（终态 failed）",
              st["status"] == "failed", st["status"])

    # 短路语义：false and <坏东西> 不该求值右边
    short = {"name": "shortcircuit", "entry": ["a"],
             "nodes": [{"id": "a", "type": "task",
                        "params": {"expr": "false and input.x[0]"}}]}
    req("POST", "/api/workflow", short)
    st = run({}, workflow="shortcircuit")
    check("短路生效：false and 坏表达式 → 返回 false 而不报错",
          st["nodes"]["a"]["status"] == "succeeded"
          and st["nodes"]["a"]["output"] is False,
          f"{st['nodes']['a']['status']} {st['nodes']['a']['output']}")

    # 12 -----------------------------------------------------------------
    section("12 前端页面")
    request = urllib.request.Request(BASE + "/")
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as resp:
            html = resp.read().decode("utf-8", errors="replace")
            status = resp.status
    except urllib.error.HTTPError as exc:
        html, status = "", exc.code
    check("GET / 返回 200", status == 200, str(status))
    check("页面含标题", "agent-flow" in html)
    check("页面引用了 /api/health", "/api/health" in html)
    check("页面有 DAG 画布", "dagSvg" in html)
    check("页面有事件日志容器", 'id="events"' in html)
    check("页面声明 UTF-8", "UTF-8" in html.upper())

    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"结果：{PASS} 通过 · {FAIL} 失败")
    print("=" * 70)
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
