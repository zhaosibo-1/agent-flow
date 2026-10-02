"""零依赖门禁：调度 / 状态机 / 表达式层不许 import 任何第三方包。

为什么值得为它单写一个 CI 步骤：

工作流引擎的价值有一半在于「它是一个可以被放进任何项目里的纯逻辑」——
不绑定 Web 框架、不绑定消息队列、不绑定数据库。DAG 校验、
跳过传播、补偿逆序、表达式白名单，这些都可以被单元测试直接覆盖，
不需要起服务、不需要 mock。

一旦核心层引入了第三方包，这条性质就没了：
测试要装环境，复用要带依赖，CI 要拉镜像。
而这种退化是**无声**的 —— 代码照跑、测试照绿，
直到有人想把这个模块拷到别处才发现拷不动。

所以这里做三层检查：

**1. 静态 AST 扫描**：顶层 ``import X`` 的 X 是否在标准库名单里。
    快，能给出精确到行的报错位置。

**2. 动态导入**：在子进程里装一个 meta-path finder，
    把所有非标准库的顶层模块拦下来，然后真的 ``import_module``。
    静态扫描漏掉的间接依赖（``requests`` 被某个包 ``__init__`` 拉进来之类）
    会在这一步暴露。

**3. 反向校验豁免名单**：被豁免的模块必须真的依赖第三方。
    否则豁免名单会无限膨胀，等"所有人都被豁免"时这条规则就名存实亡。
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: 核心模块。**这里不许出现 FastAPI / pydantic / uvicorn / celery。**
CORE_MODULES: tuple[str, ...] = (
    "app",           # 版本号与模块地图
    "app.dag",       # DAG 结构、校验、拓扑排序
    "app.state",     # 运行状态、事件日志、序列化
    "app.safe_expr",  # 白名单表达式求值
    "app.engine",    # 调度 / 分支 / 跳过传播 / 重试 / 补偿 / 审批 / 续跑
)

#: 允许引入第三方依赖的模块（HTTP 边界）
EXEMPT_MODULES: tuple[str, ...] = ("app.main",)

#: 豁免模块**必须**依赖的第三方包。反向校验用。
EXEMPT_MUST_IMPORT: dict[str, tuple[str, ...]] = {
    "app.main": ("fastapi", "pydantic"),
}

_STDLIB = frozenset(sys.stdlib_module_names)
#: sys.stdlib_module_names 在不同版本/不同打包方式下会漏掉这几个
_STDLIB |= {"__future__", "_thread", "nt", "posix", "os", "typing_extensions"}


def _emit(level: str, module: str, message: str) -> None:
    print(f"  [{level}] {module}: {message}")


def _is_third_party(name: str) -> bool:
    top = name.split(".")[0]
    if top in _STDLIB:
        return False
    if top == "app":
        return False
    return True


# ---------------------------------------------------------------------------
# 1. 静态扫描
# ---------------------------------------------------------------------------

def _module_path(module: str) -> Path | None:
    """模块名 → 文件路径。包走 __init__.py。"""
    base = ROOT / module.replace(".", "/")
    source = base.with_suffix(".py")
    if source.exists():
        return source
    init = base / "__init__.py"
    return init if init.exists() else None


def scan_static(module: str) -> list[str]:
    path = _module_path(module)
    if path is None:
        return [f"找不到模块文件 {ROOT / module.replace('.', '/')}.py"]
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    problems: list[str] = []

    def check(node: ast.Import | ast.ImportFrom, depth: int) -> None:
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif node.level and node.level > 0:
            # 相对导入（``from .dag import X``）：AST 里 node.module 是去掉
            # 点号的裸名字，直接拿它会把 "dag" 当成顶层第三方包。
            # 相对导入指向的必然是本项目内部模块，不可能是第三方。
            # 真正的第三方依赖由第 2 步的动态导入兜底，不会漏。
            return
        else:
            names = [node.module or ""]
        for name in names:
            if not name:
                continue
            if depth == 0 and _is_third_party(name):
                problems.append(f"第 {node.lineno} 行 import {name}")

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            check(node, _node_depth(tree, node))
    return problems


def _node_depth(tree: ast.Module, target: ast.AST) -> int:
    """节点嵌在多少层函数/类定义里。0 = 模块顶层。"""
    depth = 0
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            for child in ast.walk(node):
                if child is target:
                    return depth + 1
    return 0


# ---------------------------------------------------------------------------
# 2. 动态导入
# ---------------------------------------------------------------------------

_CHILD = r'''
import sys, importlib
target = sys.argv[1]

class Blocker:
    def find_spec(self, name, path=None, target=None):
        top = name.split(".")[0]
        if top in sys.stdlib_module_names or top in ("app", "__future__"):
            return None
        raise ImportError(f"被拦截：核心层不允许引入第三方包 {name!r}")

sys.meta_path.insert(0, Blocker())
importlib.import_module(target)
print("OK")
'''


def check_dynamic(module: str) -> str | None:
    """在屏蔽第三方包的子进程里真的 import 一次。"""
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, module],
        cwd=str(ROOT), capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    if proc.returncode != 0:
        lines = (proc.stderr or proc.stdout).strip().splitlines()
        for line in reversed(lines):
            if "Error" in line or "被拦截" in line:
                return line.strip()
        return lines[-1] if lines else f"退出码 {proc.returncode}"
    return None


# ---------------------------------------------------------------------------
# 3. 反向校验豁免名单
# ---------------------------------------------------------------------------

def verify_exemptions() -> list[str]:
    problems: list[str] = []
    for module, must in EXEMPT_MUST_IMPORT.items():
        path = _module_path(module)
        if path is None:
            problems.append(f"找不到豁免模块 {module}")
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        for package in must:
            if package not in imported:
                problems.append(
                    f"{module} 被豁免却没有 import {package} —— "
                    "豁免名单不该留着过期的条目"
                )
    return problems


# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 70)
    print("零依赖门禁 · agent-flow core modules")
    print("=" * 70)
    failures = 0

    print(f"\n[1/3] 静态扫描 {len(CORE_MODULES)} 个核心模块")
    print("-" * 70)
    for module in CORE_MODULES:
        problems = scan_static(module)
        if problems:
            for p in problems:
                _emit("FAIL", module, p)
            failures += 1
        else:
            _emit("ok", module, "顶层 import 全部来自标准库")
        if module in EXEMPT_MUST_IMPORT:
            _emit("WARN", module, "既是核心模块又在豁免名单里，逻辑冲突")

    print("\n[2/3] 动态导入验证（拦截第三方包）")
    print("-" * 70)
    for module in CORE_MODULES:
        error = check_dynamic(module)
        if error:
            _emit("FAIL", module, error)
            failures += 1
        else:
            _emit("ok", module, "在纯净环境里可导入")

    print(f"\n[3/3] 反向校验 {len(EXEMPT_MODULES)} 个豁免模块")
    print("-" * 70)
    exempt_problems = verify_exemptions()
    for p in exempt_problems:
        _emit("FAIL", "exemptions", p)
        failures += 1
    if not exempt_problems:
        for module in EXEMPT_MODULES:
            _emit("ok", module, "确实依赖第三方，豁免成立")

    print("\n" + "=" * 70)
    if failures:
        print(f"✗ {failures} 项不合格 —— 核心层必须是零第三方依赖的")
    else:
        print(f"✓ 通过：{len(CORE_MODULES)} 个核心模块零第三方依赖，"
              f"{len(EXEMPT_MODULES)} 个豁免模块理由成立")
    print("=" * 70)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
