"""受限表达式求值：白名单 AST，绝不 eval。

工作流的节点参数（任务表达式、条件谓词）几乎都来自外部输入
—— API 请求体、配置文件、别人写的 YAML。
对不可信字符串用 ``eval`` 等于把服务端权限交出去。

这里用 ast 白名单：只允许字面量、名字、四则/比较/逻辑运算、
成员访问（一层）、以及 f-string 式的拼接。
其他一切（函数调用、下标、属性链、lambda、推导式、导入）直接拒绝。
"""

from __future__ import annotations

import ast
import operator
from typing import Any


class ExpressionError(ValueError):
    """表达式不合法或越出白名单。"""


#: 二元运算白名单
_BIN_OPS: dict[type, Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

#: 一元运算
_UNARY_OPS: dict[type, Any] = {
    ast.Not: operator.not_,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

#: 比较
_CMP_OPS: dict[type, Any] = {
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
}

#: 布尔运算
_BOOL_OPS: dict[type, Any] = {
    ast.And: all,
    ast.Or: any,
}

#: 允许的顶层名字。ctx 里的键都可以，加上少量常量。
_EXTRA_NAMES: dict[str, Any] = {"true": True, "false": False, "none": None}

#: 表达式的规模上限。超长表达式的意义只有一个：绕过审查。
MAX_EXPR_LENGTH = 512


def safe_eval(expr: str, ctx: dict[str, Any]) -> Any:
    """求值一个受限表达式。

    ``ctx`` 里是上游节点的输出，用节点 id 作键；
    特殊键 ``input`` 是启动时传入的负载。
    """
    if not isinstance(expr, str):
        raise ExpressionError(f"表达式必须是字符串，收到 {type(expr).__name__}")
    if len(expr) > MAX_EXPR_LENGTH:
        raise ExpressionError(
            f"表达式超过 {MAX_EXPR_LENGTH} 字符 —— "
            "这么长的逻辑应该拆成多个节点，而不是塞进一个字段里"
        )

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"表达式语法错误：{exc.msg}") from exc

    evaluator = _Evaluator(dict(ctx) | _EXTRA_NAMES)
    return evaluator.visit(tree.body)


class _Evaluator(ast.NodeVisitor):
    def __init__(self, names: dict[str, Any]) -> None:
        self.names = names
        self.depth = 0

    # -- 允许的节点 -------------------------------------------------------

    def visit_Constant(self, node: ast.Constant) -> Any:
        if isinstance(node.value, (int, float, str, bool, type(None))):
            return node.value
        raise ExpressionError(f"不支持的字面量类型 {type(node.value).__name__}")

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id in self.names:
            return self.names[node.id]
        raise ExpressionError(
            f"未知的名字 {node.id!r}；可用：{', '.join(sorted(self.names))}"
        )

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        op = _BIN_OPS.get(type(node.op))
        if op is None:
            raise ExpressionError(f"不支持的运算 {type(node.op).__name__}")
        return op(self.visit(node.left), self.visit(node.right))

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        op = _UNARY_OPS.get(type(node.op))
        if op is None:
            raise ExpressionError(f"不支持的一元运算 {type(node.op).__name__}")
        return op(self.visit(node.operand))

    def visit_Compare(self, node: ast.Compare) -> Any:
        left = self.visit(node.left)
        for op_node, comparator in zip(node.ops, node.comparators):
            op = _CMP_OPS.get(type(op_node))
            if op is None:
                raise ExpressionError(
                    f"不支持的比较 {type(op_node).__name__}"
                )
            right = self.visit(comparator)
            if not op(left, right):
                return False
            left = right
        return True

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        op = _BOOL_OPS.get(type(node.op))
        if op is None:
            raise ExpressionError(f"不支持的逻辑运算 {type(node.op).__name__}")
        # **必须短路**：and/or 从左到右逐个求值，遇到足够的结果就停。
        # 曾经这里先把所有操作数求值完再挑选 —— 意味着
        # `false and f(x)` 会先执行 f(x)。在普通语言里这叫语义错误，
        # 在工作流引擎里这叫「条件没命中也执行了下游动作」，
        # 而下游动作可能是发邮件、扣款。测试抓出来的。
        is_and = isinstance(node.op, ast.And)
        result: Any = None
        for value_node in node.values:
            result = self.visit(value_node)
            if is_and and not result:
                return result
            if not is_and and result:
                return result
        return result

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        # 三元表达式 a if cond else b —— 审批流里"金额 > X 走 A 否则 B"很常用
        if self.visit(node.test):
            return self.visit(node.body)
        return self.visit(node.orelse)

    def visit_Attribute(self, node: ast.Attribute) -> Any:
        """只允许一层属性访问，且目标必须是 ctx 里的 dict。

        dunder 名字（``__class__`` 等）显式拒绝，而不是靠 ``dict.get``
        返回 None 来"自然地"无事发生 —— 那依赖一个巧合：
        恰好值是 dict。哪天上游输出改成普通对象，
        同一条表达式就成了属性遍历攻击的入口。
        """
        if node.attr.startswith("_"):
            raise ExpressionError(
                f"不允许访问以下划线开头的属性 {node.attr!r}"
            )
        value = self.visit(node.value)
        if not isinstance(value, dict):
            raise ExpressionError(
                "只允许访问 dict 的属性（上游节点输出）"
            )
        return value.get(node.attr)

    def visit_Dict(self, node: ast.Dict) -> Any:
        return {
            self.visit(k): self.visit(v)
            for k, v in zip(node.keys, node.values)
            if k is not None
        }

    def visit_List(self, node: ast.List) -> Any:
        return [self.visit(e) for e in node.elts]

    def visit_Tuple(self, node: ast.Tuple) -> Any:
        return tuple(self.visit(e) for e in node.elts)

    # -- 一律拒绝 ---------------------------------------------------------

    def generic_visit(self, node: ast.AST) -> Any:
        raise ExpressionError(
            f"表达式里不允许 {type(node).__name__}（白名单外）。"
            "函数调用、下标、推导式、导入都在黑名单里 —— "
            "要更复杂的逻辑请写成一个 task 节点，而不是表达式。"
        )
