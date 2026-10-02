"""白名单表达式求值的测试。

安全边界测试的重点不是"合法输入算得对"（那是普通单测），
而是**恶意或越界的输入必须被拒绝** —— eval 的能力边界
取决于调用方传进来的字符串，而工作流参数几乎都是外部输入。
"""

from __future__ import annotations

import pytest

from app.safe_expr import ExpressionError, safe_eval

CTX = {
    "input": {"amount": 150, "level": "gold", "items": [1, 2, 3]},
    "step1": {"total": 42, "ok": True},
    "step2": {"name": "订单A", "price": 9.9},
}


class TestArithmetic:
    def test_literals(self):
        assert safe_eval("1 + 2", {}) == 3
        assert safe_eval("2 * 3.5", {}) == 7.0
        assert safe_eval("10 / 4", {}) == 2.5
        assert safe_eval("7 // 2", {}) == 3
        assert safe_eval("7 % 3", {}) == 1
        assert safe_eval("2 ** 10", {}) == 1024

    def test_unary(self):
        assert safe_eval("-5 + 3", {}) == -2
        assert safe_eval("not false", {}) is True

    def test_context_names(self):
        assert safe_eval("input.amount", CTX) == 150
        assert safe_eval("step1.total * 2", CTX) == 84
        assert safe_eval("step2.name", CTX) == "订单A"


class TestComparisons:
    def test_basic(self):
        assert safe_eval("input.amount > 100", CTX) is True
        assert safe_eval("input.amount < 100", CTX) is False
        assert safe_eval("input.level == 'gold'", CTX) is True
        assert safe_eval("input.level != 'gold'", CTX) is False

    def test_membership(self):
        assert safe_eval("1 in input.items", CTX) is True
        assert safe_eval("9 not in input.items", CTX) is True

    def test_chained(self):
        assert safe_eval("1 < input.amount < 200", CTX) is True

    def test_boolean_ops(self):
        assert safe_eval("input.amount > 100 and step1.ok", CTX) is True
        assert safe_eval("input.amount < 100 or step1.ok", CTX) is True
        assert safe_eval("false or false", {}) is False

    def test_ternary(self):
        assert safe_eval("'big' if input.amount > 100 else 'small'", CTX) == "big"
        assert safe_eval("'big' if input.amount > 999 else 'small'", CTX) == "small"


class TestCollections:
    def test_dict_and_list_literals(self):
        assert safe_eval("{'a': 1, 'b': 2}", {}) == {"a": 1, "b": 2}
        assert safe_eval("[1, 2, 3]", {}) == [1, 2, 3]

    def test_missing_name_gives_clear_error(self):
        with pytest.raises(ExpressionError, match="未知的名字"):
            safe_eval("nonexistent.total", CTX)


class TestSecurity:
    """这一组全部要被拒绝。任何一条通过都是漏洞。"""

    @pytest.mark.parametrize("expr", [
        "__import__('os').system('echo pwned')",
        "().__class__.__bases__[0].__subclasses__()",
        "open('/etc/passwd')",
        "exec('x=1')",
        "eval('1+1')",
        "lambda: 1",
        "[x for x in range(10)]",
        "{k: v for k, v in {}.items()}",
        "input.items[0]",            # 下标不在白名单
        "(lambda a: a)(1)",
        "'a'.join(['1','2'])",       # 方法调用
        "input.__class__",
    ])
    def test_blocked(self, expr):
        with pytest.raises(ExpressionError):
            safe_eval(expr, CTX)

    def test_length_limit(self):
        with pytest.raises(ExpressionError, match="512"):
            safe_eval("1 + " * 300 + "1", {})

    def test_syntax_error_is_expression_error(self):
        with pytest.raises(ExpressionError, match="语法错误"):
            safe_eval("1 +", {})

    def test_non_string_rejected(self):
        with pytest.raises(ExpressionError, match="字符串"):
            safe_eval(123, {})  # type: ignore[arg-type]

    def test_error_message_does_not_leak_whitelist_mechanism(self):
        """报错要面向使用者，不要暴露内部实现细节。"""
        with pytest.raises(ExpressionError) as ei:
            safe_eval("foo()", {})
        assert "eval(" not in str(ei.value)


class TestSemantics:
    def test_and_returns_first_falsy_like_python(self):
        assert safe_eval("0 and 5", {}) == 0
        assert safe_eval("false and 5", {}) is False

    def test_or_returns_first_truthy_like_python(self):
        assert safe_eval("0 or 7", {}) == 7

    def test_short_circuit_does_not_visit_rest(self):
        # 右侧是不合法的名字，但短路后不会被求值
        assert safe_eval("false and nonexistent", {}) is False
        assert safe_eval("true or nonexistent", {}) is True
