"""agent-flow：带条件分支、补偿、人工审批与断点续跑的工作流 DAG 引擎。

核心模块全部零第三方依赖：
  dag.py       静态结构、校验、拓扑排序
  state.py     运行状态与事件日志（可序列化）
  safe_expr.py 白名单表达式求值
  engine.py    调度 / 分支 / 跳过传播 / 重试 / 补偿 / 审批 / 续跑
  main.py      FastAPI 路由（唯一允许第三方依赖的模块）
"""

from __future__ import annotations

__version__ = "1.0.0"
