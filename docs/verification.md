# 验证报告

## 1. 范围与环境

- 日期：2026-09-22。
- 工作目录：`data-agent`；Python 3.14.6，DuckDB 1.5.0。
- 本次验证不使用真实模型接口，不向求解代码提供公开标准答案。
- Python 3.10—3.13 和其他操作系统尚未单独验证。

## 2. 已执行检查

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| 离线单元与集成回归 | 41 项通过，最终一次约 1.14 秒 | [pytest 文本](../artifacts/validation/pytest.txt)、[JUnit](../artifacts/validation/pytest.xml) |
| Ruff 静态检查 | 通过 | [检查输出](../artifacts/validation/ruff.txt) |
| Python 编译检查 | 实现阶段通过 | `python -m compileall -q src` |
| Git 差异空白检查 | 通过 | `git diff --check` |
| CLI 入口与配置 | `status` 正确发现 50 个任务；帮助信息包含独立评分命令 | `dabench status`、`dabench evaluate --help` |
| 真实结构化输入准备 | 50 个任务，105 张表，0 个错误任务，总计约 15.924 秒 | [逐任务报告](../artifacts/validation/inputs.json) |

真实输入验证包括来源加载、类型推断、目录生成、Schema 和少量样本查询。任务包含 14 份业务文档，但该脚本只发现这些文档，不调用模型抽取。全量时间不能解读为 ReAct 求解总时间或 ETL 耗时。

最慢输入准备任务为 `task_257`，约 3.26 秒，包含 303155 行 postHistory、91966 行 posts 和 40325 行 users。279 MB Match.csv 和 166 MB records 包装 JSON 均已验证可加载。内存峰值未采样，不能据此宣称达到特定内存目标。

## 3. 回归覆盖

| 模块 | 覆盖要点 |
| --- | --- |
| 数据运行时 | CSV/JSON/SQLite 关联、空值/重复、时间/日期/定点数、大 JSON 包装、同名来源、越界符号链接 |
| SQL 边界 | 拒绝写入/多语句/配置修改/外部文件读取/间接写入；长查询中断后连接仍可使用 |
| 程序产物 | 版本冲突、未执行禁止提交、修改和失败后旧产物失效、最终结果不静默截断、拒绝非有限数值 |
| ETL | 有证据合并、冲突置空与留痕、伪造引用拒绝、复合主键、前导零/精确整数、同实体分组 |
| ETL 恢复 | 覆盖遗漏触发重试、虚报覆盖不能隐藏明确编号实体、单块失败保留成功记录 |
| 缓存 | 问题/内容/模型变化失效，损坏主键缓存重算 |
| ReAct | 解析错误后修正、失败工具保留动作与参数、完整 SQL 提交链路 |
| 编排 | 文档 ETL→关联查询→CSV 的脚本模型端到端验证；模型失败后干净重启、失败尝试留痕、坏任务隔离 |
| 子进程 | 串行批跑强制超时、spawn 路径配置失败可诊断 |
| 配置与评分 | 环境变量密钥、错误配置拒绝、重复行比较、显式舍入、前导零、未完成任务分母 |

脚本模型只用于可重复验证协议和运行状态。它的答案结果不能作为真实语言模型的推理或抽取准确率。

## 4. 验证中发现并修复的问题

1. 大型 records 包装 JSON 的 DuckDB 缓冲预分配导致内存错误：改为 ijson 流式展开，再使用逐行 JSON 加载。
2. TIME 字段不能写入工具观察 JSON：补齐时间、间隔、UUID 序列化并增加回归。
3. 抽取器可能自报处理所有段落但遗漏明确编号实体：加入确定性身份锚点覆盖检查和块重试。
4. 工具异常丢失动作、日志回调异常被误记成工具错误：区分执行与日志边界，保存参数和错误类型。
5. `.gitignore` 的未锚定 `data/` 规则会忽略新增源码模块：改为仅忽略仓库根目录数据。

## 5. 尚未完成的验收

| 项目 | 状态 / 原因 | 下一步 |
| --- | --- | --- |
| 真实模型接口联通和响应协议 | 未配置；默认密钥环境变量不存在 | 提供本地配置路径或密钥环境变量名 |
| 14 份文档的语义抽取质量 | 未测 | 抽查更正值、跨章节字段、复合身份、单位和证据 |
| 50 个任务准确率与吞吐 | 未测，不报告数值 | 用真实配置运行，再独立执行 evaluate |
| 人工审阅 | 待审阅 | 按 [review.md](review.md) 核对代码、产物和风险 |

## 6. 可复现命令

```bash
uv sync --extra dev
uv run pytest -q --junitxml=artifacts/validation/pytest.xml
uv run ruff check src tests scripts
uv run python scripts/validate_inputs.py

# 真实配置与密钥准备好后：
uv run dabench run-benchmark --config configs/react_baseline.local.yaml
uv run dabench evaluate artifacts/runs/<run_id> --gold-root public/output
```

真实验收建议先在配置中选择代表性任务：结构化跨表、文档关联、长文档；根据产物诊断后再全量运行。示例任务 ID 仅作为验收采样，不在生产代码中作为求解分支。
