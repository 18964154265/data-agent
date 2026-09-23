# ReAct 数据分析 Agent

当前分支在 DataAgent-Bench starter kit 基础上增加文档 ETL、统一 DuckDB 查询、受控 SQL 求解程序、失败恢复和独立结果比较。

- [项目结构与运行机制](docs/project.md)
- [实施计划与进度](docs/progress.md)
- [验证报告](docs/verification.md)
- [开发记录](interact/develop.md)
- [人工审阅入口](docs/review.md)

## 快速开始

```bash
uv sync --extra dev
# 需要读取文本 PDF 时额外安装：uv sync --extra dev --extra pdf
uv run dabench status --config configs/react_baseline.example.yaml
```

复制示例配置到 `configs/react_baseline.local.yaml`，配置 `agent.model`、`agent.api_base` 和密钥环境变量名 `agent.api_key_env`。也兼容 YAML 中的 `api_key`，但不要将含密钥的本地配置提交到版本控制。

数据默认位于 `public/input/task_<id>/`，其中 `task.json` 包含 `task_id`、`difficulty`、`question`，`context/` 保存数据与治理说明。旧数据目录可通过 `dataset.root_path` 指定。

```bash
uv run dabench inspect-task task_330 --config configs/react_baseline.local.yaml
uv run dabench run-task task_330 --config configs/react_baseline.local.yaml
uv run dabench run-benchmark --config configs/react_baseline.local.yaml --limit 5
```

运行前需在启动终端中配置对应密钥环境变量。用 `dataset.task_ids: [task_330, task_355]` 选择代表性任务，留空则发现全部任务。

## 运行行为

1. 结构化文件物化为独立 DuckDB，CSV、对象数组/records 包装 JSON、SQLite 可跨来源关联。
2. 文档按 Schema、身份和段落进行抽取；逐字段保留引用，错误块重试，冲突和缺口进入报告。
3. Agent 使用目录、Schema 和只读查询探查数据，编辑有版本校验的 `solution.sql`。
4. 执行程序生成结果，检查行列、非空数量和样本，再提交 `prediction.csv`。
5. 失败时从新的 SQL 脚手架重试；任务级超时、逻辑模型请求预算和 SQL 超时均有限制。

正式工具不包含任意 Python 执行，也不接受模型手写答案值。SQL 只支持 DuckDB SELECT/WITH。文本 PDF 需要可选依赖，不支持 OCR。

默认 `etl.allow_partial=false`：有来源错误、未解决冲突、拒绝记录或失败块时不发布完整答案。若显式允许部分数据，仍必须查看质量报告，不能将结果视为完整准确答案。

## 配置要点

| 配置 | 默认值 / 说明 |
| --- | --- |
| `agent.max_steps` / `attempts` | 每次尝试 24 轮，至多 2 次完整尝试 |
| `agent.max_model_calls` | 每任务 ETL 和求解共用 256 次逻辑请求预算，不含 SDK 内部重发 |
| `agent.request_timeout_seconds` / `request_retries` | 单请求 90 秒，SDK 重试至多 2 次 |
| `etl.chunk_chars` / `max_workers` / `retries` | 每块 12000 字符，任务内并发 4，失败块重试 1 次 |
| `etl.cache_dir` | `artifacts/etl_cache`，完整结果按内容和契约指纹复用 |
| `run.max_workers` | 任务并发 4；ETL 请求峰值还需乘任务内并发 |
| `run.task_timeout_seconds` | 每任务 1200 秒；`<=0` 关闭强制任务超时 |
| `run.query_timeout_seconds` | 每次查询 30 秒 |
| `run.max_result_rows` | 最终结果最多 100000 行，超过时失败而非静默截断 |
| `run.run_id` | 留空生成唯一时间戳；指定目录名已存在时失败 |

## 产物和验收

运行目录为 `artifacts/runs/<run_id>/`。每任务保存 `events.jsonl`、数据目录、ETL 报告、每次尝试的 SQL 与 trace；成功后发布 `prediction.csv`。批跑逐任务更新 `summary.json`。

```bash
uv run dabench evaluate artifacts/runs/<run_id> --gold-root public/output
# 需要严格行顺序/列名时：追加 --ordered --headers
# 需要数值舍入时：显式追加 --decimals 6
```

比较器与求解链路分离；不会将标准答案提供给模型。本地比较默认忽略行顺序和列名，保留重复行及列顺序，未完成任务计入分母；此规则不是官方评分器。

```bash
uv run pytest -q
uv run ruff check src tests scripts
uv run python scripts/validate_inputs.py
```

已有离线回归与真实输入准备验证。真实模型准确率需配置接口后运行并评分，不能从程序成功率推断。

## 上游项目

- [DataAgent-Bench 网站](https://dataagent.top)
- [上游问题反馈](https://github.com/HKUSTDial/kddcup2026-data-agents-starter-kit/issues)
