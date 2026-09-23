"""只读探查与定点 SQL 程序工具，不向模型暴露任意代码执行。"""

from __future__ import annotations

from data_agent_baseline.benchmark.schema import AnswerTable
from data_agent_baseline.data.query import QueryRuntime, Solution
from data_agent_baseline.etl.documents import read_document
from data_agent_baseline.tools.filesystem import resolve_context_path
from data_agent_baseline.tools.registry import ToolExecutionResult, ToolRegistry, ToolSpec

ANALYSIS_PROMPT = """你是严谨的数据分析 ReAct Agent，使用 DuckDB SQL 求解任务。
每次仅输出一个 JSON 代码块，键为 thought、action、action_input。用简短中文说明分析依据。
先查看数据目录、治理说明和候选 Schema，检查关联基数、空值、边界、单位及并列情况。
原始观测保留重复和空值；实体名单按实体粒度去重；聚合按题目粒度执行。仅输出要求的列。
所有目录表都可查询，文档抽取表附带质量状态。partial 或 failed 不能当作完整数据。
使用 read_document 核对 ETL 证据，不把噪声或已被更正的旧值当作事实。
最终答案必须来自 solution.sql 的执行；流程为 read_solution → edit_solution → run_solution
→ 自检样本/行数/非空数 → submit_answer。submit_answer 无参数，禁止手写答案行。
编辑必须携带最新 revision；修改后重新运行，旧结果不能提交。
查询只允许一条 SELECT/WITH；禁止写入、访问外部文件/网络、安装扩展或执行 Python。
列名有空格时使用双引号。日期、字符串、数值按真实 Schema 处理，必要时 TRY_CAST。
SQLite 治理例子仅表达业务含义，须改用 DuckDB 语法。不要为消除错误而改变问题口径。
空结果需检查筛选与关联；不能擅自增加 LIMIT、过滤空值或去重。"""


def create_analysis_tools(
    runtime: QueryRuntime, solution: Solution, knowledge: str
) -> ToolRegistry:
    def wrap(content):
        return ToolExecutionResult(ok=True, content=content)

    def list_tables(task, args):
        del task, args
        return wrap(runtime.manifest)

    def inspect(task, args):
        del task
        return wrap(runtime.schema(str(args["table"])))

    def query(task, args):
        del task
        return wrap(runtime.query(str(args["sql"]), int(args.get("limit", 50))))

    def read_knowledge(task, args):
        del task
        offset = max(0, int(args.get("offset", 0)))
        return wrap(
            {
                "text": knowledge[offset : offset + 12000],
                "total_chars": len(knowledge),
                "next_offset": offset + 12000 if offset + 12000 < len(knowledge) else None,
            }
        )

    def document(task, args):
        path = resolve_context_path(task, str(args["path"]))
        if path.suffix.lower() not in {".md", ".txt", ".pdf"}:
            raise ValueError("只能核对文本文档证据")
        paragraphs = read_document(path)
        start = max(0, int(args.get("start", 0)))
        return wrap(
            {
                "paragraphs": [p.to_dict() for p in paragraphs[start : start + 8]],
                "total_paragraphs": len(paragraphs),
            }
        )

    def read(task, args):
        del task, args
        return wrap(solution.read())

    def edit(task, args):
        del task
        return wrap(solution.edit(str(args["sql"]), str(args["revision"])))

    def run(task, args):
        del task, args
        return wrap(solution.run())

    def submit(task, args):
        del task
        if args:
            raise ValueError("提交不接受答案值，请执行程序生成结果")
        result = solution.submit()
        return ToolExecutionResult(
            ok=True,
            content={"status": "submitted", "row_count": result["row_count"]},
            is_terminal=True,
            answer=AnswerTable(result["columns"], result["rows"]),
        )

    definitions = [
        ("list_tables", "列出全部来源、表、字段、行数和 ETL 质量状态。", {}, list_tables),
        ("inspect_table", "查看单表结构、来源和三行样本。", {"table": "表名"}, inspect),
        (
            "query",
            "执行只读 DuckDB 探查查询，至多返回 200 行。",
            {"sql": "SELECT ...", "limit": 50},
            query,
        ),
        ("read_knowledge", "分页读取治理说明。", {"offset": 0}, read_knowledge),
        (
            "read_document",
            "分页核对文档段落证据，每页八段。",
            {"path": "doc/source.md", "start": 0},
            document,
        ),
        ("read_solution", "读取当前求解 SQL 和版本。", {}, read),
        (
            "edit_solution",
            "校验并修改求解 SQL，必须提供最近读取的版本。",
            {"sql": "SELECT ...", "revision": "最近读取的 revision"},
            edit,
        ),
        ("run_solution", "执行当前程序并返回产物行列、非空统计和样本。", {}, run),
        ("submit_answer", "提交当前程序最新成功执行的结果，不接受手写数据。", {}, submit),
    ]
    return ToolRegistry(
        specs={
            name: ToolSpec(name, description, schema)
            for name, description, schema, _ in definitions
        },
        handlers={name: handler for name, _, _, handler in definitions},
    )
