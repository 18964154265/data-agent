"""探查与求解共用的只读 SQL 执行器。"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import threading
from pathlib import Path
from time import perf_counter
from typing import Any

import duckdb

from data_agent_baseline.data.catalog import ident
from data_agent_baseline.storage import atomic_text, json_value


class QueryRuntime:
    def __init__(self, workspace: Path, timeout: float = 30, max_result_rows: int = 100000) -> None:
        self.workspace = workspace
        self.timeout = timeout
        self.max_result_rows = max_result_rows
        self.manifest = json.loads((workspace / "catalog.json").read_text())
        self.connection = duckdb.connect(
            str(workspace / "data.duckdb"),
            read_only=True,
            config={
                "enable_external_access": "false",
                "threads": "2",
                "memory_limit": "1GB",
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        self.connection.execute("SET lock_configuration=true")

    def close(self) -> None:
        self.connection.close()

    def validate(self, sql: str) -> str:
        statements = self.connection.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ValueError("仅允许一条 SELECT/WITH 查询，不能执行配置、写入或导出语句")
        return statements[0].query.rstrip().rstrip(";")

    def query(self, sql: str, limit: int = 100, *, final: bool = False) -> dict[str, Any]:
        sql = self.validate(sql)
        limit = self.max_result_rows if final else min(max(int(limit), 1), 200)
        started = perf_counter()
        timer = threading.Timer(self.timeout, self.connection.interrupt)
        timer.start()
        try:
            cursor = self.connection.execute(f"SELECT * FROM ({sql}) AS result LIMIT {limit + 1}")
            columns = [item[0] for item in cursor.description]
            rows = cursor.fetchall()
        finally:
            timer.cancel()
            timer.join()
        if len(rows) > limit and final:
            raise ValueError(f"最终结果超过 {limit} 行，已拒绝截断交付；请检查粒度或提高配置上限")
        data = json.loads(
            json.dumps(rows[:limit], default=json_value, ensure_ascii=False, allow_nan=False)
        )
        if not final:
            data = [
                [value[:1000] if isinstance(value, str) else value for value in row] for row in data
            ]
        return {
            "columns": columns,
            "rows": data,
            "row_count": len(data),
            "truncated": len(rows) > limit,
            "elapsed_seconds": round(perf_counter() - started, 4),
        }

    def schema(self, table: str) -> dict[str, Any]:
        metadata = next((item for item in self.manifest["tables"] if item["name"] == table), None)
        if metadata is None:
            raise ValueError(f"未知表：{table}，请先查看目录")
        return {**metadata, "sample": self.query(f"SELECT * FROM {ident(table)}", 3)}


class Solution:
    """只允许修改 solution.sql，版本变化立即使旧结果失效。"""

    def __init__(self, directory: Path, runtime: QueryRuntime) -> None:
        self.directory = directory
        self.runtime = runtime
        self.path = directory / "solution.sql"
        self.result_path = directory / "result.csv"
        self.last_result: dict[str, Any] | None = None
        self.executed_revision: str | None = None
        directory.mkdir(parents=True, exist_ok=True)
        atomic_text(self.path, "-- 请用一条只读查询实现目标列、粒度、关联和排序。\n")
        self.result_path.unlink(missing_ok=True)

    def read(self) -> dict[str, str]:
        sql = self.path.read_text()
        return {"sql": sql, "revision": hashlib.sha256(sql.encode()).hexdigest()}

    def edit(self, sql: str, expected_revision: str) -> dict[str, str]:
        if expected_revision != self.read()["revision"]:
            raise ValueError("程序版本已变化，请重新读取后编辑")
        self.runtime.validate(sql)
        atomic_text(self.path, sql)
        self.last_result = None
        self.executed_revision = None
        self.result_path.unlink(missing_ok=True)
        return self.read()

    def run(self) -> dict[str, Any]:
        self.last_result = None
        self.executed_revision = None
        self.result_path.unlink(missing_ok=True)
        program = self.read()
        result = self.runtime.query(program["sql"], final=True)
        columns = result["columns"]
        if not columns or any(not name for name in columns) or len(set(columns)) != len(columns):
            raise ValueError("结果列名必须非空且唯一")
        stream = io.StringIO(newline="")
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(result["rows"])
        atomic_text(self.result_path, stream.getvalue())
        self.last_result = result
        self.executed_revision = program["revision"]
        return {key: value for key, value in result.items() if key != "rows"} | {
            "sample": result["rows"][:5],
            "revision": self.executed_revision,
            "non_null_counts": {
                col: sum(row[i] is not None for row in result["rows"])
                for i, col in enumerate(columns)
            },
            "status": "success",
        }

    def submit(self) -> dict[str, Any]:
        if (
            self.last_result is None
            or self.executed_revision != self.read()["revision"]
            or not self.result_path.exists()
        ):
            raise ValueError("当前程序尚未成功运行；请运行并检查新结果后再提交")
        return self.last_result
