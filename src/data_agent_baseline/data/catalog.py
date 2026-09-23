"""将任务的结构化来源物化为独立 DuckDB，保留源表和列名。"""

from __future__ import annotations

import hashlib
import json
import tempfile

import ijson
import re
import sqlite3
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from data_agent_baseline.storage import write_json


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class CatalogBuilder:
    def __init__(self, context: Path, workspace: Path) -> None:
        self.context = context.resolve()
        self.workspace = workspace
        workspace.mkdir(parents=True, exist_ok=True)
        self.database = workspace / "data.duckdb"
        self.connection = duckdb.connect(str(self.database))
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self.tables: list[dict[str, Any]] = []
        self.sources: list[dict[str, Any]] = []
        self.errors: list[dict[str, str]] = []
        self._names: set[str] = set()

    def name(self, suggested: str, source: str) -> str:
        name = re.sub(r"[^\w]+", "_", suggested).strip("_") or "data"
        if name.casefold() in self._names:
            name += "_" + hashlib.sha256(source.encode()).hexdigest()[:8]
        if name.casefold() in self._names:
            raise ValueError(f"重复数据源：{source}")
        self._names.add(name.casefold())
        return name

    def describe(self, name: str, source: str, **metadata: Any) -> None:
        columns = self.connection.execute(f"DESCRIBE {ident(name)}").fetchall()
        count = self.connection.execute(f"SELECT count(*) FROM {ident(name)}").fetchone()[0]
        self.tables.append(
            {
                "name": name,
                "source": source,
                "row_count": count,
                "columns": [{"name": col[0], "type": col[1]} for col in columns],
                **metadata,
            }
        )

    def add_records(
        self,
        suggested: str,
        source: str,
        records: list[dict[str, Any]],
        columns: dict[str, str] | None = None,
        **metadata: Any,
    ) -> str:
        name = self.name(suggested, source)
        if columns:
            self.connection.execute(
                f"CREATE TABLE {ident(name)} ("
                + ", ".join(f"{ident(key)} {kind}" for key, kind in columns.items())
                + ")"
            )
        if records:
            # Arrow 类型来自规范化后的记录；不经 CSV 往返，保留字符串 ID 和空值。
            arrow = pa.Table.from_pylist(records)
            self.connection.register("_incoming", arrow)
            try:
                if columns:
                    self.connection.execute(
                        f"INSERT INTO {ident(name)} BY NAME SELECT * FROM _incoming"
                    )
                else:
                    self.connection.execute(
                        f"CREATE TABLE {ident(name)} AS SELECT * FROM _incoming"
                    )
            finally:
                self.connection.unregister("_incoming")
        elif not columns:
            raise ValueError("空记录来源缺少列定义")
        self.describe(name, source, **metadata)
        return name

    def load_structured(self) -> list[Path]:
        documents = []
        for path in sorted(self.context.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(self.context).as_posix()
            if not path.resolve().is_relative_to(self.context):
                self.errors.append({"source": rel, "error": "来源路径越出上下文目录"})
                continue
            suffix = path.suffix.lower()
            self.sources.append(
                {"path": rel, "size": path.stat().st_size, "sha256": fingerprint(path)}
            )
            if suffix in {".md", ".txt", ".pdf"}:
                if path.name.lower() != "knowledge.md":
                    documents.append(path)
                continue
            try:
                if suffix == ".csv":
                    self._csv(path, rel)
                elif suffix == ".json":
                    self._json(path, rel)
                elif suffix in {".db", ".sqlite", ".sqlite3"}:
                    self._sqlite(path, rel)
                else:
                    self.errors.append({"source": rel, "error": f"尚不支持的来源格式：{suffix}"})
            except Exception as exc:
                self.errors.append({"source": rel, "error": str(exc)})
        return documents

    def _csv(self, path: Path, source: str) -> None:
        name = self.name(path.stem, source)
        # 完整类型采样避免后段出现字符串而加载失败，DuckDB 流式执行。
        self.connection.execute(
            f"CREATE TABLE {ident(name)} AS SELECT * FROM read_csv(?, header=true, "
            "sample_size=-1, nullstr=['', 'NULL', 'null', 'NaN'], auto_detect=true)",
            [str(path)],
        )
        self.describe(name, source)

    def _json(self, path: Path, source: str) -> None:
        # 顶层 records 数组可能达数百 MB。先流式转为逐行 JSON，避免整对象分配。
        with path.open("rb") as handle:
            beginning = handle.read(1024).lstrip()
        prefix = "item" if beginning.startswith(b"[") else "records.item"
        name = self.name(path.stem, source)
        count = 0
        with tempfile.TemporaryDirectory(dir=self.workspace) as temporary:
            normalized = Path(temporary) / "records.jsonl"
            with path.open("rb") as handle, normalized.open("w", encoding="utf-8") as output:
                for record in ijson.items(handle, prefix, use_float=True):
                    if not isinstance(record, dict):
                        raise ValueError("JSON 记录必须为对象")
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
            if not count:
                raise ValueError("JSON 没有记录或不符合数组 / {records: [...]} 契约")
            relation = self.connection.read_json(
                str(normalized), sample_size=-1, format="newline_delimited"
            )
            relation.create(name)
        self.describe(name, source)

    def _sqlite(self, path: Path, source: str) -> None:
        # 使用标准库只读连接，不安装联网扩展；分批搬运，限制 Python 峰值内存。
        with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
            for (table,) in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ):
                name = self.name(table, source + ":" + table)
                info = db.execute(f"PRAGMA table_info({ident(table)})").fetchall()
                types = {}
                for col in info:
                    declared = col[2].upper()
                    types[col[1]] = (
                        "BIGINT"
                        if "INT" in declared
                        else "DOUBLE"
                        if any(t in declared for t in ("REAL", "FLOA", "DOUB", "NUM", "DEC"))
                        else "BLOB"
                        if "BLOB" in declared
                        else "VARCHAR"
                    )
                self.connection.execute(
                    f"CREATE TABLE {ident(name)} ("
                    + ", ".join(f"{ident(key)} {kind}" for key, kind in types.items())
                    + ")"
                )
                cursor = db.execute(f"SELECT * FROM {ident(table)}")
                keys = list(types)
                while batch := cursor.fetchmany(10000):
                    data = {key: [row[i] for row in batch] for i, key in enumerate(keys)}
                    arrow = pa.table(data)
                    self.connection.register("_sqlite_batch", arrow)
                    try:
                        self.connection.execute(
                            f"INSERT INTO {ident(name)} SELECT * FROM _sqlite_batch"
                        )
                    finally:
                        self.connection.unregister("_sqlite_batch")
                self.describe(name, source, source_table=table)

    def finish(self, etl: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        manifest = {
            "tables": self.tables,
            "sources": self.sources,
            "errors": self.errors,
            "etl": etl or [],
        }
        write_json(self.workspace / "catalog.json", manifest)
        self.connection.close()
        return manifest
