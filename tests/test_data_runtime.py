import json
import sqlite3

import pytest

from data_agent_baseline.data.catalog import CatalogBuilder
from data_agent_baseline.data.query import QueryRuntime, Solution


@pytest.fixture
def prepared(tmp_path):
    context = tmp_path / "input"
    context.mkdir()
    (context / "people.csv").write_text("id,full name,age\n1,Alice,20\n2,Bob,\n3,Alice,20\n")
    (context / "scores.json").write_text(
        json.dumps(
            {
                "table": "scores",
                "records": [
                    {"person_id": 1, "score": 9},
                    {"person_id": 2, "score": None},
                    {"person_id": 3, "score": 7},
                ],
            }
        )
    )
    with sqlite3.connect(context / "groups.db") as db:
        db.execute("CREATE TABLE groups (id INTEGER, label TEXT)")
        db.execute("INSERT INTO groups VALUES (1, 'A'), (2, 'B'), (3, 'A')")
    builder = CatalogBuilder(context, tmp_path / "workspace")
    builder.load_structured()
    manifest = builder.finish()
    assert not manifest["errors"]
    runtime = QueryRuntime(tmp_path / "workspace", timeout=1)
    yield runtime
    runtime.close()


def test_mixed_join_preserves_nulls_and_duplicates(prepared):
    result = prepared.query("""SELECT p."full name", p.age, s.score, g.label
        FROM people p JOIN scores s ON s.person_id=p.id
        JOIN groups g ON g.id=p.id ORDER BY p.id""")
    assert result["rows"] == [
        ["Alice", 20, 9, "A"],
        ["Bob", None, None, "B"],
        ["Alice", 20, 7, "A"],
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE people",
        "SELECT 1; SELECT 2",
        "COPY people TO '/tmp/escaped.csv'",
        "SET enable_external_access=true",
        "ATTACH '/tmp/other.duckdb' AS other",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM query('DELETE FROM people')",
    ],
)
def test_queries_cannot_write_or_read_external(prepared, sql):
    with pytest.raises(Exception):
        prepared.query(sql)
    assert prepared.query("SELECT count(*) FROM people")["rows"] == [[3]]


def test_solution_revision_and_stale_output(prepared, tmp_path):
    solution = Solution(tmp_path / "attempt", prepared)
    with pytest.raises(ValueError, match="尚未成功运行"):
        solution.submit()
    first = solution.read()
    solution.edit("SELECT * FROM people", first["revision"])
    with pytest.raises(ValueError, match="版本"):
        solution.edit("SELECT 1", first["revision"])
    result = solution.run()
    assert result["row_count"] == 3
    assert result["non_null_counts"]["age"] == 2
    assert solution.submit()["rows"][1][2] is None
    solution.edit("SELECT * FROM missing_table", solution.read()["revision"])
    with pytest.raises(Exception):
        solution.run()
    assert not solution.result_path.exists()
    with pytest.raises(ValueError):
        solution.submit()


def test_final_rows_never_silently_truncated(prepared):
    prepared.max_result_rows = 2
    with pytest.raises(ValueError, match="拒绝截断"):
        prepared.query("SELECT * FROM people", final=True)
    preview = prepared.query("SELECT * FROM people", limit=2)
    assert preview["truncated"]


def test_query_deadline_and_connection_reuse(prepared):
    prepared.timeout = 0.01
    with pytest.raises(Exception):
        prepared.query("SELECT SUM(a.i*b.i) FROM range(1000000000) a(i), range(1000000000) b(i)")
    assert prepared.query("SELECT 42")["rows"] == [[42]]


def test_source_collision_and_symlink_escape(tmp_path):
    context = tmp_path / "input"
    (context / "a").mkdir(parents=True)
    (context / "b").mkdir()
    for folder in ["a", "b"]:
        (context / folder / "same.csv").write_text("id\n1\n")
    secret = tmp_path / "secret.csv"
    secret.write_text("secret\nprivate\n")
    (context / "escape.csv").symlink_to(secret)
    builder = CatalogBuilder(context, tmp_path / "workspace")
    builder.load_structured()
    manifest = builder.finish()
    assert len({t["name"] for t in manifest["tables"]}) == 2
    assert len(manifest["errors"]) == 1


def test_json_large_wrapper_and_array(tmp_path):
    context = tmp_path / "input"
    context.mkdir()
    records = [{"id": i, "body": "x" * 1000, "code": "001"} for i in range(4000)]
    (context / "wrapped.json").write_text(json.dumps({"records": records}))
    (context / "array.json").write_text(json.dumps(records[:2]))
    builder = CatalogBuilder(context, tmp_path / "workspace")
    builder.load_structured()
    manifest = builder.finish()
    assert not manifest["errors"]
    assert {t["name"]: t["row_count"] for t in manifest["tables"]} == {"array": 2, "wrapped": 4000}
    runtime = QueryRuntime(tmp_path / "workspace")
    try:
        assert runtime.query("SELECT code FROM wrapped LIMIT 1")["rows"] == [["001"]]
    finally:
        runtime.close()


def test_temporal_decimal_values_serialize(prepared):
    result = prepared.query(
        "SELECT TIME '12:30:00', DATE '2020-01-01', INTERVAL '2 days', 1.5::DECIMAL(4,2)"
    )
    assert result["rows"][0] == ["12:30:00", "2020-01-01", "2 days, 0:00:00", "1.50"]


def test_nonfinite_result_rejected(prepared):
    with pytest.raises(ValueError):
        prepared.query("SELECT 1.0/0.0 AS invalid_value", final=True)
