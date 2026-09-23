import json
from dataclasses import replace

import pytest

from data_agent_baseline.config import ETLConfig
from data_agent_baseline.etl.documents import Paragraph, chunks
from data_agent_baseline.etl.pipeline import DocumentETL, merge_records
from data_agent_baseline.etl.schema import Column, DocumentSchema, normalize
from data_agent_baseline.storage import EventLog


SCHEMA = {
    "table": "members",
    "columns": [
        {"name": "id", "type": "VARCHAR"},
        {"name": "name", "type": "VARCHAR"},
        {"name": "amount", "type": "DOUBLE", "unit": "USD"},
    ],
    "primary_key": ["id"],
    "identity_labels": ["ID"],
}


def record(values, paragraph, quote):
    return {
        "values": values,
        "evidence": {
            name: {"paragraph": paragraph, "quote": quote}
            for name, value in values.items()
            if value is not None
        },
    }


def test_evidence_merge_conflicts_and_rejects():
    text = {0: "ID 001 Alice", 1: "ID 001 amount 12", 2: "ID 001 amount 13"}
    data = [
        record({"id": "001", "name": "Alice"}, 0, text[0]),
        record({"id": "001", "amount": 12}, 1, text[1]),
        record({"id": "001", "amount": 13}, 2, text[2]),
        record({"id": "002", "name": "Ghost"}, 0, "invented quote"),
    ]
    rows, provenance, rejects, conflicts = merge_records(DocumentSchema(**SCHEMA), data, text)
    assert rows == [{"id": "001", "name": "Alice", "amount": None}]
    assert len(conflicts) == 1 and len(rejects) == 1
    assert provenance[0]["fields"]["amount"][0]["paragraph"] == 1


def test_composite_keys_do_not_merge_visits():
    schema = DocumentSchema(
        table="labs",
        columns=[Column(name="id"), Column(name="date"), Column(name="value", type="DOUBLE")],
        primary_key=["id", "date"],
    )
    text = {0: "ID 1 date 2020-01-01 value 2", 1: "ID 1 date 2020-01-02 value 3"}
    data = [
        record({"id": "1", "date": "2020-01-01", "value": 2}, 0, text[0]),
        record({"id": "1", "date": "2020-01-02", "value": 3}, 1, text[1]),
    ]
    rows, _, rejects, conflicts = merge_records(schema, data, text)
    assert len(rows) == 2 and not rejects and not conflicts


def test_no_speculative_units_and_exact_integer():
    with pytest.raises(ValueError):
        normalize("12%", Column(name="ratio", type="DOUBLE"))
    assert normalize("9007199254740993", Column(name="id", type="BIGINT")) == 9007199254740993
    assert normalize("001", Column(name="id")) == "001"


def test_grouping_preserves_all_paragraphs_and_entity():
    paragraphs = [
        Paragraph(0, "ID 1 " + "x" * 20),
        Paragraph(1, "ID 2 " + "x" * 20),
        Paragraph(2, "ID 1 " + "y" * 20),
    ]
    blocks = chunks(paragraphs, ["ID"], 60)
    assert sorted(p.id for block in blocks for p in block) == [0, 1, 2]
    assert [p.id for p in blocks[0]] == [0, 2]


class ExtractionModel:
    def __init__(self):
        self.calls = 0

    def complete(self, messages):
        self.calls += 1
        if "制定抽取 Schema" in messages[0].content:
            return json.dumps(SCHEMA)
        payload = json.loads(messages[1].content)
        paragraphs = payload["paragraphs"]
        return json.dumps(
            {
                "records": [
                    record(
                        {"id": "001", "name": "Alice", "amount": 12},
                        paragraphs[0]["id"],
                        paragraphs[0]["text"],
                    )
                ],
                "covered_paragraphs": [p["id"] for p in paragraphs],
            }
        )


def test_cache_content_model_and_question_invalidation(tmp_path):
    path = tmp_path / "members.md"
    path.write_text("ID 001 Alice amount 12")
    model = ExtractionModel()
    etl = DocumentETL(
        model,
        ETLConfig(max_workers=1),
        tmp_path / "cache",
        EventLog(tmp_path / "events.jsonl"),
        {"model": "offline-v1"},
    )

    def extract(question="列举姓名"):
        return etl.extract(path, "", [], tmp_path / "report.json", question)

    first = extract()
    assert first["status"] == "complete"
    assert model.calls == 2
    assert extract()["cache_hit"] and model.calls == 2
    extract("列举金额")
    assert model.calls == 4
    path.write_text("ID 001 Alice amount 12\n")
    extract()
    assert model.calls == 6
    etl.model_identity = {"model": "offline-v2"}
    extract()
    assert model.calls == 8


def test_missing_coverage_retried_and_reported(tmp_path):
    class Incomplete(ExtractionModel):
        def complete(self, messages):
            if "制定抽取 Schema" in messages[0].content:
                return super().complete(messages)
            self.calls += 1
            return '{"records": [], "covered_paragraphs": []}'

    path = tmp_path / "members.md"
    path.write_text("ID 001 Alice amount 12")
    model = Incomplete()
    etl = DocumentETL(
        model,
        replace(ETLConfig(), retries=1),
        tmp_path / "cache",
        EventLog(tmp_path / "events.jsonl"),
        {},
    )
    result = etl.extract(path, "", [], tmp_path / "report.json")
    assert result["status"] == "failed"
    assert len(result["failed_chunks"]) == 1
    assert model.calls == 3
    assert not list((tmp_path / "cache").glob("*.json"))


def test_claimed_coverage_cannot_hide_missing_entities(tmp_path):
    class Omitting(ExtractionModel):
        def complete(self, messages):
            if "制定抽取 Schema" in messages[0].content:
                return super().complete(messages)
            self.calls += 1
            paragraphs = json.loads(messages[1].content)["paragraphs"]
            return json.dumps({"records": [], "covered_paragraphs": [p["id"] for p in paragraphs]})

    source = tmp_path / "members.md"
    source.write_text("ID 001 Alice amount 12")
    etl = DocumentETL(
        Omitting(),
        ETLConfig(max_workers=1),
        tmp_path / "cache",
        EventLog(tmp_path / "events.jsonl"),
        {},
    )
    result = etl.extract(source, "", [], tmp_path / "report.json")
    assert result["status"] == "failed"
    assert "遗漏" in result["failed_chunks"][0]["error"]


def test_partial_block_failure_keeps_successful_records(tmp_path):
    class Partial(ExtractionModel):
        def complete(self, messages):
            if "制定抽取 Schema" in messages[0].content:
                return super().complete(messages)
            paragraphs = json.loads(messages[1].content)["paragraphs"]
            if "ID 002" in paragraphs[0]["text"]:
                raise RuntimeError("模拟单块失败")
            return super().complete(messages)

    source = tmp_path / "members.md"
    source.write_text("ID 001 Alice amount 12 " + "x" * 700 + "\n\nID 002 Bob " + "x" * 700)
    etl = DocumentETL(
        Partial(),
        ETLConfig(chunk_chars=1000, max_workers=1, retries=0),
        tmp_path / "cache",
        EventLog(tmp_path / "events.jsonl"),
        {},
    )
    result = etl.extract(source, "", [], tmp_path / "report.json")
    assert result["status"] == "partial"
    assert result["records"][0]["id"] == "001"
    assert len(result["failed_chunks"]) == 1


def test_corrupt_cache_recomputed(tmp_path):
    source = tmp_path / "members.md"
    source.write_text("ID 001 Alice amount 12")
    model = ExtractionModel()
    etl = DocumentETL(
        model, ETLConfig(), tmp_path / "cache", EventLog(tmp_path / "events.jsonl"), {}
    )
    result = etl.extract(source, "", [], tmp_path / "report.json")
    cache_file = tmp_path / "cache" / (result["cache_key"] + ".json")
    bad = json.loads(cache_file.read_text())
    bad["records"][0]["id"] = None
    cache_file.write_text(json.dumps(bad))
    assert not etl.extract(source, "", [], tmp_path / "report.json")["cache_hit"]
    assert model.calls == 4
