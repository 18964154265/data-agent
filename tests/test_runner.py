import json
from dataclasses import replace

import pytest

from data_agent_baseline.agents.model import ScriptedModelAdapter
from data_agent_baseline.agents.react import ReActAgent, ReActAgentConfig
from data_agent_baseline.benchmark.dataset import DABenchPublicDataset
from data_agent_baseline.config import AgentConfig, AppConfig, DatasetConfig, ETLConfig, RunConfig
from data_agent_baseline.run.runner import run_benchmark, run_single_task
from data_agent_baseline.tools.registry import create_default_tool_registry


def action(name, **kwargs):
    return json.dumps({"thought": "验证当前数据", "action": name, "action_input": kwargs})


def make_task(root, name="task_1", document=False):
    context = root / name / "context"
    context.mkdir(parents=True)
    (context.parent / "task.json").write_text(
        json.dumps({"task_id": name, "difficulty": "test", "question": "列举 Alice 的金额"})
    )
    (context / "amounts.csv").write_text("id,amount\n001,12\n002,15\n")
    if document:
        (context / "members.md").write_text("ID 1 Alice\n\nID 2 Bob")
    else:
        (context / "members.json").write_text(
            json.dumps({"records": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]})
        )


class SQLModel:
    def __init__(self, fail_once=False):
        self.fail_once = fail_once
        self.seen_clean = []

    def complete(self, messages):
        assistant = [json.loads(m.content) for m in messages if m.role == "assistant"]
        if not assistant:
            return action("list_tables")
        last = assistant[-1]["action"]
        observation = json.loads(messages[-1].content.removeprefix("Observation:\n"))
        if last == "list_tables":
            return action("read_solution")
        if last == "read_solution":
            source = observation["content"]
            self.seen_clean.append("SELECT" not in source["sql"])
            return action(
                "edit_solution",
                revision=source["revision"],
                sql=(
                    "SELECT m.name, a.amount FROM members m JOIN amounts a "
                    "ON CAST(m.id AS BIGINT)=CAST(a.id AS BIGINT) WHERE m.name='Alice'"
                ),
            )
        if last == "edit_solution":
            return action("run_solution")
        if last == "run_solution":
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("模拟模型在执行成功后连接失败")
            return action("submit_answer")
        raise AssertionError(f"非预期工具：{last}, {observation}")


class ETLSQLModel(SQLModel):
    def complete(self, messages):
        if "制定抽取 Schema" in messages[0].content:
            return json.dumps(
                {
                    "table": "members",
                    "columns": [
                        {"name": "id", "type": "BIGINT"},
                        {"name": "name", "type": "VARCHAR"},
                    ],
                    "primary_key": ["id"],
                    "identity_labels": ["ID"],
                }
            )
        if "ETL 抽取器" in messages[0].content:
            paragraphs = json.loads(messages[1].content)["paragraphs"]
            records = []
            for p in paragraphs:
                _, id_, name = p["text"].split()
                records.append(
                    {
                        "values": {"id": int(id_), "name": name},
                        "evidence": {
                            field: {"paragraph": p["id"], "quote": p["text"]}
                            for field in ["id", "name"]
                        },
                    }
                )
            return json.dumps(
                {"records": records, "covered_paragraphs": [p["id"] for p in paragraphs]}
            )
        return super().complete(messages)


def config_for(tmp_path):
    return AppConfig(
        dataset=DatasetConfig(tmp_path / "input"),
        agent=AgentConfig(max_steps=8, attempts=2),
        run=RunConfig(output_dir=tmp_path / "runs", task_timeout_seconds=10),
        etl=ETLConfig(cache_dir=tmp_path / "cache", max_workers=1),
    )


@pytest.mark.parametrize("document", [False, True])
def test_full_pipeline_and_incremental_trace(tmp_path, document):
    config = config_for(tmp_path)
    make_task(config.dataset.root_path, document=document)
    model = ETLSQLModel() if document else SQLModel()
    artifact = run_single_task(
        task_id="task_1", config=config, run_output_dir=tmp_path / "run", model=model
    )
    assert artifact.succeeded, artifact.failure_reason
    trace = json.loads(artifact.trace_path.read_text())
    assert trace["answer"]["rows"] == [["Alice", 12]]
    assert (artifact.task_output_dir / "attempts/1/solution.sql").exists()
    events = [
        json.loads(line)
        for line in (artifact.task_output_dir / "events.jsonl").read_text().splitlines()
    ]
    assert any(e["event"] == "model_request" for e in events)
    assert any(e["event"] == "step" and e["action"] == "run_solution" for e in events)
    if document:
        assert (
            json.loads(next((artifact.task_output_dir / "etl").glob("*.json")).read_text())[
                "status"
            ]
            == "complete"
        )


def test_retry_clean_program_preserves_failed_attempt(tmp_path):
    config = config_for(tmp_path)
    make_task(config.dataset.root_path)
    model = SQLModel(fail_once=True)
    artifact = run_single_task(
        task_id="task_1", config=config, run_output_dir=tmp_path / "run", model=model
    )
    assert artifact.succeeded
    assert model.seen_clean == [True, True]
    failed = json.loads((artifact.task_output_dir / "attempts/1/trace.json").read_text())
    assert failed["steps"][-1]["action"] == "run_solution"
    assert not failed["succeeded"]
    assert len(json.loads(artifact.trace_path.read_text())["attempts"]) == 2


def test_one_invalid_task_does_not_abort_batch(tmp_path):
    config = config_for(tmp_path)
    make_task(config.dataset.root_path)
    broken = config.dataset.root_path / "task_2"
    broken.mkdir()
    (broken / "task.json").write_text("{}")
    output, artifacts = run_benchmark(config=config, model=SQLModel())
    assert [a.succeeded for a in artifacts] == [True, False]
    assert json.loads((output / "summary.json").read_text())["task_count"] == 2


def test_serial_benchmark_enforces_process_timeout(tmp_path):
    config = config_for(tmp_path)
    make_task(config.dataset.root_path)
    # 进程启动/加载也在任务期限内；无需外部服务即可验证串行路径没有绕过包装。
    config = replace(config, run=replace(config.run, max_workers=1, task_timeout_seconds=0.001))
    _, artifacts = run_benchmark(config=config)
    result = json.loads(artifacts[0].trace_path.read_text())
    assert result["failure_code"] == "task_timeout"
    assert not artifacts[0].prediction_csv_path


def test_spawn_worker_reports_missing_key_without_crash(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    config = config_for(tmp_path)
    make_task(config.dataset.root_path)
    config = replace(config, agent=replace(config.agent, api_key_env="TEST_ABSENT_KEY"))
    artifact = run_single_task(task_id="task_1", config=config, run_output_dir=tmp_path / "run")
    assert not artifact.succeeded
    assert "API key" in artifact.failure_reason
    assert (artifact.task_output_dir / "events.jsonl").exists()


def test_react_parse_repair(tmp_path):
    config = config_for(tmp_path)
    make_task(config.dataset.root_path)
    task = DABenchPublicDataset(config.dataset.root_path).get_task("task_1")
    model = ScriptedModelAdapter(["invalid json", action("answer", columns=["value"], rows=[[1]])])
    result = ReActAgent(
        model=model, tools=create_default_tool_registry(), config=ReActAgentConfig(max_steps=2)
    ).run(task)
    assert result.succeeded
    assert result.steps[0].action == "__error__"


def test_failed_tool_keeps_action_and_parameters(tmp_path):
    config = config_for(tmp_path)
    make_task(config.dataset.root_path)
    task = DABenchPublicDataset(config.dataset.root_path).get_task("task_1")
    model = ScriptedModelAdapter([action("read_csv", path="../outside.csv")])
    result = ReActAgent(
        model=model, tools=create_default_tool_registry(), config=ReActAgentConfig(max_steps=1)
    ).run(task)
    assert result.steps[0].action == "read_csv"
    assert result.steps[0].action_input == {"path": "../outside.csv"}
    assert result.steps[0].observation["error_type"] == "ValueError"
