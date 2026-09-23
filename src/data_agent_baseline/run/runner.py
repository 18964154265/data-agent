from __future__ import annotations

import csv
import io
import json
import multiprocessing
import os
import re
import signal
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

from data_agent_baseline.agents.model import MonitoredModel, OpenAIModelAdapter
from data_agent_baseline.agents.react import ReActAgent, ReActAgentConfig
from data_agent_baseline.benchmark.dataset import DABenchPublicDataset
from data_agent_baseline.config import AppConfig, resolve_api_key
from data_agent_baseline.data.catalog import CatalogBuilder
from data_agent_baseline.data.query import QueryRuntime, Solution
from data_agent_baseline.etl.pipeline import DocumentETL
from data_agent_baseline.storage import EventLog, atomic_text, write_json
from data_agent_baseline.tools.analysis import ANALYSIS_PROMPT, create_analysis_tools
from data_agent_baseline.tools.registry import ToolRegistry
from data_agent_baseline.tools.filesystem import resolve_context_path


@dataclass(frozen=True, slots=True)
class TaskRunArtifacts:
    task_id: str
    task_output_dir: Path
    prediction_csv_path: Path | None
    trace_path: Path
    succeeded: bool
    failure_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_output_dir": str(self.task_output_dir),
            "prediction_csv_path": str(self.prediction_csv_path)
            if self.prediction_csv_path
            else None,
            "trace_path": str(self.trace_path),
            "succeeded": self.succeeded,
            "failure_reason": self.failure_reason,
        }


def create_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def resolve_run_id(run_id: str | None = None) -> str:
    if run_id is None:
        return create_run_id()
    normalized = str(run_id).strip()
    if not normalized or normalized in {".", ".."} or "/" in normalized or "\\" in normalized:
        raise ValueError("run_id 必须是非空的单个目录名")
    return normalized


def create_run_output_dir(output_root: Path, *, run_id: str | None = None) -> tuple[str, Path]:
    effective_run_id = resolve_run_id(run_id)
    run_output_dir = output_root / effective_run_id
    run_output_dir.mkdir(parents=True, exist_ok=False)
    return effective_run_id, run_output_dir


def build_model_adapter(config: AppConfig):
    return OpenAIModelAdapter(
        model=config.agent.model,
        api_base=config.agent.api_base,
        api_key=resolve_api_key(config.agent),
        temperature=config.agent.temperature,
        timeout=config.agent.request_timeout_seconds,
        max_retries=config.agent.request_retries,
    )


def _failure(task_id: str, reason: str, code: str = "task_error") -> dict[str, Any]:
    return {
        "task_id": task_id,
        "answer": None,
        "steps": [],
        "failure_reason": reason,
        "failure_code": code,
        "succeeded": False,
    }


def _prepare(task, config, model, directory, log):
    started = perf_counter()
    builder = CatalogBuilder(task.context_dir, directory / "workspace")
    knowledge_path = task.context_dir / "knowledge.md"
    knowledge = (
        resolve_context_path(task, "knowledge.md").read_text() if knowledge_path.exists() else ""
    )
    reports = []
    try:
        documents = builder.load_structured()
        log.emit(
            "sources_loaded",
            tables=builder.tables,
            errors=builder.errors,
            elapsed_seconds=perf_counter() - started,
        )
        etl = DocumentETL(
            model,
            config.etl,
            config.etl.cache_dir,
            log,
            {
                "model": config.agent.model,
                "api_base": config.agent.api_base,
                "temperature": config.agent.temperature,
            },
        )
        # 固定结构化 Schema，避免前一文档的抽取结果影响下一文档缓存键。
        structured = list(builder.tables)
        for index, path in enumerate(documents):
            source = path.relative_to(task.context_dir).as_posix()
            try:
                report = etl.extract(
                    path,
                    knowledge,
                    structured,
                    directory / "etl" / f"{index}_{path.stem}.json",
                    task.question,
                )
                metadata = {
                    "source": source,
                    "status": report["status"],
                    "metrics": report["metrics"],
                    "cache_hit": report["cache_hit"],
                    "rejects": len(report["rejects"]),
                    "conflicts": len(report["conflicts"]),
                    "failed_chunks": len(report["failed_chunks"]),
                }
                if report["records"] and report["status"] != "failed":
                    columns = {col["name"]: col["type"] for col in report["schema"]["columns"]}
                    table = builder.add_records(
                        report["schema"]["table"],
                        source,
                        report["records"],
                        columns,
                        etl_status=report["status"],
                        schema=report["schema"],
                    )
                    metadata["table"] = table
                reports.append(metadata)
            except Exception as exc:
                reports.append({"source": source, "status": "failed", "error": str(exc)})
                log.emit("etl_failed", source=source, error=str(exc))
        manifest = builder.finish(reports)
    except BaseException:
        builder.connection.close()
        raise
    if manifest["errors"]:
        raise ValueError("结构化来源加载失败，请查看 workspace/catalog.json")
    if not config.etl.allow_partial and any(r["status"] != "complete" for r in reports):
        raise ValueError("ETL 未完整通过证据/质量检查，请查看 etl/；未发布不完整答案")
    if not manifest["tables"]:
        raise ValueError("任务没有可查询表")
    log.emit("preparation_complete", elapsed_seconds=perf_counter() - started)
    return manifest, knowledge


def _run_single_task_core(
    *,
    task_id: str,
    config: AppConfig,
    directory: Path,
    model=None,
    tools: ToolRegistry | None = None,
) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    log = EventLog(directory / "events.jsonl")
    log.emit("task_started", task_id=task_id, model=config.agent.model)
    if model is None and not resolve_api_key(config.agent):
        return _failure(
            task_id, "缺少模型 API key；请配置 api_key 或 api_key_env", "configuration_error"
        )
    task = DABenchPublicDataset(config.dataset.root_path).get_task(task_id)
    monitored = MonitoredModel(
        model or build_model_adapter(config), log, config.agent.max_model_calls
    )
    if tools is None:
        try:
            manifest, knowledge = _prepare(task, config, monitored, directory, log)
        except Exception as exc:
            result = _failure(task_id, str(exc), "preparation_error")
            result.update(model_calls=monitored.calls, usage=monitored.usage)
            return result
    else:
        manifest, knowledge = {}, ""
    attempts = []
    result = _failure(task_id, "尚未执行尝试")
    for number in range(1, config.agent.attempts + 1):
        attempt_dir = directory / "attempts" / str(number)
        attempt_dir.mkdir(parents=True, exist_ok=True)
        log.emit("attempt_started", attempt=number)
        steps = []

        def on_step(step):
            steps.append(step.to_dict())
            log.emit("step", attempt=number, **step.to_dict())
            write_json(
                attempt_dir / "trace.json",
                {"task_id": task_id, "steps": steps, "status": "running", "attempt": number},
            )

        runtime = None
        try:
            if tools is None:
                runtime = QueryRuntime(
                    directory / "workspace",
                    config.run.query_timeout_seconds,
                    config.run.max_result_rows,
                )
                solution = Solution(attempt_dir, runtime)
                registry = create_analysis_tools(runtime, solution, knowledge)
                system_prompt = ANALYSIS_PROMPT
                context = (
                    "治理说明：\n"
                    + knowledge[:12000]
                    + "\n数据目录摘要：\n"
                    + json.dumps(
                        [
                            {
                                "name": t["name"],
                                "source": t["source"],
                                "row_count": t["row_count"],
                                "columns": [c["name"] for c in t["columns"]],
                                "etl_status": t.get("etl_status"),
                            }
                            for t in manifest["tables"]
                        ],
                        ensure_ascii=False,
                    )
                )
            else:
                registry, system_prompt, context = tools, None, ""
            agent = ReActAgent(
                model=monitored,
                tools=registry,
                config=ReActAgentConfig(config.agent.max_steps, config.agent.max_context_chars),
                system_prompt=system_prompt,
                task_context=context,
                on_step=on_step,
            )
            result = agent.run(task).to_dict()
            result["failure_code"] = (
                None
                if result["succeeded"]
                else (
                    "model_error"
                    if str(result["failure_reason"]).startswith("模型请求失败")
                    else "steps_exhausted"
                )
            )
        except Exception as exc:
            result = _failure(task_id, str(exc), "attempt_error")
            result["steps"] = steps
        finally:
            if runtime is not None:
                runtime.close()
        write_json(attempt_dir / "trace.json", {**result, "attempt": number})
        attempts.append(
            {
                "attempt": number,
                "succeeded": result["succeeded"],
                "failure_reason": result["failure_reason"],
                "trace_path": str(attempt_dir / "trace.json"),
            }
        )
        log.emit("attempt_finished", **attempts[-1])
        if result["succeeded"] or monitored.calls >= config.agent.max_model_calls:
            break
    result.update(attempts=attempts, model_calls=monitored.calls, usage=monitored.usage)
    return result


def _worker(task_id: str, config: AppConfig, directory: Path) -> None:
    if os.name == "posix":
        os.setsid()
    try:
        result = _run_single_task_core(task_id=task_id, config=config, directory=directory)
    except BaseException as exc:
        result = _failure(task_id, f"任务未捕获异常：{exc}")
    write_json(directory / "_worker_result.json", result)


def _stop(process) -> None:
    # spawn 的任务进程拥有独立会话；POSIX 下同时清理其派生进程。
    if os.name == "posix":
        try:
            if os.getpgid(process.pid) == process.pid:
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
        except ProcessLookupError:
            pass
    else:
        process.terminate()
    process.join(1)
    if process.is_alive():
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            process.kill()
        process.join()


def _run_with_timeout(task_id, config, directory):
    if config.run.task_timeout_seconds <= 0:
        return _run_single_task_core(task_id=task_id, config=config, directory=directory)
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=_worker, args=(task_id, config, directory))
    process.start()
    try:
        process.join(config.run.task_timeout_seconds)
        if process.is_alive():
            _stop(process)
            result = _failure(
                task_id, f"任务超过 {config.run.task_timeout_seconds} 秒", "task_timeout"
            )
            traces = sorted(
                (directory / "attempts").glob("*/trace.json"),
                key=lambda path: int(path.parent.name),
            )
            if traces:
                result["steps"] = json.loads(traces[-1].read_text()).get("steps", [])
            return result
        output = directory / "_worker_result.json"
        if not output.exists():
            return _failure(
                task_id, f"任务进程退出且没有返回结果，退出码 {process.exitcode}", "worker_exit"
            )
        result = json.loads(output.read_text())
        output.unlink()
        return result
    finally:
        if process.is_alive():
            _stop(process)
        process.close()


def _write_task_outputs(task_id: str, run_output_dir: Path, result: dict) -> TaskRunArtifacts:
    directory = run_output_dir / task_id
    directory.mkdir(parents=True, exist_ok=True)
    trace_path = directory / "trace.json"
    prediction = directory / "prediction.csv"
    prediction.unlink(missing_ok=True)
    if result.get("succeeded") and isinstance(result.get("answer"), dict):
        stream = io.StringIO(newline="")
        writer = csv.writer(stream)
        writer.writerow(result["answer"]["columns"])
        writer.writerows(result["answer"]["rows"])
        atomic_text(prediction, stream.getvalue())
    write_json(trace_path, result)
    return TaskRunArtifacts(
        task_id,
        directory,
        prediction if prediction.exists() else None,
        trace_path,
        bool(result["succeeded"]),
        result.get("failure_reason"),
    )


def run_single_task(
    *,
    task_id: str,
    config: AppConfig,
    run_output_dir: Path,
    model=None,
    tools: ToolRegistry | None = None,
) -> TaskRunArtifacts:
    if not re.fullmatch(r"task_\d+", task_id):
        raise ValueError("任务 ID 必须是 task_<数字>")
    directory = run_output_dir / task_id
    directory.mkdir(parents=True, exist_ok=False)
    started = perf_counter()
    try:
        if model is None and tools is None:
            result = _run_with_timeout(task_id, config, directory)
        else:
            result = _run_single_task_core(
                task_id=task_id, config=config, directory=directory, model=model, tools=tools
            )
    except Exception as exc:
        result = _failure(task_id, str(exc))
    result["e2e_elapsed_seconds"] = round(perf_counter() - started, 3)
    return _write_task_outputs(task_id, run_output_dir, result)


def run_benchmark(
    *,
    config: AppConfig,
    model=None,
    tools: ToolRegistry | None = None,
    limit: int | None = None,
    progress_callback: Callable[[TaskRunArtifacts], None] | None = None,
) -> tuple[Path, list[TaskRunArtifacts]]:
    workers = config.run.max_workers
    if workers < 1:
        raise ValueError("max_workers 必须至少为 1")
    if model is not None or tools is not None:
        workers = 1
    run_id, output = create_run_output_dir(config.run.output_dir, run_id=config.run.run_id)
    dataset = DABenchPublicDataset(config.dataset.root_path)
    task_ids = config.dataset.task_ids or dataset.list_task_ids()
    task_ids = list(dict.fromkeys(task_ids))
    if any(not re.fullmatch(r"task_\d+", tid) for tid in task_ids):
        raise ValueError("任务 ID 必须是 task_<数字>")
    if limit is not None:
        task_ids = task_ids[:limit]
    indexed = {}

    def save_summary():
        artifacts = [indexed[i] for i in sorted(indexed)]
        write_json(
            output / "summary.json",
            {
                "run_id": run_id,
                "planned_task_count": len(task_ids),
                "task_ids": task_ids,
                "task_count": len(artifacts),
                "succeeded_task_count": sum(a.succeeded for a in artifacts),
                "max_workers": workers,
                "tasks": [a.to_dict() for a in artifacts],
            },
        )

    def completed(index, artifact):
        indexed[index] = artifact
        save_summary()
        if progress_callback:
            progress_callback(artifact)

    save_summary()
    if workers == 1:
        for index, task_id in enumerate(task_ids):
            completed(
                index,
                run_single_task(
                    task_id=task_id, config=config, run_output_dir=output, model=model, tools=tools
                ),
            )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    run_single_task, task_id=task_id, config=config, run_output_dir=output
                ): index
                for index, task_id in enumerate(task_ids)
            }
            for future in as_completed(futures):
                completed(futures[future], future.result())
    return output, [indexed[i] for i in sorted(indexed)]
