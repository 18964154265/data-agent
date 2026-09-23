from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    root_path: Path = field(default_factory=lambda: PROJECT_ROOT / "public" / "input")
    task_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class AgentConfig:
    model: str = "gpt-4.1-mini"
    api_base: str = "https://api.openai.com/v1"
    api_key: str = ""
    api_key_env: str = "OPENAI_API_KEY"
    max_steps: int = 24
    temperature: float = 0.0
    request_timeout_seconds: float = 90
    request_retries: int = 2
    max_model_calls: int = 256
    max_context_chars: int = 60000
    attempts: int = 2


@dataclass(frozen=True, slots=True)
class ETLConfig:
    chunk_chars: int = 12000
    max_workers: int = 4
    retries: int = 1
    use_cache: bool = True
    cache_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "artifacts" / "etl_cache")
    allow_partial: bool = False


@dataclass(frozen=True, slots=True)
class RunConfig:
    output_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "artifacts" / "runs")
    run_id: str | None = None
    max_workers: int = 4
    task_timeout_seconds: int = 1200
    query_timeout_seconds: float = 30
    max_result_rows: int = 100000


@dataclass(frozen=True, slots=True)
class AppConfig:
    dataset: DatasetConfig = field(default_factory=DatasetConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    run: RunConfig = field(default_factory=RunConfig)
    etl: ETLConfig = field(default_factory=ETLConfig)


def _path_value(value, default: Path) -> Path:
    if not value:
        return default
    candidate = Path(value).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()


def load_app_config(config_path: Path) -> AppConfig:
    payload = yaml.safe_load(config_path.read_text()) or {}
    if not isinstance(payload, dict):
        raise ValueError("配置必须是 YAML 对象")
    unknown = set(payload) - {"dataset", "agent", "run", "etl"}
    if unknown:
        raise ValueError(f"未知配置组：{sorted(unknown)}")
    groups = {}
    for name, cls in [
        ("dataset", DatasetConfig),
        ("agent", AgentConfig),
        ("run", RunConfig),
        ("etl", ETLConfig),
    ]:
        raw = payload.get(name) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"{name} 必须是对象")
        defaults = cls()
        allowed = {item.name for item in fields(cls)}
        if set(raw) - allowed:
            raise ValueError(f"{name} 中有未知配置：{sorted(set(raw) - allowed)}")
        values = {}
        for key, value in raw.items():
            default = getattr(defaults, key)
            if isinstance(default, Path):
                value = _path_value(value, default)
            elif isinstance(default, bool):
                if not isinstance(value, bool):
                    raise ValueError(f"{name}.{key} 必须为布尔值")
            elif isinstance(default, int):
                if isinstance(value, bool) or int(value) != float(value):
                    raise ValueError(f"{name}.{key} 必须为整数")
                value = int(value)
            elif isinstance(default, float):
                value = float(value)
            elif isinstance(default, str):
                value = str(value or "")
            values[key] = value
        groups[name] = cls(**values)
    config = AppConfig(**groups)
    for label, value in [
        ("agent.max_steps", config.agent.max_steps),
        ("agent.attempts", config.agent.attempts),
        ("agent.max_model_calls", config.agent.max_model_calls),
        ("agent.request_timeout_seconds", config.agent.request_timeout_seconds),
        ("run.max_workers", config.run.max_workers),
        ("run.query_timeout_seconds", config.run.query_timeout_seconds),
        ("run.max_result_rows", config.run.max_result_rows),
        ("etl.max_workers", config.etl.max_workers),
    ]:
        if value <= 0:
            raise ValueError(f"{label} 必须大于 0")
    if config.agent.request_retries < 0 or config.etl.retries < 0:
        raise ValueError("重试次数不能为负")
    if config.etl.chunk_chars < 1000 or config.agent.max_context_chars < 8000:
        raise ValueError("etl.chunk_chars 至少 1000，agent.max_context_chars 至少 8000")
    if not isinstance(config.dataset.task_ids, list) or not all(
        isinstance(value, str) for value in config.dataset.task_ids
    ):
        raise ValueError("dataset.task_ids 必须是任务 ID 列表")
    return config


def resolve_api_key(config: AgentConfig) -> str:
    return config.api_key or os.environ.get(config.api_key_env, "")
