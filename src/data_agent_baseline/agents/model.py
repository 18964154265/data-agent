from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
import threading
from time import perf_counter

from openai import APIError, OpenAI


@dataclass(frozen=True, slots=True)
class ModelMessage:
    role: str
    content: str


@dataclass(frozen=True, slots=True)
class ModelStep:
    thought: str
    action: str
    action_input: dict[str, Any]
    raw_response: str


class ModelAdapter(Protocol):
    def complete(self, messages: list[ModelMessage]) -> str:
        raise NotImplementedError


class OpenAIModelAdapter:
    def __init__(
        self,
        *,
        model: str,
        api_base: str,
        api_key: str,
        temperature: float,
        timeout: float = 90,
        max_retries: int = 2,
    ) -> None:
        self.model = model
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self.temperature = temperature
        self.timeout = timeout
        self.max_retries = max_retries
        self._local = threading.local()

    def complete(self, messages: list[ModelMessage]) -> str:
        if not self.api_key:
            raise RuntimeError("Missing model API key in config.agent.api_key.")

        if not hasattr(self._local, "client"):
            self._local.client = OpenAI(
                api_key=self.api_key,
                base_url=self.api_base,
                timeout=self.timeout,
                max_retries=self.max_retries,
            )
        client = self._local.client

        try:
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": message.role, "content": message.content} for message in messages
                ],
                temperature=self.temperature,
            )
        except APIError as exc:
            raise RuntimeError(f"Model request failed: {exc}") from exc

        self._local.usage = response.usage.model_dump() if response.usage else {}
        choices = response.choices or []
        if not choices:
            raise RuntimeError("Model response missing choices.")
        content = choices[0].message.content
        if not isinstance(content, str):
            raise RuntimeError("Model response missing text content.")
        return content

    @property
    def last_usage(self) -> dict:
        return getattr(self._local, "usage", {})


class MonitoredModel:
    """所有 ETL/求解调用共享预算，逐请求落盘；支持注入离线适配器。"""

    def __init__(self, model, log, max_calls: int):
        self.model, self.log, self.max_calls = model, log, max_calls
        self.calls = 0
        self.usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        self._lock = threading.Lock()

    def complete(self, messages: list[ModelMessage]) -> str:
        with self._lock:
            if self.calls >= self.max_calls:
                raise RuntimeError("任务模型请求预算已耗尽")
            self.calls += 1
            call_id = self.calls
        started = perf_counter()
        self.log.emit(
            "model_request",
            call_id=call_id,
            messages=[{"role": m.role, "content": m.content} for m in messages],
        )
        try:
            result = self.model.complete(messages)
            usage = getattr(self.model, "last_usage", {})
            with self._lock:
                for key in self.usage:
                    self.usage[key] += usage.get(key, 0) or 0
            self.log.emit(
                "model_response",
                call_id=call_id,
                ok=True,
                raw_response=result,
                usage=usage,
                elapsed_seconds=perf_counter() - started,
            )
            return result
        except Exception as exc:
            self.log.emit(
                "model_response",
                call_id=call_id,
                ok=False,
                error=str(exc),
                elapsed_seconds=perf_counter() - started,
            )
            raise


class ScriptedModelAdapter:
    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)

    def complete(self, messages: list[ModelMessage]) -> str:
        del messages
        if not self._responses:
            raise RuntimeError("No scripted model responses remaining.")
        return self._responses.pop(0)
