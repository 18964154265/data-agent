from __future__ import annotations

import json
import re
from dataclasses import dataclass
from time import perf_counter

from data_agent_baseline.agents.model import ModelAdapter, ModelMessage, ModelStep
from data_agent_baseline.agents.prompt import (
    REACT_SYSTEM_PROMPT,
    build_observation_prompt,
    build_system_prompt,
    build_task_prompt,
)
from data_agent_baseline.agents.runtime import AgentRunResult, AgentRuntimeState, StepRecord
from data_agent_baseline.benchmark.schema import PublicTask
from data_agent_baseline.tools.registry import ToolRegistry


@dataclass(frozen=True, slots=True)
class ReActAgentConfig:
    max_steps: int = 24
    max_context_chars: int = 60000


def _strip_json_fence(raw_response: str) -> str:
    text = raw_response.strip()
    fence_match = re.search(r"```json\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    if fence_match is not None:
        return fence_match.group(1).strip()
    generic_fence_match = re.search(r"```\s*(.*?)\s*```", text, flags=re.DOTALL)
    if generic_fence_match is not None:
        return generic_fence_match.group(1).strip()
    return text


def _load_single_json_object(text: str) -> dict[str, object]:
    payload, end = json.JSONDecoder().raw_decode(text)
    remainder = text[end:].strip()
    if remainder:
        cleaned_remainder = re.sub(r"(?:\\[nrt])+", "", remainder).strip()
        if cleaned_remainder:
            raise ValueError("Model response must contain only one JSON object.")
    if not isinstance(payload, dict):
        raise ValueError("Model response must be a JSON object.")
    return payload


def parse_model_step(raw_response: str) -> ModelStep:
    normalized = _strip_json_fence(raw_response)
    payload = _load_single_json_object(normalized)

    thought = payload.get("thought", "")
    action = payload.get("action")
    action_input = payload.get("action_input", {})
    if not isinstance(thought, str):
        raise ValueError("thought must be a string.")
    if not isinstance(action, str) or not action:
        raise ValueError("action must be a non-empty string.")
    if not isinstance(action_input, dict):
        raise ValueError("action_input must be a JSON object.")

    return ModelStep(
        thought=thought,
        action=action,
        action_input=action_input,
        raw_response=raw_response,
    )


class ReActAgent:
    def __init__(
        self,
        *,
        model: ModelAdapter,
        tools: ToolRegistry,
        config: ReActAgentConfig | None = None,
        system_prompt: str | None = None,
        task_context: str = "",
        on_step=None,
    ) -> None:
        self.model = model
        self.tools = tools
        self.config = config or ReActAgentConfig()
        self.system_prompt = system_prompt or REACT_SYSTEM_PROMPT
        self.task_context = task_context
        self.on_step = on_step

    def _build_messages(self, task: PublicTask, state: AgentRuntimeState) -> list[ModelMessage]:
        system_content = build_system_prompt(
            self.tools.describe_for_prompt(),
            system_prompt=self.system_prompt,
        )
        messages = [ModelMessage(role="system", content=system_content)]
        messages.append(
            ModelMessage(
                role="user",
                content=build_task_prompt(
                    task, "submit_answer" if "submit_answer" in self.tools.specs else "answer"
                )
                + "\n"
                + self.task_context,
            )
        )
        budget = max(2000, self.config.max_context_chars - sum(len(m.content) for m in messages))
        selected = []
        used = 0
        for step in reversed(state.steps):
            size = len(step.raw_response) + len(json.dumps(step.observation, ensure_ascii=False))
            if selected and used + size > budget:
                break
            selected.append(step)
            used += size
        omitted = len(state.steps) - len(selected)
        if omitted:
            messages.append(
                ModelMessage(
                    role="user",
                    content=(
                        f"已折叠前 {omitted} 轮历史；数据仍可查询，程序状态仍可读取。"
                        "请重新探查需要的事实，勿推测已省略的结果。"
                    ),
                )
            )
        for step in reversed(selected):
            messages.append(ModelMessage(role="assistant", content=step.raw_response))
            messages.append(
                ModelMessage(role="user", content=build_observation_prompt(step.observation))
            )
        return messages

    def run(self, task: PublicTask) -> AgentRunResult:
        state = AgentRuntimeState()
        for step_index in range(1, self.config.max_steps + 1):
            started = perf_counter()
            try:
                raw_response = self.model.complete(self._build_messages(task, state))
            except Exception as exc:
                state.failure_reason = f"模型请求失败：{exc}"
                break
            model_seconds = perf_counter() - started
            started = perf_counter()
            model_step = None
            tool_result = None
            try:
                model_step = parse_model_step(raw_response)
                tool_result = self.tools.execute(task, model_step.action, model_step.action_input)
                observation = {
                    "ok": tool_result.ok,
                    "tool": model_step.action,
                    "content": tool_result.content,
                }
            except Exception as exc:
                observation = {"ok": False, "error": str(exc), "error_type": type(exc).__name__}
            step_record = StepRecord(
                step_index=step_index,
                thought=model_step.thought if model_step else "",
                action=model_step.action if model_step else "__error__",
                action_input=model_step.action_input if model_step else {},
                raw_response=raw_response,
                observation=observation,
                ok=bool(observation["ok"]),
                model_seconds=model_seconds,
                tool_seconds=perf_counter() - started,
            )
            state.steps.append(step_record)
            # 记录失败属于基础设施问题，不能被误记为工具异常并重复追加步骤。
            if self.on_step:
                self.on_step(step_record)
            if tool_result is not None and tool_result.is_terminal:
                state.answer = tool_result.answer
                break

        if state.answer is None and state.failure_reason is None:
            state.failure_reason = "Agent did not submit an answer within max_steps."

        return AgentRunResult(
            task_id=task.task_id,
            answer=state.answer,
            steps=list(state.steps),
            failure_reason=state.failure_reason,
        )
