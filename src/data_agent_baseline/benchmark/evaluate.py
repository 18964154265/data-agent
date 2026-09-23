"""离线结果比较器；求解运行器不依赖此模块，也不能访问标准答案。"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

from data_agent_baseline.storage import write_json


def _cell(value: str, decimals: int | None):
    # 保留编号前导零；只统一普通十进制数的显示差异。
    if re.fullmatch(r"[+-]?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?", value):
        try:
            with localcontext() as context:
                context.prec = max(50, len(value) + (decimals or 0) + 20)
                number = Decimal(value)
                if decimals is not None:
                    number = number.quantize(Decimal(1).scaleb(-decimals))
                return ("number", number)
        except InvalidOperation:
            pass
    return ("text", value)


def compare_csv(
    prediction: Path,
    gold: Path,
    *,
    ordered: bool = False,
    decimals: int | None = None,
    headers: bool = False,
) -> dict:
    with prediction.open(newline="", encoding="utf-8-sig") as handle:
        actual = list(csv.reader(handle))
    with gold.open(newline="", encoding="utf-8-sig") as handle:
        expected = list(csv.reader(handle))
    if not actual or not expected:
        return {"matched": False, "reason": "文件缺少表头"}
    width = len(expected[0])
    if len(actual[0]) != width or any(len(row) != width for row in actual[1:] + expected[1:]):
        return {"matched": False, "reason": "列数或行结构不一致"}
    if headers and actual[0] != expected[0]:
        return {"matched": False, "reason": "表头不一致"}
    actual_rows = [tuple(_cell(value, decimals) for value in row) for row in actual[1:]]
    expected_rows = [tuple(_cell(value, decimals) for value in row) for row in expected[1:]]
    matched = (
        actual_rows == expected_rows if ordered else Counter(actual_rows) == Counter(expected_rows)
    )
    return {
        "matched": matched,
        "reason": None if matched else "数据行不一致",
        "prediction_rows": len(actual_rows),
        "gold_rows": len(expected_rows),
    }


def evaluate_run(
    run_dir: Path,
    gold_root: Path,
    *,
    ordered: bool = False,
    decimals: int | None = None,
    headers: bool = False,
) -> dict:
    if not gold_root.is_dir():
        raise ValueError("标准答案目录不存在")
    if decimals is not None and not 0 <= decimals <= 15:
        raise ValueError("数值小数位必须在 0 到 15 之间")
    summary = run_dir / "summary.json"
    if summary.exists():
        data = json.loads(summary.read_text())
        task_ids = data.get("task_ids") or [item["task_id"] for item in data["tasks"]]
    else:
        task_ids = sorted(path.name for path in run_dir.glob("task_*") if path.is_dir())
    if not task_ids:
        raise ValueError("运行目录没有任务")
    results = []
    for task_id in task_ids:
        if not re.fullmatch(r"task_\d+", task_id):
            raise ValueError("汇总中出现非法任务 ID")
        prediction, gold = run_dir / task_id / "prediction.csv", gold_root / task_id / "gold.csv"
        trace = run_dir / task_id / "trace.json"
        try:
            if not gold.exists():
                result = {"matched": False, "reason": "缺少标准答案"}
            elif not trace.exists() or not json.loads(trace.read_text()).get("succeeded"):
                result = {"matched": False, "reason": "任务未成功完成"}
            elif not prediction.exists():
                result = {"matched": False, "reason": "缺少预测文件"}
            else:
                result = compare_csv(
                    prediction, gold, ordered=ordered, decimals=decimals, headers=headers
                )
        except (OSError, ValueError) as exc:
            result = {"matched": False, "reason": f"无法比较：{exc}"}
        results.append({"task_id": task_id, **result})
    matched = sum(r["matched"] for r in results)
    report = {
        "metric": "本地 CSV 比较（非官方评分器）",
        "ordered": ordered,
        "decimals": decimals,
        "compare_headers": headers,
        "task_count": len(results),
        "matched_count": matched,
        "accuracy": matched / len(results),
        "tasks": results,
    }
    write_json(run_dir / "evaluation.json", report)
    return report
