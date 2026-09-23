import json

from data_agent_baseline.benchmark.evaluate import compare_csv, evaluate_run


def test_comparison_keeps_duplicates_and_optional_order(tmp_path):
    prediction, gold = tmp_path / "prediction.csv", tmp_path / "gold.csv"
    gold.write_text("value\n1\n1\n2\n")
    prediction.write_text("value\n2.0\n1.0\n1\n")
    assert compare_csv(prediction, gold)["matched"]
    assert not compare_csv(prediction, gold, ordered=True)["matched"]
    prediction.write_text("value\n1\n2\n")
    assert not compare_csv(prediction, gold)["matched"]


def test_rounding_is_explicit_and_ids_preserved(tmp_path):
    prediction, gold = tmp_path / "prediction.csv", tmp_path / "gold.csv"
    gold.write_text("id,value\n001,0.333333\n")
    prediction.write_text("id,value\n001,0.3333333\n")
    assert not compare_csv(prediction, gold)["matched"]
    assert compare_csv(prediction, gold, decimals=6)["matched"]
    prediction.write_text("id,value\n1,0.333333\n")
    assert not compare_csv(prediction, gold)["matched"]


def test_missing_task_counted_in_denominator(tmp_path):
    run, gold = tmp_path / "run", tmp_path / "gold"
    run.mkdir()
    gold.mkdir()
    (run / "summary.json").write_text(json.dumps({"task_ids": ["task_1", "task_2"]}))
    report = evaluate_run(run, gold)
    assert report["task_count"] == 2
    assert report["matched_count"] == 0
