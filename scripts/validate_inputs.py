"""只验证公开输入加载，不调用模型、不读取标准答案。"""

from __future__ import annotations

import argparse
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from data_agent_baseline.benchmark.dataset import DABenchPublicDataset
from data_agent_baseline.data.catalog import CatalogBuilder
from data_agent_baseline.data.query import QueryRuntime
from data_agent_baseline.storage import write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("public/input"))
    parser.add_argument("--report", type=Path, default=Path("artifacts/validation/inputs.json"))
    args = parser.parse_args()
    reports = []
    start = perf_counter()
    for task in DABenchPublicDataset(args.root).iter_tasks():
        started = perf_counter()
        with TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            builder = CatalogBuilder(task.context_dir, workspace)
            documents = builder.load_structured()
            manifest = builder.finish()
            runtime = QueryRuntime(workspace)
            try:
                for table in manifest["tables"]:
                    try:
                        runtime.schema(table["name"])
                    except Exception as exc:
                        manifest["errors"].append({"source": table["name"], "error": str(exc)})
            finally:
                runtime.close()
            report = {
                "task_id": task.task_id,
                "elapsed_seconds": round(perf_counter() - started, 3),
                "tables": [{"name": t["name"], "rows": t["row_count"]} for t in manifest["tables"]],
                "documents": len(documents),
                "errors": manifest["errors"],
            }
            reports.append(report)
            write_json(
                args.report,
                {
                    "task_count": len(reports),
                    "failed_tasks": sum(bool(r["errors"]) for r in reports),
                    "elapsed_seconds": round(perf_counter() - start, 3),
                    "tasks": reports,
                },
            )
            print(
                f"{task.task_id}: {len(report['tables'])} 表，{len(report['errors'])} 错误，"
                f"{report['elapsed_seconds']} 秒",
                flush=True,
            )
    write_json(
        args.report,
        {
            "task_count": len(reports),
            "failed_tasks": sum(bool(r["errors"]) for r in reports),
            "elapsed_seconds": round(perf_counter() - start, 3),
            "tasks": reports,
        },
    )
    raise SystemExit(1 if any(r["errors"] for r in reports) else 0)


if __name__ == "__main__":
    main()
