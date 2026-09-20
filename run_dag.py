#!/usr/bin/env python3
"""CLI entry point for Mini Airflow.

Usage:
    python run_dag.py example_etl
    python run_dag.py example_etl --max-workers 4

Loads `examples/<name>.py`, expects it to expose a module-level `dag`
(or `build_dag()`) object, runs it, and prints a run report: per-task
status/timing and the final task's output.
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mini_airflow import DAGRunner, TaskStatus  # noqa: E402


def load_dag(name: str):
    module = importlib.import_module(f"examples.{name}")
    if hasattr(module, "build_dag"):
        return module.build_dag()
    if hasattr(module, "dag"):
        return module.dag
    raise SystemExit(f"examples/{name}.py must define `dag` or `build_dag()`.")


def format_report(run) -> str:
    lines = []
    lines.append(f"Run ID:   {run.run_id}")
    lines.append(f"DAG:      {run.dag_id}")
    lines.append(f"Status:   {'SUCCESS' if run.success else 'FAILED'}")
    lines.append(f"Duration: {run.duration:.3f}s")
    lines.append("")
    lines.append("Task Report:")
    width = max((len(tid) for tid in run.task_results), default=8)
    for tid, result in run.task_results.items():
        duration = f"{result.duration:.3f}s" if result.duration is not None else "-"
        lines.append(
            f"  {tid.ljust(width)}  {result.status.value.upper():<16} {duration:>10}"
            + (f"  ({result.attempts} attempt(s))" if result.attempts > 1 else "")
        )
        if result.status in (TaskStatus.FAILED,) and result.error:
            lines.append(f"    error: {result.error}")

    # Print the output of whichever task(s) have no downstream (the "leaves"
    # / final outputs of the DAG), e.g. the `load` step of the example ETL.
    lines.append("")
    lines.append("Final Output:")
    leaf_ids = [tid for tid, t in _leaf_task_ids(run)]
    for tid in leaf_ids:
        result = run.task_results[tid]
        lines.append(f"  [{tid}] {result.output!r}")
    return "\n".join(lines)


def _leaf_task_ids(run):
    # A task is a "leaf" for reporting purposes if nothing else in the run
    # consumed its output as an upstream dependency -- approximate here by
    # just returning tasks with no downstream recorded on the DAG object
    # isn't available post-run, so fall back to: the task(s) that finished
    # last chronologically among successful tasks.
    successful = [
        (tid, r) for tid, r in run.task_results.items() if r.status == TaskStatus.SUCCESS
    ]
    if not successful:
        return []
    last_end = max(r.end_time for _, r in successful)
    return [(tid, r) for tid, r in successful if r.end_time == last_end]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run a Mini Airflow DAG.")
    parser.add_argument("dag_name", help="Name of the example DAG module under examples/, e.g. example_etl")
    parser.add_argument("--max-workers", type=int, default=8, help="Thread pool size (default: 8)")
    args = parser.parse_args(argv)

    dag = load_dag(args.dag_name)
    runner = DAGRunner(dag, max_workers=args.max_workers)
    run = runner.run()
    print(format_report(run))
    return 0 if run.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
