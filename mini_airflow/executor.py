"""Scheduler/executor that runs a DAG's tasks in dependency order.

Independent tasks (no dependency relationship between them) are run
concurrently in a thread pool. A task only starts once every one of its
upstream tasks has *succeeded*. If a task fails (raises, and exhausts its
retries) every task that transitively depends on it is marked
``UPSTREAM_FAILED`` and never executed -- but any task in an independent
branch of the DAG is unaffected and runs normally.
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Optional

from .dag import DAG


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    UPSTREAM_FAILED = "upstream_failed"


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    output: object = None
    error: Optional[str] = None
    attempts: int = 0

    @property
    def duration(self) -> Optional[float]:
        if self.start_time is None or self.end_time is None:
            return None
        return self.end_time - self.start_time


@dataclass
class DAGRun:
    run_id: str
    dag_id: str
    start_time: float
    end_time: Optional[float] = None
    task_results: Dict[str, TaskResult] = field(default_factory=dict)

    @property
    def duration(self) -> Optional[float]:
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    @property
    def success(self) -> bool:
        return all(r.status == TaskStatus.SUCCESS for r in self.task_results.values())

    def get_status(self, task_id: str) -> TaskStatus:
        return self.task_results[task_id].status

    def get_output(self, task_id: str):
        return self.task_results[task_id].output


class DAGRunner:
    """Executes a DAG once per call to `run()`, returning a `DAGRun` report.

    `sleep_fn` and `time_fn` are injectable so tests can run retry/backoff
    logic without waiting on a real clock.
    """

    def __init__(
        self,
        dag: DAG,
        max_workers: int = 8,
        sleep_fn: Callable[[float], None] = time.sleep,
        time_fn: Callable[[], float] = time.time,
    ):
        self.dag = dag
        self.max_workers = max_workers
        self.sleep_fn = sleep_fn
        self.time_fn = time_fn

    def run(self) -> DAGRun:
        dag = self.dag
        dag.validate()

        run_id = str(uuid.uuid4())
        run = DAGRun(run_id=run_id, dag_id=dag.dag_id, start_time=self.time_fn())
        results: Dict[str, TaskResult] = {tid: TaskResult(task_id=tid) for tid in dag.tasks}

        lock = threading.Lock()
        pending = set(dag.tasks.keys())
        in_flight: Dict[concurrent.futures.Future, str] = {}

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        def try_schedule():
            """Must be called while holding `lock`. Submits newly-ready tasks
            and resolves tasks whose upstream has failed.

            Runs to a fixed point (loops until a pass makes no further
            changes) so that UPSTREAM_FAILED propagates through multiple
            hops in one go -- e.g. a -> b -> c where a fails: b and c must
            both be resolved to UPSTREAM_FAILED here even though neither of
            them ever runs, regardless of the (unordered) iteration order
            of the `pending` set.
            """
            newly_submitted = []
            changed = True
            while changed:
                changed = False
                for tid in list(pending):
                    task = dag.tasks[tid]
                    upstream_ids = [u.task_id for u in task.upstream]
                    statuses = [results[u].status for u in upstream_ids]

                    if any(s in (TaskStatus.FAILED, TaskStatus.UPSTREAM_FAILED) for s in statuses):
                        results[tid].status = TaskStatus.UPSTREAM_FAILED
                        results[tid].error = "One or more upstream tasks did not succeed."
                        pending.discard(tid)
                        changed = True
                        continue

                    if all(s == TaskStatus.SUCCESS for s in statuses):
                        results[tid].status = TaskStatus.RUNNING
                        pending.discard(tid)
                        upstream_results = {u: results[u].output for u in upstream_ids}
                        fut = pool.submit(self._execute_task, task, upstream_results)
                        in_flight[fut] = tid
                        newly_submitted.append(fut)
                        changed = True
            return newly_submitted

        try:
            with lock:
                remaining_futures = set(try_schedule())

            while remaining_futures or pending:
                if not remaining_futures and pending:
                    # Should be unreachable for a valid (acyclic) DAG: every
                    # remaining pending task must have at least one upstream
                    # that is still unresolved and in-flight.
                    break

                done, _ = concurrent.futures.wait(
                    remaining_futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                with lock:
                    for fut in done:
                        tid = in_flight.pop(fut)
                        remaining_futures.discard(fut)
                        status, output, error, attempts, start, end = fut.result()
                        results[tid].status = status
                        results[tid].output = output
                        results[tid].error = error
                        results[tid].attempts = attempts
                        results[tid].start_time = start
                        results[tid].end_time = end
                    remaining_futures.update(try_schedule())
        finally:
            pool.shutdown(wait=True)

        run.end_time = self.time_fn()
        run.task_results = results
        return run

    def _execute_task(self, task, upstream_results):
        """Run one task to completion (including retries). Executed in a
        worker thread; returns a plain tuple so it survives crossing the
        future boundary cleanly."""
        attempts = 0
        start = self.time_fn()
        last_error = None
        max_attempts = task.retries + 1
        while True:
            attempts += 1
            try:
                output = task.func(upstream_results)
                end = self.time_fn()
                return TaskStatus.SUCCESS, output, None, attempts, start, end
            except Exception as exc:  # noqa: BLE001 - intentionally broad
                last_error = f"{exc.__class__.__name__}: {exc}"
                if attempts >= max_attempts:
                    end = self.time_fn()
                    return TaskStatus.FAILED, None, last_error, attempts, start, end
                self.sleep_fn(task.retry_delay * attempts)
