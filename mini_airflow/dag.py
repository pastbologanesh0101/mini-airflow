"""DAG and Task definitions.

Tasks are plain Python callables wrapped in a `Task` object. Dependencies
between tasks are declared either explicitly with `set_upstream` /
`set_downstream`, or with the `>>` / `<<` operators, mirroring the real
Apache Airflow API:

    a >> b >> c        # a runs before b, b runs before c
    a >> [b, c]         # a runs before both b and c
    [b, c] >> d          # d runs after both b and c

Every task function has the signature ``func(upstream_results: dict)`` where
``upstream_results`` maps ``task_id -> output`` for each of that task's
direct upstream dependencies (an empty dict for root tasks).

Cycles are rejected the moment the edge that would create them is added,
not at run time.
"""

from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Union


class DAGCycleError(Exception):
    """Raised when adding a dependency would create a cycle in the DAG."""


class DuplicateTaskError(Exception):
    """Raised when two tasks in the same DAG share a task_id."""


def _reachable(start: "Task", target: "Task") -> bool:
    """Return True if `target` is reachable from `start` via downstream edges."""
    seen = set()
    stack = [start]
    while stack:
        node = stack.pop()
        if node is target:
            return True
        if node.task_id in seen:
            continue
        seen.add(node.task_id)
        stack.extend(node.downstream)
    return False


class Task:
    """A single unit of work in a DAG.

    Parameters
    ----------
    task_id: unique (within its DAG) identifier for the task.
    func: callable with signature ``func(upstream_results: dict) -> Any``.
    retries: number of times to retry after an initial failure (0 = no retry).
    retry_delay: base delay in seconds between retries. Attempt N sleeps for
        ``retry_delay * N`` seconds (linear backoff).
    dag: optional DAG to auto-register this task with.
    """

    def __init__(
        self,
        task_id: str,
        func: Callable[[Dict[str, object]], object],
        retries: int = 0,
        retry_delay: float = 0.1,
        dag: "DAG" = None,
    ):
        self.task_id = task_id
        self.func = func
        self.retries = retries
        self.retry_delay = retry_delay
        self.upstream: set = set()
        self.downstream: set = set()
        self.dag = dag
        if dag is not None:
            dag.add_task(self)

    def set_upstream(self, other: "Task") -> "Task":
        """Declare `other` as a dependency of this task (other runs first)."""
        if other is self or _reachable(self, other):
            raise DAGCycleError(
                f"Adding dependency '{other.task_id}' -> '{self.task_id}' "
                f"would create a cycle in the DAG."
            )
        self.upstream.add(other)
        other.downstream.add(self)
        return self

    def set_downstream(self, other: "Task") -> "Task":
        """Declare `other` as depending on this task (other runs after)."""
        other.set_upstream(self)
        return self

    # -- operator sugar, mirroring real Airflow -----------------------------

    def __rshift__(self, other: Union["Task", Iterable["Task"]]):
        """self >> other : other depends on self."""
        others = other if isinstance(other, (list, tuple)) else [other]
        for o in others:
            o.set_upstream(self)
        return other

    def __lshift__(self, other: Union["Task", Iterable["Task"]]):
        """self << other : self depends on other."""
        others = other if isinstance(other, (list, tuple)) else [other]
        for o in others:
            self.set_upstream(o)
        return other

    def __rrshift__(self, other: Iterable["Task"]):
        """[a, b] >> self : self depends on every task in the list."""
        others = other if isinstance(other, (list, tuple)) else [other]
        for o in others:
            self.set_upstream(o)
        return self

    def __rlshift__(self, other: Iterable["Task"]):
        """[a, b] << self : every task in the list depends on self."""
        others = other if isinstance(other, (list, tuple)) else [other]
        for o in others:
            o.set_upstream(self)
        return self

    def __repr__(self):
        return f"Task({self.task_id!r})"

    def __hash__(self):
        return hash(self.task_id)


class DAG:
    """A collection of tasks and the dependency edges between them."""

    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task: Task) -> Task:
        if task.task_id in self.tasks:
            raise DuplicateTaskError(
                f"Task id '{task.task_id}' is already registered in DAG '{self.dag_id}'."
            )
        self.tasks[task.task_id] = task
        task.dag = self
        return task

    def task(self, task_id: str, retries: int = 0, retry_delay: float = 0.1):
        """Decorator form: @dag.task("extract") def extract(upstream): ..."""

        def decorator(func: Callable[[Dict[str, object]], object]) -> Task:
            return Task(task_id, func, retries=retries, retry_delay=retry_delay, dag=self)

        return decorator

    def validate(self) -> None:
        """Raise DAGCycleError if the graph (somehow) contains a cycle.

        Cycles are normally rejected earlier, at edge-creation time in
        `Task.set_upstream`, so this is a defensive double-check performed
        before every run.
        """
        state: Dict[str, int] = {}  # 0=unvisited, 1=in-progress, 2=done

        def visit(t: Task):
            s = state.get(t.task_id, 0)
            if s == 1:
                raise DAGCycleError(
                    f"Cycle detected in DAG '{self.dag_id}' involving task '{t.task_id}'."
                )
            if s == 2:
                return
            state[t.task_id] = 1
            for d in t.downstream:
                visit(d)
            state[t.task_id] = 2

        for t in self.tasks.values():
            visit(t)

    def topological_order(self) -> List[str]:
        """Return one valid execution order (Kahn's algorithm)."""
        self.validate()
        in_degree = {tid: len(t.upstream) for tid, t in self.tasks.items()}
        ready = [tid for tid, deg in in_degree.items() if deg == 0]
        order: List[str] = []
        ready.sort()  # deterministic ordering among ties
        while ready:
            tid = ready.pop(0)
            order.append(tid)
            for d in sorted(t.task_id for t in self.tasks[tid].downstream):
                in_degree[d] -= 1
                if in_degree[d] == 0:
                    ready.append(d)
            ready.sort()
        if len(order) != len(self.tasks):
            raise DAGCycleError(f"Cycle detected in DAG '{self.dag_id}'.")
        return order

    def __repr__(self):
        return f"DAG({self.dag_id!r}, tasks={list(self.tasks)})"
