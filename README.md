# Mini Airflow

A small, dependency-free DAG-based task/ETL orchestrator, built from scratch
in pure Python (standard library only — no pip installs required). It
implements the core ideas behind Apache Airflow at a scale you can read in
one sitting: tasks, dependencies, a concurrent scheduler, retries, failure
propagation, and per-run history.

## Why

Real orchestrators (Airflow, Dagster, Prefect) all boil down to the same
core loop: build a DAG of tasks, run whatever is ready in parallel, and stop
running a branch once something upstream of it fails. This project
implements exactly that core loop, end to end, with a real ETL example and
a test suite that pins down the tricky parts (cycle detection, concurrency,
and failure-isolation between independent branches).

## Install

Nothing to install — pure standard library.

```bash
git clone https://github.com/pastbologanesh0101/mini-airflow.git
cd mini-airflow
python run_dag.py example_etl
```

## The DAG API

Tasks are plain Python callables wrapped in a `Task`. Every task function
has the signature:

```python
def my_task(upstream_results: dict):
    ...
    return some_output
```

`upstream_results` maps `task_id -> output` for each of that task's direct
upstream dependencies (an empty dict `{}` for a task with no dependencies).
Whatever a task returns becomes its recorded `output`, and is exactly what
downstream tasks see in their own `upstream_results`.

Dependencies are declared with the `>>` / `<<` operators (or the explicit
`set_upstream` / `set_downstream` methods), mirroring real Airflow:

```python
from mini_airflow import DAG, Task

dag = DAG("my_dag")

extract = Task("extract", extract_fn, dag=dag)
transform = Task("transform", transform_fn, dag=dag)
load = Task("load", load_fn, dag=dag, retries=3, retry_delay=0.5)

extract >> transform >> load        # linear chain
# fan-out:  extract >> [transform_a, transform_b]
# fan-in:   [transform_a, transform_b] >> merge
```

or with the decorator form:

```python
dag = DAG("my_dag")

@dag.task("extract")
def extract(upstream):
    return read_source()

@dag.task("transform")
def transform(upstream):
    return clean(upstream["extract"])

extract_task = dag.tasks["extract"]
transform_task = dag.tasks["transform"]
extract_task >> transform_task
```

### Cycle detection

Cycles are rejected **the instant the offending edge is added**, not at run
time:

```python
a >> b
b >> a   # raises DAGCycleError immediately — "a" is already reachable from "b"
```

`DAG.validate()` (also called automatically at the start of every run) is a
defensive second check that walks the whole graph, but in normal use you'll
never hit it — `Task.set_upstream` catches the cycle the moment it would be
created, by checking whether the new dependency is already reachable from
the task it's being added to.

## Execution model

`DAGRunner(dag).run()` executes the DAG once and returns a `DAGRun` report.

- A **thread pool** (`concurrent.futures.ThreadPoolExecutor`, default 8
  workers, configurable via `max_workers`) runs tasks.
- A task starts as soon as **all** of its upstream tasks have finished with
  status `SUCCESS`. Tasks with no unmet dependencies are submitted
  immediately, so independent branches of the DAG run concurrently.
- After each task finishes, the scheduler re-scans for newly-ready tasks and
  submits them — this repeats until every task has reached a terminal
  state (`SUCCESS`, `FAILED`, or `UPSTREAM_FAILED`).

### Failure semantics

If a task's function raises (after exhausting its retries), that task is
marked `FAILED`. Every task that transitively depends on it — direct or
indirect — is marked `UPSTREAM_FAILED` and is **never executed**. Crucially,
this only affects that task's actual downstream closure: an independent
branch of the same DAG that shares no dependency on the failed task runs
completely normally.

```text
a (fails) --> b --> c        # b, c: UPSTREAM_FAILED, never run
d --> e                      # independent branch: runs to SUCCESS regardless
```

### Retries

Each task can set `retries` (extra attempts after the first failure) and
`retry_delay` (base backoff in seconds). Backoff is linear:
attempt `N`'s failure sleeps for `retry_delay * N` seconds before the next
attempt. With `retries=2, retry_delay=0.5`:

- attempt 1 fails -> sleep 0.5s
- attempt 2 fails -> sleep 1.0s
- attempt 3 fails -> task marked `FAILED` (no more retries left)

`DAGRunner` takes an injectable `sleep_fn` (defaults to `time.sleep`), which
the test suite uses to test backoff behavior without real delays.

### Run history

Every call to `runner.run()` produces a `DAGRun`:

- `run_id` — a fresh UUID per run.
- `dag_id`, `start_time`, `end_time`, `duration`, `success`.
- `task_results: dict[task_id -> TaskResult]`, where each `TaskResult` has
  `status`, `start_time`, `end_time`, `duration`, `output`, `error`, and
  `attempts`.

```python
run = DAGRunner(dag).run()
run.success                      # True/False across the whole DAG
run.get_status("transform")      # TaskStatus.SUCCESS / FAILED / UPSTREAM_FAILED / ...
run.get_output("load")           # whatever the load task returned
```

## The bundled example ETL DAG

`examples/example_etl.py` is a real (if small) ETL pipeline over bundled
CSV data, shaped like a genuine DAG rather than a linear chain:

```text
extract_a -> transform_a -\
                            -> merge -> aggregate -> load
extract_b -> transform_b -/
```

- **extract_a / extract_b** read `examples/data/orders_a.csv` and
  `orders_b.csv` — two independent data sources, no shared dependency, so
  they (and their transforms) run concurrently.
- **transform_a / transform_b** validate and clean rows: rows with a
  non-numeric or non-positive `amount` are dropped.
- **merge** waits for both transforms and concatenates the cleaned rows.
- **aggregate** groups the merged rows by `category`, computing
  `total_amount`, `count`, and `avg_amount` per category.
- **load** writes the aggregated summary into a SQLite table
  (`examples/data/output.db`, table `category_summary`) and returns it.

Run it:

```bash
python run_dag.py example_etl
```

Example output:

```
Run ID:   3f9c7a2e-...
DAG:      example_etl
Status:   SUCCESS
Duration: 0.014s

Task Report:
  extract_a    SUCCESS               0.000s
  extract_b    SUCCESS               0.000s
  transform_a  SUCCESS               0.000s
  transform_b  SUCCESS               0.000s
  merge        SUCCESS               0.000s
  aggregate    SUCCESS               0.000s
  load         SUCCESS               0.001s

Final Output:
  [load] {'electronics': {'total_amount': 395.75, 'count': 3, 'avg_amount': 131.92}, 'home': {'total_amount': 100.0, 'count': 2, 'avg_amount': 50.0}, 'garden': {'total_amount': 15.75, 'count': 1, 'avg_amount': 15.75}}
```

(Invalid rows in the sample CSVs — a non-numeric amount and a negative
amount — are intentionally included and correctly dropped by `transform`,
which is exactly what `tests/test_etl.py` checks for.)

## Running the tests

```bash
python -m unittest discover -s tests -t . -v
```

29 unit tests across four files cover:

- `tests/test_dag.py` — operator wiring (`>>`, `<<`, fan-in/fan-out),
  cycle rejection at construction time (direct cycles, longer cycles,
  self-loops), duplicate task-id detection, topological ordering.
- `tests/test_executor.py` — linear execution order, upstream-result
  passing, **independent tasks measurably running concurrently** (timing
  assertions), failure propagation with **independent branches continuing
  normally**, transitive `UPSTREAM_FAILED` propagation, run history
  correctness, unique run IDs.
- `tests/test_retries.py` — retry-until-success, exhausting retries,
  zero-retry behavior, and downstream tasks still running after an
  upstream retry recovers — all using an injectable `sleep_fn` so no test
  waits on a real clock.
- `tests/test_etl.py` — the bundled DAG's shape, and mathematically
  correct end-to-end aggregation (verified by hand-computed expected
  totals) plus SQLite persistence.

CI (`.github/workflows/tests.yml`) runs the same suite on Python 3.11 and
3.12 on every push and pull request.

## Project layout

```
mini_airflow/
  dag.py         DAG, Task, cycle detection, >>/<< operators
  executor.py    DAGRunner: thread-pool scheduler, retries, run history
examples/
  example_etl.py bundled ETL DAG (extract/transform/merge/aggregate/load)
  data/          sample input CSVs
run_dag.py       CLI: python run_dag.py <example_name>
tests/           unittest suite
```

## License

MIT — see [LICENSE](LICENSE).
