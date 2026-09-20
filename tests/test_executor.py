"""Tests for the scheduler/executor: ordering, concurrency, failure
propagation and run history."""

import threading
import time
import unittest

from mini_airflow import DAG, DAGRunner, Task, TaskStatus


class TestExecutionOrder(unittest.TestCase):
    def test_linear_dag_executes_in_dependency_order(self):
        dag = DAG("linear")
        order = []
        lock = threading.Lock()

        def make(name):
            def fn(_upstream):
                with lock:
                    order.append(name)
                return name
            return fn

        a = Task("a", make("a"), dag=dag)
        b = Task("b", make("b"), dag=dag)
        c = Task("c", make("c"), dag=dag)
        a >> b >> c

        run = DAGRunner(dag).run()

        self.assertEqual(order, ["a", "b", "c"])
        self.assertTrue(run.success)

    def test_upstream_results_are_passed_to_downstream_tasks(self):
        dag = DAG("chain")
        a = Task("a", lambda _u: 42, dag=dag)
        b = Task("b", lambda u: u["a"] + 1, dag=dag)
        a >> b

        run = DAGRunner(dag).run()

        self.assertEqual(run.get_output("a"), 42)
        self.assertEqual(run.get_output("b"), 43)


class TestConcurrency(unittest.TestCase):
    def test_independent_tasks_run_concurrently_not_sequentially(self):
        """Two tasks with no dependency between them, each sleeping ~0.2s,
        should finish in roughly 0.2s total (parallel), not ~0.4s (serial)."""
        dag = DAG("parallel")
        SLEEP = 0.2

        def slow(_upstream):
            time.sleep(SLEEP)
            return "done"

        Task("x", slow, dag=dag)
        Task("y", slow, dag=dag)
        # no edges between x and y: fully independent

        start = time.time()
        run = DAGRunner(dag, max_workers=8).run()
        elapsed = time.time() - start

        self.assertTrue(run.success)
        # Generous thresholds to avoid flakiness on slower CI machines while
        # still clearly distinguishing parallel (~0.2s) from serial (~0.4s).
        self.assertLess(elapsed, SLEEP * 2 * 0.85, "tasks did not run concurrently")

    def test_independent_branches_of_a_wider_dag_overlap_in_time(self):
        dag = DAG("wide")
        SLEEP = 0.15
        starts = {}
        lock = threading.Lock()

        def make(name):
            def fn(_upstream):
                with lock:
                    starts[name] = time.time()
                time.sleep(SLEEP)
                return name
            return fn

        tasks = [Task(n, make(n), dag=dag) for n in ("p", "q", "r", "s")]
        # two independent 1-step branches
        run = DAGRunner(dag, max_workers=8).run()

        self.assertTrue(run.success)
        start_times = list(starts.values())
        self.assertLess(max(start_times) - min(start_times), SLEEP, "tasks did not start together")


class TestFailurePropagation(unittest.TestCase):
    def test_failing_task_marks_downstream_upstream_failed_independent_branch_continues(self):
        dag = DAG("branches")

        def boom(_upstream):
            raise ValueError("boom")

        def ok(_upstream):
            return "ok"

        a = Task("a", boom, dag=dag)          # will fail
        b = Task("b", ok, dag=dag)            # depends on a -> should be skipped
        c = Task("c", ok, dag=dag)            # independent branch
        d = Task("d", ok, dag=dag)            # depends on c -> should run normally
        a >> b
        c >> d

        run = DAGRunner(dag).run()

        self.assertEqual(run.get_status("a"), TaskStatus.FAILED)
        self.assertEqual(run.get_status("b"), TaskStatus.UPSTREAM_FAILED)
        self.assertEqual(run.get_status("c"), TaskStatus.SUCCESS)
        self.assertEqual(run.get_status("d"), TaskStatus.SUCCESS)
        self.assertFalse(run.success)

    def test_upstream_failure_propagates_transitively(self):
        dag = DAG("transitive")

        def boom(_upstream):
            raise ValueError("boom")

        def ok(_upstream):
            return "ok"

        a = Task("a", boom, dag=dag)
        b = Task("b", ok, dag=dag)
        c = Task("c", ok, dag=dag)
        a >> b >> c

        run = DAGRunner(dag).run()

        self.assertEqual(run.get_status("a"), TaskStatus.FAILED)
        self.assertEqual(run.get_status("b"), TaskStatus.UPSTREAM_FAILED)
        self.assertEqual(run.get_status("c"), TaskStatus.UPSTREAM_FAILED)

    def test_task_with_multiple_upstreams_needs_all_to_succeed(self):
        dag = DAG("fanin")

        def boom(_upstream):
            raise ValueError("boom")

        def ok(_upstream):
            return "ok"

        a = Task("a", ok, dag=dag)
        b = Task("b", boom, dag=dag)
        c = Task("c", ok, dag=dag)  # depends on both a (success) and b (failure)
        [a, b] >> c

        run = DAGRunner(dag).run()

        self.assertEqual(run.get_status("a"), TaskStatus.SUCCESS)
        self.assertEqual(run.get_status("b"), TaskStatus.FAILED)
        self.assertEqual(run.get_status("c"), TaskStatus.UPSTREAM_FAILED)


class TestRunHistory(unittest.TestCase):
    def test_run_history_records_status_timing_and_output(self):
        dag = DAG("history")
        Task("a", lambda _u: "hello", dag=dag)

        run = DAGRunner(dag).run()
        result = run.task_results["a"]

        self.assertEqual(result.status, TaskStatus.SUCCESS)
        self.assertEqual(result.output, "hello")
        self.assertIsNone(result.error)
        self.assertIsNotNone(result.start_time)
        self.assertIsNotNone(result.end_time)
        self.assertGreaterEqual(result.end_time, result.start_time)
        self.assertEqual(result.attempts, 1)
        self.assertIsNotNone(run.duration)
        self.assertGreaterEqual(run.duration, 0)

    def test_run_ids_are_unique_per_run(self):
        dag = DAG("history2")
        Task("a", lambda _u: 1, dag=dag)

        runner = DAGRunner(dag)
        run1 = runner.run()
        run2 = runner.run()

        self.assertNotEqual(run1.run_id, run2.run_id)


if __name__ == "__main__":
    unittest.main()
