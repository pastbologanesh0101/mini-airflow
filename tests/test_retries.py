"""Tests for per-task retry/backoff logic, using an injectable sleep_fn so
no test has to wait on a real clock."""

import unittest

from mini_airflow import DAG, DAGRunner, Task, TaskStatus


class TestRetries(unittest.TestCase):
    def test_task_succeeds_after_configured_number_of_retries(self):
        dag = DAG("retry-success")
        calls = {"n": 0}
        sleep_calls = []

        def flaky(_upstream):
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError(f"transient failure #{calls['n']}")
            return "ok"

        Task("flaky", flaky, retries=2, retry_delay=0.01, dag=dag)

        runner = DAGRunner(dag, sleep_fn=sleep_calls.append)
        run = runner.run()
        result = run.task_results["flaky"]

        self.assertEqual(result.status, TaskStatus.SUCCESS)
        self.assertEqual(result.output, "ok")
        self.assertEqual(result.attempts, 3)
        self.assertEqual(calls["n"], 3)
        # linear backoff: retry_delay * attempt_number, for the 2 retries
        self.assertEqual(sleep_calls, [0.01 * 1, 0.01 * 2])

    def test_task_gives_up_after_exhausting_retries(self):
        dag = DAG("retry-exhausted")
        calls = {"n": 0}
        sleep_calls = []

        def always_fails(_upstream):
            calls["n"] += 1
            raise RuntimeError("permanent failure")

        Task("bad", always_fails, retries=2, retry_delay=0.01, dag=dag)

        runner = DAGRunner(dag, sleep_fn=sleep_calls.append)
        run = runner.run()
        result = run.task_results["bad"]

        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertEqual(result.attempts, 3)  # 1 initial + 2 retries
        self.assertEqual(calls["n"], 3)
        self.assertIn("permanent failure", result.error)
        self.assertEqual(len(sleep_calls), 2)  # slept between attempts 1->2 and 2->3, not after the last

    def test_zero_retries_fails_immediately_after_one_attempt(self):
        dag = DAG("retry-zero")
        calls = {"n": 0}

        def always_fails(_upstream):
            calls["n"] += 1
            raise RuntimeError("nope")

        Task("bad", always_fails, retries=0, dag=dag)

        runner = DAGRunner(dag, sleep_fn=lambda s: (_ for _ in ()).throw(AssertionError("should not sleep")))
        run = runner.run()
        result = run.task_results["bad"]

        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertEqual(result.attempts, 1)
        self.assertEqual(calls["n"], 1)

    def test_downstream_of_a_retried_but_ultimately_successful_task_still_runs(self):
        dag = DAG("retry-then-downstream")
        calls = {"n": 0}

        def flaky(_upstream):
            calls["n"] += 1
            if calls["n"] < 2:
                raise ValueError("once")
            return 10

        a = Task("a", flaky, retries=1, retry_delay=0.01, dag=dag)
        b = Task("b", lambda u: u["a"] + 1, dag=dag)
        a >> b

        run = DAGRunner(dag, sleep_fn=lambda s: None).run()

        self.assertTrue(run.success)
        self.assertEqual(run.get_output("a"), 10)
        self.assertEqual(run.get_output("b"), 11)


if __name__ == "__main__":
    unittest.main()
