"""End-to-end test of the bundled example ETL DAG: verifies real dependency
wiring (two independent extract/transform branches merged before a final
aggregation step) and mathematically correct aggregated output."""

import os
import sqlite3
import tempfile
import unittest

from mini_airflow import DAGRunner, TaskStatus
from examples import example_etl


class TestExampleETL(unittest.TestCase):
    def test_dag_shape_has_two_independent_branches_before_merge(self):
        dag = example_etl.build_dag()
        self.assertEqual(dag.tasks["merge"].upstream, {dag.tasks["transform_a"], dag.tasks["transform_b"]})
        self.assertEqual(dag.tasks["transform_a"].upstream, {dag.tasks["extract_a"]})
        self.assertEqual(dag.tasks["transform_b"].upstream, {dag.tasks["extract_b"]})
        # the two branches share no dependency until "merge"
        self.assertEqual(dag.tasks["extract_a"].upstream, set())
        self.assertEqual(dag.tasks["extract_b"].upstream, set())

    def test_end_to_end_run_produces_correct_aggregation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "output.db")
            dag = example_etl.build_dag(db_path=db_path)
            run = DAGRunner(dag).run()

            self.assertTrue(run.success, msg={tid: r.status for tid, r in run.task_results.items()})
            for tid in dag.tasks:
                self.assertEqual(run.get_status(tid), TaskStatus.SUCCESS)

            summary = run.get_output("load")

            # Hand-computed from examples/data/orders_a.csv + orders_b.csv:
            #   electronics: 120.50 + 75.25 + 200.00 = 395.75, count 3
            #   home:        40.00 + 60.00            = 100.00, count 2
            #   garden:      15.75                     =  15.75, count 1
            # (A4 "notanumber" and B4 "-5.00" are invalid and must be dropped
            # by the transform step's validation.)
            self.assertEqual(set(summary.keys()), {"electronics", "home", "garden"})

            self.assertAlmostEqual(summary["electronics"]["total_amount"], 395.75, places=2)
            self.assertEqual(summary["electronics"]["count"], 3)
            self.assertAlmostEqual(summary["electronics"]["avg_amount"], 131.92, places=2)

            self.assertAlmostEqual(summary["home"]["total_amount"], 100.00, places=2)
            self.assertEqual(summary["home"]["count"], 2)
            self.assertAlmostEqual(summary["home"]["avg_amount"], 50.00, places=2)

            self.assertAlmostEqual(summary["garden"]["total_amount"], 15.75, places=2)
            self.assertEqual(summary["garden"]["count"], 1)
            self.assertAlmostEqual(summary["garden"]["avg_amount"], 15.75, places=2)

    def test_invalid_rows_are_dropped_by_transform(self):
        rows = example_etl._extract(example_etl.SOURCE_A_CSV)
        cleaned = example_etl._transform(rows)
        cleaned_ids = {r["category"] for r in cleaned}
        # "invalid"/notanumber row must not survive transform
        self.assertNotIn("invalid", cleaned_ids)
        self.assertEqual(len(cleaned), 3)

    def test_load_persists_summary_to_sqlite(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "output.db")
            dag = example_etl.build_dag(db_path=db_path)
            run = DAGRunner(dag).run()
            self.assertTrue(run.success)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    "SELECT category, total_amount, count, avg_amount FROM category_summary ORDER BY category"
                ).fetchall()
            finally:
                conn.close()

            by_category = {r[0]: r for r in rows}
            self.assertEqual(set(by_category.keys()), {"electronics", "garden", "home"})
            self.assertAlmostEqual(by_category["electronics"][1], 395.75, places=2)
            self.assertEqual(by_category["electronics"][2], 3)


if __name__ == "__main__":
    unittest.main()
