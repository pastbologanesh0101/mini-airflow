"""Tests for DAG/Task construction: dependency wiring and cycle rejection."""

import unittest

from mini_airflow import DAG, DAGCycleError, DuplicateTaskError, Task


def noop(_upstream):
    return None


class TestTaskOperators(unittest.TestCase):
    def test_set_upstream_wires_both_sides(self):
        dag = DAG("d")
        a = Task("a", noop, dag=dag)
        b = Task("b", noop, dag=dag)
        b.set_upstream(a)
        self.assertIn(a, b.upstream)
        self.assertIn(b, a.downstream)

    def test_rshift_chain(self):
        dag = DAG("d")
        a, b, c = (Task(x, noop, dag=dag) for x in "abc")
        a >> b >> c
        self.assertEqual(b.upstream, {a})
        self.assertEqual(c.upstream, {b})
        self.assertEqual(a.downstream, {b})

    def test_rshift_fan_out_to_list(self):
        dag = DAG("d")
        a, b, c = (Task(x, noop, dag=dag) for x in "abc")
        a >> [b, c]
        self.assertEqual(b.upstream, {a})
        self.assertEqual(c.upstream, {a})
        self.assertEqual(a.downstream, {b, c})

    def test_list_rshift_fan_in(self):
        dag = DAG("d")
        a, b, c = (Task(x, noop, dag=dag) for x in "abc")
        [a, b] >> c
        self.assertEqual(c.upstream, {a, b})

    def test_lshift_operator(self):
        dag = DAG("d")
        a, b = (Task(x, noop, dag=dag) for x in "ab")
        b << a  # b depends on a
        self.assertEqual(b.upstream, {a})


class TestCycleDetection(unittest.TestCase):
    def test_direct_cycle_rejected_at_construction(self):
        dag = DAG("d")
        a = Task("a", noop, dag=dag)
        b = Task("b", noop, dag=dag)
        a >> b
        with self.assertRaises(DAGCycleError):
            b >> a

    def test_longer_cycle_rejected_at_construction(self):
        dag = DAG("d")
        a, b, c = (Task(x, noop, dag=dag) for x in "abc")
        a >> b >> c
        with self.assertRaises(DAGCycleError):
            c >> a

    def test_self_loop_rejected(self):
        dag = DAG("d")
        a = Task("a", noop, dag=dag)
        with self.assertRaises(DAGCycleError):
            a.set_upstream(a)

    def test_dag_still_usable_after_rejected_cycle(self):
        """A rejected `>>` must not leave partial state behind."""
        dag = DAG("d")
        a, b, c = (Task(x, noop, dag=dag) for x in "abc")
        a >> b >> c
        with self.assertRaises(DAGCycleError):
            c >> a
        # existing valid edges are untouched, and the DAG is still runnable
        order = dag.topological_order()
        self.assertEqual(order, ["a", "b", "c"])


class TestDAGRegistry(unittest.TestCase):
    def test_duplicate_task_id_raises(self):
        dag = DAG("d")
        Task("x", noop, dag=dag)
        with self.assertRaises(DuplicateTaskError):
            Task("x", noop, dag=dag)

    def test_topological_order_respects_diamond_dependencies(self):
        dag = DAG("d")
        a, b, c, d = (Task(x, noop, dag=dag) for x in "abcd")
        a >> [b, c] >> d
        order = dag.topological_order()
        self.assertLess(order.index("a"), order.index("b"))
        self.assertLess(order.index("a"), order.index("c"))
        self.assertLess(order.index("b"), order.index("d"))
        self.assertLess(order.index("c"), order.index("d"))

    def test_task_decorator_registers_task(self):
        dag = DAG("d")

        @dag.task("only")
        def only_task(_upstream):
            return 1

        self.assertIn("only", dag.tasks)
        self.assertIsInstance(dag.tasks["only"], Task)


if __name__ == "__main__":
    unittest.main()
