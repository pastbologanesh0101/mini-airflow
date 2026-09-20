"""Bundled example: a small ETL DAG with real data flow.

Shape of the DAG (two independent sources merged before a final
aggregation step -- not just a linear chain):

    extract_a -> transform_a -\
                                 -> merge -> aggregate -> load
    extract_b -> transform_b -/

`extract_a`/`transform_a` and `extract_b`/`transform_b` are two fully
independent branches (no shared dependency) so the executor runs them
concurrently; `merge` waits for both before running.

Run it with:

    python run_dag.py example_etl

`build_dag()` returns a *fresh* DAG + task objects each call, which is
what both the CLI and the test suite use, so repeated runs never share
mutable state.
"""

from __future__ import annotations

import csv
import os
import sqlite3
from typing import Dict, List

from mini_airflow import DAG

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
SOURCE_A_CSV = os.path.join(DATA_DIR, "orders_a.csv")
SOURCE_B_CSV = os.path.join(DATA_DIR, "orders_b.csv")
OUTPUT_DB = os.path.join(DATA_DIR, "output.db")


def _extract(path: str) -> List[Dict[str, str]]:
    """Read raw rows from a CSV file. Extract does no cleaning -- that is
    transform's job -- it just gets the data into memory."""
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _transform(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    """Clean and validate rows: keep only rows with a parseable, positive
    amount. Returns a list of {category, amount} dicts with amount as float."""
    cleaned = []
    for row in rows:
        try:
            amount = float(row["amount"])
        except (KeyError, TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        cleaned.append({"category": row["category"], "amount": amount})
    return cleaned


def _merge(*groups: List[Dict[str, object]]) -> List[Dict[str, object]]:
    merged: List[Dict[str, object]] = []
    for g in groups:
        merged.extend(g)
    return merged


def _aggregate(rows: List[Dict[str, object]]) -> Dict[str, Dict[str, float]]:
    """Aggregate cleaned rows into per-category totals/counts/averages."""
    totals: Dict[str, Dict[str, float]] = {}
    for row in rows:
        cat = row["category"]
        bucket = totals.setdefault(cat, {"total_amount": 0.0, "count": 0})
        bucket["total_amount"] += row["amount"]
        bucket["count"] += 1
    for bucket in totals.values():
        bucket["avg_amount"] = round(bucket["total_amount"] / bucket["count"], 2)
        bucket["total_amount"] = round(bucket["total_amount"], 2)
    return totals


def _load(summary: Dict[str, Dict[str, float]], db_path: str = OUTPUT_DB) -> Dict[str, Dict[str, float]]:
    """Persist the aggregated summary to a SQLite table and return it
    unchanged so it also ends up in the run report."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DROP TABLE IF EXISTS category_summary")
        conn.execute(
            "CREATE TABLE category_summary ("
            "category TEXT PRIMARY KEY, total_amount REAL, count INTEGER, avg_amount REAL)"
        )
        for cat, bucket in summary.items():
            conn.execute(
                "INSERT INTO category_summary (category, total_amount, count, avg_amount) "
                "VALUES (?, ?, ?, ?)",
                (cat, bucket["total_amount"], bucket["count"], bucket["avg_amount"]),
            )
        conn.commit()
    finally:
        conn.close()
    return summary


def build_dag(source_a: str = SOURCE_A_CSV, source_b: str = SOURCE_B_CSV, db_path: str = OUTPUT_DB) -> DAG:
    dag = DAG("example_etl")

    def extract_a(_upstream):
        return _extract(source_a)

    def extract_b(_upstream):
        return _extract(source_b)

    def transform_a(upstream):
        return _transform(upstream["extract_a"])

    def transform_b(upstream):
        return _transform(upstream["extract_b"])

    def merge(upstream):
        return _merge(upstream["transform_a"], upstream["transform_b"])

    def aggregate(upstream):
        return _aggregate(upstream["merge"])

    def load(upstream):
        return _load(upstream["aggregate"], db_path=db_path)

    t_extract_a = dag.task("extract_a")(extract_a)
    t_extract_b = dag.task("extract_b")(extract_b)
    t_transform_a = dag.task("transform_a")(transform_a)
    t_transform_b = dag.task("transform_b")(transform_b)
    t_merge = dag.task("merge")(merge)
    t_aggregate = dag.task("aggregate")(aggregate)
    t_load = dag.task("load")(load)

    t_extract_a >> t_transform_a
    t_extract_b >> t_transform_b
    [t_transform_a, t_transform_b] >> t_merge
    t_merge >> t_aggregate >> t_load

    return dag


# Module-level DAG instance, used by the CLI (`python run_dag.py example_etl`).
dag = build_dag()
