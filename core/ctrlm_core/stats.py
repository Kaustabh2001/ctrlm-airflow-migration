"""Partition statistics shared by both strategies (docs/impl-contracts.md).

compute_stats() returns EXACTLY the documented keys — the dashboard and the
verifiers depend on them. All dict keys are emitted in sorted / fixed order.
"""
from __future__ import annotations

from .model import CrossLink, CtmGraph, DagSpec

SIZE_BUCKETS = ("1", "2-5", "6-15", "16-50", "51+")


def _bucket(size: int) -> str:
    if size <= 1:
        return "1"
    if size <= 5:
        return "2-5"
    if size <= 15:
        return "6-15"
    if size <= 50:
        return "16-50"
    return "51+"


def _counts(values: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in sorted(set(values)):
        out[value] = values.count(value)
    return out


def compute_stats(
    graph: CtmGraph, dags: list[DagSpec], cross_links: list[CrossLink]
) -> dict:
    """Stats over a finished partition. Key set is load-bearing — do not change."""
    histogram = {bucket: 0 for bucket in SIZE_BUCKETS}
    for dag in dags:
        histogram[_bucket(len(dag.jobs))] += 1

    largest = {"dag_id": "", "size": 0}
    for dag in sorted(dags, key=lambda d: (-len(d.jobs), d.dag_id)):
        largest = {"dag_id": dag.dag_id, "size": len(dag.jobs)}
        break

    return {
        "n_jobs": len(graph.nodes),
        "n_dags": len(dags),
        "n_cross_links": len(cross_links),
        "cross_links_by_kind": _counts([link.kind for link in cross_links]),
        "cross_links_by_mechanism": _counts([link.mechanism for link in cross_links]),
        "single_job_dags": sum(1 for dag in dags if len(dag.jobs) == 1),
        "multi_root_dags": sum(1 for dag in dags if len(dag.roots) > 1),
        "dataset_triggered_dags": sum(1 for dag in dags if dag.dataset_triggered),
        "largest_dag": largest,
        "size_histogram": histogram,
    }
