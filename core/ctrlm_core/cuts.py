"""Shared cut phases 1-3 (docs/partition-algorithm.md).

Each phase moves edges from graph.e_edges to graph.w_edges with the right kind,
preserving the (already canonical) edge order — deterministic by construction.
"""
from __future__ import annotations

from typing import Callable

from .model import (
    EDGE_CYCLIC,
    EDGE_HUB,
    EDGE_PATTERN,
    CtmGraph,
    GraphEdge,
    PartitionConfig,
)


def _move_edges(
    graph: CtmGraph, predicate: Callable[[GraphEdge], bool], kind: str
) -> list[GraphEdge]:
    """Move e_edges matching predicate into w_edges with the given kind."""
    kept: list[GraphEdge] = []
    moved: list[GraphEdge] = []
    for edge in graph.e_edges:
        if predicate(edge):
            moved.append(
                GraphEdge(source=edge.source, target=edge.target, cond=edge.cond, kind=kind)
            )
        else:
            kept.append(edge)
    graph.e_edges[:] = kept
    graph.w_edges.extend(moved)
    return moved


def extract_cyclic(graph: CtmGraph) -> list[str]:
    """Phase 1: cyclic jobs get their own DAG; their E-edges move to W (CYCLIC).

    Returns the sorted uids of cyclic jobs (each becomes a single-job DAG).
    """
    cyclic_uids = sorted(uid for uid, job in graph.nodes.items() if job.cyclic)
    cyclic_set = set(cyclic_uids)
    _move_edges(
        graph,
        lambda e: e.source in cyclic_set or e.target in cyclic_set,
        EDGE_CYCLIC,
    )
    return cyclic_uids


def hub_cuts(graph: CtmGraph, config: PartitionConfig) -> None:
    """Phase 2: cut broadcast (hub) conditions.

    Fan statistics come from the jobs' own in/out condition lists (the Phase 0
    producer/consumer multimaps), so counts are independent of earlier cuts.
    Cut when fan_in >= hub_fan, fan_out >= hub_fan, or the consumers span
    >= hub_spread distinct folders. Synthetic "__"-prefixed conditions are
    EXEMPT: they are intra-folder by construction and a folder with many entry
    jobs must not have its own start node severed.
    """
    producers: dict[str, set[str]] = {}
    consumers: dict[str, set[str]] = {}
    for uid in sorted(graph.nodes):
        job = graph.nodes[uid]
        for cond in job.out_conds:
            name = cond.name.strip()
            if not name or name.startswith("__"):
                continue
            if (cond.sign or "").strip().upper() == "DEL":
                continue
            producers.setdefault(name, set()).add(uid)
        for cond in job.in_conds:
            name = cond.name.strip()
            if not name or name.startswith("__"):
                continue
            consumers.setdefault(name, set()).add(uid)

    hub_conds: set[str] = set()
    for cond_name in sorted(set(producers) | set(consumers)):
        fan_in = len(producers.get(cond_name, ()))
        fan_out = len(consumers.get(cond_name, ()))
        spread = len(
            {graph.nodes[uid].folder for uid in consumers.get(cond_name, ())}
        )
        if fan_in >= config.hub_fan or fan_out >= config.hub_fan or spread >= config.hub_spread:
            hub_conds.add(cond_name)

    if hub_conds:
        _move_edges(graph, lambda e: e.cond in hub_conds, EDGE_HUB)


def pattern_cuts(graph: CtmGraph) -> None:
    """Phase 3: cut direct day-pattern conflicts (components strategy only).

    An edge whose producer AND consumer are both day-scheduled with different
    canonical day_patterns moves to W (PATTERN). TIMEFROM plays no role here.
    """

    def conflicts(edge: GraphEdge) -> bool:
        producer = graph.nodes.get(edge.source)
        consumer = graph.nodes.get(edge.target)
        if producer is None or consumer is None:
            return False
        return (
            producer.day_pattern is not None
            and consumer.day_pattern is not None
            and producer.day_pattern != consumer.day_pattern
        )

    _move_edges(graph, conflicts, EDGE_PATTERN)
