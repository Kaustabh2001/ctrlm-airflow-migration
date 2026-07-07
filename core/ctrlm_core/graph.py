"""Phase 0 — build the condition graph (docs/partition-algorithm.md).

Hash-join producers/consumers by condition name:
  ODAT <-> ODAT           -> e_edges (kind E, the only clustering edges)
  consumer qualifier PREV -> w_edges (kind PREV_RUN)
  STAT / literal dates    -> w_edges (kind REVIEW) + flag
DEL-sign out-conditions never make edges (flagged); self-edges dropped (flagged);
orphans / dead-ends recorded, excluding synthetic "__"-prefixed conditions.
Deterministic: sorted condition names, sorted producer/consumer uids.
"""
from __future__ import annotations

from .model import (
    EDGE_INTRA,
    EDGE_PREV_RUN,
    EDGE_REVIEW,
    CtmGraph,
    Deftable,
    GraphEdge,
    PartitionConfig,
)


def _flag(graph: CtmGraph, level: str, code: str, message: str, subject: str) -> None:
    graph.flags.append(
        {"level": level, "code": code, "message": message, "subject": subject}
    )


def build_graph(deftable: Deftable, config: PartitionConfig) -> CtmGraph:
    """Deftable (post-desugar, post-normalize) -> CtmGraph. Spec Phase 0."""
    graph = CtmGraph()
    index = deftable.job_index()
    for uid in sorted(index):
        graph.nodes[uid] = index[uid]

    # cond -> {uid: qualifier}; first qualifier wins per (cond, uid) — the
    # in/out lists have a fixed parse order, so this is deterministic.
    producers: dict[str, dict[str, str]] = {}
    consumers: dict[str, dict[str, str]] = {}

    for uid in sorted(index):
        job = index[uid]
        for cond in job.out_conds:
            name = cond.name.strip()
            if not name:
                continue
            sign = (cond.sign or "").strip().upper()
            if sign == "DEL":
                _flag(
                    graph, "warn", "DEL_CONDITION",
                    f"out-condition '{name}' with sign DEL on {uid} creates no edge",
                    uid,
                )
                continue
            qualifier = (cond.odate or "").strip().upper() or "ODAT"
            producers.setdefault(name, {}).setdefault(uid, qualifier)
        for cond in job.in_conds:
            name = cond.name.strip()
            if not name:
                continue
            qualifier = (cond.odate or "").strip().upper() or "ODAT"
            consumers.setdefault(name, {}).setdefault(uid, qualifier)

    for cond_name in sorted(set(producers) | set(consumers)):
        prods = producers.get(cond_name, {})
        cons = consumers.get(cond_name, {})
        synthetic = cond_name.startswith("__")

        if not prods:
            if not synthetic:
                graph.orphan_conds.append(
                    {"cond": cond_name, "consumers": sorted(cons)}
                )
            continue
        if not cons:
            if not synthetic:
                graph.dead_end_conds.append(
                    {"cond": cond_name, "producers": sorted(prods)}
                )
            continue

        for producer_uid in sorted(prods):
            for consumer_uid in sorted(cons):
                if producer_uid == consumer_uid:
                    _flag(
                        graph, "warn", "SELF_CONDITION",
                        f"{producer_uid} both produces and consumes '{cond_name}'"
                        " — edge dropped",
                        producer_uid,
                    )
                    continue
                consumer_q = cons[consumer_uid]
                producer_q = prods[producer_uid]
                if consumer_q == "PREV":
                    graph.w_edges.append(
                        GraphEdge(
                            source=producer_uid, target=consumer_uid,
                            cond=cond_name, kind=EDGE_PREV_RUN,
                        )
                    )
                elif producer_q == "ODAT" and consumer_q == "ODAT":
                    graph.e_edges.append(
                        GraphEdge(
                            source=producer_uid, target=consumer_uid,
                            cond=cond_name, kind=EDGE_INTRA,
                        )
                    )
                else:
                    graph.w_edges.append(
                        GraphEdge(
                            source=producer_uid, target=consumer_uid,
                            cond=cond_name, kind=EDGE_REVIEW,
                        )
                    )
                    _flag(
                        graph, "warn", "REVIEW_QUALIFIER",
                        f"condition '{cond_name}' {producer_uid} -> {consumer_uid}"
                        f" has qualifiers {producer_q} -> {consumer_q}: wired for review",
                        cond_name,
                    )
    return graph
