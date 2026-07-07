"""Single-entry strategy — ownership propagation (docs/impl-contracts.md).

Shared cut phases run first (Phase 1 extract_cyclic + Phase 2 hub_cuts, NO
pattern_cuts), then ownership propagates over the directed e_edges in Kahn
topological order (ties lexicographic):

- roots (no incoming e_edge) own themselves;
- a node becomes a NEW owner iff its predecessors' owner-set has >= 2 members,
  OR its own day_pattern is non-None and differs from its sole owner root's
  day_pattern (an owner pattern of None counts as a mismatch);
- every e_edge whose endpoints end with different owners moves to the wiring
  set with kind OWNER_SPLIT;
- leftover nodes (condition cycles and anything trapped behind them) fall back
  to weakly-connected groups owned by the lexicographically smallest uid, with
  a warn diagnostic CYCLE_FALLBACK;
- NO singleton coalescing — that is the point of comparison with the
  components strategy; the count is reported (info diagnostic SINGLE_JOB_DAGS).

Determinism: every iteration that affects output is sorted; the Kahn frontier
is a lexicographic heap; dag_id collisions are suffixed in canonical (owner
uid) order. No wall-clock, no randomness.
"""
from __future__ import annotations

import heapq

from ctrlm_core import schedule as sched
from ctrlm_core.cuts import extract_cyclic, hub_cuts
from ctrlm_core.model import (
    EDGE_OWNER,
    EDGE_PREV_RUN,
    EDGE_REVIEW,
    WIRE_DATASET,
    WIRE_PREV,
    WIRE_SENSOR,
    CrossLink,
    CtmGraph,
    DagSpec,
    Diagnostic,
    GraphEdge,
    PartitionConfig,
    PartitionResult,
)
from ctrlm_core.stats import compute_stats

STRATEGY = "single_entry"
START_JOB_NAME = "__FOLDER_START__"


def snake_case(name: str) -> str:
    """Conventions: lowercase, non-alphanumeric -> '_', collapse repeats, strip '_'."""
    raw = "".join(ch.lower() if ch.isalnum() else "_" for ch in name)
    while "__" in raw:
        raw = raw.replace("__", "_")
    return raw.strip("_")


# ------------------------------------------------------------ ownership

def _propagate_owners(
    graph: CtmGraph, node_uids: list[str]
) -> tuple[dict[str, str], list[str]]:
    """Kahn-order ownership propagation over the directed e_edges.

    Returns (owner map for every processed node, sorted leftover uids that
    never reached in-degree 0 — i.e. nodes in or behind condition cycles).
    """
    node_set = set(node_uids)
    preds: dict[str, set[str]] = {u: set() for u in node_uids}
    succs: dict[str, set[str]] = {u: set() for u in node_uids}
    for edge in graph.e_edges:
        if edge.source in node_set and edge.target in node_set and edge.source != edge.target:
            preds[edge.target].add(edge.source)
            succs[edge.source].add(edge.target)

    indeg = {u: len(preds[u]) for u in node_uids}
    heap = sorted(u for u in node_uids if indeg[u] == 0)
    heapq.heapify(heap)

    owner: dict[str, str] = {}
    processed: set[str] = set()
    while heap:
        uid = heapq.heappop(heap)
        processed.add(uid)
        owners = sorted({owner[p] for p in preds[uid]})
        if not owners:
            owner[uid] = uid                      # root owns itself
        elif len(owners) >= 2:
            owner[uid] = uid                      # convergence -> new owner
        else:
            sole = owners[0]
            own_pattern = graph.nodes[uid].day_pattern
            root_pattern = graph.nodes[sole].day_pattern
            # own non-None pattern differing from the owner root's pattern
            # (root pattern None counts as a mismatch) -> new owner
            if own_pattern is not None and own_pattern != root_pattern:
                owner[uid] = uid
            else:
                owner[uid] = sole
        for succ in sorted(succs[uid]):
            indeg[succ] -= 1
            if indeg[succ] == 0:
                heapq.heappush(heap, succ)

    leftover = sorted(node_set - processed)
    return owner, leftover


def _fallback_groups(graph: CtmGraph, leftover: list[str]) -> list[list[str]]:
    """Weakly-connected components of the leftover subgraph, each sorted;
    groups emitted in order of their lexicographically smallest member."""
    leftover_set = set(leftover)
    adj: dict[str, set[str]] = {u: set() for u in leftover}
    for edge in graph.e_edges:
        if edge.source in leftover_set and edge.target in leftover_set:
            adj[edge.source].add(edge.target)
            adj[edge.target].add(edge.source)

    groups: list[list[str]] = []
    seen: set[str] = set()
    for uid in leftover:                          # already sorted
        if uid in seen:
            continue
        component: set[str] = set()
        stack = [uid]
        while stack:
            node = stack.pop()
            if node in component:
                continue
            component.add(node)
            stack.extend(sorted(adj[node] - component))
        seen |= component
        groups.append(sorted(component))
    return groups


# ------------------------------------------------------------ partition

def partition(graph: CtmGraph, config: PartitionConfig) -> PartitionResult:
    """Single-entry partition: shared cuts, ownership propagation, wiring."""
    diagnostics: list[Diagnostic] = []

    # Phases 1-2 (shared). Phase 3 pattern_cuts is intentionally NOT run:
    # pattern conflicts are handled inside the ownership rule instead.
    cyclic_uids = extract_cyclic(graph)
    hub_cuts(graph, config)
    cyclic_set = set(cyclic_uids)

    prop_nodes = sorted(uid for uid in graph.nodes if uid not in cyclic_set)
    owner, leftover = _propagate_owners(graph, prop_nodes)

    for group in _fallback_groups(graph, leftover):
        head = group[0]                           # lexicographically smallest uid
        for uid in group:
            owner[uid] = head
        diagnostics.append(
            Diagnostic(
                level="warn",
                code="CYCLE_FALLBACK",
                message=(
                    f"condition cycle: {len(group)} job(s) grouped by weak "
                    f"connectivity under {head}: {', '.join(group)}"
                ),
                subject=head,
            )
        )

    # e_edges crossing owners -> wiring set, kind OWNER_SPLIT
    kept: list[GraphEdge] = []
    for edge in graph.e_edges:
        if owner.get(edge.source) == owner.get(edge.target):
            kept.append(edge)
        else:
            graph.w_edges.append(
                GraphEdge(source=edge.source, target=edge.target, cond=edge.cond, kind=EDGE_OWNER)
            )
    graph.e_edges[:] = kept

    # groups: owner uid -> sorted members; cyclic jobs are single-job groups
    members: dict[str, list[str]] = {}
    for uid in prop_nodes:                        # sorted -> member lists sorted
        members.setdefault(owner[uid], []).append(uid)
    for uid in cyclic_uids:
        members[uid] = [uid]

    intra_preds: dict[str, set[str]] = {uid: set() for uid in graph.nodes}
    for edge in graph.e_edges:
        if edge.source != edge.target:
            intra_preds[edge.target].add(edge.source)

    # naming + specs, in canonical (sorted owner uid) order
    used_ids: set[str] = set()
    assignments: dict[str, str] = {}
    specs: list[DagSpec] = []
    spec_by_id: dict[str, DagSpec] = {}

    for key in sorted(members):
        owner_job = graph.nodes[key]
        if owner_job.synthetic and owner_job.name == START_JOB_NAME:
            base = snake_case(owner_job.folder)   # folder-start -> folder name
        else:
            base = snake_case(owner_job.name)
        base = base or "dag"
        dag_id = base
        suffix = 2
        while dag_id in used_ids:                 # canonical collision suffixes
            dag_id = f"{base}_{suffix}"
            suffix += 1
        used_ids.add(dag_id)

        uids = members[key]
        member_set = set(uids)
        roots = [u for u in uids if not (intra_preds[u] & member_set)]
        folders = sorted({graph.nodes[u].folder for u in uids})

        if key in cyclic_set:
            spec = DagSpec(
                dag_id=dag_id, jobs=uids, roots=roots, folders=folders,
                day_pattern=owner_job.day_pattern,
                anchor=owner_job.timefrom.strip(),
                schedule=sched.cyclic_cron(owner_job),
            )
        elif owner_job.day_pattern is None:       # condition-driven root
            spec = DagSpec(
                dag_id=dag_id, jobs=uids, roots=roots, folders=folders,
                day_pattern=None, anchor="", schedule=None,
                dataset_triggered=True,
            )
        else:
            anchor = owner_job.timefrom.strip() or config.default_timefrom
            spec = DagSpec(
                dag_id=dag_id, jobs=uids, roots=roots, folders=folders,
                day_pattern=owner_job.day_pattern, anchor=anchor,
                schedule=sched.cron_for(owner_job.day_pattern, anchor),
            )
            if sched.cron_and_approx(owner_job.day_pattern):
                diagnostics.append(
                    Diagnostic(
                        level="warn", code="CRON_AND_APPROX",
                        message=(
                            f"day pattern '{owner_job.day_pattern}' of {dag_id} uses "
                            "weekday-AND-monthday; cron approximates with monthdays only"
                        ),
                        subject=dag_id,
                    )
                )
        if len(uids) > config.max_tasks:
            diagnostics.append(
                Diagnostic(
                    level="warn", code="OVERSIZED",
                    message=f"{dag_id} has {len(uids)} tasks (> max_tasks={config.max_tasks})",
                    subject=dag_id,
                )
            )

        specs.append(spec)
        spec_by_id[dag_id] = spec
        for uid in uids:
            assignments[uid] = dag_id

    # wiring: final w_edges (endpoints existing) -> cross_links,
    # deduped by (source, target, kind) with cond names merged
    merged: dict[tuple[str, str, str], set[str]] = {}
    for edge in graph.w_edges:
        if edge.source in graph.nodes and edge.target in graph.nodes:
            merged.setdefault((edge.source, edge.target, edge.kind), set()).add(edge.cond)

    cross_links: list[CrossLink] = []
    for source, target, kind in sorted(merged):
        conds = sorted(merged[(source, target, kind)])
        consumer_spec = spec_by_id[assignments[target]]
        if kind == EDGE_PREV_RUN:
            mechanism = WIRE_PREV
        elif kind == EDGE_REVIEW:
            mechanism = WIRE_SENSOR
            diagnostics.append(
                Diagnostic(
                    level="warn", code="REVIEW_QUALIFIER",
                    message=(
                        f"REVIEW-qualified link {source} -> {target} "
                        f"({', '.join(conds)}) wired as sensor; verify semantics"
                    ),
                    subject=",".join(conds),
                )
            )
        elif consumer_spec.schedule is not None:  # consumer time-scheduled
            mechanism = WIRE_SENSOR
        else:
            mechanism = WIRE_DATASET
        cross_links.append(
            CrossLink(source=source, target=target, conds=conds, kind=kind, mechanism=mechanism)
        )

    # inbound dataset URIs for dataset-triggered consumers
    inbound: dict[str, set[str]] = {}
    for link in cross_links:
        if link.mechanism == WIRE_DATASET:
            dag_id = assignments[link.target]
            for cond in link.conds:
                inbound.setdefault(dag_id, set()).add(f"ctrlm://cond/{cond}")
    for dag_id in sorted(inbound):
        spec_by_id[dag_id].datasets = sorted(inbound[dag_id])

    specs.sort(key=lambda s: s.dag_id)
    single_jobs = sum(1 for s in specs if len(s.jobs) == 1)
    diagnostics.append(
        Diagnostic(
            level="info", code="SINGLE_JOB_DAGS",
            message=(
                f"{single_jobs} single-job DAG(s); the single_entry strategy "
                "never coalesces singletons"
            ),
            subject=STRATEGY,
        )
    )

    result = PartitionResult(
        strategy=STRATEGY,
        dags=specs,
        assignments=dict(sorted(assignments.items())),
        cross_links=cross_links,
        diagnostics=diagnostics,
    )
    result.stats = compute_stats(graph, specs, cross_links)
    return result
