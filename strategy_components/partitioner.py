"""Components strategy — Phases 1-9 of docs/partition-algorithm.md.

partition(graph, config) -> PartitionResult with strategy="components":
connected components of the condition graph become DAGs, after the shared
cuts (cyclic / hub / pattern), singleton coalescing, transitive-pattern
min-cut resolution (AUTO_RESOLVED) and root anchor-spread min-cuts (ANCHOR).

Determinism (invariant I4): every iteration that affects output is sorted,
min-cut inputs are fed in canonical (source, target, cond) order and solved
with edmonds_karp on a graph built in canonical insertion order.
"""
from __future__ import annotations

import re
from collections import deque

import networkx as nx
from networkx.algorithms.flow import edmonds_karp

from ctrlm_core import cuts, schedule, stats
from ctrlm_core.model import (
    EDGE_ANCHOR,
    EDGE_AUTO,
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
    Job,
    PartitionConfig,
    PartitionResult,
)

STRATEGY = "components"
_BIG = 10**9
_SOURCE = "__MINCUT_SOURCE__"
_SINK = "__MINCUT_SINK__"


# ---------------------------------------------------------------- helpers

def snake_case(name: str) -> str:
    """lowercase, non-alphanumeric -> '_', collapse repeats, strip '_'."""
    out = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return out or "dag"


class _UnionFind:
    """Deterministic union-find: the lexicographically smaller root wins."""

    def __init__(self, items: list[str]) -> None:
        self.parent: dict[str, str] = {item: item for item in items}

    def find(self, item: str) -> str:
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[item] != root:          # path compression
            self.parent[item], item = root, self.parent[item]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if rb < ra:
            ra, rb = rb, ra
        self.parent[rb] = ra

    def groups(self) -> list[list[str]]:
        by_root: dict[str, list[str]] = {}
        for item in sorted(self.parent):
            by_root.setdefault(self.find(item), []).append(item)
        return [by_root[root] for root in sorted(by_root)]


def _cluster_edges(graph: CtmGraph, members: set[str]) -> list[GraphEdge]:
    """Surviving e_edges with both endpoints inside the cluster."""
    return [e for e in graph.e_edges if e.source in members and e.target in members]


def _split(graph: CtmGraph, members: list[str]) -> list[list[str]]:
    """Re-split a cluster into components over the surviving e_edges."""
    uf = _UnionFind(sorted(members))
    for edge in _cluster_edges(graph, set(members)):
        uf.union(edge.source, edge.target)
    return uf.groups()


def _roots_of(graph: CtmGraph, members: list[str]) -> list[str]:
    """Members with no incoming e_edge from inside the cluster (sorted)."""
    mset = set(members)
    targets = {e.target for e in graph.e_edges if e.source in mset and e.target in mset}
    return [uid for uid in sorted(members) if uid not in targets]


def _effective_timefrom(job: Job, config: PartitionConfig) -> str:
    return job.timefrom.strip() or config.default_timefrom


def _anchor_rel(job: Job, config: PartitionConfig) -> int:
    """Root anchor time on the ODATE clock (spec Phase 8 `rel`)."""
    return schedule.rel_minutes(_effective_timefrom(job, config), config.new_day_time)


def _move_cut_edges(graph: CtmGraph, cut_edges: list[GraphEdge], kind: str) -> None:
    """Move the given e_edges into w_edges with the given kind (order-preserving)."""
    cut_keys = {(e.source, e.target, e.cond) for e in cut_edges}
    kept: list[GraphEdge] = []
    for edge in graph.e_edges:
        if (edge.source, edge.target, edge.cond) in cut_keys:
            graph.w_edges.append(
                GraphEdge(source=edge.source, target=edge.target, cond=edge.cond, kind=kind)
            )
        else:
            kept.append(edge)
    graph.e_edges[:] = kept


def _min_cut_edges(
    graph: CtmGraph,
    members: set[str],
    source_group: list[str],
    sink_group: list[str],
) -> list[GraphEdge]:
    """Deterministic minimum edge cut between two terminal groups (Phase 6).

    Undirected projection as a DiGraph with both directions; capacity = number
    of parallel condition pairs between the endpoints; super source/sink with
    capacity 10**9 to the terminal groups. The cut is every e_edge crossing
    the reachable partition.
    """
    edges = _cluster_edges(graph, members)
    dg = nx.DiGraph()
    for uid in sorted(members):
        dg.add_node(uid)
    capacity: dict[tuple[str, str], int] = {}
    for edge in sorted(edges, key=lambda e: (e.source, e.target, e.cond)):
        for a, b in ((edge.source, edge.target), (edge.target, edge.source)):
            capacity[(a, b)] = capacity.get((a, b), 0) + 1
    for (a, b) in sorted(capacity):
        dg.add_edge(a, b, capacity=capacity[(a, b)])
    for uid in sorted(source_group):
        dg.add_edge(_SOURCE, uid, capacity=_BIG)
    for uid in sorted(sink_group):
        dg.add_edge(uid, _SINK, capacity=_BIG)

    _, (reachable, _) = nx.minimum_cut(dg, _SOURCE, _SINK, flow_func=edmonds_karp)
    reachable_set = set(reachable)
    return [e for e in edges if (e.source in reachable_set) != (e.target in reachable_set)]


# ---------------------------------------------------------------- Phase 5

def _coalesce_singletons(graph: CtmGraph, clusters: list[list[str]]) -> list[list[str]]:
    """Merge size-1 components sharing (folder, day_pattern) into one cluster each."""
    multi = [c for c in clusters if len(c) > 1]
    groups: dict[tuple[str, str], list[str]] = {}
    for cluster in clusters:
        if len(cluster) != 1:
            continue
        uid = cluster[0]
        job = graph.nodes[uid]
        key = (job.folder, job.day_pattern or "")
        groups.setdefault(key, []).append(uid)
    merged = [sorted(members) for _, members in sorted(groups.items())]
    return sorted(multi + merged, key=lambda c: c[0])


# ---------------------------------------------------------------- Phase 6

def _pattern_purity(graph: CtmGraph, clusters: list[list[str]]) -> list[list[str]]:
    """Worklist min-cut re-splitting until every cluster has <= 1 day_pattern (I3)."""
    queue: deque[list[str]] = deque(sorted(clusters, key=lambda c: c[0]))
    final: list[list[str]] = []
    while queue:
        members = queue.popleft()
        patterns = sorted(
            {graph.nodes[u].day_pattern for u in members} - {None}  # type: ignore[arg-type]
        )
        if len(patterns) <= 1:
            final.append(sorted(members))
            continue
        counts = {
            p: sum(1 for u in members if graph.nodes[u].day_pattern == p) for p in patterns
        }
        rarest = min(patterns, key=lambda p: (counts[p], p))
        minor = [u for u in sorted(members) if graph.nodes[u].day_pattern == rarest]
        rest = [
            u for u in sorted(members)
            if graph.nodes[u].day_pattern not in (None, rarest)
        ]
        cut_edges = _min_cut_edges(graph, set(members), minor, rest)
        if not cut_edges:                      # defensive: terminals already split
            final.append(sorted(members))
            continue
        _move_cut_edges(graph, cut_edges, EDGE_AUTO)
        queue.extend(_split(graph, members))   # parts re-checked until pure
    return sorted(final, key=lambda c: c[0])


# ---------------------------------------------------------------- anchor purity

def _anchor_purity(
    graph: CtmGraph, clusters: list[list[str]], config: PartitionConfig
) -> list[list[str]]:
    """While scheduled roots spread > anchor_spread_hours*60: min-cut rarest bucket."""
    threshold = config.anchor_spread_hours * 60
    queue: deque[list[str]] = deque(sorted(clusters, key=lambda c: c[0]))
    final: list[list[str]] = []
    while queue:
        members = queue.popleft()
        roots = _roots_of(graph, members)
        scheduled_roots = [u for u in roots if graph.nodes[u].day_pattern is not None]
        if len(scheduled_roots) < 2:
            final.append(sorted(members))
            continue
        rels = {u: _anchor_rel(graph.nodes[u], config) for u in scheduled_roots}
        if max(rels.values()) - min(rels.values()) <= threshold:
            final.append(sorted(members))
            continue
        buckets: dict[int, list[str]] = {}
        for uid in scheduled_roots:            # already sorted
            buckets.setdefault(rels[uid], []).append(uid)
        rarest_rel = min(buckets, key=lambda r: (len(buckets[r]), r))
        minor = buckets[rarest_rel]
        rest = [u for u in scheduled_roots if rels[u] != rarest_rel]
        cut_edges = _min_cut_edges(graph, set(members), minor, rest)
        if not cut_edges:                      # defensive: cannot separate
            final.append(sorted(members))
            continue
        _move_cut_edges(graph, cut_edges, EDGE_ANCHOR)
        queue.extend(_split(graph, members))
    return sorted(final, key=lambda c: c[0])


# ---------------------------------------------------------------- Phase 8

def _modal_folder(graph: CtmGraph, members: list[str]) -> str:
    counts: dict[str, int] = {}
    for uid in members:
        folder = graph.nodes[uid].folder
        counts[folder] = counts.get(folder, 0) + 1
    return min(sorted(counts), key=lambda f: (-counts[f], f))


def _assign_dag_ids(graph: CtmGraph, clusters: list[list[str]]) -> list[str]:
    """snake_case(modal folder); collisions get _2, _3 in canonical cluster order."""
    seen: dict[str, int] = {}
    names: list[str] = []
    for members in clusters:
        base = snake_case(_modal_folder(graph, members))
        n = seen.get(base, 0) + 1
        seen[base] = n
        names.append(base if n == 1 else f"{base}_{n}")
    return names


def _build_dag_spec(
    graph: CtmGraph,
    config: PartitionConfig,
    dag_id: str,
    members: list[str],
    is_cyclic: bool,
    diagnostics: list[Diagnostic],
) -> DagSpec:
    """Phase 8: schedule, anchor, roots, folders for one cluster."""
    jobs = sorted(members)
    roots = _roots_of(graph, jobs)
    folders = sorted({graph.nodes[u].folder for u in jobs})

    if is_cyclic:
        job = graph.nodes[jobs[0]]
        return DagSpec(
            dag_id=dag_id, jobs=jobs, roots=jobs, folders=folders,
            day_pattern=job.day_pattern, anchor=job.timefrom.strip(),
            schedule=schedule.cyclic_cron(job), dataset_triggered=False,
        )

    patterns = sorted({graph.nodes[u].day_pattern for u in jobs} - {None})  # type: ignore[arg-type]
    pattern = patterns[0] if patterns else None
    if pattern is None:
        # pattern-less cluster -> dataset-triggered (datasets filled in Phase 9)
        return DagSpec(
            dag_id=dag_id, jobs=jobs, roots=roots, folders=folders,
            day_pattern=None, anchor="", schedule=None, dataset_triggered=True,
        )

    scheduled_roots = [u for u in roots if graph.nodes[u].day_pattern is not None]
    anchored = scheduled_roots or [
        u for u in jobs if graph.nodes[u].day_pattern is not None
    ]
    best = min(anchored, key=lambda u: (_anchor_rel(graph.nodes[u], config), u))
    anchor = _effective_timefrom(graph.nodes[best], config)
    cron = schedule.cron_for(pattern, anchor)
    if schedule.cron_and_approx(pattern):
        diagnostics.append(Diagnostic(
            level="warn", code="CRON_AND_APPROX",
            message=f"day pattern '{pattern}' mixes weekdays AND monthdays;"
                    " cron approximates with the monthday field only",
            subject=dag_id,
        ))
    return DagSpec(
        dag_id=dag_id, jobs=jobs, roots=roots, folders=folders,
        day_pattern=pattern, anchor=anchor, schedule=cron, dataset_triggered=False,
    )


# ---------------------------------------------------------------- Phase 9

def _build_cross_links(
    graph: CtmGraph,
    dag_of: dict[str, str],
    dag_by_id: dict[str, DagSpec],
    diagnostics: list[Diagnostic],
) -> list[CrossLink]:
    """Dedupe w_edges by (source, target, kind), merge conds, pick mechanisms."""
    merged: dict[tuple[str, str, str], set[str]] = {}
    for edge in graph.w_edges:
        if edge.source not in graph.nodes or edge.target not in graph.nodes:
            continue
        merged.setdefault((edge.source, edge.target, edge.kind), set()).add(edge.cond)

    links: list[CrossLink] = []
    for (source, target, kind) in sorted(merged):
        conds = sorted(merged[(source, target, kind)])
        if kind == EDGE_PREV_RUN:
            mechanism = WIRE_PREV
        elif kind == EDGE_REVIEW:
            mechanism = WIRE_SENSOR
            diagnostics.append(Diagnostic(
                level="warn", code="REVIEW_QUALIFIER",
                message=f"non-ODAT qualifier link {source} -> {target}"
                        f" ({', '.join(conds)}) wired as sensor — review",
                subject=f"{source}->{target}",
            ))
        else:
            consumer_dag = dag_by_id[dag_of[target]]
            mechanism = WIRE_SENSOR if consumer_dag.schedule is not None else WIRE_DATASET
        links.append(CrossLink(
            source=source, target=target, conds=conds, kind=kind, mechanism=mechanism,
        ))
    return links


def _fill_datasets(dags: list[DagSpec], dag_of: dict[str, str], links: list[CrossLink]) -> None:
    """Dataset-triggered DAGs are scheduled on their inbound dataset URIs."""
    inbound: dict[str, set[str]] = {}
    for link in links:
        if link.mechanism != WIRE_DATASET:
            continue
        dag_id = dag_of[link.target]
        for cond in link.conds:
            inbound.setdefault(dag_id, set()).add(f"ctrlm://cond/{cond}")
    for dag in dags:
        if dag.dataset_triggered:
            dag.datasets = sorted(inbound.get(dag.dag_id, set()))


# ---------------------------------------------------------------- entry point

def partition(graph: CtmGraph, config: PartitionConfig) -> PartitionResult:
    """Phases 1-9 of docs/partition-algorithm.md (components strategy).

    Mutates the graph in place (cut phases move e_edges into w_edges), exactly
    like the shared cut helpers — emit consumes the final edge sets.
    """
    diagnostics: list[Diagnostic] = []

    # Phases 1-3: shared cuts (Phase 4 manual cuts: none in one-time mode)
    cyclic_uids = cuts.extract_cyclic(graph)
    cuts.hub_cuts(graph, config)
    cuts.pattern_cuts(graph)

    # Phase 5: connected components (undirected) over the surviving e_edges
    cyclic_set = set(cyclic_uids)
    uf = _UnionFind(sorted(u for u in graph.nodes if u not in cyclic_set))
    for edge in graph.e_edges:
        uf.union(edge.source, edge.target)
    clusters = uf.groups()
    if config.coalesce_singletons:
        clusters = _coalesce_singletons(graph, clusters)

    # Phase 6: transitive pattern conflicts (AUTO_RESOLVED), then anchor purity
    clusters = _pattern_purity(graph, clusters)
    clusters = _anchor_purity(graph, clusters, config)

    # Phase 8: canonical cluster order (min member uid), names, schedules
    all_clusters: list[tuple[list[str], bool]] = (
        [(members, False) for members in clusters]
        + [([uid], True) for uid in cyclic_uids]
    )
    all_clusters.sort(key=lambda item: item[0][0])
    dag_ids = _assign_dag_ids(graph, [members for members, _ in all_clusters])

    dags: list[DagSpec] = []
    dag_of: dict[str, str] = {}
    for (members, is_cyclic), dag_id in zip(all_clusters, dag_ids):
        spec = _build_dag_spec(graph, config, dag_id, members, is_cyclic, diagnostics)
        dags.append(spec)
        for uid in spec.jobs:
            dag_of[uid] = dag_id
        # Phase 7: size guardrail (report-only)
        if len(spec.jobs) > config.max_tasks:
            diagnostics.append(Diagnostic(
                level="warn", code="OVERSIZED",
                message=f"DAG '{dag_id}' has {len(spec.jobs)} tasks"
                        f" (> max_tasks={config.max_tasks}); generated as-is",
                subject=dag_id,
            ))

    # Phase 9: wiring set
    dag_by_id = {dag.dag_id: dag for dag in dags}
    cross_links = _build_cross_links(graph, dag_of, dag_by_id, diagnostics)
    _fill_datasets(dags, dag_of, cross_links)

    for uid in sorted(graph.nodes):
        if graph.nodes[uid].confirm:
            diagnostics.append(Diagnostic(
                level="warn", code="CONFIRM_JOB",
                message=f"{uid} requires manual CONFIRM — needs an approval gate",
                subject=uid,
            ))
    single = sum(1 for dag in dags if len(dag.jobs) == 1)
    if single:
        diagnostics.append(Diagnostic(
            level="info", code="SINGLE_JOB_DAGS",
            message=f"{single} single-job DAG(s) in the components partition",
            subject=STRATEGY,
        ))

    dags.sort(key=lambda d: d.dag_id)
    assignments = {uid: dag_of[uid] for uid in sorted(dag_of)}
    return PartitionResult(
        strategy=STRATEGY,
        dags=dags,
        assignments=assignments,
        cross_links=cross_links,
        diagnostics=diagnostics,
        stats=stats.compute_stats(graph, dags, cross_links),
    )
