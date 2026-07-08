"""Build the offline comparison dashboard (contracts: impl-contracts-v2.md V2-4,
impl-contracts-v5.md V5-2).

v2: each strategy output dir is a SCOPE TREE — one sub-directory per input XML
file plus a run-level scopes.json. For each scope this reads graph.json +
ir.json from the components output dir (--a) and partition.json + dag_plans.json
from both strategy output dirs (--a, --b), then renders ONE self-contained HTML
file: the vis-network library bundled inside the installed pyvis package is
inlined once, the per-scope data is embedded as JSON, and
dashboard/template.html supplies the page + custom JS (scope selector, five
per-scope tabbed views, plus a run-level overview: cross_scope_links,
dag_id_collisions, per-scope stats). No CDN / network requests at view time.

v5: the Control-M structure view is level-wise (hierarchical LR); per-node
`level` = longest-path depth from the roots of the ORIGINAL dependency graph
(e_edges + w_edges) is computed HERE (deterministically, with a cycle guard)
and embedded. Both strategy tabs gain a "DAG graph" sub-mode fed by each
scope's dag_plans.json (task-level plan of every generated DAG, written by
emit); per-task levels over the `upstream` edges are computed here too.

Determinism: every iteration that affects output is sorted; the embedded JSON
is dumped with sort_keys; no wall-clock, no randomness.

Usage:
    python dashboard/build.py --a output/components --b output/single_entry \
        -o output/dashboard/index.html
"""
from __future__ import annotations

import argparse
import heapq
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "template.html"


# ------------------------------------------------------------------ inputs

def load_json(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"required input not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_dag_plans(path: Path) -> dict:
    """dag_plans.json is written by v5 emit; old output trees lack it."""
    if not path.is_file():
        raise FileNotFoundError(
            f"dag_plans.json not found: {path} — the strategy output predates "
            "v5; regenerate it (strategy_components/run.py and "
            "strategy_single_entry/run.py now write <scope>/dag_plans.json)"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def find_vis_js() -> str:
    """Locate vis-network.min.js bundled inside the installed pyvis package.

    Picks the highest bundled version deterministically and returns its source
    with any '</script' neutralised so it can be inlined into a <script> tag.
    """
    try:
        import pyvis
    except ImportError as exc:  # pragma: no cover - pyvis is a hard requirement
        raise RuntimeError(
            "pyvis is required (its bundled vis-network is inlined into the "
            "dashboard); pip install pyvis"
        ) from exc

    base = Path(pyvis.__file__).resolve().parent
    candidates = sorted(base.glob("templates/lib/vis-*/vis-network.min.js"))
    candidates += sorted(base.glob("lib/vis-*/vis-network.min.js"))
    if not candidates:
        candidates = sorted(base.rglob("vis-network.min.js"))
    if not candidates:
        raise RuntimeError(f"vis-network.min.js not found under {base}")

    def version_key(p: Path) -> tuple:
        m = re.search(r"vis-(\d+(?:\.\d+)*)", p.parent.name)
        ver = tuple(int(x) for x in m.group(1).split(".")) if m else (0,)
        return (ver, str(p))  # path as deterministic tie-break

    best = max(candidates, key=version_key)
    return best.read_text(encoding="utf-8").replace("</script", "<\\/script")


# ------------------------------------------------------------------ levels

def compute_levels(
    node_ids: list[str], edges: list[tuple[str, str]]
) -> tuple[dict[str, int], set[str]]:
    """Longest-path depth from roots for every node, with a cycle guard.

    Returns (levels, cyclic): `levels[n]` is the longest-path distance from any
    in-degree-0 node (roots = 0). Nodes that a topological pass cannot resolve
    (on a cycle, or only reachable through one) get
    level = max(level of already-levelled predecessors) + 1 (0 if none) and are
    listed in `cyclic` so the UI can flag the approximation. Deterministic:
    node order is sorted everywhere; self-loops are ignored.
    """
    ids = sorted(set(node_ids))
    known = set(ids)
    succ: dict[str, list[str]] = {n: [] for n in ids}
    pred: dict[str, list[str]] = {n: [] for n in ids}
    indeg: dict[str, int] = {n: 0 for n in ids}
    for src, tgt in sorted(set(edges)):
        if src == tgt or src not in known or tgt not in known:
            continue
        succ[src].append(tgt)
        pred[tgt].append(src)
        indeg[tgt] += 1

    levels: dict[str, int] = {}
    best: dict[str, int] = {}
    queue = sorted(n for n in ids if indeg[n] == 0)
    heapq.heapify(queue)
    remaining = dict(indeg)
    while queue:
        node = heapq.heappop(queue)
        levels[node] = best.get(node, 0)
        for nxt in succ[node]:
            best[nxt] = max(best.get(nxt, 0), levels[node] + 1)
            remaining[nxt] -= 1
            if remaining[nxt] == 0:
                heapq.heappush(queue, nxt)

    cyclic = sorted(n for n in ids if n not in levels)
    for node in cyclic:  # sorted: earlier cyclic levels feed later ones
        seen = [levels[p] for p in pred[node] if p in levels]
        levels[node] = (max(seen) + 1) if seen else 0
    return levels, set(cyclic)


# ------------------------------------------------------------------ per-scope payload

def merge_edges(edges: list[dict]) -> list[dict]:
    """Collapse parallel edges into one per (source, target, kind), conds merged."""
    merged: dict[tuple[str, str, str], set[str]] = {}
    for e in edges:
        key = (e["source"], e["target"], e.get("kind", "E"))
        merged.setdefault(key, set()).add(e.get("cond", ""))
    return [
        {"source": s, "target": t, "kind": k, "conds": sorted(conds)}
        for (s, t, k), conds in sorted(merged.items())
    ]


def dag_plan_payload(plan: dict) -> dict:
    """One DAG's task plan (from dag_plans.json) + per-task longest-path level."""
    tasks = sorted(plan.get("tasks", []), key=lambda t: t["task_id"])
    edges = [
        (up, t["task_id"]) for t in tasks for up in t.get("upstream", [])
    ]
    levels, cyclic = compute_levels([t["task_id"] for t in tasks], edges)
    return {
        "schedule": plan.get("schedule"),
        "dataset_triggered": bool(plan.get("dataset_triggered")),
        "datasets": sorted(plan.get("datasets", [])),
        "tasks": [
            {
                "task_id": t["task_id"],
                "kind": t.get("kind", "job"),
                "operator": t.get("operator", ""),
                "source_uid": t.get("source_uid"),
                "task_group": t.get("task_group"),
                "upstream": sorted(t.get("upstream", [])),
                "level": levels[t["task_id"]],
                "cycle": t["task_id"] in cyclic,
            }
            for t in tasks
        ],
        "outlets": sorted(
            (
                {"task_id": o["task_id"], "dataset": o["dataset"]}
                for o in plan.get("outlets", [])
            ),
            key=lambda o: (o["task_id"], o["dataset"]),
        ),
        "external_waits": sorted(
            (
                {
                    "task_id": w["task_id"],
                    "external_dag_id": w.get("external_dag_id", ""),
                    "external_task_id": w.get("external_task_id", ""),
                }
                for w in plan.get("external_waits", [])
            ),
            key=lambda w: (w["task_id"], w["external_dag_id"], w["external_task_id"]),
        ),
    }


def strategy_payload(part: dict, dag_plans: dict) -> dict:
    dags = sorted(part.get("dags", []), key=lambda d: d["dag_id"])
    cross = sorted(
        (
            {
                "source": c["source"],
                "target": c["target"],
                "kind": c.get("kind", ""),
                "mechanism": c.get("mechanism", ""),
                "conds": sorted(c.get("conds", [])),
            }
            for c in part.get("cross_links", [])
        ),
        key=lambda c: (c["source"], c["target"], c["kind"]),
    )
    return {
        "strategy": part.get("strategy", ""),
        "dags": [
            {
                "dag_id": d["dag_id"],
                "n_jobs": len(d.get("jobs", [])),
                "schedule": d.get("schedule"),
                "dataset_triggered": bool(d.get("dataset_triggered")),
                "datasets": sorted(d.get("datasets", [])),
                "day_pattern": d.get("day_pattern"),
                "anchor": d.get("anchor", ""),
            }
            for d in dags
        ],
        "cross_links": cross,
        "dag_plans": {
            dag_id: dag_plan_payload(plan)
            for dag_id, plan in sorted(dag_plans.items())
        },
        "diagnostics": [
            {
                "level": d.get("level", "info"),
                "code": d.get("code", ""),
                "message": d.get("message", ""),
                "subject": d.get("subject", ""),
            }
            for d in part.get("diagnostics", [])
        ],
        "stats": part.get("stats", {}),
    }


def divergence_rows(part_a: dict, part_b: dict) -> list[dict]:
    """For each components DAG: which single-entry DAGs its jobs landed in."""
    b_assign = part_b.get("assignments", {})
    rows: list[dict] = []
    for dag in sorted(part_a.get("dags", []), key=lambda d: d["dag_id"]):
        jobs = sorted(dag.get("jobs", []))
        counts: dict[str, int] = {}
        for uid in jobs:
            target = b_assign.get(uid, "(unassigned)")
            counts[target] = counts.get(target, 0) + 1
        targets = [
            {"dag_b": k, "count": v}
            for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ]
        co_grouped = max(counts.values()) if counts else 0
        rows.append(
            {
                "dag_a": dag["dag_id"],
                "n_jobs": len(jobs),
                "targets": targets,
                "co_grouped": co_grouped,
                "split": len(jobs) - co_grouped,
            }
        )
    return rows


def scope_payload(
    graph: dict,
    ir: dict,
    part_a: dict,
    part_b: dict,
    plans_a: dict,
    plans_b: dict,
) -> dict:
    """The full data bundle for ONE scope (drives the five per-scope views)."""
    a_assign = part_a.get("assignments", {})
    b_assign = part_b.get("assignments", {})

    # v5: longest-path levels over the ORIGINAL job graph (e_edges + w_edges;
    # graph.json is written before strategy cuts) drive the level-wise view.
    dep_edges = [
        (e["source"], e["target"])
        for e in graph.get("e_edges", []) + graph.get("w_edges", [])
    ]
    levels, cyclic = compute_levels(sorted(graph.get("nodes", {})), dep_edges)

    nodes = []
    for uid, job in sorted(graph.get("nodes", {}).items()):
        nodes.append(
            {
                "id": uid,
                "name": job.get("name", uid),
                "folder": job.get("folder", ""),
                "task_type": job.get("task_type", ""),
                "day_pattern": job.get("day_pattern"),
                "timefrom": job.get("timefrom", ""),
                "cyclic": bool(job.get("cyclic")),
                "synthetic": bool(job.get("synthetic")),
                "level": levels.get(uid, 0),
                "cycle": uid in cyclic,
                "dag_a": a_assign.get(uid),
                "dag_b": b_assign.get(uid),
            }
        )

    folders = [
        {
            "name": f.get("name", ""),
            "smart": bool(f.get("smart")),
            "parent": f.get("parent", ""),
            "datacenter": f.get("datacenter", ""),
            "n_jobs": len(f.get("jobs", [])),
        }
        for f in sorted(ir.get("folders", []), key=lambda f: f.get("name", ""))
    ]

    return {
        "nodes": nodes,
        "e_edges": merge_edges(graph.get("e_edges", [])),
        "w_edges": merge_edges(graph.get("w_edges", [])),
        "orphan_conds": sorted(
            graph.get("orphan_conds", []), key=lambda o: o.get("cond", "")
        ),
        "dead_end_conds": sorted(
            graph.get("dead_end_conds", []), key=lambda o: o.get("cond", "")
        ),
        "folders": folders,
        "a": strategy_payload(part_a, plans_a),
        "b": strategy_payload(part_b, plans_b),
        "divergence": divergence_rows(part_a, part_b),
    }


# ------------------------------------------------------------------ run-level payload

def scope_names(summary: dict) -> list[str]:
    return sorted(e.get("scope", "") for e in summary.get("scopes", []))


def _collision_rows(summary: dict) -> list[dict]:
    return sorted(
        (
            {
                "dag_id": c.get("dag_id", ""),
                "kept_by": c.get("kept_by", ""),
                "renamed_in": c.get("renamed_in", ""),
                "renamed_to": c.get("renamed_to", ""),
            }
            for c in summary.get("dag_id_collisions", [])
        ),
        key=lambda c: (c["dag_id"], c["renamed_in"], c["renamed_to"]),
    )


def _stat_slice(entry: dict) -> dict:
    stats = entry.get("stats", {}) or {}
    return {
        "n_jobs": stats.get("n_jobs", 0),
        "n_dags": stats.get("n_dags", 0),
        "n_cross_links": stats.get("n_cross_links", 0),
        "diagnostics": entry.get("diagnostics", 0),
    }


def run_payload(summary_a: dict, summary_b: dict) -> dict:
    """Run-level panel data: cross-scope links, dag_id collisions, overview.

    cross_scope_links are computed by the pipeline BEFORE partitioning, so both
    strategies report the same set; the union is taken defensively.
    """
    seen: dict[tuple, dict] = {}
    for summary in (summary_a, summary_b):
        for link in summary.get("cross_scope_links", []):
            row = {
                "cond": link.get("cond", ""),
                "producer_scope": link.get("producer_scope", ""),
                "producer": link.get("producer", ""),
                "consumer_scope": link.get("consumer_scope", ""),
                "consumer": link.get("consumer", ""),
            }
            key = (row["cond"], row["producer_scope"], row["producer"],
                   row["consumer_scope"], row["consumer"])
            seen[key] = row
    links = [seen[k] for k in sorted(seen)]

    a_entries = {e.get("scope", ""): e for e in summary_a.get("scopes", [])}
    b_entries = {e.get("scope", ""): e for e in summary_b.get("scopes", [])}
    overview = []
    for scope in sorted(a_entries):
        ea = a_entries[scope]
        eb = b_entries.get(scope, {})
        overview.append(
            {
                "scope": scope,
                "file": ea.get("file", ""),
                "a": _stat_slice(ea),
                "b": _stat_slice(eb),
            }
        )

    return {
        "cross_scope_links": links,
        "dag_id_collisions": {
            "a": _collision_rows(summary_a),
            "b": _collision_rows(summary_b),
        },
        "overview": overview,
    }


# ------------------------------------------------------------------ render

def build_html(payload: dict) -> str:
    template = TEMPLATE.read_text(encoding="utf-8")
    # '<\\/' is a valid JSON escape for '</'; it keeps the inline <script
    # type="application/json"> block from being terminated early.
    data_json = json.dumps(
        payload, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    ).replace("</", "<\\/")
    html = template.replace("@@VIS_JS@@", find_vis_js())
    html = html.replace("@@DATA_JSON@@", data_json)
    return html


def build_dashboard(a_dir: Path, b_dir: Path, out_path: Path) -> Path:
    summary_a = load_json(a_dir / "scopes.json")
    summary_b = load_json(b_dir / "scopes.json")
    names_a = scope_names(summary_a)
    names_b = scope_names(summary_b)
    if names_a != names_b:
        raise ValueError(
            f"scope sets differ between strategy outputs: --a has {names_a}, "
            f"--b has {names_b} (run both strategies over the same XML inputs)"
        )
    if not names_a:
        raise ValueError(f"no scopes listed in {a_dir / 'scopes.json'}")

    per_scope = {}
    for scope in names_a:
        graph = load_json(a_dir / scope / "graph.json")
        ir = load_json(a_dir / scope / "ir.json")
        part_a = load_json(a_dir / scope / "partition.json")
        part_b = load_json(b_dir / scope / "partition.json")
        plans_a = load_dag_plans(a_dir / scope / "dag_plans.json")
        plans_b = load_dag_plans(b_dir / scope / "dag_plans.json")
        per_scope[scope] = scope_payload(graph, ir, part_a, part_b, plans_a, plans_b)

    payload = {
        "scopes": names_a,
        "per_scope": per_scope,
        "run": run_payload(summary_a, summary_b),
    }
    html = build_html(payload)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8", newline="\n")
    return out_path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Render the offline Control-M -> Airflow comparison dashboard."
    )
    ap.add_argument("--a", required=True, help="components strategy output dir (scope tree)")
    ap.add_argument("--b", required=True, help="single_entry strategy output dir (scope tree)")
    ap.add_argument("-o", "--out", required=True, help="path of the index.html to write")
    args = ap.parse_args(argv)

    out = build_dashboard(Path(args.a), Path(args.b), Path(args.out))
    print(f"dashboard written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
