"""End-to-end pipeline: XML files -> per-XML scopes -> IR -> graph -> partition -> DAGs.

SCOPE RULE (v2): each input XML file is an independent conversion scope.
- Conditions match only within their own scope (one XML).
- Cross-file condition matches are NOT wired; they are reported in
  <out>/scopes.json under "cross_scope_links" and surface inside each scope as
  orphan/dead-end conditions.
- Outputs land in <out>/<scope>/ (ir.json, graph.json, partition.json,
  cluster-map.yaml, dags/*.py); <out>/scopes.json is the run-level summary.
- dag_ids are kept unique ACROSS scopes (all DAGs eventually share one Airflow
  instance): a collision keeps the first scope's name and renames later ones to
  "<scope>__<dag_id>", recorded in scopes.json "dag_id_collisions".
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Callable, Iterable

import yaml

from .model import CtmGraph, Deftable, PartitionConfig, PartitionResult


def collect_xml_files(inputs: Iterable[str | Path]) -> list[Path]:
    files: list[Path] = []
    for raw in inputs:
        p = Path(raw)
        if p.is_dir():
            files.extend(sorted(p.glob("*.xml")))
        elif p.exists():
            files.append(p)
        else:
            raise FileNotFoundError(f"input not found: {p}")
    if not files:
        raise FileNotFoundError("no .xml files found in the given inputs")
    return files


def scope_name(path: Path) -> str:
    stem = re.sub(r"[^a-zA-Z0-9_]+", "_", path.stem).strip("_").lower()
    return stem or "scope"


def write_cluster_map(result: PartitionResult, path: Path) -> None:
    data = {
        "strategy": result.strategy,
        "assignments": dict(sorted(result.assignments.items())),
        "cuts": [
            {"kind": c.kind, "source": c.source, "target": c.target, "conds": sorted(c.conds)}
            for c in sorted(result.cross_links, key=lambda c: (c.kind, c.source, c.target))
        ],
    }
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _rename_dag(result: PartitionResult, old: str, new: str) -> None:
    for spec in result.dags:
        if spec.dag_id == old:
            spec.dag_id = new
    result.assignments = {u: (new if d == old else d) for u, d in result.assignments.items()}


def _cross_scope_links(scoped: list[tuple[str, Deftable]]) -> list[dict]:
    """Condition matches that WOULD have wired across XML files (pre-desugar).

    These are severed by the one-XML scope rule; each entry names both sides so
    humans can decide whether the boundary is real or the estate should be
    exported into one file. Synthetic "__" conditions and DEL signs excluded.
    """
    producers: dict[str, list[tuple[str, str]]] = {}
    consumers: dict[str, list[tuple[str, str]]] = {}
    for scope, deftable in scoped:
        for job in deftable.all_jobs():
            for c in job.out_conds:
                if c.sign == "ADD" and not c.name.startswith("__"):
                    producers.setdefault(c.name, []).append((scope, job.uid))
            for c in job.in_conds:
                if not c.name.startswith("__"):
                    consumers.setdefault(c.name, []).append((scope, job.uid))
        # folder-level conditions surface post-desugar on the synthetic
        # __FOLDER_START__ / __FOLDER_END__ nodes — name those sides here so
        # cross-scope matches involving smart-folder conds are not missed.
        for folder in deftable.folders:
            for c in folder.in_conds:
                if not c.name.startswith("__"):
                    consumers.setdefault(c.name, []).append(
                        (scope, f"{folder.name}/__FOLDER_START__"))
            for c in folder.out_conds:
                if c.sign == "ADD" and not c.name.startswith("__"):
                    producers.setdefault(c.name, []).append(
                        (scope, f"{folder.name}/__FOLDER_END__"))
    links: list[dict] = []
    for cond in sorted(set(producers) & set(consumers)):
        for p_scope, p_uid in sorted(producers[cond]):
            for c_scope, c_uid in sorted(consumers[cond]):
                if p_scope != c_scope:
                    links.append({
                        "cond": cond,
                        "producer_scope": p_scope, "producer": p_uid,
                        "consumer_scope": c_scope, "consumer": c_uid,
                    })
    return links


def run_pipeline(
    strategy_name: str,
    partition_fn: Callable[[CtmGraph, PartitionConfig], PartitionResult],
    inputs: Iterable[str | Path],
    out_dir: str | Path,
    config: PartitionConfig | None = None,
) -> dict[str, PartitionResult]:
    # local imports so this module stays importable while siblings are being built
    from .desugar import desugar
    from .emit import emit_dags
    from .graph import build_graph
    from .parser import parse_files
    from .schedule import normalize_jobs

    config = config or PartitionConfig()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # one scope per XML file; scope names deduped deterministically
    scoped: list[tuple[str, Path, Deftable]] = []
    used_scopes: set[str] = set()
    for f in collect_xml_files(inputs):
        base = scope_name(f)
        s, i = base, 2
        while s in used_scopes:
            s, i = f"{base}_{i}", i + 1
        used_scopes.add(s)
        scoped.append((s, f, parse_files([f])))

    cross_links = _cross_scope_links([(s, d) for s, _, d in scoped])

    results: dict[str, PartitionResult] = {}
    dag_owner: dict[str, str] = {}          # dag_id -> first scope that claimed it
    collisions: list[dict] = []
    summary: list[dict] = []

    for s, f, deftable in scoped:
        desugar(deftable, config)
        normalize_jobs(deftable, config)
        graph = build_graph(deftable, config)

        sdir = out / s
        (sdir / "dags").mkdir(parents=True, exist_ok=True)
        (sdir / "ir.json").write_text(deftable.model_dump_json(indent=2), encoding="utf-8")
        (sdir / "graph.json").write_text(graph.model_dump_json(indent=2), encoding="utf-8")

        result = partition_fn(graph, config)
        assert result.strategy == strategy_name

        for dag_id in [spec.dag_id for spec in result.dags]:
            if dag_id in dag_owner and dag_owner[dag_id] != s:
                new = f"{s}__{dag_id}"
                collisions.append({"dag_id": dag_id, "kept_by": dag_owner[dag_id],
                                   "renamed_in": s, "renamed_to": new})
                _rename_dag(result, dag_id, new)
                dag_owner[new] = s
            else:
                dag_owner[dag_id] = s

        (sdir / "partition.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")
        write_cluster_map(result, sdir / "cluster-map.yaml")
        emit_dags(graph, result, sdir / "dags", config)

        results[s] = result
        summary.append({"scope": s, "file": str(f), "stats": result.stats,
                        "diagnostics": len(result.diagnostics)})

    (out / "scopes.json").write_text(json.dumps({
        "strategy": strategy_name,
        "scopes": summary,
        "cross_scope_links": cross_links,
        "dag_id_collisions": collisions,
    }, indent=2), encoding="utf-8")

    print(json.dumps({
        "strategy": strategy_name,
        "out": str(out),
        "scopes": {s: {"jobs": r.stats.get("n_jobs"), "dags": r.stats.get("n_dags"),
                       "cross_links": r.stats.get("n_cross_links")} for s, r in results.items()},
        "cross_scope_links": len(cross_links),
        "dag_id_collisions": len(collisions),
    }, indent=2))
    return results
