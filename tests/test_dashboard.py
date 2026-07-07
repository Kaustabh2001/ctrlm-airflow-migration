"""Tests for dashboard/build.py — v2 scope trees, fixture outputs, real CLI run.

The strategy/parser modules are built concurrently, so the per-scope
graph.json / partition.json / ir.json inputs and the run-level scopes.json are
generated here from the contract models in core/ctrlm_core/model.py rather
than by running the real pipelines. Two scopes ("fin" and "ops") exercise the
scope selector, per-scope views and the run-level cross-scope panel.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from ctrlm_core.model import (
    Condition,
    CrossLink,
    CtmGraph,
    DagSpec,
    Deftable,
    Diagnostic,
    FolderDef,
    GraphEdge,
    Job,
    PartitionResult,
)

REPO = Path(__file__).resolve().parents[1]
BUILD = REPO / "dashboard" / "build.py"

WD_15 = "WD=1,2,3,4,5|MD=ALL|M=ALL|OP=OR"
WD_MON = "WD=1|MD=ALL|M=ALL|OP=OR"


def _stats(**overrides) -> dict:
    base = {
        "n_jobs": 3,
        "n_dags": 1,
        "n_cross_links": 0,
        "cross_links_by_kind": {},
        "cross_links_by_mechanism": {},
        "single_job_dags": 0,
        "multi_root_dags": 0,
        "dataset_triggered_dags": 0,
        "largest_dag": {"dag_id": "fin_dw", "size": 3},
        "size_histogram": {"1": 0, "2-5": 1, "6-15": 0, "16-50": 0, "51+": 0},
    }
    base.update(overrides)
    return base


# ------------------------------------------------------------------ scope "fin"

def _fin_scope() -> tuple[Deftable, CtmGraph, PartitionResult, PartitionResult]:
    load = Job(
        name="LOAD", folder="FIN", weekdays="1,2,3,4,5", timefrom="2100",
        day_pattern=WD_15,
        in_conds=[Condition(name="EXT-FEED-OK")],
        out_conds=[Condition(name="LOADED")],
    )
    mart = Job(
        name="MART", folder="FIN",
        in_conds=[Condition(name="LOADED")],
        out_conds=[Condition(name="MART-OK")],
    )
    publish = Job(
        name="PUBLISH", folder="FIN",
        in_conds=[Condition(name="MART-OK")],
    )
    jobs = [load, mart, publish]

    deftable = Deftable(
        folders=[FolderDef(name="FIN", datacenter="DC1", jobs=jobs)],
        source_files=["sample_fin.xml"],
    )

    graph = CtmGraph(
        nodes={j.uid: j for j in jobs},
        e_edges=[
            GraphEdge(source="FIN/LOAD", target="FIN/MART", cond="LOADED"),
            GraphEdge(source="FIN/MART", target="FIN/PUBLISH", cond="MART-OK"),
        ],
        w_edges=[],
        orphan_conds=[{"cond": "EXT-FEED-OK", "consumers": ["FIN/LOAD"]}],
        # MART-OK also feeds OPS/WEEKLY in the other scope; within this scope
        # it is fully consumed, so no dead-end here — the cross_scope_links
        # entry in scopes.json carries that information instead.
        dead_end_conds=[],
        flags=[],
    )

    part_a = PartitionResult(
        strategy="components",
        dags=[
            DagSpec(dag_id="fin_dw", jobs=["FIN/LOAD", "FIN/MART", "FIN/PUBLISH"],
                    roots=["FIN/LOAD"], folders=["FIN"], day_pattern=WD_15,
                    anchor="2100", schedule="0 21 * * 1-5"),
        ],
        assignments={
            "FIN/LOAD": "fin_dw", "FIN/MART": "fin_dw", "FIN/PUBLISH": "fin_dw",
        },
        cross_links=[],
        diagnostics=[
            Diagnostic(level="warn", code="CRON_AND_APPROX",
                       message="cron cannot AND day fields", subject="fin_dw"),
        ],
        stats=_stats(),
    )

    part_b = PartitionResult(
        strategy="single_entry",
        dags=[
            DagSpec(dag_id="fin_load", jobs=["FIN/LOAD", "FIN/MART"],
                    roots=["FIN/LOAD"], folders=["FIN"], day_pattern=WD_15,
                    anchor="2100", schedule="0 21 * * 1-5"),
            DagSpec(dag_id="fin_publish", jobs=["FIN/PUBLISH"],
                    roots=["FIN/PUBLISH"], folders=["FIN"],
                    dataset_triggered=True,
                    datasets=["ctrlm://cond/MART-OK"]),
        ],
        assignments={
            "FIN/LOAD": "fin_load", "FIN/MART": "fin_load",
            "FIN/PUBLISH": "fin_publish",
        },
        cross_links=[
            CrossLink(source="FIN/MART", target="FIN/PUBLISH", conds=["MART-OK"],
                      kind="OWNER_SPLIT", mechanism="dataset"),
        ],
        diagnostics=[
            Diagnostic(level="info", code="SINGLE_JOB_DAGS",
                       message="1 single-job dag", subject=""),
        ],
        stats=_stats(
            n_dags=2, n_cross_links=1,
            cross_links_by_kind={"OWNER_SPLIT": 1},
            cross_links_by_mechanism={"dataset": 1},
            single_job_dags=1, dataset_triggered_dags=1,
            largest_dag={"dag_id": "fin_load", "size": 2},
            size_histogram={"1": 1, "2-5": 1, "6-15": 0, "16-50": 0, "51+": 0},
        ),
    )
    return deftable, graph, part_a, part_b


# ------------------------------------------------------------------ scope "ops"

def _ops_scope() -> tuple[Deftable, CtmGraph, PartitionResult, PartitionResult]:
    poll = Job(
        name="POLL", folder="OPS", cyclic=True, interval_minutes=15,
        timefrom="0600", timeto="2000",
    )
    weekly = Job(
        name="WEEKLY", folder="OPS", weekdays="1", day_pattern=WD_MON,
        in_conds=[Condition(name="MART-OK")],   # produced only in scope "fin"
    )
    jobs = [poll, weekly]

    deftable = Deftable(
        folders=[FolderDef(name="OPS", smart=True, jobs=jobs)],
        source_files=["sample_ops.xml"],
    )

    graph = CtmGraph(
        nodes={j.uid: j for j in jobs},
        e_edges=[],
        w_edges=[],
        # MART-OK is never produced inside this scope: orphan (scope rule)
        orphan_conds=[{"cond": "MART-OK", "consumers": ["OPS/WEEKLY"]}],
        dead_end_conds=[],
        flags=[],
    )

    dags = [
        DagSpec(dag_id="ops_poll", jobs=["OPS/POLL"], roots=["OPS/POLL"],
                folders=["OPS"], schedule="*/15 6-19 * * *"),
        DagSpec(dag_id="ops_weekly", jobs=["OPS/WEEKLY"], roots=["OPS/WEEKLY"],
                folders=["OPS"], day_pattern=WD_MON, anchor="0600",
                schedule="0 6 * * 1"),
    ]
    assignments = {"OPS/POLL": "ops_poll", "OPS/WEEKLY": "ops_weekly"}
    stats = _stats(
        n_jobs=2, n_dags=2, single_job_dags=2,
        largest_dag={"dag_id": "ops_poll", "size": 1},
        size_histogram={"1": 2, "2-5": 0, "6-15": 0, "16-50": 0, "51+": 0},
    )

    part_a = PartitionResult(
        strategy="components", dags=dags, assignments=assignments,
        cross_links=[], diagnostics=[], stats=stats,
    )
    part_b = PartitionResult(
        strategy="single_entry",
        dags=[d.model_copy(deep=True) for d in dags],
        assignments=dict(assignments),
        cross_links=[],
        diagnostics=[
            Diagnostic(level="info", code="SINGLE_JOB_DAGS",
                       message="2 single-job dags", subject=""),
        ],
        stats=stats,
    )
    return deftable, graph, part_a, part_b


# ------------------------------------------------------------------ scope trees

CROSS_SCOPE_LINKS = [
    {
        "cond": "MART-OK",
        "producer_scope": "fin", "producer": "FIN/MART",
        "consumer_scope": "ops", "consumer": "OPS/WEEKLY",
    },
]

COLLISIONS_A = [
    {"dag_id": "daily_report", "kept_by": "fin",
     "renamed_in": "ops", "renamed_to": "ops__daily_report"},
]


def _scopes_json(strategy: str, entries: list[dict], collisions: list[dict]) -> str:
    return json.dumps(
        {
            "strategy": strategy,
            "scopes": entries,
            "cross_scope_links": CROSS_SCOPE_LINKS,
            "dag_id_collisions": collisions,
        },
        indent=2,
    )


def _fixture_dirs(tmp_path: Path) -> tuple[Path, Path]:
    """Write two-scope output trees for both strategies (v2 layout)."""
    fin_ir, fin_graph, fin_a, fin_b = _fin_scope()
    ops_ir, ops_graph, ops_a, ops_b = _ops_scope()

    a_dir = tmp_path / "components"
    b_dir = tmp_path / "single_entry"
    scopes = {
        "fin": (fin_ir, fin_graph, fin_a, fin_b),
        "ops": (ops_ir, ops_graph, ops_a, ops_b),
    }
    for scope, (ir, graph, part_a, part_b) in scopes.items():
        for base, part in ((a_dir, part_a), (b_dir, part_b)):
            sdir = base / scope
            sdir.mkdir(parents=True)
            (sdir / "ir.json").write_text(ir.model_dump_json(indent=2), encoding="utf-8")
            (sdir / "graph.json").write_text(graph.model_dump_json(indent=2), encoding="utf-8")
            (sdir / "partition.json").write_text(part.model_dump_json(indent=2), encoding="utf-8")

    def entries(parts: dict[str, PartitionResult]) -> list[dict]:
        return [
            {"scope": s, "file": f"examples/exports/sample_{s}.xml",
             "stats": p.stats, "diagnostics": len(p.diagnostics)}
            for s, p in sorted(parts.items())
        ]

    (a_dir / "scopes.json").write_text(
        _scopes_json("components", entries({"fin": fin_a, "ops": ops_a}), COLLISIONS_A),
        encoding="utf-8",
    )
    (b_dir / "scopes.json").write_text(
        _scopes_json("single_entry", entries({"fin": fin_b, "ops": ops_b}), []),
        encoding="utf-8",
    )
    return a_dir, b_dir


def _run_build(a_dir: Path, b_dir: Path, out: Path) -> str:
    proc = subprocess.run(
        [sys.executable, str(BUILD),
         "--a", str(a_dir), "--b", str(b_dir), "-o", str(out)],
        cwd=REPO, capture_output=True, text=True,
    )
    assert proc.returncode == 0, f"build.py failed:\n{proc.stdout}\n{proc.stderr}"
    assert out.is_file()
    return out.read_text(encoding="utf-8")


# ------------------------------------------------------------------ tests

def test_dashboard_builds_offline_html(tmp_path):
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out = tmp_path / "dashboard" / "index.html"
    html = _run_build(a_dir, b_dir, out)

    # five per-scope view containers + the run-level view
    for view_id in ("view-structure", "view-components", "view-single",
                    "view-full", "view-compare", "view-run"):
        assert f'id="{view_id}"' in html, f"missing container {view_id}"

    # vis-network is inlined; nothing is fetched from the network
    assert "vis-network" in html
    assert "<script src" not in html
    assert 'src="http' not in html
    assert "src='http" not in html
    assert 'href="http' not in html
    assert "@@VIS_JS@@" not in html and "@@DATA_JSON@@" not in html

    # per-scope data of BOTH scopes is embedded
    for marker in (
        '"fin"', '"ops"',                            # scope names
        '"FIN/MART"', '"OPS/POLL"',                  # node uids per scope
        '"fin_dw"', '"fin_load"', '"fin_publish"',   # dag ids of both strategies
        '"ops_poll"', '"ops_weekly"',
        '"OWNER_SPLIT"',                             # cross-link kind
        '"size_histogram"', '"cross_links_by_mechanism"',  # stats keys
        '"EXT-FEED-OK"',                             # orphan condition (fin)
    ):
        assert marker in html, f"fixture marker {marker} not embedded"


def test_dashboard_scope_selector(tmp_path):
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out = tmp_path / "index.html"
    html = _run_build(a_dir, b_dir, out)

    # the selector element exists and the scope list is embedded for it
    assert 'id="scope-select"' in html
    assert '"scopes":["fin","ops"]' in html
    # per-scope payloads keyed by scope name
    assert '"per_scope":{"fin":' in html
    assert '"ops":{' in html


def test_dashboard_cross_scope_panel(tmp_path):
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out = tmp_path / "index.html"
    html = _run_build(a_dir, b_dir, out)

    # run-level cross-scope table + data (keys sorted by json.dumps)
    assert 'id="cross-scope-table"' in html
    assert '"cross_scope_links":[{' in html
    assert ('"cond":"MART-OK","consumer":"OPS/WEEKLY","consumer_scope":"ops",'
            '"producer":"FIN/MART","producer_scope":"fin"') in html

    # dag_id collisions of the components run are embedded
    assert '"ops__daily_report"' in html
    assert '"kept_by":"fin"' in html

    # per-scope stats overview table container
    assert 'id="scope-overview-table"' in html
    # overview rows carry the source file of each scope
    assert "sample_fin.xml" in html and "sample_ops.xml" in html


def test_dashboard_divergence_and_assignments(tmp_path):
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out = tmp_path / "index.html"
    html = _run_build(a_dir, b_dir, out)

    # divergence (scope fin): fin_dw's 3 jobs land in fin_load (x2) + fin_publish (x1)
    assert '"dag_a":"fin_dw"' in html
    assert '"co_grouped":2' in html
    assert '"split":1' in html
    # per-node dag assignment under both strategies (for tooltips)
    assert '"dag_a":"fin_dw"' in html and '"dag_b":"fin_load"' in html


def test_dashboard_deterministic(tmp_path):
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out1 = tmp_path / "one.html"
    out2 = tmp_path / "two.html"
    html1 = _run_build(a_dir, b_dir, out1)
    html2 = _run_build(a_dir, b_dir, out2)
    assert html1 == html2


def test_dashboard_rejects_mismatched_scope_trees(tmp_path):
    a_dir, b_dir = _fixture_dirs(tmp_path)
    # remove one scope from --b: the scope sets no longer match
    sj = json.loads((b_dir / "scopes.json").read_text(encoding="utf-8"))
    sj["scopes"] = [e for e in sj["scopes"] if e["scope"] != "ops"]
    (b_dir / "scopes.json").write_text(json.dumps(sj, indent=2), encoding="utf-8")

    out = tmp_path / "index.html"
    proc = subprocess.run(
        [sys.executable, str(BUILD),
         "--a", str(a_dir), "--b", str(b_dir), "-o", str(out)],
        cwd=REPO, capture_output=True, text=True,
    )
    assert proc.returncode != 0
    assert "scope sets differ" in (proc.stderr + proc.stdout)
