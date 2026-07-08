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


# ------------------------------------------------------------------ dag_plans.json fixtures (v5)
# Plain dicts following the V5-1 contract schema (the emit module is built
# concurrently; the dashboard consumes the schema, not the implementation).

def _fin_plans_a() -> dict:
    """Components strategy, scope fin: one DAG exercising every task kind
    except confirm/force, plus an outlet and an external wait."""
    return {
        "fin_dw": {
            "schedule": "0 21 * * 1-5",
            "dataset_triggered": False,
            "datasets": [],
            "tasks": [
                {"task_id": "start_fin", "kind": "folder_start",
                 "operator": "EmptyOperator", "source_uid": None,
                 "task_group": None, "upstream": []},
                {"task_id": "wait_ext_feed", "kind": "wait",
                 "operator": "ExternalTaskSensor", "source_uid": None,
                 "task_group": None, "upstream": ["start_fin"]},
                {"task_id": "load", "kind": "job", "operator": "SSHOperator",
                 "source_uid": "FIN/LOAD", "task_group": "FIN",
                 "upstream": ["wait_ext_feed"]},
                {"task_id": "gate_mart", "kind": "gate",
                 "operator": "DateTimeSensorAsync", "source_uid": None,
                 "task_group": None, "upstream": ["load"]},
                {"task_id": "mart", "kind": "job", "operator": "CtmDatabaseJob",
                 "source_uid": "FIN/MART", "task_group": "FIN",
                 "upstream": ["gate_mart"]},
                {"task_id": "publish", "kind": "job", "operator": "SSHOperator",
                 "source_uid": "FIN/PUBLISH", "task_group": "FIN",
                 "upstream": ["mart"]},
                {"task_id": "end_fin", "kind": "folder_end",
                 "operator": "EmptyOperator", "source_uid": None,
                 "task_group": None, "upstream": ["publish"]},
            ],
            "outlets": [{"task_id": "mart", "dataset": "ctrlm://cond/MART-OK"}],
            "external_waits": [
                {"task_id": "wait_ext_feed", "external_dag_id": "ext_feed",
                 "external_task_id": "feed_done"},
            ],
        },
    }


def _fin_plans_b() -> dict:
    """Single-entry strategy, scope fin: producer DAG with an outlet + a
    dataset-triggered consumer DAG with confirm and force tasks."""
    return {
        "fin_load": {
            "schedule": "0 21 * * 1-5",
            "dataset_triggered": False,
            "datasets": [],
            "tasks": [
                {"task_id": "load", "kind": "job", "operator": "SSHOperator",
                 "source_uid": "FIN/LOAD", "task_group": "FIN", "upstream": []},
                {"task_id": "mart", "kind": "job", "operator": "CtmDatabaseJob",
                 "source_uid": "FIN/MART", "task_group": "FIN",
                 "upstream": ["load"]},
            ],
            "outlets": [{"task_id": "mart", "dataset": "ctrlm://cond/MART-OK"}],
            "external_waits": [],
        },
        "fin_publish": {
            "schedule": None,
            "dataset_triggered": True,
            "datasets": ["ctrlm://cond/MART-OK"],
            "tasks": [
                {"task_id": "confirm_publish", "kind": "confirm",
                 "operator": "CtmApprovalGateSensor", "source_uid": None,
                 "task_group": None, "upstream": []},
                {"task_id": "publish", "kind": "job", "operator": "SSHOperator",
                 "source_uid": "FIN/PUBLISH", "task_group": None,
                 "upstream": ["confirm_publish"]},
                {"task_id": "force_notify", "kind": "force",
                 "operator": "TriggerDagRunOperator", "source_uid": None,
                 "task_group": None, "upstream": ["publish"]},
            ],
            "outlets": [],
            "external_waits": [],
        },
    }


def _ops_plans() -> dict:
    """Scope ops: identical single-job DAGs under both strategies."""
    return {
        "ops_poll": {
            "schedule": "*/15 6-19 * * *",
            "dataset_triggered": False,
            "datasets": [],
            "tasks": [
                {"task_id": "poll", "kind": "job", "operator": "SSHOperator",
                 "source_uid": "OPS/POLL", "task_group": None, "upstream": []},
            ],
            "outlets": [],
            "external_waits": [],
        },
        "ops_weekly": {
            "schedule": "0 6 * * 1",
            "dataset_triggered": False,
            "datasets": [],
            "tasks": [
                {"task_id": "weekly", "kind": "job", "operator": "SSHOperator",
                 "source_uid": "OPS/WEEKLY", "task_group": None, "upstream": []},
            ],
            "outlets": [],
            "external_waits": [],
        },
    }


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
        "fin": (fin_ir, fin_graph, fin_a, fin_b, _fin_plans_a(), _fin_plans_b()),
        "ops": (ops_ir, ops_graph, ops_a, ops_b, _ops_plans(), _ops_plans()),
    }
    for scope, (ir, graph, part_a, part_b, plans_a, plans_b) in scopes.items():
        for base, part, plans in ((a_dir, part_a, plans_a), (b_dir, part_b, plans_b)):
            sdir = base / scope
            sdir.mkdir(parents=True)
            (sdir / "ir.json").write_text(ir.model_dump_json(indent=2), encoding="utf-8")
            (sdir / "graph.json").write_text(graph.model_dump_json(indent=2), encoding="utf-8")
            (sdir / "partition.json").write_text(part.model_dump_json(indent=2), encoding="utf-8")
            (sdir / "dag_plans.json").write_text(
                json.dumps(plans, indent=2, sort_keys=True), encoding="utf-8")

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


def test_dashboard_levelwise_structure(tmp_path):
    """V5-2: structure tab is level-wise (hierarchical LR) with a layout toggle."""
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out = tmp_path / "index.html"
    html = _run_build(a_dir, b_dir, out)

    # layout toggle (Level-wise default | Force)
    assert 'id="layout-toggle"' in html
    assert 'data-layout="level"' in html
    assert 'data-layout="force"' in html
    assert "Level-wise" in html
    # hierarchical LR layout config is part of the view code
    assert "hierarchical" in html
    assert "'LR'" in html or '"LR"' in html

    # longest-path node levels are computed in build.py and embedded
    # (node dict keys are sorted by json.dumps: ..."id":X,"level":N,...)
    assert '"id":"FIN/LOAD","level":0' in html
    assert '"id":"FIN/MART","level":1' in html
    assert '"id":"FIN/PUBLISH","level":2' in html
    # cycle-guard flag present on every node (no cycles in the fixtures)
    assert '"cycle":false' in html and '"cycle":true' not in html


def test_dashboard_dag_graph_view(tmp_path):
    """V5-2: strategy tabs gain a Partition overview | DAG graph sub-mode fed
    by dag_plans.json, with ghost external stubs and a task-kinds legend."""
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out = tmp_path / "index.html"
    html = _run_build(a_dir, b_dir, out)

    # sub-mode switch + per-strategy DAG selector
    assert 'data-mode="overview"' in html
    assert 'data-mode="dag"' in html
    assert 'id="dag-select-components"' in html
    assert 'id="dag-select-single"' in html

    # ghost/external stub markers (dataset in/out + external task stubs)
    assert "ghost_ext:" in html
    assert "ghost_ds_in:" in html
    assert "ghost_ds_out:" in html

    # task-kinds legend
    assert "kinds-legend" in html

    # dag_plans of BOTH strategies are embedded, with per-task levels
    assert '"dag_plans":' in html
    for marker in (
        '"task_id":"gate_mart"',                      # gate task (components)
        '"task_id":"wait_ext_feed"',                  # wait task
        '"external_dag_id":"ext_feed"',               # external wait target
        '"task_id":"confirm_publish"',                # confirm (single-entry)
        '"task_id":"force_notify"',                   # force task
        '"kind":"folder_start","level":0',            # computed task levels
        '"kind":"folder_end","level":6',
        '"dataset_triggered":true',                   # ghost inbound datasets
        '"dataset":"ctrlm://cond/MART-OK"',           # outlet entry
    ):
        assert marker in html, f"dag_plans marker {marker} not embedded"


def test_dashboard_folder_toggle_and_overview_layout(tmp_path):
    """v5.1: folder gate nodes hidden by default (with edge contraction) and
    partition overviews default to the level-wise layout."""
    a_dir, b_dir = _fixture_dirs(tmp_path)
    out = tmp_path / "index.html"
    html = _run_build(a_dir, b_dir, out)

    # global toggle exists and defaults off (showFolders = false)
    assert 'id="folder-toggle"' in html
    assert "Show folder nodes" in html
    assert "var showFolders = false" in html

    # display-level filtering + transitive contraction machinery is present
    assert "visibleNodes" in html and "hiddenNodeIds" in html
    assert "contract" in html  # contraction note/logic

    # strategy overviews default to level-wise with a Force toggle
    assert 'id="olayout-components"' in html and 'id="olayout-single"' in html
    assert html.count('class="segbtn active" data-layout="level"') >= 2


def test_dashboard_missing_dag_plans_fails(tmp_path):
    """v5 requires <scope>/dag_plans.json; a pre-v5 tree must fail loudly."""
    a_dir, b_dir = _fixture_dirs(tmp_path)
    (b_dir / "ops" / "dag_plans.json").unlink()

    out = tmp_path / "index.html"
    proc = subprocess.run(
        [sys.executable, str(BUILD),
         "--a", str(a_dir), "--b", str(b_dir), "-o", str(out)],
        cwd=REPO, capture_output=True, text=True,
    )
    assert proc.returncode != 0
    err = proc.stderr + proc.stdout
    assert "dag_plans.json" in err
    assert "regenerate" in err


def test_compute_levels_cycle_guard():
    """V5-2: longest-path levels with the cycle guard — cyclic nodes get
    level = max(level of already-levelled predecessors) + 1 and are flagged."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("dashboard_build", BUILD)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # a -> b -> c -> b (cycle), b -> d (only reachable through the cycle)
    levels, cyclic = mod.compute_levels(
        ["a", "b", "c", "d"],
        [("a", "b"), ("b", "c"), ("c", "b"), ("b", "d")],
    )
    assert levels["a"] == 0
    assert cyclic == {"b", "c", "d"}       # d resolves only through the cycle
    assert levels["b"] == 1                # pred a levelled at 0
    assert levels["c"] == 2                # pred b approximated at 1
    assert levels["d"] == 2

    # acyclic diamond: longest path wins; nothing flagged
    levels2, cyclic2 = mod.compute_levels(
        ["r", "x", "y", "z"],
        [("r", "x"), ("r", "y"), ("x", "y"), ("y", "z")],
    )
    assert cyclic2 == set()
    assert levels2 == {"r": 0, "x": 1, "y": 2, "z": 3}


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
