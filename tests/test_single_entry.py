"""End-to-end + unit tests for the single-entry (ownership propagation) strategy.

End-to-end tests run the real core pipeline stages (parser -> desugar ->
normalize -> build_graph) over the four sample XML exports and assert the
contract's expected outcomes that concern single_entry. Unit tests use small
inline CtmGraph fixtures (no dependency on other agents' modules).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from ctrlm_core.desugar import desugar
from ctrlm_core.graph import build_graph
from ctrlm_core.model import (
    EDGE_OWNER,
    EDGE_PREV_RUN,
    WIRE_DATASET,
    WIRE_PREV,
    CtmGraph,
    GraphEdge,
    Job,
    PartitionConfig,
)
from ctrlm_core.parser import parse_files
from ctrlm_core.schedule import normalize_jobs
from strategy_single_entry.partitioner import partition, snake_case

EXPORTS = Path(__file__).resolve().parents[1] / "examples" / "exports"

LOADERS = ["FIN_DW/DW_LOAD_CUSTOMERS", "FIN_DW/DW_LOAD_ORDERS", "FIN_DW/DW_LOAD_PRODUCTS"]
OPS_APPS = [f"OPS/OPS_APP{i:02d}" for i in range(1, 11)]


def _build_graph() -> tuple[CtmGraph, PartitionConfig]:
    config = PartitionConfig()
    deftable = parse_files(sorted(EXPORTS.glob("*.xml")))
    desugar(deftable, config)
    normalize_jobs(deftable, config)
    return build_graph(deftable, config), config


@pytest.fixture(scope="module")
def result():
    graph, config = _build_graph()
    return partition(graph, config)


def _dag_by_id(result, dag_id):
    return next(d for d in result.dags if d.dag_id == dag_id)


# ------------------------------------------------------------ invariants

def test_strategy_and_full_partition(result):
    assert result.strategy == "single_entry"
    all_jobs = [uid for dag in result.dags for uid in dag.jobs]
    assert len(all_jobs) == len(set(all_jobs))          # no job duplicated
    assert sorted(all_jobs) == sorted(result.assignments)
    assert result.stats["n_jobs"] == 47                 # every sample job assigned
    assert len(all_jobs) == 47
    for dag in result.dags:
        assert dag.jobs == sorted(dag.jobs)


def test_stats_keys(result):
    expected = {
        "n_jobs", "n_dags", "n_cross_links", "cross_links_by_kind",
        "cross_links_by_mechanism", "single_job_dags", "multi_root_dags",
        "dataset_triggered_dags", "largest_dag", "size_histogram",
    }
    assert set(result.stats) == expected


# ------------------------------------------------------------ contract outcomes

def test_build_mart_separated_from_loaders(result):
    """single_entry: DW_BUILD_MART must NOT share a dag with any loader."""
    mart_dag = result.assignments["FIN_DW/DW_BUILD_MART"]
    for loader in LOADERS:
        assert result.assignments[loader] != mart_dag
    # DW_PUBLISH has a single owner (the mart) and no own pattern -> folds in
    assert result.assignments["FIN_DW/DW_PUBLISH"] == mart_dag


def test_build_mart_dag_is_dataset_triggered_with_owner_split_links(result):
    mart_dag = _dag_by_id(result, result.assignments["FIN_DW/DW_BUILD_MART"])
    assert mart_dag.dataset_triggered
    assert mart_dag.schedule is None
    assert mart_dag.datasets == [
        "ctrlm://cond/DW-CUSTOMERS-LOADED",
        "ctrlm://cond/DW-ORDERS-LOADED",
        "ctrlm://cond/DW-PRODUCTS-LOADED",
    ]
    splits = [
        link for link in result.cross_links
        if link.kind == EDGE_OWNER and link.target == "FIN_DW/DW_BUILD_MART"
    ]
    assert sorted(link.source for link in splits) == LOADERS
    assert all(link.mechanism == WIRE_DATASET for link in splits)


def test_ops_apps_ten_separate_dags(result):
    """No singleton coalescing: OPS_APP01..10 are 10 distinct single-job dags."""
    dag_ids = {result.assignments[uid] for uid in OPS_APPS}
    assert len(dag_ids) == 10
    for uid in OPS_APPS:
        dag = _dag_by_id(result, result.assignments[uid])
        assert dag.jobs == [uid]
        assert dag.schedule is not None            # weekdays 1-5 -> time-scheduled


def test_dag_count_exceeds_components_floor(result):
    """More dags than the components strategy would produce (hardcoded floor)."""
    assert result.stats["n_dags"] >= 20


def test_hub_cut_separates_batch_open(result):
    """No dag contains OPS_OPEN_BATCH together with any BATCH-OPEN consumer."""
    open_dag = result.assignments["OPS/OPS_OPEN_BATCH"]
    consumers = OPS_APPS + ["FIN_EOD/__FOLDER_START__"]
    for uid in consumers:
        assert result.assignments[uid] != open_dag


def test_cyclic_job_own_dag(result):
    dag = _dag_by_id(result, result.assignments["OPS/OPS_FS_POLL"])
    assert dag.jobs == ["OPS/OPS_FS_POLL"]
    assert dag.schedule == "*/15 6-19 * * *"
    assert not dag.dataset_triggered


def test_stg_chain_stays_one_dataset_triggered_dag(result):
    """STG_QUALITY has a single owner (STG_INGEST) -> one dataset-triggered dag."""
    ingest_dag = result.assignments["STG/STG_INGEST"]
    assert result.assignments["STG/STG_QUALITY"] == ingest_dag
    dag = _dag_by_id(result, ingest_dag)
    assert sorted(dag.jobs) == ["STG/STG_INGEST", "STG/STG_QUALITY"]
    assert dag.dataset_triggered
    assert dag.schedule is None
    assert dag.datasets == ["ctrlm://cond/FILE-ARRIVED"]
    assert dag.roots == ["STG/STG_INGEST"]


def test_risk_folds_into_fin_eod_and_rpt_splits(result):
    """RISK_CALC matches the FIN_EOD owner pattern; RPT_WEEKLY_PACK does not."""
    fin_dag = result.assignments["FIN_EOD/__FOLDER_START__"]
    assert fin_dag == "fin_eod"                     # folder-start -> folder name
    assert result.assignments["RISK/RISK_CALC"] == fin_dag
    assert result.assignments["FIN_EOD/FIN_EXTRACT"] == fin_dag
    rpt_dag = result.assignments["RPT/RPT_WEEKLY_PACK"]
    assert rpt_dag != fin_dag
    split = [
        link for link in result.cross_links
        if link.kind == EDGE_OWNER and link.target == "RPT/RPT_WEEKLY_PACK"
    ]
    assert len(split) == 1
    assert split[0].source == "FIN_EOD/FIN_EXTRACT"
    assert split[0].conds == ["FIN-DONE"]


def test_hr_calc_not_a_root_and_grouped_with_folder_start(result):
    """HR_CALC hangs off HR_PAY/__FOLDER_START__ (not a root of its dag)."""
    start_dag = result.assignments["HR_PAY/__FOLDER_START__"]
    assert result.assignments["HR_PAY/HR_CALC"] == start_dag
    dag = _dag_by_id(result, start_dag)
    assert "HR_PAY/HR_CALC" not in dag.roots
    assert dag.roots == ["HR_PAY/__FOLDER_START__"]


def test_prev_run_cross_link(result):
    prev = [link for link in result.cross_links if link.kind == EDGE_PREV_RUN]
    assert len(prev) == 1
    link = prev[0]
    assert link.conds == ["HR-PAY-DONE"]
    assert link.target == "HR_PAY/HR_CALC"
    assert link.mechanism == WIRE_PREV


def test_single_job_dags_diagnostic(result):
    diags = [d for d in result.diagnostics if d.code == "SINGLE_JOB_DAGS"]
    assert len(diags) == 1
    assert diags[0].level == "info"
    count = result.stats["single_job_dags"]
    assert count == 18
    assert str(count) in diags[0].message


# ------------------------------------------------------------ determinism

def test_determinism_byte_identical():
    graph_a, config_a = _build_graph()
    graph_b, config_b = _build_graph()
    result_a = partition(graph_a, config_a)
    result_b = partition(graph_b, config_b)
    assert result_a.model_dump_json(indent=2) == result_b.model_dump_json(indent=2)


# ------------------------------------------------------------ unit fixtures

def _job(name: str, folder: str = "F", pattern: str | None = None, **kw) -> Job:
    job = Job(name=name, folder=folder, **kw)
    job.day_pattern = pattern
    return job


def _mini_graph(jobs: list[Job], edges: list[tuple[str, str, str]]) -> CtmGraph:
    graph = CtmGraph()
    for job in jobs:
        graph.nodes[job.uid] = job
    graph.e_edges = [GraphEdge(source=s, target=t, cond=c) for s, t, c in edges]
    return graph


def test_snake_case():
    assert snake_case("DW_BUILD_MART") == "dw_build_mart"
    assert snake_case("__FOLDER_START__") == "folder_start"
    assert snake_case("Job--Name.01") == "job_name_01"


def test_convergence_creates_new_owner():
    graph = _mini_graph(
        [_job("R1"), _job("R2"), _job("M"), _job("T")],
        [("F/R1", "F/M", "C1"), ("F/R2", "F/M", "C2"), ("F/M", "F/T", "C3")],
    )
    result = partition(graph, PartitionConfig())
    assert result.assignments["F/M"] == result.assignments["F/T"] == "m"
    assert result.assignments["F/R1"] == "r1"
    assert result.assignments["F/R2"] == "r2"
    splits = [l for l in result.cross_links if l.kind == EDGE_OWNER]
    assert len(splits) == 2


def test_pattern_mismatch_creates_new_owner():
    daily = "WD=ALL|MD=|M=|OP=OR"
    monday = "WD=1|MD=|M=|OP=OR"
    graph = _mini_graph(
        [_job("A", pattern=daily), _job("B", pattern=monday), _job("C", pattern=None)],
        [("F/A", "F/B", "C1"), ("F/B", "F/C", "C2")],
    )
    result = partition(graph, PartitionConfig())
    assert result.assignments["F/A"] == "a"
    assert result.assignments["F/B"] == "b"        # non-None pattern differs
    assert result.assignments["F/C"] == "b"        # pattern None folds into owner


def test_owner_pattern_none_counts_as_mismatch():
    graph = _mini_graph(
        [_job("A", pattern=None), _job("B", pattern="WD=1|MD=|M=|OP=OR")],
        [("F/A", "F/B", "C1")],
    )
    result = partition(graph, PartitionConfig())
    assert result.assignments["F/A"] == "a"
    assert result.assignments["F/B"] == "b"


def test_cycle_fallback_group():
    graph = _mini_graph(
        [_job("A"), _job("B"), _job("Z")],
        [("F/A", "F/B", "C1"), ("F/B", "F/A", "C2"), ("F/A", "F/Z", "C3")],
    )
    result = partition(graph, PartitionConfig())
    # A, B and the node trapped behind the cycle form ONE fallback group
    assert result.assignments["F/A"] == result.assignments["F/B"] == "a"
    assert result.assignments["F/Z"] == "a"
    diags = [d for d in result.diagnostics if d.code == "CYCLE_FALLBACK"]
    assert len(diags) == 1
    assert diags[0].level == "warn"
    assert diags[0].subject == "F/A"


def test_dag_id_collision_canonical_suffix():
    graph = _mini_graph([_job("JOB.A", folder="F1"), _job("JOB_A", folder="F2")], [])
    result = partition(graph, PartitionConfig())
    assert result.assignments["F1/JOB.A"] == "job_a"   # canonical (owner uid) order
    assert result.assignments["F2/JOB_A"] == "job_a_2"
