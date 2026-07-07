"""Tests for the components strategy (strategy_components/partitioner.py).

End-to-end over the sample XMLs (parse -> desugar -> normalize -> build_graph
-> partition) asserting the contract's expected outcomes, plus inline-fixture
tests for the min-cut machinery (AUTO_RESOLVED capacity behavior, ANCHOR
splits, singleton coalescing) and determinism.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from ctrlm_core import graph as graph_mod
from ctrlm_core import schedule
from ctrlm_core.desugar import desugar
from ctrlm_core.model import (
    EDGE_ANCHOR,
    EDGE_AUTO,
    EDGE_HUB,
    EDGE_PREV_RUN,
    Condition,
    CtmGraph,
    Deftable,
    FolderDef,
    Job,
    PartitionConfig,
)
from ctrlm_core.parser import parse_files
from strategy_components.partitioner import partition, snake_case

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = sorted((ROOT / "examples" / "exports").glob("sample_*.xml"))


# ---------------------------------------------------------------- fixtures

def build_sample_graph(config: PartitionConfig | None = None) -> CtmGraph:
    config = config or PartitionConfig()
    deftable = parse_files(list(SAMPLES))
    desugar(deftable, config)
    schedule.normalize_jobs(deftable, config)
    return graph_mod.build_graph(deftable, config)


@pytest.fixture(scope="module")
def sample():
    """(graph, result) for the four sample XMLs with the default config."""
    graph = build_sample_graph()
    result = partition(graph, PartitionConfig())
    return graph, result


def make_graph(jobs: list[Job], folders: list[FolderDef] | None = None) -> CtmGraph:
    """Inline fixture: jobs grouped into plain folders, normalized, graphed."""
    config = PartitionConfig()
    if folders is None:
        by_folder: dict[str, list[Job]] = {}
        for job in jobs:
            by_folder.setdefault(job.folder, []).append(job)
        folders = [
            FolderDef(name=name, jobs=members)
            for name, members in sorted(by_folder.items())
        ]
    deftable = Deftable(folders=folders)
    desugar(deftable, config)
    schedule.normalize_jobs(deftable, config)
    return graph_mod.build_graph(deftable, config)


def dag_by_id(result, dag_id):
    return {d.dag_id: d for d in result.dags}[dag_id]


# ---------------------------------------------------------------- expected outcomes

def test_fin_dw_all_five_jobs_in_one_dag(sample):
    _, result = sample
    uids = [
        "FIN_DW/DW_LOAD_CUSTOMERS", "FIN_DW/DW_LOAD_ORDERS", "FIN_DW/DW_LOAD_PRODUCTS",
        "FIN_DW/DW_BUILD_MART", "FIN_DW/DW_PUBLISH",
    ]
    dag_ids = {result.assignments[u] for u in uids}
    assert len(dag_ids) == 1
    dag = dag_by_id(result, dag_ids.pop())
    assert sorted(dag.jobs) == sorted(uids)
    assert dag.dag_id == "fin_dw"
    # scheduled at the loaders' 2100 anchor, weekdays 1-5
    assert dag.anchor == "2100"
    assert dag.schedule == "0 21 * * 1,2,3,4,5"
    assert sorted(dag.roots) == [
        "FIN_DW/DW_LOAD_CUSTOMERS", "FIN_DW/DW_LOAD_ORDERS", "FIN_DW/DW_LOAD_PRODUCTS",
    ]


def test_hub_cut_isolates_batch_open_producer(sample):
    graph, result = sample
    producer_dag = result.assignments["OPS/OPS_OPEN_BATCH"]
    consumers = sorted(
        uid for uid, job in graph.nodes.items()
        if any(c.name == "BATCH-OPEN" for c in job.in_conds)
    )
    assert consumers  # 10 OPS_APPs + FIN_EOD folder start
    for uid in consumers:
        assert result.assignments[uid] != producer_dag
    hub_links = [l for l in result.cross_links if l.kind == EDGE_HUB]
    assert {l.source for l in hub_links} == {"OPS/OPS_OPEN_BATCH"}
    assert {l.target for l in hub_links} == set(consumers)
    assert all(l.conds == ["BATCH-OPEN"] for l in hub_links)


def test_ops_apps_coalesce_into_one_dag(sample):
    _, result = sample
    apps = [f"OPS/OPS_APP{i:02d}" for i in range(1, 11)]
    dag_ids = {result.assignments[u] for u in apps}
    assert len(dag_ids) == 1
    dag = dag_by_id(result, dag_ids.pop())
    assert sorted(dag.jobs) == sorted(apps)          # nothing else joined in
    assert dag.roots == sorted(apps)                 # edge-less: all roots
    assert dag.schedule == "0 21 * * 1,2,3,4,5"


def test_rpt_weekly_pack_split_from_fin_eod_chain(sample):
    _, result = sample
    rpt_dag = result.assignments["RPT/RPT_WEEKLY_PACK"]
    chain_dag = result.assignments["FIN_EOD/FIN_EXTRACT"]
    assert rpt_dag != chain_dag
    links = [
        l for l in result.cross_links
        if l.source == "FIN_EOD/FIN_EXTRACT" and l.target == "RPT/RPT_WEEKLY_PACK"
    ]
    assert len(links) == 1
    # FIN_EXTRACT has its own TIMEFROM, so the cascade leaves it day-pattern-
    # less: the WD=1-5 vs WD=1 conflict is hidden behind an unscheduled middle
    # and is resolved transitively by the Phase 6 min-cut (AUTO_RESOLVED),
    # exactly as the contract's expected outcomes require.
    assert links[0].kind == EDGE_AUTO
    assert links[0].conds == ["FIN-DONE"]
    # RPT rides its own Monday-only cron -> sensor mechanism
    assert links[0].mechanism == "sensor"
    assert dag_by_id(result, rpt_dag).schedule == "0 6 * * 1"


def test_risk_calc_merged_with_fin_eod_chain(sample):
    _, result = sample
    chain = [
        "FIN_EOD/__FOLDER_START__", "FIN_EOD/FIN_LOAD_GL", "FIN_EOD/FIN_POST_GL",
        "FIN_EOD/FIN_EXTRACT", "FIN_EOD/__FOLDER_END__", "RISK/RISK_CALC",
    ]
    dag_ids = {result.assignments[u] for u in chain}
    assert len(dag_ids) == 1
    dag = dag_by_id(result, dag_ids.pop())
    assert dag.dag_id == "fin_eod"                    # modal folder wins
    assert sorted(dag.jobs) == sorted(chain)
    assert dag.folders == ["FIN_EOD", "RISK"]
    assert dag.day_pattern == "WD=1,2,3,4,5|MD=|M=|OP=OR"


def test_hr_calc_gated_by_folder_start_and_prev_run_link(sample):
    graph, result = sample
    # HR_CALC has an incoming e_edge from the synthetic folder start
    assert any(
        e.source == "HR_PAY/__FOLDER_START__" and e.target == "HR_PAY/HR_CALC"
        for e in graph.e_edges
    )
    dag = dag_by_id(result, result.assignments["HR_PAY/HR_CALC"])
    assert "HR_PAY/HR_CALC" not in dag.roots
    assert dag.roots == ["HR_PAY/__FOLDER_START__"]
    # PREV_RUN wiring entry for HR-PAY-DONE (valid intra-DAG too)
    prev = [l for l in result.cross_links if l.kind == EDGE_PREV_RUN]
    assert len(prev) == 1
    assert prev[0].target == "HR_PAY/HR_CALC"
    assert prev[0].conds == ["HR-PAY-DONE"]
    assert prev[0].mechanism == "prev_run_sensor"


def test_cyclic_job_gets_own_dag_with_cyclic_cron(sample):
    _, result = sample
    dag = dag_by_id(result, result.assignments["OPS/OPS_FS_POLL"])
    assert dag.jobs == ["OPS/OPS_FS_POLL"]
    assert dag.roots == ["OPS/OPS_FS_POLL"]
    assert dag.schedule == "*/15 6-19 * * *"
    assert dag.dataset_triggered is False
    assert dag.anchor == "0600"


def test_stg_chain_is_dataset_triggered(sample):
    _, result = sample
    dag = dag_by_id(result, result.assignments["STG/STG_INGEST"])
    assert sorted(dag.jobs) == ["STG/STG_INGEST", "STG/STG_QUALITY"]
    assert dag.dataset_triggered is True
    assert dag.schedule is None
    assert dag.day_pattern is None
    assert dag.datasets == ["ctrlm://cond/FILE-ARRIVED"]
    # the inbound cyclic link is realized as a dataset
    link = next(
        l for l in result.cross_links
        if l.source == "OPS/OPS_FS_POLL" and l.target == "STG/STG_INGEST"
    )
    assert link.mechanism == "dataset"


def test_naming_collisions_get_canonical_suffixes(sample):
    _, result = sample
    # three OPS clusters: coalesced APPs, cyclic FS_POLL, hub-cut OPEN_BATCH
    assert result.assignments["OPS/OPS_APP01"] == "ops"
    assert result.assignments["OPS/OPS_FS_POLL"] == "ops_2"
    assert result.assignments["OPS/OPS_OPEN_BATCH"] == "ops_3"
    # two HR_IN singletons with different day patterns stay separate
    assert result.assignments["HR_IN/HR_EXT_FEED_CHECK"] == "hr_in"
    assert result.assignments["HR_IN/HR_FW"] == "hr_in_2"


# ---------------------------------------------------------------- invariants

def test_i1_every_job_in_exactly_one_dag(sample):
    graph, result = sample
    assert sorted(result.assignments) == sorted(graph.nodes)
    covered: list[str] = []
    for dag in result.dags:
        covered.extend(dag.jobs)
    assert sorted(covered) == sorted(graph.nodes)     # no dupes, no drops
    for dag in result.dags:
        assert all(result.assignments[u] == dag.dag_id for u in dag.jobs)


def test_i2_cross_links_cover_all_wiring_edges(sample):
    graph, result = sample
    expected = {
        (e.source, e.target, e.kind, e.cond)
        for e in graph.w_edges
        if e.source in graph.nodes and e.target in graph.nodes
    }
    realized = {
        (l.source, l.target, l.kind, c)
        for l in result.cross_links for c in l.conds
    }
    assert realized == expected
    # dedupe key is (source, target, kind)
    keys = [(l.source, l.target, l.kind) for l in result.cross_links]
    assert len(keys) == len(set(keys))


def test_i3_every_dag_has_at_most_one_day_pattern(sample):
    graph, result = sample
    for dag in result.dags:
        patterns = {
            graph.nodes[u].day_pattern for u in dag.jobs
            if graph.nodes[u].day_pattern is not None
        }
        assert len(patterns) <= 1
        if patterns:
            assert dag.day_pattern == patterns.pop()


def test_stats_shape_and_headline_numbers(sample):
    graph, result = sample
    stats = result.stats
    assert stats["n_jobs"] == len(graph.nodes) == 47
    assert stats["n_dags"] == len(result.dags) == 12
    assert stats["n_cross_links"] == len(result.cross_links)
    assert stats["dataset_triggered_dags"] == 2      # stg + hr_in (orphan feed)
    assert stats["cross_links_by_kind"][EDGE_HUB] == 11
    assert stats["cross_links_by_kind"][EDGE_PREV_RUN] == 1
    assert set(stats["size_histogram"]) == {"1", "2-5", "6-15", "16-50", "51+"}


def test_strategy_and_result_shape(sample):
    _, result = sample
    assert result.strategy == "components"
    assert result.dags == sorted(result.dags, key=lambda d: d.dag_id)
    for dag in result.dags:
        assert dag.jobs == sorted(dag.jobs)
        assert dag.folders == sorted(dag.folders)


# ---------------------------------------------------------------- determinism

def test_partition_is_deterministic_end_to_end():
    first = partition(build_sample_graph(), PartitionConfig())
    second = partition(build_sample_graph(), PartitionConfig())
    assert first.model_dump() == second.model_dump()
    assert first.model_dump_json() == second.model_dump_json()


# ---------------------------------------------------------------- Phase 6 machinery

def _job(name, folder="ETL", ins=(), outs=(), **kw):
    return Job(
        name=name, folder=folder,
        in_conds=[Condition(name=c) for c in ins],
        out_conds=[Condition(name=c) for c in outs],
        **kw,
    )


def test_auto_resolved_min_cut_uses_parallel_pair_capacity():
    # daily A ==(X1,X2)==> unscheduled B --(Y)--> weekly C : transitive conflict.
    # capacity(A,B)=2 > capacity(B,C)=1, so the min cut severs Y and the
    # unscheduled middle B stays with A.
    graph = make_graph([
        _job("A", outs=("X1", "X2"), weekdays="1,2,3,4,5"),
        _job("B", ins=("X1", "X2"), outs=("Y",)),
        _job("C", ins=("Y",), weekdays="1"),
    ])
    result = partition(graph, PartitionConfig())
    assert result.assignments["ETL/B"] == result.assignments["ETL/A"]
    assert result.assignments["ETL/C"] != result.assignments["ETL/A"]
    auto = [l for l in result.cross_links if l.kind == EDGE_AUTO]
    assert len(auto) == 1
    assert (auto[0].source, auto[0].target, auto[0].conds) == ("ETL/B", "ETL/C", ["Y"])
    # both sides keep a pure schedule
    assert dag_by_id(result, result.assignments["ETL/A"]).day_pattern == "WD=1,2,3,4,5|MD=|M=|OP=OR"
    assert dag_by_id(result, result.assignments["ETL/C"]).day_pattern == "WD=1|MD=|M=|OP=OR"


def test_auto_resolved_worklist_resplits_until_pure():
    # three patterns chained through unscheduled middles: two cuts required
    graph = make_graph([
        _job("A", outs=("P",), weekdays="1,2,3,4,5"),
        _job("M1", ins=("P",), outs=("Q",)),
        _job("B", ins=("Q",), outs=("R",), weekdays="1"),
        _job("M2", ins=("R",), outs=("S",)),
        _job("C", ins=("S",), monthdays="1,15"),
    ])
    result = partition(graph, PartitionConfig())
    dag_ids = {result.assignments[u] for u in ("ETL/A", "ETL/B", "ETL/C")}
    assert len(dag_ids) == 3
    auto = [l for l in result.cross_links if l.kind == EDGE_AUTO]
    assert len(auto) == 2
    for dag in result.dags:
        patterns = {
            graph.nodes[u].day_pattern for u in dag.jobs
            if graph.nodes[u].day_pattern is not None
        }
        assert len(patterns) <= 1


def test_anchor_spread_min_cut_splits_rarest_bucket():
    # two same-pattern roots 16h apart on the ODATE clock feed one join job:
    # spread 960 min > 6h*60 -> ANCHOR cut of the rarest (earliest) bucket.
    graph = make_graph([
        _job("R1", folder="NIGHT", outs=("P",), weekdays="1,2,3,4,5", timefrom="0700"),
        _job("R2", folder="NIGHT", outs=("Q",), weekdays="1,2,3,4,5", timefrom="2300"),
        _job("M", folder="NIGHT", ins=("P", "Q")),
    ])
    result = partition(graph, PartitionConfig())
    assert result.assignments["NIGHT/R1"] != result.assignments["NIGHT/R2"]
    anchor_links = [l for l in result.cross_links if l.kind == EDGE_ANCHOR]
    assert len(anchor_links) == 1
    # both single-edge cuts are minimal; edmonds_karp's residual reachability
    # deterministically keeps the interior join M on the source (R1) side
    assert (anchor_links[0].source, anchor_links[0].target) == ("NIGHT/R2", "NIGHT/M")
    assert result.assignments["NIGHT/M"] == result.assignments["NIGHT/R1"]
    assert dag_by_id(result, result.assignments["NIGHT/R1"]).anchor == "0700"
    assert dag_by_id(result, result.assignments["NIGHT/R2"]).anchor == "2300"


def test_anchor_spread_within_threshold_stays_together():
    graph = make_graph([
        _job("R1", folder="NIGHT", outs=("P",), weekdays="1,2,3,4,5", timefrom="2100"),
        _job("R2", folder="NIGHT", outs=("Q",), weekdays="1,2,3,4,5", timefrom="0200"),
        _job("M", folder="NIGHT", ins=("P", "Q")),
    ])
    # rel(2100)=900, rel(0200)=1200 -> spread 300 <= 360: one DAG
    result = partition(graph, PartitionConfig())
    assert len({result.assignments[u] for u in ("NIGHT/R1", "NIGHT/R2", "NIGHT/M")}) == 1
    assert not [l for l in result.cross_links if l.kind == EDGE_ANCHOR]
    # anchor is the EARLIEST root on the ODATE clock: 2100, not 0200
    assert dag_by_id(result, result.assignments["NIGHT/M"]).anchor == "2100"


# ---------------------------------------------------------------- coalescing

def test_coalesce_singletons_toggle():
    jobs = [
        _job(f"LONER{i}", folder="OPS", weekdays="1,2,3,4,5", timefrom="2100")
        for i in range(1, 4)
    ] + [_job("OTHER", folder="OPS", weekdays="ALL")]

    coalesced = partition(make_graph(jobs), PartitionConfig(coalesce_singletons=True))
    loner_dags = {coalesced.assignments[f"OPS/LONER{i}"] for i in range(1, 4)}
    assert len(loner_dags) == 1                       # same (folder, pattern)
    assert coalesced.assignments["OPS/OTHER"] not in loner_dags  # other pattern

    separate = partition(make_graph(jobs), PartitionConfig(coalesce_singletons=False))
    loner_dags = {separate.assignments[f"OPS/LONER{i}"] for i in range(1, 4)}
    assert len(loner_dags) == 3


def test_oversized_warning_not_split():
    jobs = [_job(f"J{i:02d}", folder="BIG", weekdays="1", outs=(f"C{i:02d}",),
                 ins=() if i == 0 else (f"C{i - 1:02d}",))
            for i in range(5)]
    result = partition(make_graph(jobs), PartitionConfig(max_tasks=3))
    assert len(result.dags) == 1                      # generated as-is
    oversized = [d for d in result.diagnostics if d.code == "OVERSIZED"]
    assert len(oversized) == 1 and oversized[0].level == "warn"
    assert oversized[0].subject == result.dags[0].dag_id


# ---------------------------------------------------------------- misc contract bits

def test_snake_case():
    assert snake_case("FIN_DW") == "fin_dw"
    assert snake_case("My-Folder  (v2)") == "my_folder_v2"
    assert snake_case("__") == "dag"


def test_orphan_ext_feed_ok_recorded_in_graph(sample):
    graph, _ = sample
    assert any(o["cond"] == "EXT-FEED-OK" for o in graph.orphan_conds)


def test_confirm_job_diagnostic(sample):
    _, result = sample
    confirm = [d for d in result.diagnostics if d.code == "CONFIRM_JOB"]
    assert [d.subject for d in confirm] == ["HR_PAY/HR_PAY_RUN"]
