"""Tests for ctrlm_core.graph (Phase 0), cuts (Phases 1-3) and stats.

All fixtures are small inline Deftables built from ctrlm_core.model — no
dependency on the parser or the sample XML files.
"""
from __future__ import annotations

from ctrlm_core import cuts, graph as graph_mod, stats as stats_mod
from ctrlm_core.model import (
    EDGE_CYCLIC,
    EDGE_HUB,
    EDGE_INTRA,
    EDGE_PATTERN,
    EDGE_PREV_RUN,
    EDGE_REVIEW,
    Condition,
    CrossLink,
    DagSpec,
    Deftable,
    FolderDef,
    Job,
    PartitionConfig,
)


def make_job(
    name: str,
    folder: str = "F1",
    ins: list[Condition] | None = None,
    outs: list[Condition] | None = None,
    **kw,
) -> Job:
    return Job(
        name=name, folder=folder,
        in_conds=list(ins or []), out_conds=list(outs or []),
        **kw,
    )


def build(jobs: list[Job]) -> "graph_mod.CtmGraph":
    folders: dict[str, list[Job]] = {}
    for job in jobs:
        folders.setdefault(job.folder, []).append(job)
    deftable = Deftable(
        folders=[FolderDef(name=name, jobs=members) for name, members in sorted(folders.items())]
    )
    return graph_mod.build_graph(deftable, PartitionConfig())


def edge_tuples(edges):
    return [(e.source, e.target, e.cond, e.kind) for e in edges]


def flag_codes(g):
    return [f["code"] for f in g.flags]


# ---------------------------------------------------------------- Phase 0

def test_odat_pair_becomes_e_edge():
    g = build([
        make_job("A", outs=[Condition(name="X")]),
        make_job("B", ins=[Condition(name="X")]),
    ])
    assert edge_tuples(g.e_edges) == [("F1/A", "F1/B", "X", EDGE_INTRA)]
    assert g.w_edges == []
    assert g.orphan_conds == [] and g.dead_end_conds == []


def test_prev_qualifier_routes_to_prev_run():
    g = build([
        make_job("A", outs=[Condition(name="X")]),
        make_job("B", ins=[Condition(name="X", odate="PREV")]),
    ])
    assert g.e_edges == []
    assert edge_tuples(g.w_edges) == [("F1/A", "F1/B", "X", EDGE_PREV_RUN)]


def test_stat_qualifier_routes_to_review_with_flag():
    g = build([
        make_job("A", outs=[Condition(name="X")]),
        make_job("B", ins=[Condition(name="X", odate="STAT")]),
    ])
    assert g.e_edges == []
    assert edge_tuples(g.w_edges) == [("F1/A", "F1/B", "X", EDGE_REVIEW)]
    assert flag_codes(g) == ["REVIEW_QUALIFIER"]


def test_literal_date_producer_qualifier_routes_to_review():
    g = build([
        make_job("A", outs=[Condition(name="X", odate="0131")]),
        make_job("B", ins=[Condition(name="X")]),
    ])
    assert edge_tuples(g.w_edges) == [("F1/A", "F1/B", "X", EDGE_REVIEW)]
    assert flag_codes(g) == ["REVIEW_QUALIFIER"]


def test_del_sign_makes_no_edge_and_is_flagged():
    g = build([
        make_job("A", outs=[Condition(name="X", sign="DEL")]),
        make_job("B", ins=[Condition(name="X")]),
    ])
    assert g.e_edges == [] and g.w_edges == []
    assert "DEL_CONDITION" in flag_codes(g)
    # with no ADD producer, X is consumed-but-never-produced
    assert g.orphan_conds == [{"cond": "X", "consumers": ["F1/B"]}]


def test_self_edge_dropped_and_flagged():
    g = build([
        make_job("A", ins=[Condition(name="X")], outs=[Condition(name="X")]),
        make_job("B", ins=[Condition(name="X")]),
    ])
    # the A->A pair is dropped; the A->B pair survives
    assert edge_tuples(g.e_edges) == [("F1/A", "F1/B", "X", EDGE_INTRA)]
    assert "SELF_CONDITION" in flag_codes(g)


def test_orphans_and_dead_ends_exclude_synthetic():
    g = build([
        make_job(
            "A",
            ins=[Condition(name="EXT-FEED-OK"), Condition(name="__start__F1")],
            outs=[Condition(name="NOBODY-CARES"), Condition(name="__done__F1/A")],
        ),
    ])
    assert g.orphan_conds == [{"cond": "EXT-FEED-OK", "consumers": ["F1/A"]}]
    assert g.dead_end_conds == [{"cond": "NOBODY-CARES", "producers": ["F1/A"]}]


def test_fan_out_expands_to_all_pairs_sorted():
    g = build([
        make_job("P", outs=[Condition(name="X")]),
        make_job("C2", ins=[Condition(name="X")]),
        make_job("C1", ins=[Condition(name="X")]),
    ])
    assert edge_tuples(g.e_edges) == [
        ("F1/P", "F1/C1", "X", EDGE_INTRA),
        ("F1/P", "F1/C2", "X", EDGE_INTRA),
    ]


def test_build_graph_is_deterministic():
    def jobs():
        return [
            make_job("B", folder="F2", ins=[Condition(name="X"), Condition(name="Q")]),
            make_job("A", outs=[Condition(name="X"), Condition(name="Z")]),
            make_job("C", ins=[Condition(name="Z", odate="PREV")]),
        ]

    first = graph_mod.build_graph(
        Deftable(folders=[FolderDef(name="F", jobs=jobs())]), PartitionConfig()
    )
    second = graph_mod.build_graph(
        Deftable(folders=[FolderDef(name="F", jobs=jobs())]), PartitionConfig()
    )
    assert first.model_dump_json() == second.model_dump_json()
    assert list(first.nodes) == sorted(first.nodes)


# ---------------------------------------------------------------- Phase 1: cyclic

def test_extract_cyclic_moves_touching_edges():
    g = build([
        make_job("UP", outs=[Condition(name="GO")]),
        make_job("POLL", cyclic=True, interval_minutes=15,
                 ins=[Condition(name="GO")], outs=[Condition(name="FILE-ARRIVED")]),
        make_job("DOWN", ins=[Condition(name="FILE-ARRIVED")]),
    ])
    assert len(g.e_edges) == 2
    cyclic = cuts.extract_cyclic(g)
    assert cyclic == ["F1/POLL"]
    assert g.e_edges == []
    assert sorted(edge_tuples(g.w_edges)) == [
        ("F1/POLL", "F1/DOWN", "FILE-ARRIVED", EDGE_CYCLIC),
        ("F1/UP", "F1/POLL", "GO", EDGE_CYCLIC),
    ]


def test_extract_cyclic_noop_without_cyclic_jobs():
    g = build([
        make_job("A", outs=[Condition(name="X")]),
        make_job("B", ins=[Condition(name="X")]),
    ])
    assert cuts.extract_cyclic(g) == []
    assert len(g.e_edges) == 1 and g.w_edges == []


# ---------------------------------------------------------------- Phase 2: hubs

def hub_fixture(n_consumers: int, cond: str = "BATCH-OPEN") -> "graph_mod.CtmGraph":
    jobs = [make_job("OPEN", folder="OPS", outs=[Condition(name=cond)])]
    jobs += [
        make_job(f"APP{i:02d}", folder="OPS", ins=[Condition(name=cond)])
        for i in range(1, n_consumers + 1)
    ]
    return build(jobs)


def test_hub_cut_at_fan_out_threshold():
    g = hub_fixture(10)
    cuts.hub_cuts(g, PartitionConfig(hub_fan=10, hub_spread=3))
    assert g.e_edges == []
    assert len(g.w_edges) == 10
    assert {e.kind for e in g.w_edges} == {EDGE_HUB}


def test_no_hub_cut_below_fan_out_threshold():
    g = hub_fixture(9)
    cuts.hub_cuts(g, PartitionConfig(hub_fan=10, hub_spread=3))
    assert len(g.e_edges) == 9
    assert g.w_edges == []


def test_hub_cut_counts_orphan_consumers_too():
    # fan statistics come from the condition lists, not the surviving edges:
    # 9 matched consumers + 1 consumer of the same cond behind a PREV qualifier
    g = hub_fixture(9)
    extra = make_job("APP_PREV", folder="OPS", ins=[Condition(name="BATCH-OPEN", odate="PREV")])
    g.nodes[extra.uid] = extra
    # rebuild-free shortcut: hub_cuts reads graph.nodes only
    cuts.hub_cuts(g, PartitionConfig(hub_fan=10, hub_spread=3))
    assert g.e_edges == [] and len(g.w_edges) == 9


def test_hub_cut_on_consumer_folder_spread():
    jobs = [make_job("P", folder="FA", outs=[Condition(name="X")])]
    jobs += [
        make_job("C", folder=f, ins=[Condition(name="X")]) for f in ("FB", "FC", "FD")
    ]
    g = build(jobs)
    cuts.hub_cuts(g, PartitionConfig(hub_fan=10, hub_spread=3))
    assert g.e_edges == []
    assert {e.kind for e in g.w_edges} == {EDGE_HUB}


def test_no_hub_cut_below_spread_threshold():
    # 1 producer folder + consumers in only 2 folders: kept (fan is small too)
    jobs = [make_job("P", folder="FA", outs=[Condition(name="X")])]
    jobs += [make_job("C", folder=f, ins=[Condition(name="X")]) for f in ("FB", "FC")]
    g = build(jobs)
    cuts.hub_cuts(g, PartitionConfig(hub_fan=10, hub_spread=3))
    assert len(g.e_edges) == 2 and g.w_edges == []


def test_synthetic_conditions_exempt_from_hub_cuts():
    g = hub_fixture(12, cond="__start__OPS")
    cuts.hub_cuts(g, PartitionConfig(hub_fan=10, hub_spread=3))
    assert len(g.e_edges) == 12
    assert g.w_edges == []


# ---------------------------------------------------------------- Phase 3: patterns

def test_pattern_cut_on_direct_conflict():
    daily = make_job("A", weekdays="1,2,3,4,5", outs=[Condition(name="X")])
    weekly = make_job("B", weekdays="1", ins=[Condition(name="X")])
    g = build([daily, weekly])
    for uid, job in g.nodes.items():
        from ctrlm_core import schedule
        job.day_pattern = schedule.day_pattern_of(job)
    cuts.pattern_cuts(g)
    assert g.e_edges == []
    assert edge_tuples(g.w_edges) == [("F1/A", "F1/B", "X", EDGE_PATTERN)]


def test_pattern_cut_ignores_unscheduled_endpoint():
    daily = make_job("A", weekdays="1,2,3,4,5", outs=[Condition(name="X")])
    condition_driven = make_job("B", ins=[Condition(name="X")])
    g = build([daily, condition_driven])
    from ctrlm_core import schedule
    for job in g.nodes.values():
        job.day_pattern = schedule.day_pattern_of(job)
    cuts.pattern_cuts(g)
    assert len(g.e_edges) == 1 and g.w_edges == []


def test_pattern_cut_keeps_same_pattern_different_timefrom():
    early = make_job("A", weekdays="1,2,3,4,5", timefrom="2100", outs=[Condition(name="X")])
    late = make_job("B", weekdays="1,2,3,4,5", timefrom="0200", ins=[Condition(name="X")])
    g = build([early, late])
    from ctrlm_core import schedule
    for job in g.nodes.values():
        job.day_pattern = schedule.day_pattern_of(job)
    cuts.pattern_cuts(g)
    assert len(g.e_edges) == 1 and g.w_edges == []


# ---------------------------------------------------------------- stats

def test_compute_stats_exact_keys_and_values():
    g = build([
        make_job("A", outs=[Condition(name="X")]),
        make_job("B", ins=[Condition(name="X")]),
        make_job("C"),
    ])
    dags = [
        DagSpec(dag_id="big", jobs=["F1/A", "F1/B"], roots=["F1/A"]),
        DagSpec(dag_id="lone", jobs=["F1/C"], roots=["F1/C"],
                dataset_triggered=True, datasets=["ctrlm://cond/X"]),
    ]
    links = [
        CrossLink(source="F1/A", target="F1/C", conds=["X"], kind="HUB", mechanism="sensor"),
        CrossLink(source="F1/B", target="F1/C", conds=["Y"], kind="HUB", mechanism="dataset"),
        CrossLink(source="F1/A", target="F1/B", conds=["Z"], kind="PREV_RUN",
                  mechanism="prev_run_sensor"),
    ]
    result = stats_mod.compute_stats(g, dags, links)

    assert set(result) == {
        "n_jobs", "n_dags", "n_cross_links", "cross_links_by_kind",
        "cross_links_by_mechanism", "single_job_dags", "multi_root_dags",
        "dataset_triggered_dags", "largest_dag", "size_histogram",
    }
    assert result["n_jobs"] == 3
    assert result["n_dags"] == 2
    assert result["n_cross_links"] == 3
    assert result["cross_links_by_kind"] == {"HUB": 2, "PREV_RUN": 1}
    assert result["cross_links_by_mechanism"] == {
        "dataset": 1, "prev_run_sensor": 1, "sensor": 1,
    }
    assert result["single_job_dags"] == 1
    assert result["multi_root_dags"] == 0
    assert result["dataset_triggered_dags"] == 1
    assert result["largest_dag"] == {"dag_id": "big", "size": 2}
    assert result["size_histogram"] == {"1": 1, "2-5": 1, "6-15": 0, "16-50": 0, "51+": 0}


def test_stats_histogram_buckets_boundaries():
    g = build([make_job("A")])
    def dag(i, size):
        return DagSpec(dag_id=f"d{i}", jobs=[f"F/{j}" for j in range(size)])
    dags = [dag(0, 1), dag(1, 2), dag(2, 5), dag(3, 6), dag(4, 15),
            dag(5, 16), dag(6, 50), dag(7, 51)]
    result = stats_mod.compute_stats(g, dags, [])
    assert result["size_histogram"] == {"1": 1, "2-5": 2, "6-15": 2, "16-50": 2, "51+": 1}
    assert result["largest_dag"] == {"dag_id": "d7", "size": 51}


def test_stats_empty_partition():
    g = build([make_job("A")])
    result = stats_mod.compute_stats(g, [], [])
    assert result["largest_dag"] == {"dag_id": "", "size": 0}
    assert result["size_histogram"] == {"1": 0, "2-5": 0, "6-15": 0, "16-50": 0, "51+": 0}
    assert result["cross_links_by_kind"] == {}


# ---------------------------------------------------------------- phases compose

def test_full_cut_sequence_is_deterministic():
    def make_graph():
        jobs = [
            make_job("OPEN", folder="OPS", weekdays="ALL", timefrom="2000",
                     outs=[Condition(name="BATCH-OPEN")]),
        ]
        jobs += [
            make_job(f"APP{i:02d}", folder="OPS", weekdays="1,2,3,4,5", timefrom="2100",
                     ins=[Condition(name="BATCH-OPEN")])
            for i in range(1, 11)
        ]
        jobs += [
            make_job("POLL", folder="OPS", cyclic=True, interval_minutes=15,
                     timefrom="0600", timeto="2000", outs=[Condition(name="FILE-ARRIVED")]),
            make_job("INGEST", folder="STG", ins=[Condition(name="FILE-ARRIVED")],
                     outs=[Condition(name="STG-LOADED")]),
            make_job("QUALITY", folder="STG", ins=[Condition(name="STG-LOADED")]),
        ]
        g = build(jobs)
        from ctrlm_core import schedule
        for job in g.nodes.values():
            job.day_pattern = schedule.day_pattern_of(job)
        return g

    def run(g):
        config = PartitionConfig()
        cyclic = cuts.extract_cyclic(g)
        cuts.hub_cuts(g, config)
        cuts.pattern_cuts(g)
        return cyclic, g.model_dump_json()

    first = run(make_graph())
    second = run(make_graph())
    assert first == second
    cyclic, _ = first
    assert cyclic == ["OPS/POLL"]

    g = make_graph()
    cuts.extract_cyclic(g)
    cuts.hub_cuts(g, PartitionConfig())
    cuts.pattern_cuts(g)
    kinds = {e.kind for e in g.w_edges}
    # BATCH-OPEN (fan-out 10) -> HUB; POLL edge -> CYCLIC; STG chain stays intra
    assert kinds == {EDGE_CYCLIC, EDGE_HUB}
    assert edge_tuples(g.e_edges) == [("STG/INGEST", "STG/QUALITY", "STG-LOADED", EDGE_INTRA)]
