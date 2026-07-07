"""Tests for ctrlm_core.desugar (Phase -1 of docs/partition-algorithm.md)."""
from pathlib import Path

import pytest

from ctrlm_core.desugar import desugar
from ctrlm_core.model import Condition, Deftable, FolderDef, Job, PartitionConfig
from ctrlm_core.parser import parse_files

ROOT = Path(__file__).resolve().parents[1]
EXPORTS = ROOT / "examples" / "exports"
SAMPLES = [
    EXPORTS / "sample_finance.xml",
    EXPORTS / "sample_ops.xml",
    EXPORTS / "sample_hr.xml",
]
MFG = EXPORTS / "sample_mfg.xml"


def desugared():
    dt = parse_files(SAMPLES)
    desugar(dt, PartitionConfig())
    return dt


@pytest.fixture(scope="module")
def dt():
    return desugared()


@pytest.fixture(scope="module")
def mfg():
    deftable = parse_files([MFG])
    desugar(deftable, PartitionConfig())
    return deftable


def folder_of(deftable, name):
    (folder,) = [f for f in deftable.folders if f.name == name]
    return folder


def job_of(deftable, uid):
    return deftable.job_index()[uid]


# ------------------------------------------------------------------ counts

def test_job_counts_after_desugar(dt):
    by_folder = {f.name: len(f.jobs) for f in dt.folders}
    # FIN_EOD and HR_PAY each gain __FOLDER_START__ + __FOLDER_END__
    assert by_folder == {
        "FIN_DW": 5, "FIN_EOD": 5, "RISK": 1, "RPT": 1,
        "OPS": 12, "STG": 2,
        "HR_IN": 2, "HR_PAY": 5,
    }
    assert sum(by_folder.values()) == 33   # 29 real + 4 synthetic


def test_plain_folders_get_no_synthetics(dt):
    for name in ["FIN_DW", "RISK", "RPT", "OPS", "STG", "HR_IN"]:
        assert not any(j.synthetic for j in folder_of(dt, name).jobs), name
    # terminal jobs of un-gated folders gain nothing
    publish = job_of(dt, "FIN_DW/DW_PUBLISH")
    assert not any(c.name.startswith("__done__") for c in publish.out_conds)
    loader = job_of(dt, "FIN_DW/DW_LOAD_CUSTOMERS")
    assert not any(c.name.startswith("__start__") for c in loader.in_conds)


# ------------------------------------------------------------------ FIN_EOD

def test_fin_load_gl_gains_start_incond(dt):
    gl = job_of(dt, "FIN_EOD/FIN_LOAD_GL")
    assert "__start__FIN_EOD" in [c.name for c in gl.in_conds]


def test_fin_eod_start_node(dt):
    start = job_of(dt, "FIN_EOD/__FOLDER_START__")
    assert start.synthetic is True
    assert start.task_type == "Dummy"
    # carries the folder in-conds + produces the __start__ condition
    assert [(c.name, c.odate) for c in start.in_conds] == [("BATCH-OPEN", "ODAT")]
    assert [(c.name, c.sign) for c in start.out_conds] == [("__start__FIN_EOD", "ADD")]
    # folder schedule attrs land on the start node
    assert start.weekdays == "1,2,3,4,5"
    assert start.monthdays == ""
    assert start.months == ""
    assert start.days_and_or == "OR"
    assert start.timezone == ""


def test_fin_eod_end_node_and_terminal(dt):
    end = job_of(dt, "FIN_EOD/__FOLDER_END__")
    assert end.synthetic is True and end.task_type == "Dummy"
    # FIN_EXTRACT is the only terminal (FIN-DONE is consumed outside the folder)
    assert [c.name for c in end.in_conds] == ["__done__FIN_EOD/FIN_EXTRACT"]
    assert [(c.name, c.sign) for c in end.out_conds] == [("FIN-EOD-DONE", "ADD")]

    extract = job_of(dt, "FIN_EOD/FIN_EXTRACT")
    done = [c for c in extract.out_conds if c.name == "__done__FIN_EOD/FIN_EXTRACT"]
    assert len(done) == 1 and done[0].sign == "ADD"


def test_fin_eod_non_entries_non_terminals_untouched(dt):
    post = job_of(dt, "FIN_EOD/FIN_POST_GL")
    assert not any(c.name.startswith("__start__") for c in post.in_conds)
    assert not any(c.name.startswith("__done__") for c in post.out_conds)
    gl = job_of(dt, "FIN_EOD/FIN_LOAD_GL")
    assert not any(c.name.startswith("__done__") for c in gl.out_conds)


# ------------------------------------------------------------------ HR_PAY

def test_hr_pay_synthetic_nodes_exist(dt):
    names = {j.name for j in folder_of(dt, "HR_PAY").jobs}
    assert "__FOLDER_START__" in names
    assert "__FOLDER_END__" in names
    start = job_of(dt, "HR_PAY/__FOLDER_START__")
    assert start.synthetic and start.task_type == "Dummy"
    assert [c.name for c in start.in_conds] == ["HR-FILES-READY"]
    assert [c.name for c in start.out_conds] == ["__start__HR_PAY"]
    # folder DAYS="1,15" lands on the start node
    assert start.monthdays == "1,15"
    assert start.weekdays == ""


def test_hr_calc_is_entry_despite_prev_incond(dt):
    # HR_CALC's only in-cond is PREV-qualified -> still an entry job, so it is
    # wired under __FOLDER_START__ and no longer masquerades as a root.
    calc = job_of(dt, "HR_PAY/HR_CALC")
    assert "__start__HR_PAY" in [c.name for c in calc.in_conds]
    # the PREV in-cond is preserved untouched
    assert ("HR-PAY-DONE", "PREV") in [(c.name, c.odate) for c in calc.in_conds]


def test_hr_report_terminal_gains_done_outcond(dt):
    end = job_of(dt, "HR_PAY/__FOLDER_END__")
    assert [c.name for c in end.in_conds] == ["__done__HR_PAY/HR_REPORT"]
    assert [(c.name, c.sign) for c in end.out_conds] == [("HR-PAY-DONE", "ADD")]
    report = job_of(dt, "HR_PAY/HR_REPORT")
    assert "__done__HR_PAY/HR_REPORT" in [c.name for c in report.out_conds]
    # non-terminals untouched
    for uid in ["HR_PAY/HR_CALC", "HR_PAY/HR_PAY_RUN"]:
        j = job_of(dt, uid)
        assert not any(c.name.startswith("__done__") for c in j.out_conds), uid


# ------------------------------------------------------------------ mfg (nested folders, v2)

def test_mfg_synthetics_only_on_gated_folders(mfg):
    by_folder = {f.name: sorted(j.name for j in f.jobs if j.synthetic)
                 for f in mfg.folders}
    assert by_folder == {
        "MFG_IN": [],
        "MFG_NIGHT": ["__FOLDER_END__", "__FOLDER_START__"],
        "MFG_NIGHT/PRESS_SHOP": ["__FOLDER_END__", "__FOLDER_START__"],
        "MFG_NIGHT/PRESS_SHOP/QA": [],   # no folder conds -> no synthetics
    }
    assert sum(1 for _ in mfg.all_jobs()) == 9   # 5 real + 4 synthetic


def test_mfg_night_start_and_entry(mfg):
    start = job_of(mfg, "MFG_NIGHT/__FOLDER_START__")
    assert [(c.name, c.odate) for c in start.in_conds] == [("MFG-PLANT-READY", "ODAT")]
    assert [c.name for c in start.out_conds] == ["__start__MFG_NIGHT"]
    assert start.weekdays == "ALL"       # folder schedule attrs land on the start
    extract = job_of(mfg, "MFG_NIGHT/MFG_EXTRACT")
    assert "__start__MFG_NIGHT" in [c.name for c in extract.in_conds]


def test_press_shop_start_gains_parent_start(mfg):
    start = job_of(mfg, "MFG_NIGHT/PRESS_SHOP/__FOLDER_START__")
    names = [c.name for c in start.in_conds]
    assert "MFG-EXTRACTED" in names                  # its own folder gate
    assert "__start__MFG_NIGHT" in names             # cascaded from the parent
    assert [c.name for c in start.out_conds] == ["__start__MFG_NIGHT/PRESS_SHOP"]
    # entry job of PRESS_SHOP hangs under its OWN start only
    load = job_of(mfg, "MFG_NIGHT/PRESS_SHOP/PRESS_LOAD")
    load_in = [c.name for c in load.in_conds]
    assert "__start__MFG_NIGHT/PRESS_SHOP" in load_in
    assert "__start__MFG_NIGHT" not in load_in


def test_qa_check_is_not_a_root(mfg):
    # QA has no folder conds and no start node: its entry jobs gain the
    # nearest ancestor's __start__ in-cond instead (deep false-root test).
    check = job_of(mfg, "MFG_NIGHT/PRESS_SHOP/QA/QA_CHECK")
    names = [c.name for c in check.in_conds]
    assert "__start__MFG_NIGHT/PRESS_SHOP" in names
    assert "__start__MFG_NIGHT" not in names         # nearest ancestor only
    # and QA's terminal feeds the nearest ancestor END node
    assert "__done__MFG_NIGHT/PRESS_SHOP/QA/QA_CHECK" in [
        c.name for c in check.out_conds]


def test_mfg_end_nodes_mirror_the_cascade(mfg):
    press_end = job_of(mfg, "MFG_NIGHT/PRESS_SHOP/__FOLDER_END__")
    in_names = sorted(c.name for c in press_end.in_conds)
    assert in_names == [
        "__done__MFG_NIGHT/PRESS_SHOP/PRESS_REPORT",     # own terminal
        "__done__MFG_NIGHT/PRESS_SHOP/QA/QA_CHECK",      # child QA terminal
    ]
    # child end feeds the parent end
    assert "__done__MFG_NIGHT/PRESS_SHOP/__FOLDER_END__" in [
        c.name for c in press_end.out_conds]

    night_end = job_of(mfg, "MFG_NIGHT/__FOLDER_END__")
    assert sorted(c.name for c in night_end.in_conds) == [
        "__done__MFG_NIGHT/MFG_EXTRACT",
        "__done__MFG_NIGHT/PRESS_SHOP/__FOLDER_END__",
    ]
    assert [(c.name, c.sign) for c in night_end.out_conds] == [
        ("MFG-NIGHT-DONE", "ADD")]


def test_mfg_desugar_idempotent(mfg):
    again = parse_files([MFG])
    desugar(again, PartitionConfig())
    desugar(again, PartitionConfig())
    assert again.model_dump_json() == mfg.model_dump_json()
    assert sum(1 for _ in again.all_jobs()) == 9


def test_nested_variables_cascade_nearest_wins():
    top = FolderDef(name="TOP", variables={"A": "top", "B": "top", "C": "top"},
                    jobs=[Job(name="T1", folder="TOP")])
    mid = FolderDef(name="TOP/MID", parent="TOP", variables={"B": "mid"},
                    jobs=[Job(name="M1", folder="TOP/MID")])
    leaf = FolderDef(name="TOP/MID/LEAF", parent="TOP/MID",
                     jobs=[Job(name="L1", folder="TOP/MID/LEAF",
                               variables={"C": "job"})])
    dt = Deftable(folders=[top, mid, leaf])
    desugar(dt, PartitionConfig())
    idx = dt.job_index()
    assert idx["TOP/T1"].variables == {"A": "top", "B": "top", "C": "top"}
    assert idx["TOP/MID/M1"].variables == {"A": "top", "B": "mid", "C": "top"}
    assert idx["TOP/MID/LEAF/L1"].variables == {"A": "top", "B": "mid", "C": "job"}


def test_cascade_skips_ungated_middle_folder():
    """Ancestor lookup is TRANSITIVE: gated grandparent, ungated middle."""
    gp = FolderDef(name="GP", smart=True,
                   in_conds=[Condition(name="GP-GATE")],
                   jobs=[Job(name="G1", folder="GP")])
    mid = FolderDef(name="GP/MID", parent="GP",
                    jobs=[Job(name="M1", folder="GP/MID")])
    leaf = FolderDef(name="GP/MID/LEAF", parent="GP/MID",
                     jobs=[Job(name="L1", folder="GP/MID/LEAF")])
    dt = Deftable(folders=[gp, mid, leaf])
    desugar(dt, PartitionConfig())
    idx = dt.job_index()
    # neither MID nor LEAF has a start node; both cascade to GP's __start__
    assert "__start__GP" in [c.name for c in idx["GP/MID/M1"].in_conds]
    assert "__start__GP" in [c.name for c in idx["GP/MID/LEAF/L1"].in_conds]
    # end mirror: both terminals feed GP's end directly
    end = idx["GP/__FOLDER_END__"]
    assert sorted(c.name for c in end.in_conds) == [
        "__done__GP/G1", "__done__GP/MID/LEAF/L1", "__done__GP/MID/M1"]


# ------------------------------------------------------------------ variables

def test_folder_variables_merge_job_wins(dt):
    gl = job_of(dt, "FIN_EOD/FIN_LOAD_GL")
    assert gl.variables["ENV"] == "gl_override"     # job value wins
    post = job_of(dt, "FIN_EOD/FIN_POST_GL")
    assert post.variables["ENV"] == "prod"          # folder value cascades


# ------------------------------------------------------------------ inline fixtures

def _fixture_deftable():
    """Smart folder whose entry job's in-cond is produced in ANOTHER folder:
    entry detection must be intra-folder only."""
    producer = Job(name="P1", folder="OTHER",
                   out_conds=[Condition(name="FROM-OTHER")])
    j1 = Job(name="J1", folder="GATED",
             in_conds=[Condition(name="FROM-OTHER")],
             out_conds=[Condition(name="MID")])
    j2 = Job(name="J2", folder="GATED", in_conds=[Condition(name="MID")])
    gated = FolderDef(name="GATED", smart=True, weekdays="6,7",
                      in_conds=[Condition(name="GATE-OPEN")],
                      out_conds=[Condition(name="GATED-DONE")])
    gated.jobs = [j1, j2]
    other = FolderDef(name="OTHER", jobs=[producer])
    return Deftable(folders=[gated, other])


def test_entry_detection_is_intra_folder():
    dt = _fixture_deftable()
    desugar(dt, PartitionConfig())
    j1 = dt.job_index()["GATED/J1"]
    # FROM-OTHER is produced outside GATED, so J1 is still an entry job
    assert "__start__GATED" in [c.name for c in j1.in_conds]
    j2 = dt.job_index()["GATED/J2"]
    assert "__start__GATED" not in [c.name for c in j2.in_conds]
    # J2 is the terminal; J1 is not
    assert "__done__GATED/J2" in [c.name for c in j2.out_conds]
    assert not any(c.name.startswith("__done__") for c in j1.out_conds)
    # producer folder has no folder conds -> untouched
    other = [f for f in dt.folders if f.name == "OTHER"][0]
    assert not any(j.synthetic for j in other.jobs)


def test_del_outconds_do_not_make_jobs_non_terminal():
    j1 = Job(name="J1", folder="F",
             out_conds=[Condition(name="LOCK", sign="DEL")])
    j2 = Job(name="J2", folder="F", in_conds=[Condition(name="LOCK")])
    f = FolderDef(name="F", smart=True, in_conds=[Condition(name="GATE")])
    f.jobs = [j1, j2]
    dt = Deftable(folders=[f])
    desugar(dt, PartitionConfig())
    # J1's only out-cond is DEL-signed: it produces nothing -> terminal
    j1 = dt.job_index()["F/J1"]
    assert "__done__F/J1" in [c.name for c in j1.out_conds]
    # J2 consumes LOCK which is never ADD-produced intra-folder -> entry too
    j2 = dt.job_index()["F/J2"]
    assert "__start__F" in [c.name for c in j2.in_conds]


def test_folder_start_always_flag():
    def make():
        smart = FolderDef(name="SMART_NOCONDS", smart=True,
                          jobs=[Job(name="A", folder="SMART_NOCONDS")])
        plain = FolderDef(name="PLAIN_NOCONDS", smart=False,
                          jobs=[Job(name="B", folder="PLAIN_NOCONDS")])
        return Deftable(folders=[smart, plain])

    off = make()
    desugar(off, PartitionConfig())
    assert not any(j.synthetic for j in off.all_jobs())

    on = make()
    desugar(on, PartitionConfig(folder_start_always=True))
    smart_names = {j.name for j in on.folders[0].jobs}
    assert {"__FOLDER_START__", "__FOLDER_END__"} <= smart_names
    # plain folder is never forced
    assert not any(j.synthetic for j in on.folders[1].jobs)


def test_desugar_is_idempotent_and_deterministic():
    a = parse_files(SAMPLES)
    desugar(a, PartitionConfig())
    desugar(a, PartitionConfig())          # second pass must not duplicate
    b = desugared()
    assert a.model_dump_json() == b.model_dump_json()
    assert sum(1 for _ in a.all_jobs()) == 33
