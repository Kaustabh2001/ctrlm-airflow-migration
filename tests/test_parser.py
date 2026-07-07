"""Tests for ctrlm_core.parser against the contract sample XMLs + inline fixtures."""
from pathlib import Path

import pytest

from ctrlm_core.parser import parse_files

ROOT = Path(__file__).resolve().parents[1]
EXPORTS = ROOT / "examples" / "exports"

FINANCE = EXPORTS / "sample_finance.xml"
OPS = EXPORTS / "sample_ops.xml"
HR = EXPORTS / "sample_hr.xml"
MFG = EXPORTS / "sample_mfg.xml"
BANK = EXPORTS / "sample_bank.xml"


def jobs_of(deftable, folder_name):
    (folder,) = [f for f in deftable.folders if f.name == folder_name]
    return {j.name: j for j in folder.jobs}


def folder_of(deftable, folder_name):
    (folder,) = [f for f in deftable.folders if f.name == folder_name]
    return folder


@pytest.fixture(scope="module")
def finance():
    return parse_files([FINANCE])


@pytest.fixture(scope="module")
def ops():
    return parse_files([OPS])


@pytest.fixture(scope="module")
def hr():
    return parse_files([HR])


@pytest.fixture(scope="module")
def mfg():
    return parse_files([MFG])


@pytest.fixture(scope="module")
def bank():
    return parse_files([BANK])


# ------------------------------------------------------------------ counts

def test_job_counts_per_file(finance, ops, hr):
    assert sum(1 for _ in finance.all_jobs()) == 10
    assert sum(1 for _ in ops.all_jobs()) == 14
    assert sum(1 for _ in hr.all_jobs()) == 5


def test_combined_parse_uids_unique():
    dt = parse_files([FINANCE, OPS, HR])
    uids = [j.uid for j in dt.all_jobs()]
    assert len(uids) == 29
    assert len(set(uids)) == 29
    assert dt.source_files == [str(FINANCE), str(OPS), str(HR)]


def test_folder_inventory(finance, ops, hr):
    assert [f.name for f in finance.folders] == ["FIN_DW", "FIN_EOD", "RISK", "RPT"]
    assert [f.name for f in ops.folders] == ["OPS", "STG"]
    assert [f.name for f in hr.folders] == ["HR_IN", "HR_PAY"]
    assert [f.smart for f in finance.folders] == [False, True, False, False]
    assert folder_of(hr, "HR_PAY").smart is True
    assert folder_of(hr, "HR_IN").smart is False


# ------------------------------------------------------------------ finance

def test_fin_dw_loaders(finance):
    jobs = jobs_of(finance, "FIN_DW")
    for name, cond in [
        ("DW_LOAD_CUSTOMERS", "DW-CUSTOMERS-LOADED"),
        ("DW_LOAD_ORDERS", "DW-ORDERS-LOADED"),
        ("DW_LOAD_PRODUCTS", "DW-PRODUCTS-LOADED"),
    ]:
        j = jobs[name]
        assert j.weekdays == "1,2,3,4,5"
        assert j.timefrom == "2100"
        assert j.in_conds == []
        assert [c.name for c in j.out_conds] == [cond]
        assert j.out_conds[0].sign == "ADD"
        assert j.node_id == "dwagent01"
        assert j.maxrerun == 2
        assert j.rerun_interval_minutes == 10


def test_dw_build_mart_and_publish(finance):
    jobs = jobs_of(finance, "FIN_DW")
    mart = jobs["DW_BUILD_MART"]
    assert mart.weekdays == "" and mart.monthdays == "" and mart.months == ""
    assert sorted(c.name for c in mart.in_conds) == [
        "DW-CUSTOMERS-LOADED", "DW-ORDERS-LOADED", "DW-PRODUCTS-LOADED"]
    assert all(c.and_or == "AND" and c.odate == "ODAT" for c in mart.in_conds)
    assert [c.name for c in mart.out_conds] == ["DW-MART-OK"]
    publish = jobs["DW_PUBLISH"]
    assert publish.weekdays == ""
    assert [c.name for c in publish.in_conds] == ["DW-MART-OK"]
    assert publish.out_conds == []


def test_fin_eod_smart_folder(finance):
    f = folder_of(finance, "FIN_EOD")
    assert f.smart is True
    assert f.weekdays == "1,2,3,4,5"
    assert [c.name for c in f.in_conds] == ["BATCH-OPEN"]
    assert [c.name for c in f.out_conds] == ["FIN-EOD-DONE"]
    assert f.out_conds[0].sign == "ADD"
    assert f.variables == {"ENV": "prod"}

    jobs = {j.name: j for j in f.jobs}
    gl = jobs["FIN_LOAD_GL"]
    assert gl.timefrom == "2200"
    assert gl.in_conds == []               # false-root before desugar
    assert gl.weekdays == ""               # inherits from folder later, not at parse
    assert [c.name for c in gl.out_conds] == ["FIN-GL-LOADED"]
    assert gl.variables == {"ENV": "gl_override"}

    post = jobs["FIN_POST_GL"]
    assert [c.name for c in post.in_conds] == ["FIN-GL-LOADED"]
    assert [c.name for c in post.out_conds] == ["FIN-POSTED"]

    extract = jobs["FIN_EXTRACT"]
    assert extract.timefrom == "0200"
    assert [c.name for c in extract.in_conds] == ["FIN-POSTED"]
    assert [c.name for c in extract.out_conds] == ["FIN-DONE"]


def test_risk_and_rpt(finance):
    risk = jobs_of(finance, "RISK")["RISK_CALC"]
    assert risk.weekdays == "1,2,3,4,5"
    assert risk.timefrom == "0300"
    assert risk.maxwait == 1
    assert [c.name for c in risk.in_conds] == ["FIN-DONE"]

    rpt = jobs_of(finance, "RPT")["RPT_WEEKLY_PACK"]
    assert rpt.weekdays == "1"             # Monday-only -> transitive conflict
    assert [c.name for c in rpt.in_conds] == ["FIN-DONE"]


# ------------------------------------------------------------------ ops

def test_ops_open_batch_and_apps(ops):
    jobs = jobs_of(ops, "OPS")
    assert len(jobs) == 12
    ob = jobs["OPS_OPEN_BATCH"]
    assert ob.weekdays == "ALL"
    assert ob.timefrom == "2000"
    assert [c.name for c in ob.out_conds] == ["BATCH-OPEN"]

    app_names = [f"OPS_APP{i:02d}" for i in range(1, 11)]
    for name in app_names:
        j = jobs[name]
        assert j.weekdays == "1,2,3,4,5"
        assert j.timefrom == "2100"
        assert [c.name for c in j.in_conds] == ["BATCH-OPEN"]
        assert j.out_conds == []


def test_ops_fs_poll_cyclic(ops):
    j = jobs_of(ops, "OPS")["OPS_FS_POLL"]
    assert j.cyclic is True
    assert j.interval_minutes == 15
    assert j.timefrom == "0600"
    assert j.timeto == "2000"
    assert [c.name for c in j.out_conds] == ["FILE-ARRIVED"]


def test_stg_chain(ops):
    jobs = jobs_of(ops, "STG")
    ingest = jobs["STG_INGEST"]
    assert ingest.weekdays == "" and ingest.monthdays == "" and ingest.months == ""
    assert [c.name for c in ingest.in_conds] == ["FILE-ARRIVED"]
    assert [c.name for c in ingest.out_conds] == ["STG-LOADED"]
    quality = jobs["STG_QUALITY"]
    assert [c.name for c in quality.in_conds] == ["STG-LOADED"]
    assert quality.out_conds == []


# ------------------------------------------------------------------ hr

def test_hr_fw_filewatch_memlib_join(hr):
    j = jobs_of(hr, "HR_IN")["HR_FW"]
    assert j.task_type == "FileWatch"
    assert j.weekdays == "ALL"
    assert j.timefrom == "0700"
    assert j.command == "/data/hr/incoming/payroll_input.csv"   # MEMLIB/MEMNAME join
    assert [c.name for c in j.out_conds] == ["HR-FILES-READY"]


def test_hr_ext_feed_check_orphan_and_del_sign(hr):
    j = jobs_of(hr, "HR_IN")["HR_EXT_FEED_CHECK"]
    assert [c.name for c in j.in_conds] == ["EXT-FEED-OK"]      # orphan: never produced
    assert [c.name for c in j.out_conds] == ["HR-STALE-LOCK"]
    assert j.out_conds[0].sign == "DEL"


def test_hr_pay_folder_and_prev_qualifier(hr):
    f = folder_of(hr, "HR_PAY")
    assert f.smart is True
    assert f.monthdays == "1,15"           # attr DAYS -> monthdays
    assert f.weekdays == ""
    assert [c.name for c in f.in_conds] == ["HR-FILES-READY"]
    assert [c.name for c in f.out_conds] == ["HR-PAY-DONE"]

    jobs = {j.name: j for j in f.jobs}
    calc = jobs["HR_CALC"]
    assert calc.weekdays == "" and calc.monthdays == ""
    assert [(c.name, c.odate) for c in calc.in_conds] == [("HR-PAY-DONE", "PREV")]
    assert [c.name for c in calc.out_conds] == ["HR-CALC-OK"]


def test_hr_pay_run_confirm_resource_on(hr):
    j = jobs_of(hr, "HR_PAY")["HR_PAY_RUN"]
    assert j.confirm is True
    assert [c.name for c in j.in_conds] == ["HR-CALC-OK"]
    assert [c.name for c in j.out_conds] == ["HR-PAY-OK"]
    assert len(j.resources) == 1
    res = j.resources[0]
    assert res.name == "DB_SLOTS" and res.kind == "quantitative" and res.quant == 2
    assert len(j.on_actions) == 1
    on = j.on_actions[0]
    assert on.stmt == "*" and on.code == "NOTOK"
    assert on.actions[0]["type"] == "DOMAIL"
    assert on.actions[0]["DEST"] == "hr-ops@example.com"


def test_hr_report_shout(hr):
    j = jobs_of(hr, "HR_PAY")["HR_REPORT"]
    assert [c.name for c in j.in_conds] == ["HR-PAY-OK"]
    assert j.shouts == [
        {"when": "NOTOK", "dest": "EM", "message": "HR payroll report failed"}]


# ------------------------------------------------------------------ mfg (nested folders, v2)

def test_mfg_depth3_flattening_names_and_parents(mfg):
    assert [f.name for f in mfg.folders] == [
        "MFG_IN",
        "MFG_NIGHT",
        "MFG_NIGHT/PRESS_SHOP",
        "MFG_NIGHT/PRESS_SHOP/QA",
    ]
    assert [f.parent for f in mfg.folders] == [
        "", "", "MFG_NIGHT", "MFG_NIGHT/PRESS_SHOP",
    ]
    # SUB_FOLDER tags do not say smart; SMART_FOLDER does
    assert [f.smart for f in mfg.folders] == [False, True, False, False]
    # DATACENTER inherited down the chain
    assert [f.datacenter for f in mfg.folders] == ["CTM_PROD"] * 4


def test_mfg_jobs_carry_full_folder_paths(mfg):
    uids = sorted(j.uid for j in mfg.all_jobs())
    assert uids == [
        "MFG_IN/MFG_SENSORS",
        "MFG_NIGHT/MFG_EXTRACT",
        "MFG_NIGHT/PRESS_SHOP/PRESS_LOAD",
        "MFG_NIGHT/PRESS_SHOP/PRESS_REPORT",
        "MFG_NIGHT/PRESS_SHOP/QA/QA_CHECK",
    ]
    assert len(set(uids)) == 5
    by_folder = {f.name: [j.name for j in f.jobs] for f in mfg.folders}
    # each job belongs to its IMMEDIATE folder only (flattened, not duplicated)
    assert by_folder == {
        "MFG_IN": ["MFG_SENSORS"],
        "MFG_NIGHT": ["MFG_EXTRACT"],
        "MFG_NIGHT/PRESS_SHOP": ["PRESS_LOAD", "PRESS_REPORT"],
        "MFG_NIGHT/PRESS_SHOP/QA": ["QA_CHECK"],
    }


def test_mfg_sensors_filewatch(mfg):
    j = jobs_of(mfg, "MFG_IN")["MFG_SENSORS"]
    assert j.task_type == "FileWatch"
    assert j.weekdays == "ALL"
    assert j.timefrom == "0500"
    assert [(c.name, c.sign) for c in j.out_conds] == [("MFG-PLANT-READY", "ADD")]


def test_mfg_night_folder_level_conds_and_schedule(mfg):
    night = folder_of(mfg, "MFG_NIGHT")
    assert night.smart is True
    assert night.weekdays == "ALL"
    assert [c.name for c in night.in_conds] == ["MFG-PLANT-READY"]
    assert [c.name for c in night.out_conds] == ["MFG-NIGHT-DONE"]
    extract = jobs_of(mfg, "MFG_NIGHT")["MFG_EXTRACT"]
    assert extract.node_id == "prdnode2"
    assert extract.timefrom == "2100"
    assert extract.command == "/opt/mfg/extract.sh %%ODATE"
    assert [c.name for c in extract.out_conds] == ["MFG-EXTRACTED"]


def test_mfg_press_shop_subfolder(mfg):
    press = folder_of(mfg, "MFG_NIGHT/PRESS_SHOP")
    # folder-level INCOND recognized before the first JOB/sub-folder child
    assert [c.name for c in press.in_conds] == ["MFG-EXTRACTED"]
    assert press.out_conds == []
    assert press.weekdays == "" and press.monthdays == "" and press.months == ""

    jobs = {j.name: j for j in press.jobs}
    load = jobs["PRESS_LOAD"]
    assert load.node_id == "winnode1"
    assert load.command == "powershell -File C:\\jobs\\press_load.ps1 %%ODATE"
    assert [c.name for c in load.out_conds] == ["PRESS-LOADED"]

    report = jobs["PRESS_REPORT"]
    assert report.node_id == "winnode1"
    assert report.command == "C:\\jobs/press_report.ps1"   # MEMLIB/MEMNAME join
    assert [c.name for c in report.in_conds] == ["PRESS-LOADED"]
    assert report.out_conds == []


def test_mfg_qa_deep_subfolder_no_conds_no_schedule(mfg):
    qa = folder_of(mfg, "MFG_NIGHT/PRESS_SHOP/QA")
    assert qa.in_conds == [] and qa.out_conds == []
    (check,) = qa.jobs
    assert check.name == "QA_CHECK"
    assert check.folder == "MFG_NIGHT/PRESS_SHOP/QA"
    assert check.in_conds == [] and check.out_conds == []
    assert check.weekdays == "" and check.monthdays == "" and check.months == ""
    assert check.timefrom == "" and check.timeto == ""
    assert check.node_id == "winnode1"


def test_existing_samples_are_all_top_level(finance, ops, hr):
    for dt in (finance, ops, hr):
        assert all(f.parent == "" for f in dt.folders)
        assert all("/" not in f.name for f in dt.folders)


def test_mfg_parse_is_deterministic():
    assert parse_files([MFG]).model_dump_json() == parse_files([MFG]).model_dump_json()


# ------------------------------------------------------------------ bank (appl_type/priority/critical, v3)

def test_bank_folder_and_job_count(bank):
    assert [f.name for f in bank.folders] == ["BANK_EOD"]
    folder = folder_of(bank, "BANK_EOD")
    assert folder.smart is False
    assert folder.datacenter == "CTM_PROD"
    assert [j.name for j in folder.jobs] == [
        "BANK_BAL_CHECK", "BANK_SETTLE", "BANK_RECON",
        "BANK_FT_STATEMENTS", "BANK_MAINFRAME_SYNC",
    ]
    assert sum(1 for _ in bank.all_jobs()) == 5


def test_bank_bal_check_database_job(bank):
    j = jobs_of(bank, "BANK_EOD")["BANK_BAL_CHECK"]
    assert j.appl_type == "DATABASE"
    assert j.task_type == "Command"
    assert j.node_id == "dbnode1"
    assert j.command == "SELECT COUNT(*) FROM balances WHERE ds='%%ODATE'"
    assert j.weekdays == "1,2,3,4,5"
    assert j.timefrom == "1800"
    assert j.in_conds == []
    assert [(c.name, c.sign) for c in j.out_conds] == [("BANK-BAL-OK", "ADD")]
    # defaults where the sample sets nothing
    assert j.priority == "" and j.critical is False


def test_bank_settle_priority_critical_quant_on_shout(bank):
    j = jobs_of(bank, "BANK_EOD")["BANK_SETTLE"]
    assert j.appl_type == ""               # plain OS command job
    assert j.priority == "AA"
    assert j.critical is True
    assert j.timefrom == "1900" and j.timeto == "2300"
    assert [c.name for c in j.in_conds] == ["BANK-BAL-OK"]
    assert [c.name for c in j.out_conds] == ["BANK-SETTLED"]
    (res,) = j.resources
    assert res.name == "SETTLE_SLOTS" and res.kind == "quantitative" and res.quant == 3
    (on,) = j.on_actions
    assert on.stmt == "*" and on.code == "NOTOK"
    assert [a["type"] for a in on.actions] == ["DOMAIL", "DOFORCEJOB"]
    assert on.actions[0]["DEST"] == "ops@corp.com"
    assert on.actions[1]["JOBNAME"] == "BANK_RECON"
    assert j.shouts == [{
        "when": "LATE", "dest": "OPS",
        "message": "BANK_SETTLE running past its window"}]


def test_bank_recon_condition_driven_only(bank):
    j = jobs_of(bank, "BANK_EOD")["BANK_RECON"]
    assert j.weekdays == "" and j.monthdays == "" and j.months == ""
    assert j.timefrom == "" and j.timeto == ""
    assert [(c.name, c.and_or) for c in j.in_conds] == [("BANK-SETTLED", "AND")]
    assert j.out_conds == []
    assert j.priority == "" and j.critical is False


def test_bank_manual_showcases_appl_type_verbatim(bank):
    jobs = jobs_of(bank, "BANK_EOD")
    ft = jobs["BANK_FT_STATEMENTS"]
    assert ft.appl_type == "FILE_TRANS"
    assert ft.task_type == "Job"
    assert [c.name for c in ft.in_conds] == ["BANK-SETTLED"]
    sap = jobs["BANK_MAINFRAME_SYNC"]
    assert sap.appl_type == "SAP"
    assert sap.task_type == "Job"
    assert [c.name for c in sap.in_conds] == ["BANK-SETTLED"]


def test_other_samples_unchanged_by_v3_fields(finance, ops, hr, mfg):
    for dt in (finance, ops, hr, mfg):
        for j in dt.all_jobs():
            assert j.appl_type == ""
            assert j.priority == ""
            assert j.critical is False


def test_bank_parse_is_deterministic():
    assert parse_files([BANK]).model_dump_json() == parse_files([BANK]).model_dump_json()


# ------------------------------------------------------------------ dialect tolerance (inline fixtures)

def _parse_str(tmp_path, text, name="fixture.xml"):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return parse_files([p])


def test_sign_and_qualifier_normalization(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <FOLDER FOLDER_NAME="F1">
    <JOB JOBNAME="J1">
      <INCOND NAME="C-IN" ODATE=" prev " AND_OR="O"/>
      <OUTCOND NAME="C-PLUS" SIGN="+"/>
      <OUTCOND NAME="C-MINUS" ODATE="ODAT" SIGN="-"/>
    </JOB>
  </FOLDER>
</DEFTABLE>""")
    (j,) = list(dt.all_jobs())
    assert j.in_conds[0].odate == "PREV"
    assert j.in_conds[0].and_or == "OR"
    assert j.out_conds[0].sign == "ADD"    # "+" normalized
    assert j.out_conds[0].odate == "ODAT"  # missing ODATE -> default
    assert j.out_conds[1].sign == "DEL"    # "-" normalized


def test_unknown_attrs_and_elements_tolerated(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <SOMETHING_NEW/>
  <FOLDER FOLDER_NAME="F1" FUTURE_FOLDER_ATTR="x">
    <WEIRD_FOLDER_CHILD/>
    <JOB JOBNAME="J1" CREATED_BY="alice" VERSION_SERIAL="7">
      <FUTURE_CHILD SOME="thing"/>
      <OUTCOND NAME="OK"/>
    </JOB>
  </FOLDER>
</DEFTABLE>""")
    (j,) = list(dt.all_jobs())              # job survives, nothing lost
    assert j.name == "J1"
    assert [c.name for c in j.out_conds] == ["OK"]


def test_legacy_table_dialect(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <TABLE TABLE_NAME="LEGACY" DATACENTER="DC8">
    <JOB JOBNAME="L1" MEMNAME="run.sh" MEMLIB="/opt/legacy"/>
  </TABLE>
  <SMART_TABLE TABLE_NAME="LEGACY_SMART">
    <INCOND NAME="GATE"/>
    <JOB JOBNAME="L2" CMDLINE="echo hi"/>
  </SMART_TABLE>
</DEFTABLE>""")
    assert [f.name for f in dt.folders] == ["LEGACY", "LEGACY_SMART"]
    assert dt.folders[0].smart is False and dt.folders[0].datacenter == "DC8"
    assert dt.folders[1].smart is True
    assert [c.name for c in dt.folders[1].in_conds] == ["GATE"]
    (l1,) = dt.folders[0].jobs
    assert l1.command == "/opt/legacy/run.sh"


def test_interval_and_flag_parsing(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <FOLDER FOLDER_NAME="F1">
    <JOB JOBNAME="J1" CYCLIC="1" INTERVAL="2H" MAXWAIT="3" CONFIRM="0"/>
    <JOB JOBNAME="J2" INTERVAL="45" RERUNINTERVAL="1H"/>
  </FOLDER>
</DEFTABLE>""")
    j1, j2 = list(dt.all_jobs())
    assert j1.cyclic is True and j1.interval_minutes == 120 and j1.maxwait == 3
    assert j1.confirm is False
    assert j2.cyclic is False
    assert j2.interval_minutes == 45       # bare number = minutes
    assert j2.rerun_interval_minutes == 60


def test_folder_level_conds_only_before_first_job(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <SMART_FOLDER FOLDER_NAME="F1">
    <INCOND NAME="BEFORE"/>
    <JOB JOBNAME="J1"/>
    <INCOND NAME="AFTER"/>
  </SMART_FOLDER>
</DEFTABLE>""")
    (f,) = dt.folders
    assert [c.name for c in f.in_conds] == ["BEFORE"]   # trailing one ignored
    assert len(f.jobs) == 1


def test_nested_folder_family_tags_and_smartness(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <FOLDER FOLDER_NAME="TOP" DATACENTER="DC1">
    <JOB JOBNAME="T1"/>
    <SMART_FOLDER FOLDER_NAME="INNER_SMART" WEEKDAYS="1,2">
      <INCOND NAME="GATE"/>
      <JOB JOBNAME="S1"/>
      <SUB_FOLDER FOLDER_NAME="LEAF" DATACENTER="DC2">
        <JOB JOBNAME="L1"/>
      </SUB_FOLDER>
    </SMART_FOLDER>
  </FOLDER>
</DEFTABLE>""")
    assert [f.name for f in dt.folders] == [
        "TOP", "TOP/INNER_SMART", "TOP/INNER_SMART/LEAF"]
    assert [f.parent for f in dt.folders] == ["", "TOP", "TOP/INNER_SMART"]
    # nested folders are smart iff their TAG says so; own sched attrs kept
    assert [f.smart for f in dt.folders] == [False, True, False]
    assert dt.folders[1].weekdays == "1,2"
    assert [c.name for c in dt.folders[1].in_conds] == ["GATE"]
    # datacenter: inherited unless overridden
    assert [f.datacenter for f in dt.folders] == ["DC1", "DC1", "DC2"]
    assert sorted(j.uid for j in dt.all_jobs()) == [
        "TOP/INNER_SMART/LEAF/L1", "TOP/INNER_SMART/S1", "TOP/T1"]


def test_folder_level_conds_stop_at_first_subfolder(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <SMART_FOLDER FOLDER_NAME="F1">
    <INCOND NAME="BEFORE"/>
    <SUB_FOLDER FOLDER_NAME="SUB">
      <JOB JOBNAME="J1"/>
    </SUB_FOLDER>
    <INCOND NAME="AFTER"/>
    <JOB JOBNAME="J2"/>
  </SMART_FOLDER>
</DEFTABLE>""")
    f1 = [f for f in dt.folders if f.name == "F1"][0]
    assert [c.name for c in f1.in_conds] == ["BEFORE"]   # trailing one ignored
    assert [j.name for j in f1.jobs] == ["J2"]           # J1 belongs to the sub-folder
    sub = [f for f in dt.folders if f.name == "F1/SUB"][0]
    assert [j.name for j in sub.jobs] == ["J1"]


def test_appl_type_priority_critical_attrs(tmp_path):
    dt = _parse_str(tmp_path, """<?xml version="1.0"?>
<DEFTABLE>
  <FOLDER FOLDER_NAME="F1">
    <JOB JOBNAME="J1" APPL_TYPE=" INFORMATICA_CLOUD " PRIORITY=" 1A " CRITICAL="1"/>
    <JOB JOBNAME="J2" CRITICAL="0"/>
    <JOB JOBNAME="J3" APPL_TYPE="database"/>
  </FOLDER>
</DEFTABLE>""")
    j1, j2, j3 = list(dt.all_jobs())
    # unknown appl_types pass through verbatim (whitespace-trimmed only)
    assert j1.appl_type == "INFORMATICA_CLOUD"
    assert j1.priority == "1A"
    assert j1.critical is True
    assert j2.appl_type == "" and j2.priority == "" and j2.critical is False
    assert j3.appl_type == "database"      # no case-folding: verbatim


def test_parse_is_deterministic():
    a = parse_files([FINANCE, OPS, HR]).model_dump_json()
    b = parse_files([FINANCE, OPS, HR]).model_dump_json()
    assert a == b
