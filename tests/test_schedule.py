"""Tests for ctrlm_core.schedule — cascade, day_pattern, rel_minutes, cron."""
from __future__ import annotations

from pathlib import Path

from ctrlm_core import schedule
from ctrlm_core.desugar import desugar
from ctrlm_core.model import Deftable, FolderDef, Job, PartitionConfig
from ctrlm_core.parser import parse_files

MFG = Path(__file__).resolve().parents[1] / "examples" / "exports" / "sample_mfg.xml"


def make_job(name: str = "J1", folder: str = "F1", **kw) -> Job:
    return Job(name=name, folder=folder, **kw)


# ---------------------------------------------------------------- day_pattern_of

def test_day_pattern_none_when_unscheduled():
    assert schedule.day_pattern_of(make_job()) is None


def test_day_pattern_weekdays_sorted_and_deduped():
    job = make_job(weekdays="5,1,3,1")
    assert schedule.day_pattern_of(job) == "WD=1,3,5|MD=|M=|OP=OR"


def test_day_pattern_all_keyword_any_case():
    assert schedule.day_pattern_of(make_job(weekdays="ALL")) == "WD=ALL|MD=|M=|OP=OR"
    assert schedule.day_pattern_of(make_job(weekdays="all")) == "WD=ALL|MD=|M=|OP=OR"


def test_day_pattern_full_sets_normalize_to_all():
    assert (
        schedule.day_pattern_of(make_job(weekdays="1,2,3,4,5,6,7"))
        == "WD=ALL|MD=|M=|OP=OR"
    )
    monthdays = ",".join(str(d) for d in range(1, 32))
    assert (
        schedule.day_pattern_of(make_job(monthdays=monthdays))
        == "WD=|MD=ALL|M=|OP=OR"
    )
    months = ",".join(str(m) for m in range(1, 13))
    assert schedule.day_pattern_of(make_job(months=months)) == "WD=|MD=|M=ALL|OP=OR"


def test_day_pattern_whitespace_and_empty_tokens():
    job = make_job(weekdays=" 2 , 1 ,, ")
    assert schedule.day_pattern_of(job) == "WD=1,2|MD=|M=|OP=OR"
    # tokens that collapse to nothing -> no pattern at all
    assert schedule.day_pattern_of(make_job(weekdays=" , ")) is None


def test_day_pattern_and_operator():
    job = make_job(weekdays="1,5", monthdays="15,1", days_and_or="AND")
    assert schedule.day_pattern_of(job) == "WD=1,5|MD=1,15|M=|OP=AND"


def test_day_pattern_op_defaults_to_or():
    job = make_job(weekdays="1", days_and_or="")
    assert schedule.day_pattern_of(job) == "WD=1|MD=|M=|OP=OR"


# ---------------------------------------------------------------- cascade

def _cascade_deftable() -> Deftable:
    folder = FolderDef(
        name="FIN_EOD",
        smart=True,
        weekdays="1,2,3,4,5",
        timezone="Europe/London",
        jobs=[
            # own TIMEFROM counts as a scheduling attr -> does NOT inherit:
            # the job stays day-pattern-less (unscheduled middle, Phase 6)
            Job(name="FIN_LOAD_GL", folder="FIN_EOD", timefrom="2200"),
            # no scheduling attrs at all -> inherits the folder day attrs
            Job(name="FIN_POST_GL", folder="FIN_EOD"),
            # own weekdays -> keeps them
            Job(name="RPT_LIKE", folder="FIN_EOD", weekdays="1"),
            # own monthdays -> keeps them, no weekday inheritance
            Job(name="MD_JOB", folder="FIN_EOD", monthdays="1,15"),
        ],
    )
    plain = FolderDef(
        name="PLAIN",
        jobs=[Job(name="LONER", folder="PLAIN")],
    )
    return Deftable(folders=[folder, plain])


def test_cascade_fills_raw_fields_then_pattern():
    deftable = _cascade_deftable()
    schedule.normalize_jobs(deftable, PartitionConfig())
    jobs = {j.name: j for j in deftable.all_jobs()}

    inherited = jobs["FIN_POST_GL"]
    assert inherited.weekdays == "1,2,3,4,5"
    assert inherited.timezone == "Europe/London"
    assert inherited.day_pattern == "WD=1,2,3,4,5|MD=|M=|OP=OR"

    # own timefrom blocks the cascade: stays condition-driven (pattern None)
    gated = jobs["FIN_LOAD_GL"]
    assert gated.weekdays == ""
    assert gated.day_pattern is None
    assert gated.timefrom == "2200"  # untouched

    assert jobs["RPT_LIKE"].day_pattern == "WD=1|MD=|M=|OP=OR"
    assert jobs["MD_JOB"].weekdays == ""  # own attrs -> no inheritance
    assert jobs["MD_JOB"].day_pattern == "WD=|MD=1,15|M=|OP=OR"


def test_cascade_no_folder_attrs_leaves_pattern_none():
    deftable = _cascade_deftable()
    schedule.normalize_jobs(deftable, PartitionConfig())
    jobs = {j.name: j for j in deftable.all_jobs()}
    assert jobs["LONER"].weekdays == ""
    assert jobs["LONER"].day_pattern is None


def _nested_deftable() -> Deftable:
    """Depth-3 chain: day attrs only on the grandparent; MID2 has its own."""
    gp = FolderDef(
        name="GP", smart=True, weekdays="1,2,3,4,5", timezone="Europe/London",
        jobs=[Job(name="G1", folder="GP")],
    )
    mid = FolderDef(
        name="GP/MID", parent="GP",
        jobs=[Job(name="M1", folder="GP/MID")],
    )
    leaf = FolderDef(
        name="GP/MID/LEAF", parent="GP/MID",
        jobs=[
            Job(name="L1", folder="GP/MID/LEAF"),                      # inherits GP
            Job(name="L2", folder="GP/MID/LEAF", timefrom="0300"),     # own attr
            Job(name="L3", folder="GP/MID/LEAF", weekdays="6"),        # own days
        ],
    )
    mid2 = FolderDef(
        name="GP/MID2", parent="GP", monthdays="1,15",
        jobs=[Job(name="N1", folder="GP/MID2")],
    )
    return Deftable(folders=[gp, mid, leaf, mid2])


def test_cascade_walks_parent_chain():
    deftable = _nested_deftable()
    schedule.normalize_jobs(deftable, PartitionConfig())
    jobs = {j.name: j for j in deftable.all_jobs()}

    # depth-1 and depth-2/3 jobs all inherit the grandparent's day attrs
    for name in ("G1", "M1", "L1"):
        assert jobs[name].weekdays == "1,2,3,4,5", name
        assert jobs[name].timezone == "Europe/London", name
        assert jobs[name].day_pattern == "WD=1,2,3,4,5|MD=|M=|OP=OR", name

    # own time window blocks the cascade even deep in the tree
    assert jobs["L2"].weekdays == ""
    assert jobs["L2"].day_pattern is None

    # own day attrs win over any ancestor
    assert jobs["L3"].day_pattern == "WD=6|MD=|M=|OP=OR"

    # the NEAREST ancestor with day attrs wins (MID2 beats GP)
    assert jobs["N1"].monthdays == "1,15"
    assert jobs["N1"].weekdays == ""
    assert jobs["N1"].day_pattern == "WD=|MD=1,15|M=|OP=OR"


def test_mfg_day_attr_inheritance_walks_to_mfg_night():
    deftable = parse_files([MFG])
    desugar(deftable, PartitionConfig())
    schedule.normalize_jobs(deftable, PartitionConfig())
    idx = deftable.job_index()

    # jobs two levels below MFG_NIGHT inherit its WEEKDAYS="ALL"
    qa = idx["MFG_NIGHT/PRESS_SHOP/QA/QA_CHECK"]
    assert qa.weekdays == "ALL"
    assert qa.day_pattern == "WD=ALL|MD=|M=|OP=OR"
    load = idx["MFG_NIGHT/PRESS_SHOP/PRESS_LOAD"]
    assert load.day_pattern == "WD=ALL|MD=|M=|OP=OR"

    # the sub-folder's start node (no attrs of its own) inherits too —
    # "a sub-folder acting for its jobs"
    press_start = idx["MFG_NIGHT/PRESS_SHOP/__FOLDER_START__"]
    assert press_start.day_pattern == "WD=ALL|MD=|M=|OP=OR"

    # own TIMEFROM blocks inheritance: MFG_EXTRACT stays condition-driven
    extract = idx["MFG_NIGHT/MFG_EXTRACT"]
    assert extract.weekdays == ""
    assert extract.day_pattern is None
    assert extract.timefrom == "2100"


def test_normalize_is_idempotent():
    deftable = _cascade_deftable()
    schedule.normalize_jobs(deftable, PartitionConfig())
    first = deftable.model_dump_json()
    schedule.normalize_jobs(deftable, PartitionConfig())
    assert deftable.model_dump_json() == first


# ---------------------------------------------------------------- rel_minutes

def test_rel_minutes_wraparound_odate_clock():
    # with New Day 0600: 22:00 is 16h into the ODATE day, 02:00 is 20h in
    assert schedule.rel_minutes("2200", "0600") == 960
    assert schedule.rel_minutes("0200", "0600") == 1200
    assert schedule.rel_minutes("2200", "0600") < schedule.rel_minutes("0200", "0600")


def test_rel_minutes_zero_at_new_day():
    assert schedule.rel_minutes("0600", "0600") == 0
    assert schedule.rel_minutes("0559", "0600") == 1439


def test_rel_minutes_empty_counts_as_midnight():
    assert schedule.rel_minutes("", "0600") == (0 - 360) % 1440


# ---------------------------------------------------------------- cron_for

def test_cron_for_none_pattern():
    assert schedule.cron_for(None, "0600") is None


def test_cron_for_weekdays_only():
    cron = schedule.cron_for("WD=1,2,3,4,5|MD=|M=|OP=OR", "2230")
    assert cron == "30 22 * * 1,2,3,4,5"


def test_cron_for_full_weekday_set_collapses_to_star():
    assert schedule.cron_for("WD=ALL|MD=|M=|OP=OR", "0600") == "0 6 * * *"


def test_cron_for_monthdays_only():
    assert schedule.cron_for("WD=|MD=1,15|M=|OP=OR", "0600") == "0 6 1,15 * *"


def test_cron_for_both_with_or_uses_native_cron_or():
    cron = schedule.cron_for("WD=1|MD=1,15|M=|OP=OR", "0700")
    assert cron == "0 7 1,15 * 1"
    assert schedule.cron_and_approx("WD=1|MD=1,15|M=|OP=OR") is False


def test_cron_for_both_with_and_approximates_via_dom():
    pattern = "WD=1|MD=1,15|M=|OP=AND"
    cron = schedule.cron_for(pattern, "0700")
    assert cron == "0 7 1,15 * *"           # dom only — cron cannot AND
    assert schedule.cron_and_approx(pattern) is True


def test_cron_for_months_field():
    cron = schedule.cron_for("WD=1,2,3,4,5|MD=|M=1,6|OP=OR", "0930")
    assert cron == "30 9 * 1,6 1,2,3,4,5"


def test_cron_for_all_weekdays_or_monthdays_means_every_day():
    # WD=ALL OR MD=1,15 -> every day matches
    assert schedule.cron_for("WD=ALL|MD=1,15|M=|OP=OR", "0600") == "0 6 * * *"
    # WD=ALL AND MD=1,15 -> reduces to the monthday list
    assert schedule.cron_for("WD=ALL|MD=1,15|M=|OP=AND", "0600") == "0 6 1,15 * *"


# ---------------------------------------------------------------- cyclic_cron

def test_cyclic_cron_interval_window():
    job = make_job(cyclic=True, interval_minutes=15, timefrom="0600", timeto="2000")
    assert schedule.cyclic_cron(job) == "*/15 6-19 * * *"


def test_cyclic_cron_no_window():
    job = make_job(cyclic=True, interval_minutes=30)
    assert schedule.cyclic_cron(job) == "*/30 * * * *"


def test_cyclic_cron_hourly_uses_timefrom_minute():
    job = make_job(cyclic=True, interval_minutes=60, timefrom="0630")
    assert schedule.cyclic_cron(job) == "30 6-23 * * *"


def test_cyclic_cron_two_hourly_step():
    job = make_job(cyclic=True, interval_minutes=120, timefrom="0600", timeto="2000")
    assert schedule.cyclic_cron(job) == "0 6-19/2 * * *"


def test_cyclic_cron_zero_interval_defaults_hourly():
    job = make_job(cyclic=True, interval_minutes=0)
    assert schedule.cyclic_cron(job) == "0 * * * *"


def test_cyclic_cron_honors_day_pattern():
    job = make_job(
        cyclic=True, interval_minutes=15, timefrom="0600", timeto="2000",
        weekdays="1,2,3,4,5",
    )
    job.day_pattern = schedule.day_pattern_of(job)
    assert schedule.cyclic_cron(job) == "*/15 6-19 * * 1,2,3,4,5"
