"""Tests for plugins/ctm_plugins (contracts V3-2 / V4-1 revised).

Pure-logic modules (_odate, callbacks, timetables helpers, _params) are
unit-tested for real; airflow-importing files (operators.py, sensors.py,
ctm_plugin.py) are only syntax-checked with py_compile — Airflow is NOT
installed on this platform and must never be imported here.
"""
from __future__ import annotations

import py_compile
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PLUGINS_DIR = REPO / "plugins"
# Mirror the MWAA runtime, where the plugins.zip root is on sys.path and
# ctm_plugins is imported as a top-level package.
if str(PLUGINS_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGINS_DIR))

from ctm_plugins import _odate, _params, callbacks, timetables  # noqa: E402


# --------------------------------------------------------------- import law

def test_pure_modules_do_not_import_airflow():
    # Airflow is not installed: had any pure module imported it at module
    # level, the imports above would have raised. Belt and braces:
    assert "airflow" not in sys.modules


# ------------------------------------------------------------------- ODATE

def test_odate_evening_fire_same_date():
    assert _odate.ctm_odate(datetime(2026, 3, 10, 22, 0)) == "20260310"


def test_odate_0200_fire_belongs_to_previous_date():
    assert _odate.ctm_odate(datetime(2026, 3, 11, 2, 0)) == "20260310"


def test_odate_0500_fire_before_new_day_0600():
    assert _odate.ctm_odate(datetime(2026, 3, 11, 5, 0)) == "20260310"


def test_odate_one_minute_before_new_day_shifts():
    assert _odate.ctm_odate(datetime(2026, 3, 11, 5, 59)) == "20260310"


def test_odate_exactly_at_new_day_is_new_odate():
    assert _odate.ctm_odate(datetime(2026, 3, 11, 6, 0)) == "20260311"


def test_odate_midnight_fire_shifts_back():
    assert _odate.ctm_odate(datetime(2026, 3, 11, 0, 0)) == "20260310"


def test_odate_new_day_midnight_never_shifts():
    assert _odate.ctm_odate(datetime(2026, 3, 11, 0, 0), new_day_hhmm="0000") == "20260311"
    assert _odate.ctm_odate(datetime(2026, 3, 11, 23, 59), new_day_hhmm="0000") == "20260311"


def test_odate_year_boundary():
    assert _odate.ctm_odate(datetime(2026, 1, 1, 1, 0)) == "20251231"


def test_odate_custom_new_day_and_fmt():
    # New Day 22:00: a 21:30 fire is still the previous odate
    assert _odate.ctm_odate(datetime(2026, 3, 11, 21, 30), new_day_hhmm="2200") == "20260310"
    assert _odate.ctm_odate(datetime(2026, 3, 10, 22, 0), fmt="%Y-%m-%d") == "2026-03-10"


def test_odate_seconds_do_not_promote_past_new_day():
    # 05:59:59 is still before 06:00
    assert _odate.ctm_odate(datetime(2026, 3, 11, 5, 59, 59)) == "20260310"


def test_parse_hhmm_accepts_three_digits_and_rejects_garbage():
    assert _odate.parse_hhmm("600") == (6, 0)
    assert _odate.parse_hhmm("2359") == (23, 59)
    for bad in ("2460", "9999", "12", "ab00", "", "12345"):
        with pytest.raises(ValueError):
            _odate.parse_hhmm(bad)


# ------------------------------------------------------------- gate_target

def test_gate_2200_fire_0200_gate_lands_next_morning():
    # Classic Control-M: job fires 22:00, TIMEFROM gate 02:00 belongs to the
    # SAME odate -> next calendar day 02:00.
    target = _odate.gate_target(datetime(2026, 3, 10, 22, 0), "0200")
    assert target == datetime(2026, 3, 11, 2, 0)


def test_gate_pre_new_day_fire_keeps_same_calendar_date():
    # Fire 05:00 (odate 2026-03-10); gate 05:30 (< New Day) stays on 03-11.
    target = _odate.gate_target(datetime(2026, 3, 11, 5, 0), "0530")
    assert target == datetime(2026, 3, 11, 5, 30)


def test_gate_pre_new_day_fire_evening_gate_is_previous_calendar_date():
    # Fire 05:00 belongs to odate 2026-03-10, so the 22:00 gate of that odate
    # is 2026-03-10 22:00 (already in the past — sensor completes immediately).
    target = _odate.gate_target(datetime(2026, 3, 11, 5, 0), "2200")
    assert target == datetime(2026, 3, 10, 22, 0)


def test_gate_exactly_at_new_day_belongs_to_odate_date():
    target = _odate.gate_target(datetime(2026, 3, 10, 22, 0), "0600")
    assert target == datetime(2026, 3, 10, 6, 0)


def test_gate_midnight_gate_lands_on_next_calendar_date():
    target = _odate.gate_target(datetime(2026, 3, 10, 22, 0), "0000")
    assert target == datetime(2026, 3, 11, 0, 0)


def test_gate_new_day_midnight_keeps_everything_same_date():
    target = _odate.gate_target(datetime(2026, 3, 10, 22, 0), "0100", new_day_hhmm="0000")
    assert target == datetime(2026, 3, 10, 1, 0)


def test_gate_preserves_tzinfo_and_zeroes_seconds():
    fire = datetime(2026, 3, 10, 22, 15, 42, 123456, tzinfo=timezone.utc)
    target = _odate.gate_target(fire, "0200")
    assert target.tzinfo is timezone.utc
    assert (target.second, target.microsecond) == (0, 0)
    assert (target.year, target.month, target.day, target.hour, target.minute) == (
        2026, 3, 11, 2, 0,
    )


def test_gate_month_boundary():
    target = _odate.gate_target(datetime(2026, 1, 31, 23, 0), "0300")
    assert target == datetime(2026, 2, 1, 3, 0)


# ---------------------------------------------------------------- callbacks

def test_resolve_dest_email_passthrough_for_at_sign():
    spec = callbacks.resolve_dest("ops@corp.com", notify_map={})
    assert spec == {"type": "email", "target": "ops@corp.com"}


def test_resolve_dest_unknown_falls_back_to_log():
    spec = callbacks.resolve_dest("NOBODY", notify_map={})
    assert spec == {"type": "log", "target": "NOBODY"}


def test_resolve_dest_uses_notify_map():
    nmap = {
        "PAGER": {"type": "sns", "target": "arn:aws:sns:eu-west-1:1:t"},
        "OPS": {"type": "log", "target": ""},
    }
    assert callbacks.resolve_dest("PAGER", nmap)["type"] == "sns"
    # log entries with empty target keep the dest name as target
    assert callbacks.resolve_dest("OPS", nmap) == {"type": "log", "target": "OPS"}


def test_resolve_dest_rejects_bogus_types():
    spec = callbacks.resolve_dest("X", {"X": {"type": "carrier_pigeon", "target": "y"}})
    assert spec["type"] == "log"


def test_load_notify_map_shipped_file():
    nmap = callbacks.load_notify_map(REPO / "mapping-config" / "notify.yaml")
    assert nmap["OPS"]["type"] == "log"
    assert nmap["OPS-EMAIL"] == {"type": "email", "target": "ops@corp.com"}
    assert nmap["OPS-PAGER"]["type"] == "sns"


def test_load_notify_map_default_path_finds_repo_config():
    # no explicit path: the repo-layout candidate must resolve
    nmap = callbacks.load_notify_map()
    assert "OPS" in nmap


def test_load_notify_map_missing_and_malformed(tmp_path, monkeypatch):
    # explicit path is authoritative: missing file -> {} (log-only), no fallback
    assert callbacks.load_notify_map(tmp_path / "nope.yaml") == {}
    # same for the env var
    monkeypatch.setenv(callbacks.NOTIFY_ENV_VAR, str(tmp_path / "nope2.yaml"))
    assert callbacks.load_notify_map() == {}
    monkeypatch.delenv(callbacks.NOTIFY_ENV_VAR)
    # malformed yaml (top level not a mapping) -> {}
    bad = tmp_path / "bad.yaml"
    bad.write_text("- just\n- a list\n", encoding="utf-8")
    assert callbacks.load_notify_map(bad) == {}


def test_load_notify_map_env_var_override(tmp_path, monkeypatch):
    cfg = tmp_path / "notify.yaml"
    cfg.write_text("TEAM_X: {type: sns, target: arn:x}\n", encoding="utf-8")
    monkeypatch.setenv(callbacks.NOTIFY_ENV_VAR, str(cfg))
    nmap = callbacks.load_notify_map()
    assert nmap == {"TEAM_X": {"type": "sns", "target": "arn:x"}}


def test_summarize_context_variants():
    class FakeDag:
        dag_id = "bank_eod"

    class FakeTi:
        task_id = "bank_settle"

    ctx = {"dag": FakeDag(), "task_instance": FakeTi(), "run_id": "r1", "ds": "2026-03-10"}
    assert callbacks.summarize_context(ctx) == {
        "dag_id": "bank_eod",
        "task_id": "bank_settle",
        "run_id": "r1",
        "ds": "2026-03-10",
    }
    # plain values (tests / partial contexts)
    assert callbacks.summarize_context({"dag": "d1"}) == {"dag_id": "d1"}
    # sla_miss_callback passes a DAG object positionally, not a dict
    assert callbacks.summarize_context(FakeDag()) == {"dag_id": "bank_eod"}
    assert callbacks.summarize_context(None) == {}


def test_format_message_deterministic():
    msg = callbacks.format_message(
        "OPS", "settle failed", "NOTOK", {"dag_id": "d", "task_id": "t", "ds": "2026-01-02"}
    )
    assert msg == "[CTM SHOUT when=NOTOK dest=OPS] settle failed (dag_id=d, task_id=t, ds=2026-01-02)"
    assert callbacks.format_message("OPS", "", "OK", {}) == "[CTM SHOUT when=OK dest=OPS]"


def test_ctm_shout_returns_annotated_callable():
    cb = callbacks.ctm_shout("OPS", message="boom", when="NOTOK")
    assert callable(cb)
    assert cb.ctm_dest == "OPS"
    assert cb.ctm_message == "boom"
    assert cb.ctm_when == "NOTOK"


def test_ctm_shout_log_dest_runs_without_airflow(caplog):
    cb = callbacks.ctm_shout("OPS", message="job failed", when="NOTOK")
    with caplog.at_level("WARNING", logger="ctm_plugins.callbacks"):
        cb({"dag": "bank_eod", "run_id": "manual__1"})
    assert any("job failed" in r.message and "bank_eod" in r.message for r in caplog.records)
    assert "airflow" not in sys.modules


def test_ctm_shout_unknown_dest_and_ok_when_logs_info(caplog):
    cb = callbacks.ctm_shout("NO_SUCH_DEST", when="OK")
    with caplog.at_level("INFO", logger="ctm_plugins.callbacks"):
        cb(None)  # sla-style invocation with no context
    assert any("when=OK dest=NO_SUCH_DEST" in r.message for r in caplog.records)


# --------------------------------------------------------------- timetables

def test_load_calendars_shipped_file_has_bank_bus_days():
    cals = timetables.load_calendars(REPO / "mapping-config" / "calendars.yaml")
    assert "BANK_BUS_DAYS" in cals
    dates = cals["BANK_BUS_DAYS"]
    assert dates == sorted(dates)
    assert "2026-01-02" in dates
    assert "2026-01-19" not in dates  # holiday skipped
    assert all(len(d) == 10 for d in dates)


def test_calendar_dates_unknown_calendar_is_empty():
    assert timetables.calendar_dates("NO_SUCH_CAL", REPO / "mapping-config" / "calendars.yaml") == []


def test_load_calendars_drops_bad_rows(tmp_path):
    cfg = tmp_path / "calendars.yaml"
    cfg.write_text(
        "CAL_A:\n  - '2026-02-02'\n  - 'not-a-date'\n  - '2026-01-05'\n  - '2026-01-05'\n"
        "CAL_B: not-a-list\n",
        encoding="utf-8",
    )
    cals = timetables.load_calendars(cfg)
    assert cals == {"CAL_A": ["2026-01-05", "2026-02-02"]}


def test_select_next_fire_first_and_strictly_after():
    dates = ["2026-01-05", "2026-01-02", "2026-01-02", "2026-01-09"]  # unsorted + dup
    first = timetables.select_next_fire(dates, "1800", after=None)
    assert first == datetime(2026, 1, 2, 18, 0)
    # exactly at a fire time -> strictly after -> next date
    nxt = timetables.select_next_fire(dates, "1800", after=datetime(2026, 1, 2, 18, 0))
    assert nxt == datetime(2026, 1, 5, 18, 0)
    # just before a fire time -> that fire
    same = timetables.select_next_fire(dates, "1800", after=datetime(2026, 1, 5, 17, 59))
    assert same == datetime(2026, 1, 5, 18, 0)


def test_select_next_fire_exhausted_returns_none():
    assert timetables.select_next_fire(["2026-01-02"], "0600", after=datetime(2026, 1, 2, 6, 0)) is None
    assert timetables.select_next_fire([], "0600") is None


def test_select_next_fire_anchor_parsing():
    fire = timetables.select_next_fire(["2026-03-02"], "2330")
    assert fire == datetime(2026, 3, 2, 23, 30)
    with pytest.raises(ValueError):
        timetables.select_next_fire(["2026-03-02"], "2500")


def test_timetable_serialize_round_trip_is_pure():
    tt = timetables.CtmCalendarTimetable("BANK_BUS_DAYS", "1800")
    data = tt.serialize()
    assert data == {
        "calendar_name": "BANK_BUS_DAYS",
        "anchor_hhmm": "1800",
        "calendars_path": None,
    }
    clone = timetables.CtmCalendarTimetable.deserialize(data)
    assert clone.summary == "ctm-calendar:BANK_BUS_DAYS@1800"
    with pytest.raises(ValueError):
        timetables.CtmCalendarTimetable("X", "9am")


# ----------------------------------------------------- resolve_node (V4-1)

NODES_YAML = REPO / "mapping-config" / "nodes.yaml"


def test_resolve_node_shipped_file_v2_schema():
    # v2 schema: defaults + nodes with conn_id/os/type
    assert _params.resolve_node("dbnode1", path=NODES_YAML) == {
        "conn_id": "bank_dwh",
        "os": "linux",  # inherited from defaults
        "type": "db",
    }
    assert _params.resolve_node("winnode1", path=NODES_YAML) == {
        "conn_id": "winrm_winnode1",
        "os": "windows",
        "type": "",
    }


def test_resolve_node_default_path_finds_repo_config():
    # no explicit path: the repo-layout candidate must resolve
    assert _params.resolve_node("dbnode1")["conn_id"] == "bank_dwh"


def test_resolve_node_v2_defaults_os_inheritance(tmp_path):
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text(
        "defaults: {os: windows}\n"
        "nodes:\n"
        "  w1: {conn_id: winrm_w1}\n"
        "  l1: {conn_id: ssh_l1, os: linux}\n"
        "  bare: {}\n",
        encoding="utf-8",
    )
    assert _params.resolve_node("w1", path=cfg) == {
        "conn_id": "winrm_w1", "os": "windows", "type": "",
    }
    assert _params.resolve_node("l1", path=cfg)["os"] == "linux"
    # entry without conn_id falls back to ssh_<node> but keeps the default os
    assert _params.resolve_node("bare", path=cfg) == {
        "conn_id": "ssh_bare", "os": "windows", "type": "",
    }


def test_resolve_node_v1_flat_schema(tmp_path):
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text("nodeA: conn_a\nnodeB: conn_b\n", encoding="utf-8")
    assert _params.resolve_node("nodeA", path=cfg) == {
        "conn_id": "conn_a", "os": "linux", "type": "",
    }
    assert _params.resolve_node("nodeB", path=cfg)["conn_id"] == "conn_b"


def test_resolve_node_fallback_unmapped_and_empty():
    # unmapped node -> ssh_<node>/linux; empty node -> ssh_default
    assert _params.resolve_node("nowhere", node_map={}) == {
        "conn_id": "ssh_nowhere", "os": "linux", "type": "",
    }
    assert _params.resolve_node("", node_map={"x": {"conn_id": "c"}}) == {
        "conn_id": "ssh_default", "os": "linux", "type": "",
    }
    assert _params.resolve_node("  spaced  ", node_map={})["conn_id"] == "ssh_spaced"


def test_resolve_node_env_var_override(tmp_path, monkeypatch):
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text("nodes:\n  envnode: {conn_id: env_conn}\n", encoding="utf-8")
    monkeypatch.setenv(_params.NODES_ENV_VAR, str(cfg))
    assert _params.resolve_node("envnode")["conn_id"] == "env_conn"
    # env var is authoritative: nodes mapped only in the repo file now fall back
    assert _params.resolve_node("dbnode1")["conn_id"] == "ssh_dbnode1"


def test_resolve_node_env_var_missing_file_is_authoritative(tmp_path, monkeypatch):
    monkeypatch.setenv(_params.NODES_ENV_VAR, str(tmp_path / "nope.yaml"))
    assert _params.resolve_node("dbnode1")["conn_id"] == "ssh_dbnode1"


def test_resolve_node_missing_and_malformed_degrade_to_fallback(tmp_path):
    # explicit path is authoritative: missing file -> fallbacks, no search
    assert _params.resolve_node("dbnode1", path=tmp_path / "nope.yaml") == {
        "conn_id": "ssh_dbnode1", "os": "linux", "type": "",
    }
    bad = tmp_path / "bad.yaml"
    bad.write_text("- just\n- a list\n", encoding="utf-8")
    assert _params.resolve_node("dbnode1", path=bad)["conn_id"] == "ssh_dbnode1"


def test_resolve_node_pure_with_explicit_map():
    nmap = {"n1": {"conn_id": "c1", "os": "windows", "type": "db"}}
    assert _params.resolve_node("n1", node_map=nmap) == {
        "conn_id": "c1", "os": "windows", "type": "db",
    }
    # sparse entries fill defaults
    assert _params.resolve_node("n2", node_map={"n2": {}}) == {
        "conn_id": "ssh_n2", "os": "linux", "type": "",
    }


def test_params_surface_is_only_resolve_node():
    # Revised V4-1: the priority formula stays in core/ctrlm_core/
    # operator_registry.py and the blanket kwarg-translation surface is gone.
    for removed in ("priority_weight_for", "pool_kwargs", "translate_common_kwargs"):
        assert not hasattr(_params, removed)


def test_operators_module_source_has_exactly_two_classes():
    # operators.py cannot be imported here (airflow); assert on its source
    # that the abandoned blanket wrappers stayed dead.
    src = (REPO / "plugins" / "ctm_plugins" / "operators.py").read_text(encoding="utf-8")
    # names built at runtime so a repo-wide grep for the abandoned-attempt
    # class names stays clean (same trick as the emit-side guard)
    for removed_suffix in ("CommandJob", "PowerShellJob", "Dummy", "JobMixin"):
        assert "Ctm" + removed_suffix not in src
    assert "class CtmDatabaseJob(SQLExecuteQueryOperator):" in src
    assert "class CtmManualJob(BaseOperator):" in src
    assert '__all__ = ["CtmDatabaseJob", "CtmManualJob"]' in src


def test_package_lazily_reexports_operator_classes():
    import ctm_plugins

    assert "CtmDatabaseJob" in ctm_plugins.__all__
    assert "CtmManualJob" in ctm_plugins.__all__
    # accessing them would import airflow (not installed) — only the lazy
    # hook's existence is checked here, plus a clean error for unknown names
    assert callable(getattr(ctm_plugins, "__getattr__"))
    with pytest.raises(AttributeError):
        ctm_plugins.no_such_export
    assert "airflow" not in sys.modules


# ------------------------------------------------- airflow wrappers compile

@pytest.mark.parametrize(
    "relpath",
    [
        "plugins/ctm_plugin.py",
        "plugins/ctm_plugins/__init__.py",
        "plugins/ctm_plugins/_odate.py",
        "plugins/ctm_plugins/_params.py",
        "plugins/ctm_plugins/callbacks.py",
        "plugins/ctm_plugins/operators.py",
        "plugins/ctm_plugins/sensors.py",
        "plugins/ctm_plugins/timetables.py",
    ],
)
def test_plugin_files_py_compile(relpath):
    py_compile.compile(str(REPO / relpath), doraise=True)
