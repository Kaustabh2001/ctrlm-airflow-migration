"""Control-M calendar schedules -> Airflow timetable (contract V3-2).

``CtmCalendarTimetable`` fires a DAG at a fixed anchor time (HHMM) on exactly
the dates listed for a named calendar in ``mapping-config/calendars.yaml``
(schema: ``{calendar_name: ["YYYY-MM-DD", ...]}``). This maps Control-M
DAYSCAL/WEEKCAL/periodic-calendar jobs whose dates are exported as explicit
lists.

The airflow import is guarded so the PURE date-selection helpers
(``select_next_fire``, ``load_calendars``, ``calendar_dates``) stay
unit-testable on the Windows dev box where Airflow cannot be installed; the
``Timetable`` subclass itself is exercised only on MWAA and is otherwise just
syntax-checked. Never call its ``next_dagrun_info`` in tool code or tests.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence

import yaml

from ._odate import parse_hhmm

try:  # airflow is only present on the scheduler/worker (MWAA), never on dev
    # Path verified UNCHANGED in Airflow 3 (airflow.timetables.base), so this
    # single guard covers both the 2.x fallback and the 3.x target (V6-2).
    from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

    _HAS_AIRFLOW = True
except ImportError:  # pure-logic environment: helpers below still importable
    DagRunInfo = DataInterval = TimeRestriction = None  # type: ignore[assignment]
    Timetable = object  # type: ignore[assignment,misc]
    _HAS_AIRFLOW = False

CALENDARS_ENV_VAR = "CTM_CALENDARS_CONFIG"
_CALENDARS_BASENAME = Path("mapping-config") / "calendars.yaml"
DEFAULT_ANCHOR = "0600"


# ------------------------------------------------------------- pure helpers

def _candidate_paths(explicit: str | Path | None) -> list[Path]:
    """Candidate locations for calendars.yaml (first existing wins).

    An explicit path, or failing that the CTM_CALENDARS_CONFIG env var, is
    authoritative (missing file -> empty calendars, no fallback search).
    """
    if explicit:
        return [Path(explicit)]
    env = os.environ.get(CALENDARS_ENV_VAR, "")
    if env:
        return [Path(env)]
    here = Path(__file__).resolve()
    return [
        here.parents[1] / _CALENDARS_BASENAME,  # deployed plugins.zip layout
        here.parents[2] / _CALENDARS_BASENAME,  # repo dev layout
        Path.cwd() / _CALENDARS_BASENAME,
    ]


def load_calendars(path: str | Path | None = None) -> dict[str, list[str]]:
    """Load calendars.yaml -> {calendar_name: sorted unique "YYYY-MM-DD" list}.

    PURE (file read only). Missing file -> {}. Entries that are not
    parseable dates are dropped (a bad row must not break the scheduler).
    """
    for candidate in _candidate_paths(path):
        if not candidate.is_file():
            continue
        raw = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            return {}
        out: dict[str, list[str]] = {}
        for name in sorted(raw):
            dates = raw[name]
            if not isinstance(dates, (list, tuple)):
                continue
            valid: set[str] = set()
            for d in dates:
                text = str(d).strip()
                try:
                    datetime.strptime(text, "%Y-%m-%d")
                except ValueError:
                    continue
                valid.add(text)
            out[str(name)] = sorted(valid)
        return out
    return {}


def calendar_dates(name: str, path: str | Path | None = None) -> list[str]:
    """Sorted date strings of one named calendar ([] when unknown)."""
    return load_calendars(path).get(name, [])


def select_next_fire(
    dates: Sequence[str],
    anchor_hhmm: str = DEFAULT_ANCHOR,
    after: Optional[datetime] = None,
) -> Optional[datetime]:
    """PURE date-selection core of CtmCalendarTimetable.

    Returns the earliest ``date + anchor`` (naive datetime) strictly after
    ``after`` — or the first fire when ``after`` is None — from the listed
    dates. None when the calendar is exhausted. Input order is irrelevant
    (sorted internally); duplicates are ignored.
    """
    hour, minute = parse_hhmm(anchor_hhmm)
    for text in sorted(set(dates)):
        fire = datetime.strptime(text, "%Y-%m-%d").replace(hour=hour, minute=minute)
        if after is None or fire > after:
            return fire
    return None


# ------------------------------------------------------- airflow timetable

class CtmCalendarTimetable(Timetable):  # type: ignore[misc]
    """Fires at ``anchor_hhmm`` UTC on exactly the calendar's listed dates.

    Registered by plugins/ctm_plugin.py. Usage in a generated DAG::

        from ctm_plugins.timetables import CtmCalendarTimetable
        with DAG(..., schedule=CtmCalendarTimetable("BANK_BUS_DAYS", "1800")):
            ...
    """

    def __init__(
        self,
        calendar_name: str,
        anchor_hhmm: str = DEFAULT_ANCHOR,
        calendars_path: str | None = None,
    ) -> None:
        parse_hhmm(anchor_hhmm)  # fail fast on bad anchors
        self.calendar_name = calendar_name
        self.anchor_hhmm = anchor_hhmm
        self.calendars_path = calendars_path

    # -- serialization (Airflow persists timetables in the DB) --------------
    def serialize(self) -> dict:
        return {
            "calendar_name": self.calendar_name,
            "anchor_hhmm": self.anchor_hhmm,
            "calendars_path": self.calendars_path,
        }

    @classmethod
    def deserialize(cls, data: dict) -> "CtmCalendarTimetable":
        return cls(**data)

    @property
    def summary(self) -> str:
        return f"ctm-calendar:{self.calendar_name}@{self.anchor_hhmm}"

    # -- scheduling (only ever executed on MWAA) -----------------------------
    def infer_manual_data_interval(self, *, run_after):  # noqa: ANN001, ANN201
        return DataInterval.exact(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):  # noqa: ANN001, ANN201
        dates = calendar_dates(self.calendar_name, self.calendars_path)
        after: Optional[datetime] = None
        if last_automated_data_interval is not None:
            after = _naive_utc(last_automated_data_interval.end)
        elif restriction.earliest is not None:
            # first run may fall exactly ON earliest -> make the bound inclusive
            after = _naive_utc(restriction.earliest) - timedelta(microseconds=1)
        if not restriction.catchup:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            after = now if after is None else max(after, now)
        fire = select_next_fire(dates, self.anchor_hhmm, after)
        if fire is None:
            return None
        aware = fire.replace(tzinfo=timezone.utc)
        if restriction.latest is not None and aware > restriction.latest:
            return None
        return DagRunInfo.exact(aware)


def _naive_utc(dt: datetime) -> datetime:
    """Aware datetime -> naive UTC (helper for next_dagrun_info)."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)
