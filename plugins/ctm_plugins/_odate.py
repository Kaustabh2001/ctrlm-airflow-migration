"""Control-M ODATE arithmetic — PURE python, no airflow imports (contract V3-2).

Control-M's scheduling day (the "order date", ODATE) does not flip at midnight
but at the *New Day* time (default 06:00). A job that fires at 02:00 or 05:00
wall-clock still belongs to the PREVIOUS calendar date's ODATE when the New Day
time is 06:00.

Exposed as Airflow macros by plugins/ctm_plugin.py:

    {{ macros.ctm_odate(data_interval_end) }}
    {{ macros.gate_target(data_interval_end, "0200") }}

Everything here is deterministic: no wall-clock, no randomness.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

DEFAULT_NEW_DAY = "0600"


def parse_hhmm(hhmm: str) -> tuple[int, int]:
    """Parse a Control-M "HHMM" string into (hour, minute).

    Accepts 3-digit values ("600" -> 06:00) since some exports strip leading
    zeros. Raises ValueError on anything that is not a valid time of day.
    """
    s = str(hhmm).strip()
    if not s.isdigit() or not 3 <= len(s) <= 4:
        raise ValueError(f"invalid HHMM value: {hhmm!r}")
    s = s.zfill(4)
    hour, minute = int(s[:2]), int(s[2:])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid HHMM value: {hhmm!r}")
    return hour, minute


def odate_date(logical_dt: datetime, new_day_hhmm: str = DEFAULT_NEW_DAY) -> date:
    """Calendar date of the Control-M order date owning ``logical_dt``.

    The fire time's calendar date, shifted back one day when the fire
    time-of-day is strictly before the New Day time. A fire exactly AT the
    New Day time belongs to the new (same-date) ODATE.
    """
    nd_hour, nd_minute = parse_hhmm(new_day_hhmm)
    day = logical_dt.date()
    if (logical_dt.hour, logical_dt.minute) < (nd_hour, nd_minute):
        day = day - timedelta(days=1)
    return day


def ctm_odate(
    logical_dt: datetime,
    new_day_hhmm: str = DEFAULT_NEW_DAY,
    fmt: str = "%Y%m%d",
) -> str:
    """The run's Control-M order date as a formatted string (default %%ODATE style).

    >>> ctm_odate(datetime(2026, 3, 10, 22, 0))
    '20260310'
    >>> ctm_odate(datetime(2026, 3, 11, 5, 0))   # pre-New-Day fire
    '20260310'
    """
    return odate_date(logical_dt, new_day_hhmm).strftime(fmt)


def gate_target(
    logical_dt: datetime,
    gate_hhmm: str,
    new_day_hhmm: str = DEFAULT_NEW_DAY,
) -> datetime:
    """Datetime of the occurrence of ``gate_hhmm`` belonging to the SAME odate.

    Each ODATE owns exactly one wall-clock window
    ``[odate + new_day, odate + 1 day + new_day)``; every HHMM occurs exactly
    once inside it. Gate times at-or-after the New Day time land on the odate's
    own calendar date, earlier ones land on the next calendar date:

    - fire 22:00 (odate D), gate "0200"  -> D+1 02:00 (next morning)
    - fire 05:00 (odate D-1), gate "0530" -> same date 05:30

    A result earlier than ``logical_dt`` is intentional (the gate already
    passed inside this odate; a DateTimeSensor on it completes immediately).
    tzinfo of ``logical_dt`` is preserved; seconds/microseconds are zeroed.
    """
    gate_hour, gate_minute = parse_hhmm(gate_hhmm)
    nd_hour, nd_minute = parse_hhmm(new_day_hhmm)
    day = odate_date(logical_dt, new_day_hhmm)
    if (gate_hour, gate_minute) < (nd_hour, nd_minute):
        day = day + timedelta(days=1)
    return logical_dt.replace(
        year=day.year,
        month=day.month,
        day=day.day,
        hour=gate_hour,
        minute=gate_minute,
        second=0,
        microsecond=0,
    )
