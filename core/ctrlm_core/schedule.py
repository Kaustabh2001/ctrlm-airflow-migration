"""Schedule normalization + cron helpers (DESIGN.md §4.2, algorithm Phase 8).

Implements the folder->job scheduling cascade, the canonical day_pattern string,
the ODATE-clock helper rel_minutes(), and cron generation (cron_for / cyclic_cron).
All iteration orders are sorted: output is a pure function of the input IR.
"""
from __future__ import annotations

from .model import Deftable, FolderDef, Job, PartitionConfig

# full sets that collapse to "ALL" during canonicalization
_WD_FULL = frozenset(range(1, 8))    # 1=Mon .. 7=Sun
_MD_FULL = frozenset(range(1, 32))
_M_FULL = frozenset(range(1, 13))


# ---------------------------------------------------------------- primitives

def _to_minutes(hhmm: str) -> int:
    """'2230' -> 1350. Empty/blank strings count as midnight ('0000')."""
    raw = (hhmm or "").strip()
    if not raw:
        return 0
    value = int(raw)
    hours, minutes = divmod(value, 100)
    return (hours % 24) * 60 + minutes


def rel_minutes(hhmm: str, new_day: str) -> int:
    """Minutes since the Control-M New Day time: (t - new_day) % 1440.

    On the ODATE clock 02:00 sorts AFTER 22:00 when new_day is 06:00.
    """
    return (_to_minutes(hhmm) - _to_minutes(new_day)) % 1440


def _canon_part(raw: str, full: frozenset[int]) -> str:
    """Canonicalize one day field: '' | 'ALL' | sorted comma list of ints.

    'ALL' (any case) and explicit full sets normalize to 'ALL'. Unparsable
    tokens (calendar names etc.) are kept verbatim, sorted after the ints.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.upper() == "ALL":
        return "ALL"
    ints: set[int] = set()
    others: set[str] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            ints.add(int(token))
        except ValueError:
            others.add(token)
    if not ints and not others:
        return ""
    if not others and ints >= full:
        return "ALL"
    return ",".join([str(i) for i in sorted(ints)] + sorted(others))


# ---------------------------------------------------------------- day pattern

def day_pattern_of(job: Job) -> str | None:
    """Canonical day-pattern string of a job's (already cascaded) raw fields.

    None when weekdays == monthdays == months == "" (condition-driven only).
    Else "WD={wd}|MD={md}|M={m}|OP={AND|OR}" with each part '' | 'ALL' | sorted ints.
    """
    wd = _canon_part(job.weekdays, _WD_FULL)
    md = _canon_part(job.monthdays, _MD_FULL)
    months = _canon_part(job.months, _M_FULL)
    if not (wd or md or months):
        return None
    op = "AND" if (job.days_and_or or "").strip().upper() == "AND" else "OR"
    return f"WD={wd}|MD={md}|M={months}|OP={op}"


def _job_has_own_sched_attrs(job: Job) -> bool:
    """True when the job carries ANY scheduling attribute of its own.

    Own day fields (weekdays/monthdays/months) or an own time window
    (timefrom/timeto) both count: a job that says only "start at 02:00" is
    treated as self-scheduled and stays day-pattern-less (condition-driven).
    This is what makes folder-gated chains like FIN_EOD's FIN_EXTRACT an
    *unscheduled middle*, so downstream day-pattern conflicts are transitive
    (algorithm Phase 6 / AUTO_RESOLVED) rather than direct (Phase 3).

    Unchanged in v2 (docs/impl-contracts-v2.md §V2-2): the nested-folder work
    only changed WHERE the cascade looks (the parent chain), not WHAT blocks
    it — e.g. MFG_NIGHT/MFG_EXTRACT (TIMEFROM 2100, no day fields) stays
    day-pattern-less, exactly like FIN_LOAD_GL in v1.
    """
    return bool(
        job.weekdays.strip()
        or job.monthdays.strip()
        or job.months.strip()
        or job.timefrom.strip()
        or job.timeto.strip()
    )


def _folder_has_day_attrs(folder: FolderDef) -> bool:
    return bool(
        folder.weekdays.strip() or folder.monthdays.strip() or folder.months.strip()
    )


def _day_attr_provider(
    folder: FolderDef, by_name: dict[str, FolderDef]
) -> FolderDef | None:
    """Nearest folder in the ancestor chain (self first) with day attrs.

    Nested folders (docs/impl-contracts-v2.md §V2-2): the cascade walks the
    PARENT CHAIN, so a job in a sub-folder with no day attrs of its own
    inherits from the nearest ancestor folder that has them.
    """
    current: FolderDef | None = folder
    seen: set[str] = set()
    while current is not None and current.name not in seen:
        seen.add(current.name)
        if _folder_has_day_attrs(current):
            return current
        current = by_name.get(current.parent) if current.parent else None
    return None


def _nearest_timezone(folder: FolderDef, by_name: dict[str, FolderDef]) -> str:
    """Nearest non-empty folder timezone in the ancestor chain (self first)."""
    current: FolderDef | None = folder
    seen: set[str] = set()
    while current is not None and current.name not in seen:
        seen.add(current.name)
        if current.timezone.strip():
            return current.timezone
        current = by_name.get(current.parent) if current.parent else None
    return ""


def normalize_jobs(deftable: Deftable, config: PartitionConfig) -> None:
    """Folder->job scheduling cascade + day_pattern fill (DESIGN.md §4.2).

    A job with NO scheduling attrs of its own (no day fields AND no time
    window) inherits the day attrs of the nearest ancestor folder (its own
    folder first, then the parent chain — nested folders, v2) that HAS
    folder-level day attrs; the raw fields are filled BEFORE the canonical
    day_pattern is computed. Jobs with any own attrs keep theirs.
    """
    by_name = {f.name: f for f in deftable.folders}
    for folder in sorted(deftable.folders, key=lambda f: f.name):
        provider = _day_attr_provider(folder, by_name)
        for job in sorted(folder.jobs, key=lambda j: j.name):
            if provider is not None and not _job_has_own_sched_attrs(job):
                job.weekdays = provider.weekdays
                job.monthdays = provider.monthdays
                job.months = provider.months
                job.days_and_or = provider.days_and_or
                if not job.timezone.strip():
                    job.timezone = _nearest_timezone(folder, by_name)
            job.day_pattern = day_pattern_of(job)


# ---------------------------------------------------------------- cron

def _parse_pattern(day_pattern: str) -> tuple[str, str, str, str]:
    """'WD=..|MD=..|M=..|OP=..' -> (wd, md, months, op)."""
    parts = {"WD": "", "MD": "", "M": "", "OP": "OR"}
    for chunk in day_pattern.split("|"):
        key, _, value = chunk.partition("=")
        if key in parts:
            parts[key] = value
    return parts["WD"], parts["MD"], parts["M"], parts["OP"]


def cron_and_approx(day_pattern: str | None) -> bool:
    """True when cron_for() can only APPROXIMATE the pattern (WD AND MD).

    Cron cannot express weekday-AND-monthday; cron_for sets the dom field only.
    Callers must attach a warn diagnostic (code CRON_AND_APPROX) when this is True.
    """
    if not day_pattern:
        return False
    wd, md, _, op = _parse_pattern(day_pattern)
    wd_restricted = wd not in ("", "ALL")
    md_restricted = md not in ("", "ALL")
    return op == "AND" and wd_restricted and md_restricted


def _day_fields(day_pattern: str) -> tuple[str, str, str]:
    """Pattern -> cron (dom, month, dow) fields; dow uses 1=Mon..7=Sun."""
    wd, md, months, op = _parse_pattern(day_pattern)
    month = "*" if months in ("", "ALL") else months
    wd_restricted = wd not in ("", "ALL")
    md_restricted = md not in ("", "ALL")

    if wd_restricted and md_restricted:
        if op == "OR":
            return md, month, wd      # cron's native dom-OR-dow semantics
        return md, month, "*"         # AND: approximation — caller must warn
    if wd_restricted:
        # md is "" or "ALL"; "ALL" + OR means every day already matches
        if md == "ALL" and op == "OR":
            return "*", month, "*"
        return "*", month, wd
    if md_restricted:
        if wd == "ALL" and op == "OR":
            return "*", month, "*"
        return md, month, "*"
    return "*", month, "*"


def cron_for(day_pattern: str | None, anchor_hhmm: str) -> str | None:
    """Day pattern + 'HHMM' anchor -> 5-field cron string; None for None pattern.

    OR of weekday/monthday maps to cron's native dom/dow OR; AND is approximated
    by the dom field alone — check cron_and_approx() and add a warn diagnostic.
    """
    if day_pattern is None:
        return None
    total = _to_minutes(anchor_hhmm)
    hour, minute = divmod(total, 60)
    dom, month, dow = _day_fields(day_pattern)
    return f"{minute} {hour} {dom} {month} {dow}"


def _hour_window(timefrom: str, timeto: str) -> str:
    """TIMEFROM/TIMETO -> cron hour range ('6-19' for 0600..2000), '*' if open."""
    timefrom = (timefrom or "").strip()
    timeto = (timeto or "").strip()
    start = _to_minutes(timefrom) // 60 if timefrom else None
    end: int | None = None
    if timeto:
        end_minutes = _to_minutes(timeto)
        end = (end_minutes - 1) // 60 if end_minutes > 0 else 23
    if start is None and end is None:
        return "*"
    lo = start if start is not None else 0
    hi = end if end is not None else 23
    if hi < lo:            # window wraps midnight: not expressible in one range
        return "*"
    if lo == 0 and hi == 23:
        return "*"
    if lo == hi:
        return str(lo)
    return f"{lo}-{hi}"


def cyclic_cron(job: Job) -> str:
    """Cyclic job -> interval cron restricted to its TIMEFROM..TIMETO window.

    e.g. INTERVAL 15M, TIMEFROM 0600, TIMETO 2000 -> '*/15 6-19 * * *'.
    Day-pattern fields are honored when the job has one.
    """
    interval = job.interval_minutes if job.interval_minutes > 0 else 60
    hours = _hour_window(job.timefrom, job.timeto)
    if job.day_pattern:
        dom, month, dow = _day_fields(job.day_pattern)
    else:
        dom, month, dow = "*", "*", "*"

    if interval % 60 == 0:                       # whole-hour cadence
        step = interval // 60
        minute = str(_to_minutes(job.timefrom) % 60 if job.timefrom.strip() else 0)
        if step == 1:
            hour_field = hours
        elif hours == "*":
            hour_field = f"*/{step}"
        else:
            hour_field = f"{hours}/{step}"
        return f"{minute} {hour_field} {dom} {month} {dow}"

    return f"*/{interval} {hours} {dom} {month} {dow}"
