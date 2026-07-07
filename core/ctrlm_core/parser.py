"""Control-M DEFTABLE XML -> typed IR (contracts: docs/impl-contracts.md §parser.py
+ docs/impl-contracts-v2.md §V2-2 nested folders + docs/impl-contracts-v3.md §V3-4:
APPL_TYPE / PRIORITY / CRITICAL job attributes, unknown appl_types verbatim).

Dialect: root ``<DEFTABLE>`` containing ``<FOLDER>`` / ``<SMART_FOLDER>``
(legacy ``<TABLE>`` / ``<SMART_TABLE>``) elements. A folder holds folder-level
``<INCOND>`` / ``<OUTCOND>`` / ``<VARIABLE>`` children — recognized only when
they appear BEFORE the first ``<JOB>`` or nested-folder child — followed by
``<JOB>`` elements and, at ARBITRARY depth, nested folders (``<SUB_FOLDER>``
or any tag of the folder family). Nested folders are FLATTENED into
``Deftable.folders`` in document (pre-)order: ``FolderDef.name`` is the full
slash path (``"MFG_NIGHT/PRESS_SHOP/QA"``), ``FolderDef.parent`` the parent
path (``""`` for top level), and each job's ``Job.folder`` is its immediate
folder's full path — so ``Job.uid`` stays globally unique. Sub-folders are
``smart=True`` iff their tag says so (``SMART_FOLDER`` / ``SMART_TABLE``) and
may carry their own scheduling attributes; DATACENTER is inherited from the
parent when absent.

Tolerance: unknown elements and attributes never crash the parse and never
lose jobs; they are counted and reported once on stderr. Missing attributes
fall back to the model defaults. Every value is whitespace-trimmed.
"""
from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

from .model import Condition, Deftable, FolderDef, Job, OnAction, Resource

# folder tag -> smart? (SUB_FOLDER: the nested-folder tag, not smart by itself)
_FOLDER_TAGS: dict[str, bool] = {
    "FOLDER": False,
    "TABLE": False,
    "SMART_FOLDER": True,
    "SMART_TABLE": True,
    "SUB_FOLDER": False,
}

_FOLDER_ATTRS = {
    "FOLDER_NAME", "TABLE_NAME", "DATACENTER",
    "WEEKDAYS", "DAYS", "MONTHS", "DAYS_AND_OR", "TIMEZONE",
}

_JOB_ATTRS = {
    "JOBNAME", "DESCRIPTION", "APPLICATION", "SUB_APPLICATION", "TASKTYPE",
    "CMDLINE", "MEMNAME", "MEMLIB", "NODEID", "RUN_AS",
    "WEEKDAYS", "DAYS", "MONTHS", "DAYS_AND_OR",
    "TIMEFROM", "TIMETO", "TIMEZONE",
    "CYCLIC", "INTERVAL", "MAXWAIT", "MAXRERUN", "RERUNINTERVAL", "CONFIRM",
    "APPL_TYPE", "PRIORITY", "CRITICAL",
}

_ON_ACTION_TAGS = {"DOMAIL", "DOSHOUT", "DOFORCEJOB", "DOCOND", "DOACTION"}

_QUALIFIER_KEYWORDS = {"ODAT", "PREV", "STAT"}


# ------------------------------------------------------------------ helpers

def _attr(el: ET.Element, *names: str, default: str = "") -> str:
    """First present attribute among names, whitespace-trimmed."""
    for name in names:
        if name in el.attrib:
            return el.attrib[name].strip()
    return default


def _flag(value: str) -> bool:
    return value.strip().upper() in {"1", "Y", "YES", "TRUE"}


def _int(value: str) -> int:
    try:
        return int(value.strip())
    except (ValueError, AttributeError):
        return 0


def _duration_minutes(value: str) -> int:
    """'15M' -> 15, '2H' -> 120, '1D' -> 1440, '15' -> 15, junk -> 0."""
    v = value.strip().upper()
    if not v:
        return 0
    mult = 1
    if v[-1] in "MHD":
        mult = {"M": 1, "H": 60, "D": 1440}[v[-1]]
        v = v[:-1]
    try:
        return int(v) * mult
    except ValueError:
        return 0


def _qualifier(value: str) -> str:
    """Normalize ODATE qualifier keywords; literal dates pass through verbatim."""
    v = value.strip()
    return v.upper() if v.upper() in _QUALIFIER_KEYWORDS else v


def _and_or(value: str) -> str:
    v = value.strip().upper()
    return {"A": "AND", "O": "OR"}.get(v, v or "AND")


def _sign(value: str) -> str:
    v = value.strip().upper()
    return {"+": "ADD", "-": "DEL"}.get(v, v or "ADD")


def _in_cond(el: ET.Element) -> Condition:
    return Condition(
        name=_attr(el, "NAME"),
        odate=_qualifier(_attr(el, "ODATE", default="ODAT") or "ODAT"),
        and_or=_and_or(_attr(el, "AND_OR")),
    )


def _out_cond(el: ET.Element) -> Condition:
    return Condition(
        name=_attr(el, "NAME"),
        odate=_qualifier(_attr(el, "ODATE", default="ODAT") or "ODAT"),
        sign=_sign(_attr(el, "SIGN")),
    )


def _count_unknown_attrs(el: ET.Element, known: set[str], scope: str, unknown: Counter) -> None:
    for key in el.attrib:
        if key not in known:
            unknown[f"{scope}@{key}"] += 1


# ------------------------------------------------------------------ job / folder

def _parse_job(el: ET.Element, folder_name: str, unknown: Counter) -> Job:
    _count_unknown_attrs(el, _JOB_ATTRS, "JOB", unknown)

    cmdline = _attr(el, "CMDLINE")
    memname = _attr(el, "MEMNAME")
    memlib = _attr(el, "MEMLIB")
    if cmdline:
        command = cmdline
    elif memlib and memname:
        command = f"{memlib.rstrip('/')}/{memname}"
    else:
        command = memname or memlib

    job = Job(
        name=_attr(el, "JOBNAME"),
        folder=folder_name,
        application=_attr(el, "APPLICATION"),
        sub_application=_attr(el, "SUB_APPLICATION"),
        description=_attr(el, "DESCRIPTION"),
        task_type=_attr(el, "TASKTYPE") or "Command",
        command=command,
        node_id=_attr(el, "NODEID"),
        run_as=_attr(el, "RUN_AS"),
        weekdays=_attr(el, "WEEKDAYS"),
        monthdays=_attr(el, "DAYS"),
        months=_attr(el, "MONTHS"),
        days_and_or=_and_or(_attr(el, "DAYS_AND_OR", default="OR") or "OR"),
        timefrom=_attr(el, "TIMEFROM"),
        timeto=_attr(el, "TIMETO"),
        timezone=_attr(el, "TIMEZONE"),
        cyclic=_flag(_attr(el, "CYCLIC")),
        interval_minutes=_duration_minutes(_attr(el, "INTERVAL")),
        maxwait=_int(_attr(el, "MAXWAIT")),
        maxrerun=_int(_attr(el, "MAXRERUN")),
        rerun_interval_minutes=_duration_minutes(_attr(el, "RERUNINTERVAL")),
        confirm=_flag(_attr(el, "CONFIRM")),
        appl_type=_attr(el, "APPL_TYPE"),   # unknown types pass through verbatim
        priority=_attr(el, "PRIORITY"),
        critical=_flag(_attr(el, "CRITICAL")),
    )

    for child in el:
        tag = child.tag.upper() if isinstance(child.tag, str) else ""
        if tag == "INCOND":
            job.in_conds.append(_in_cond(child))
        elif tag == "OUTCOND":
            job.out_conds.append(_out_cond(child))
        elif tag == "VARIABLE":
            job.variables[_attr(child, "NAME")] = _attr(child, "VALUE")
        elif tag == "QUANTITATIVE":
            job.resources.append(Resource(
                name=_attr(child, "NAME"),
                kind="quantitative",
                quant=_int(_attr(child, "QUANT")) or 1,
            ))
        elif tag == "CONTROL":
            job.resources.append(Resource(
                name=_attr(child, "NAME"),
                kind="control",
                control_type=_attr(child, "TYPE") or "E",
            ))
        elif tag == "ON":
            job.on_actions.append(_parse_on(child, unknown))
        elif tag == "SHOUT":
            job.shouts.append({
                "when": _attr(child, "WHEN"),
                "dest": _attr(child, "DEST"),
                "message": _attr(child, "MESSAGE"),
            })
        else:
            unknown[f"JOB/{tag or '?'}"] += 1
    return job


def _parse_on(el: ET.Element, unknown: Counter) -> OnAction:
    on = OnAction(
        stmt=_attr(el, "STMT") or "*",
        code=_attr(el, "CODE") or "NOTOK",
    )
    for child in el:
        tag = child.tag.upper() if isinstance(child.tag, str) else ""
        if tag in _ON_ACTION_TAGS:
            action = {"type": tag}
            action.update({k: v.strip() for k, v in child.attrib.items()})
            on.actions.append(action)
        else:
            unknown[f"ON/{tag or '?'}"] += 1
    return on


def _parse_folder(
    el: ET.Element,
    smart: bool,
    unknown: Counter,
    out: list[FolderDef],
    parent: str = "",
    parent_datacenter: str = "",
) -> None:
    """Parse one folder element (arbitrary nesting) and append the flattened
    FolderDef(s) to ``out`` in document pre-order (self, then sub-folders)."""
    _count_unknown_attrs(el, _FOLDER_ATTRS, "FOLDER", unknown)
    base_name = _attr(el, "FOLDER_NAME", "TABLE_NAME")
    full_name = f"{parent}/{base_name}" if parent else base_name
    folder = FolderDef(
        name=full_name,
        datacenter=_attr(el, "DATACENTER") or parent_datacenter,
        smart=smart,
        parent=parent,
        weekdays=_attr(el, "WEEKDAYS"),
        monthdays=_attr(el, "DAYS"),
        months=_attr(el, "MONTHS"),
        days_and_or=_and_or(_attr(el, "DAYS_AND_OR", default="OR") or "OR"),
        timezone=_attr(el, "TIMEZONE"),
    )
    out.append(folder)
    seen_body = False   # folder-level conds/vars end at the first JOB/sub-folder
    for child in el:
        tag = child.tag.upper() if isinstance(child.tag, str) else ""
        if tag == "JOB":
            seen_body = True
            folder.jobs.append(_parse_job(child, full_name, unknown))
        elif tag in _FOLDER_TAGS:
            seen_body = True
            _parse_folder(
                child, _FOLDER_TAGS[tag], unknown, out,
                parent=full_name, parent_datacenter=folder.datacenter,
            )
        elif tag == "INCOND" and not seen_body:
            folder.in_conds.append(_in_cond(child))
        elif tag == "OUTCOND" and not seen_body:
            folder.out_conds.append(_out_cond(child))
        elif tag == "VARIABLE" and not seen_body:
            folder.variables[_attr(child, "NAME")] = _attr(child, "VALUE")
        else:
            unknown[f"FOLDER/{tag or '?'}"] += 1


# ------------------------------------------------------------------ entry point

def parse_files(files: list[Path]) -> Deftable:
    """Parse DEFTABLE XML exports into one Deftable IR (document order kept)."""
    deftable = Deftable()
    unknown: Counter = Counter()
    for raw in files:
        path = Path(raw)
        root = ET.parse(path).getroot()
        root_tag = root.tag.upper() if isinstance(root.tag, str) else ""
        if root_tag in _FOLDER_TAGS:
            candidates: list[ET.Element] = [root]
        else:
            if root_tag != "DEFTABLE":
                unknown[f"ROOT/{root_tag or '?'}"] += 1
            candidates = list(root)
        for el in candidates:
            tag = el.tag.upper() if isinstance(el.tag, str) else ""
            if tag in _FOLDER_TAGS:
                _parse_folder(el, _FOLDER_TAGS[tag], unknown, deftable.folders)
            else:
                unknown[f"DEFTABLE/{tag or '?'}"] += 1
        deftable.source_files.append(str(path))
    if unknown:
        summary = ", ".join(f"{k} x{v}" for k, v in sorted(unknown.items()))
        print(f"parser: ignored unknown XML constructs: {summary}", file=sys.stderr)
    return deftable
