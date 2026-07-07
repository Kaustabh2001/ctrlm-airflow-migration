r"""Declarative Control-M job-type registry — the single source of truth the
DAG emitter consults (docs/impl-contracts-v3.md §V3-3).

The registry is an ORDERED list of :class:`RegistryEntry` rows; resolution is
first-match and the LAST row is a catch-all MANUAL stub, so every job maps to
exactly one entry. Each entry builds a :class:`TaskPlan` — a pure, renderable
description of the Airflow task (operator class name, literal kwargs, extra
imports/comments, upstream/downstream helper tasks, diagnostics).

On top of operator selection, :func:`apply_common_params` applies the
param-by-param Control-M -> Airflow mapping that holds for EVERY task
regardless of operator (retries, retry_delay, doc_md, priority_weight,
CONFIRM gate, pools, params, ON/DO actions, SLA...). The user-facing
documentation lives in docs/job-mapping-catalog.md and is kept in sync with
this module by tests/test_registry.py.

PRIORITY formula (documented here and in the catalog): Control-M priorities
are two-letter codes 'AA' (highest) .. 'ZZ' (lowest), i.e. 676 codes indexed
idx = (c0-'A')*26 + (c1-'A').  priority_weight = 100 - round(idx * 99 / 675),
a linear map onto 1..100 with 'AA' -> 100 and 'ZZ' -> 1. Purely numeric
priorities are clamped into 1..100. CRITICAL=1 floors the weight at 90.

This module never imports airflow (Airflow only appears in *generated* text)
and is fully deterministic: no wall clock, no randomness, sorted iteration.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

from .autoedit import translate
from .model import Job, PartitionConfig

# ---------------------------------------------------------------- statuses

FULL = "FULL"          # automatic, faithful mapping
PARTIAL = "PARTIAL"    # automatic mapping with caveats (diagnostics attached)
MANUAL = "MANUAL"      # placeholder stub — a human must migrate the job

_DEFAULT_SENSOR_TIMEOUT = 21600  # 6h, when the job has no MAXWAIT

# PowerShell sniff (v2 contract, unchanged): a `.ps1` script reference
# anywhere in the command, or a command whose first word is `powershell`.
_PS_SNIFF = re.compile(r"(?i)(?:\.ps1\b|\A\s*powershell\b)")

# appl_types (row 4) that ARE file transfers — endpoints need humans.
FILE_TRANSFER_APPL_TYPES = frozenset({"FILE_TRANS", "AFT", "MFT"})

# appl_types (row 5) we recognize but deliberately stub for manual migration.
KNOWN_MANUAL_APPL_TYPES = frozenset(
    {"SAP", "INFORMATICA", "HADOOP", "PEOPLESOFT", "WEBSERVICES", "JAVA", "MQ", "EMR"}
)

# task_types that are command-like (v1 semantics: Command | Job).
_COMMANDISH = frozenset({"", "command", "job"})
# appl_types that mean "plain OS command".
_OS_APPL = frozenset({"", "OS"})


# ---------------------------------------------------------------- rendering


class Raw(str):
    """A code expression rendered VERBATIM into the generated DAG file
    (as opposed to plain values, which are rendered via repr)."""

    __slots__ = ()


def render_value(value: object) -> str:
    """Render a TaskPlan kwarg value as Python source text (deterministic)."""
    if isinstance(value, Raw):
        return str(value)
    if isinstance(value, bool) or value is None:
        return repr(value)
    if isinstance(value, (int, float, str)):
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(render_value(v) for v in value) + "]"
    if isinstance(value, dict):
        return (
            "{"
            + ", ".join(f"{render_value(k)}: {render_value(v)}" for k, v in value.items())
            + "}"
        )
    raise TypeError(f"unrenderable TaskPlan value: {value!r}")  # pragma: no cover


def snake_case(name: str) -> str:
    """lowercase, non-alphanumeric -> _, collapse repeats, strip _."""
    s = re.sub(r"[^a-z0-9]+", "_", name.lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "task"


# ---------------------------------------------------------------- dataclasses


@dataclass
class ExtraTask:
    """A helper task emitted next to the main one (gate / sensor / trigger).

    ``base_id`` is the *suggested* task id; the emitter claims a unique final
    id. ``relation`` is "upstream" (extra >> main) or "downstream"
    (main >> extra)."""

    base_id: str
    operator: str
    kwargs: dict = field(default_factory=dict)
    imports: list[str] = field(default_factory=list)
    comments: list[str] = field(default_factory=list)
    relation: str = "upstream"


@dataclass
class TaskPlan:
    """Renderable description of one Airflow task (pure data, no airflow)."""

    entry: str                      # RegistryEntry.name that produced it
    status: str                     # FULL | PARTIAL | MANUAL
    operator: str                   # Airflow operator/sensor class name
    kwargs: dict = field(default_factory=dict)      # ordered, literal-renderable
    comments: list[str] = field(default_factory=list)   # lines above the task
    imports: list[str] = field(default_factory=list)    # full import lines
    upstream: list[ExtraTask] = field(default_factory=list)
    downstream: list[ExtraTask] = field(default_factory=list)
    outlets: list[str] = field(default_factory=list)    # extra Dataset URIs
    diagnostics: list[tuple[str, str, str, str]] = field(default_factory=list)
    # ^ (level, code, message, subject) — merged into PartitionResult by emit
    needs_manual_stub: bool = False  # generated file must define _ctm_manual_stub


@dataclass
class RegistryContext:
    """What builders may consult: node mapping, config, scope information."""

    node_map: dict[str, dict[str, str]] = field(default_factory=dict)
    config: PartitionConfig = field(default_factory=PartitionConfig)
    scope: str = ""
    assignments: dict[str, str] = field(default_factory=dict)  # uid -> dag_id
    # pools collected across the whole emit run -> <scope>/config/pools.json
    pools: dict[str, dict] = field(default_factory=dict)       # name -> {slots, source}


@dataclass(frozen=True)
class RegistryEntry:
    """One ordered row of the job-type registry."""

    name: str                       # stable identifier (synced with the catalog)
    ctm_type: str                   # human description of what it matches
    status: str                     # nominal status: FULL | PARTIAL | MANUAL
    operator: str                   # nominal Airflow operator (for docs)
    imports: tuple[str, ...]        # nominal imports (for docs)
    matches: Callable[[Job, RegistryContext], bool]
    build: Callable[[Job, RegistryContext], TaskPlan]


# ---------------------------------------------------------------- helpers


def _is_powershell(command: str) -> bool:
    return bool(_PS_SNIFF.search(command or ""))


def _is_plain_command(job: Job) -> bool:
    """Command/Job task with a plain-OS appl_type (specific appl_types are
    caught by earlier registry rows; unknown ones fall to the catch-all)."""
    return (
        (job.task_type or "").strip().lower() in _COMMANDISH
        and (job.appl_type or "").strip().upper() in _OS_APPL
    )


def _node_os(job: Job, ctx: RegistryContext) -> str:
    entry = ctx.node_map.get(job.node_id) if job.node_id else None
    return (entry or {}).get("os", "linux")


def _sensor_timeout(job: Job) -> int:
    return job.maxwait * 86400 if job.maxwait > 0 else _DEFAULT_SENSOR_TIMEOUT


def _minutes(hhmm: str) -> int:
    return int(hhmm[:2]) * 60 + int(hhmm[2:4])


def _translate_command(job: Job, plan: TaskPlan) -> str:
    """AUTOEDIT-translate the job command; record unresolved-variable TODOs."""
    command, unresolved = translate(job.command)
    command = command.replace("%%JOBNAME", job.name)
    if unresolved:
        plan.comments.append(f"# TODO unresolved AUTOEDIT: {', '.join(unresolved)}")
        plan.diagnostics.append(
            (
                "warn",
                "UNRESOLVED_AUTOEDIT",
                f"unresolved AUTOEDIT variables {', '.join(unresolved)} in command",
                job.uid,
            )
        )
    return command


def _resolve_ssh_conn(job: Job, ctx: RegistryContext, plan: TaskPlan) -> str:
    """v1/v2 connection resolution: nodes.yaml, else ssh_<nodeid> + diagnostic."""
    entry = ctx.node_map.get(job.node_id) if job.node_id else None
    if entry is not None:
        return entry["conn_id"]
    conn = f"ssh_{job.node_id or 'default'}"
    if job.node_id:
        plan.diagnostics.append(
            (
                "warn",
                "UNMAPPED_NODE",
                f"NODEID {job.node_id!r} not in nodes.yaml; using {conn!r}",
                job.node_id,
            )
        )
    return conn


# ---------------------------------------------------------------- builders


def _build_dummy(job: Job, ctx: RegistryContext) -> TaskPlan:
    return TaskPlan(
        entry="dummy",
        status=FULL,
        operator="EmptyOperator",
        imports=["from airflow.operators.empty import EmptyOperator"],
    )


def _build_filewatch(job: Job, ctx: RegistryContext) -> TaskPlan:
    plan = TaskPlan(
        entry="filewatch",
        status=FULL,
        operator="CtmFileWatcherSensor",
        imports=["from ctm_plugins.sensors import CtmFileWatcherSensor"],
        comments=[
            "# custom component: ctm_plugins.sensors.CtmFileWatcherSensor (plugins.zip)"
        ],
    )
    path = _translate_command(job, plan)
    if not path:
        plan.comments.append(
            "# TODO FILEWATCH: the export carries no path — set `path` manually"
        )
    # deferrable-safe defaults: reschedule mode frees the worker slot between pokes
    plan.kwargs = {
        "path": path,
        "mode": "reschedule",
        "poke_interval": 60,
        "timeout": _sensor_timeout(job),
    }
    return plan


def _build_database(job: Job, ctx: RegistryContext) -> TaskPlan:
    plan = TaskPlan(
        entry="database",
        status=FULL,
        operator="SQLExecuteQueryOperator",
        imports=[
            "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator"
        ],
        comments=["# provider: apache-airflow-providers-common-sql"],
    )
    entry = ctx.node_map.get(job.node_id) if job.node_id else None
    if entry is not None:
        conn = entry["conn_id"]
    else:  # PARTIAL: db connection unmapped — placeholder conn id + diagnostic
        conn = f"db_{job.node_id or 'default'}"
        plan.status = PARTIAL
        plan.comments.append(
            f"# TODO DATABASE node {job.node_id or '?'} unmapped — create conn {conn!r}"
        )
        plan.diagnostics.append(
            (
                "warn",
                "UNMAPPED_NODE",
                f"NODEID {job.node_id!r} not in nodes.yaml; using {conn!r}",
                job.node_id or job.uid,
            )
        )
    sql = _translate_command(job, plan)
    plan.kwargs = {"conn_id": conn, "sql": sql}
    return plan


def _manual_stub_plan(entry_name: str, job: Job, extra_comments: list[str]) -> TaskPlan:
    job_type = f"TASKTYPE={job.task_type or '-'}/APPL_TYPE={job.appl_type or '-'}"
    plan = TaskPlan(
        entry=entry_name,
        status=MANUAL,
        operator="PythonOperator",
        imports=["from airflow.operators.python import PythonOperator"],
        comments=["# MANUAL: no automatic Airflow mapping — migrate this job by hand"]
        + extra_comments,
        needs_manual_stub=True,
    )
    plan.kwargs = {
        "python_callable": Raw("_ctm_manual_stub"),
        "op_kwargs": {"job_type": job_type, "job_name": job.name},
    }
    plan.diagnostics.append(
        (
            "warn",
            "UNSUPPORTED_TYPE",
            f"job type {job_type} has no automatic mapping (registry: {entry_name})",
            job.uid,
        )
    )
    return plan


def _build_file_transfer(job: Job, ctx: RegistryContext) -> TaskPlan:
    hints = [
        f"# TODO FILE_TRANS ({job.appl_type}): transfer direction and endpoints need humans.",
        f"# source/target hints: NODEID={job.node_id or '-'}, RUN_AS={job.run_as or '-'}",
    ]
    if job.description:
        hints.append(f"# description: {job.description}")
    for name in sorted(job.variables):
        hints.append(f"# variable {name}={job.variables[name]}")
    return _manual_stub_plan("file_transfer", job, hints)


def _build_known_manual(job: Job, ctx: RegistryContext) -> TaskPlan:
    hints = [
        f"# TODO APPL_TYPE {job.appl_type}: map to the matching Airflow provider manually."
    ]
    if job.description:
        hints.append(f"# description: {job.description}")
    return _manual_stub_plan("known_manual", job, hints)


def _build_winrm(job: Job, ctx: RegistryContext) -> TaskPlan:
    plan = TaskPlan(
        entry="windows_command",
        status=FULL,
        operator="WinRMOperator",
        imports=[
            "from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator"
        ],
        comments=["# provider: apache-airflow-providers-microsoft-winrm"],
    )
    conn = _resolve_ssh_conn(job, ctx, plan)
    command = _translate_command(job, plan)
    plan.kwargs = {"ssh_conn_id": conn, "command": command}
    return plan


def _build_ssh(job: Job, ctx: RegistryContext) -> TaskPlan:
    plan = TaskPlan(
        entry="ssh_command",
        status=FULL,
        operator="SSHOperator",
        imports=["from airflow.providers.ssh.operators.ssh import SSHOperator"],
    )
    conn = _resolve_ssh_conn(job, ctx, plan)
    command = _translate_command(job, plan)
    plan.kwargs = {"ssh_conn_id": conn, "command": command}
    return plan


def _build_unsupported(job: Job, ctx: RegistryContext) -> TaskPlan:
    return _manual_stub_plan(
        "unsupported",
        job,
        [
            f"# original TASKTYPE={job.task_type or '-'}, APPL_TYPE={job.appl_type or '-'}"
        ],
    )


# ---------------------------------------------------------------- the registry

REGISTRY: list[RegistryEntry] = [
    RegistryEntry(
        name="dummy",
        ctm_type="TASKTYPE=Dummy or synthetic __FOLDER_START__/__FOLDER_END__",
        status=FULL,
        operator="EmptyOperator",
        imports=("from airflow.operators.empty import EmptyOperator",),
        matches=lambda job, ctx: job.synthetic or (job.task_type or "") == "Dummy",
        build=_build_dummy,
    ),
    RegistryEntry(
        name="filewatch",
        ctm_type="TASKTYPE=FileWatch or APPL_TYPE=FILEWATCH",
        status=FULL,
        operator="CtmFileWatcherSensor",
        imports=("from ctm_plugins.sensors import CtmFileWatcherSensor",),
        matches=lambda job, ctx: (job.task_type or "").strip().lower() == "filewatch"
        or (job.appl_type or "").strip().upper() == "FILEWATCH",
        build=_build_filewatch,
    ),
    RegistryEntry(
        name="database",
        ctm_type="APPL_TYPE=DATABASE",
        status=FULL,
        operator="SQLExecuteQueryOperator",
        imports=(
            "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator",
        ),
        matches=lambda job, ctx: (job.appl_type or "").strip().upper() == "DATABASE",
        build=_build_database,
    ),
    RegistryEntry(
        name="file_transfer",
        ctm_type="APPL_TYPE in {FILE_TRANS, AFT, MFT}",
        status=MANUAL,
        operator="PythonOperator",
        imports=("from airflow.operators.python import PythonOperator",),
        matches=lambda job, ctx: (job.appl_type or "").strip().upper()
        in FILE_TRANSFER_APPL_TYPES,
        build=_build_file_transfer,
    ),
    RegistryEntry(
        name="known_manual",
        ctm_type="APPL_TYPE in {SAP, INFORMATICA, HADOOP, PEOPLESOFT, WEBSERVICES, JAVA, MQ, EMR}",
        status=MANUAL,
        operator="PythonOperator",
        imports=("from airflow.operators.python import PythonOperator",),
        matches=lambda job, ctx: (job.appl_type or "").strip().upper()
        in KNOWN_MANUAL_APPL_TYPES,
        build=_build_known_manual,
    ),
    RegistryEntry(
        name="windows_command",
        ctm_type="TASKTYPE=Command/Job on a windows node or a PowerShell command",
        status=FULL,
        operator="WinRMOperator",
        imports=(
            "from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator",
        ),
        matches=lambda job, ctx: _is_plain_command(job)
        and (_node_os(job, ctx) == "windows" or _is_powershell(job.command)),
        build=_build_winrm,
    ),
    RegistryEntry(
        name="ssh_command",
        ctm_type="TASKTYPE=Command/Job (linux, default)",
        status=FULL,
        operator="SSHOperator",
        imports=("from airflow.providers.ssh.operators.ssh import SSHOperator",),
        matches=lambda job, ctx: _is_plain_command(job),
        build=_build_ssh,
    ),
    # LAST row: the catch-all MANUAL stub — matches everything.
    RegistryEntry(
        name="unsupported",
        ctm_type="any other TASKTYPE/APPL_TYPE (catch-all)",
        status=MANUAL,
        operator="PythonOperator",
        imports=("from airflow.operators.python import PythonOperator",),
        matches=lambda job, ctx: True,
        build=_build_unsupported,
    ),
]


def resolve(job: Job, ctx: RegistryContext) -> RegistryEntry:
    """First-match resolution over the ordered registry (never fails: the
    last entry is a catch-all)."""
    for entry in REGISTRY:
        if entry.matches(job, ctx):
            return entry
    raise AssertionError("unreachable: catch-all registry row matches everything")


def build_task_plan(job: Job, ctx: RegistryContext) -> TaskPlan:
    """resolve + build (operator selection only — no common params yet)."""
    return resolve(job, ctx).build(job, ctx)


# ---------------------------------------------------------------- priorities


_PRIORITY_CODE = re.compile(r"[A-Z]{2}\Z")


def priority_weight_of(priority: str, critical: bool) -> int | None:
    """PRIORITY/CRITICAL -> Airflow priority_weight (see module docstring).

    'AA' -> 100 down to 'ZZ' -> 1 (linear over the 676 codes); numeric strings
    are clamped into 1..100; CRITICAL floors the result at 90 (and yields 90
    on its own when no priority is set). None = leave Airflow's default.
    """
    weight: int | None = None
    p = (priority or "").strip().upper()
    if _PRIORITY_CODE.fullmatch(p):
        idx = (ord(p[0]) - 65) * 26 + (ord(p[1]) - 65)  # AA=0 .. ZZ=675
        weight = 100 - round(idx * 99 / 675)
    elif p.isdigit():
        weight = max(1, min(100, int(p)))
    if critical:
        weight = max(weight or 0, 90)
    return weight


# ---------------------------------------------------------------- common params


def _add_pool(ctx: RegistryContext, name: str, slots: int, source: str) -> None:
    """Collect a pool for <scope>/config/pools.json (slots = max seen)."""
    existing = ctx.pools.get(name)
    if existing is None:
        ctx.pools[name] = {"slots": slots, "source": source}
        return
    existing["slots"] = max(existing["slots"], slots)
    if source == "quantitative":  # quantitative sizing wins over control's 1
        existing["source"] = source


def _apply_resources(plan: TaskPlan, job: Job, ctx: RegistryContext) -> None:
    """QUANTITATIVE -> pool/pool_slots; CONTROL type E -> pool with 1 slot."""
    quants = [r for r in job.resources if r.kind == "quantitative"]
    ctrl_e = [
        r
        for r in job.resources
        if r.kind == "control" and (r.control_type or "E").strip().upper() != "S"
    ]
    ctrl_s = [
        r
        for r in job.resources
        if r.kind == "control" and (r.control_type or "").strip().upper() == "S"
    ]
    for r in quants:
        _add_pool(ctx, r.name, max(1, r.quant), "quantitative")
    for r in ctrl_e:
        _add_pool(ctx, r.name, 1, "control")
    pool_res = quants[0] if quants else (ctrl_e[0] if ctrl_e else None)
    if pool_res is not None:
        plan.kwargs["pool"] = pool_res.name
        if pool_res.kind == "quantitative":
            plan.kwargs["pool_slots"] = max(1, pool_res.quant)
        others = sorted(
            {r.name for r in quants + ctrl_e if r.name != pool_res.name}
        )
        if len(quants) + len(ctrl_e) > 1:
            plan.comments.append(
                "# TODO multiple resources: pool maps "
                f"{pool_res.name!r} only; also declared: {', '.join(others) or '-'}"
            )
            plan.diagnostics.append(
                (
                    "warn",
                    "MULTI_RESOURCE",
                    f"only resource {pool_res.name!r} mapped to a pool; "
                    f"extra resources: {', '.join(others) or '(duplicates)'}",
                    job.uid,
                )
            )
    for r in ctrl_s:
        plan.comments.append(
            f"# NOTE CONTROL {r.name} type S (shared) has no pool mapping"
        )


def _apply_notifications(
    plan: TaskPlan, job: Job, ctx: RegistryContext, task_id: str
) -> None:
    """ON/DO actions + SHOUTs -> callbacks, email, TriggerDagRun, outlets, SLA."""
    notify_used = False

    def _use_notify(dest: str, message: str, is_domail: bool, label: str) -> None:
        nonlocal notify_used
        if notify_used:  # only the FIRST NOTOK notification becomes the callback
            plan.comments.append(
                f"# TODO additional NOTOK notification not mapped: {label}"
            )
            plan.diagnostics.append(
                (
                    "warn",
                    "UNMAPPED_ACTION",
                    f"additional NOTOK notification {label} not mapped",
                    job.uid,
                )
            )
            return
        notify_used = True
        plan.comments.append(f"# ON NOTOK {label} -> on_failure_callback (ctm_shout)")
        plan.kwargs["on_failure_callback"] = Raw(
            f"ctm_shout(dest={dest!r}, message={message!r})"
        )
        plan.imports.append("from ctm_plugins.callbacks import ctm_shout")
        if is_domail and "@" in dest:
            plan.kwargs["email"] = [dest]
            plan.kwargs["email_on_failure"] = True

    def _unmapped(label: str) -> None:
        plan.comments.append(f"# TODO unmapped ON/DO action: {label}")
        plan.diagnostics.append(
            ("warn", "UNMAPPED_ACTION", f"unmapped ON/DO action {label}", job.uid)
        )

    for on in job.on_actions:
        code = (on.code or "").strip().upper()
        is_notok = "NOTOK" in code
        for act in on.actions:
            a_type = str(act.get("type", "")).upper()
            if a_type in ("DOMAIL", "DOSHOUT") and is_notok:
                dest = act.get("DEST", "")
                message = act.get("MESSAGE", "") or act.get("SUBJECT", "")
                _use_notify(dest, message, a_type == "DOMAIL", f"{a_type} {dest}")
            elif a_type == "DOFORCEJOB":
                target = act.get("JOBNAME", "")
                _apply_forcejob(plan, job, ctx, target, is_notok, on.code)
            elif a_type == "DOCOND":
                sign = str(act.get("SIGN", "ADD")).strip().upper()
                name = act.get("NAME", "")
                if sign in ("ADD", "+") and name:
                    uri = f"ctrlm://cond/{name}"
                    if uri not in plan.outlets:
                        plan.comments.append(
                            f"# ON {on.stmt} {on.code} DOCOND ADD -> Dataset outlet {uri}"
                        )
                        plan.outlets.append(uri)
                else:
                    _unmapped(f"DOCOND {name} SIGN={sign or '?'}")
            else:
                _unmapped(f"{a_type or '?'} (ON {on.stmt} {on.code})")

    for shout in job.shouts:
        when = str(shout.get("when", "")).strip().upper()
        dest = str(shout.get("dest", ""))
        message = str(shout.get("message", ""))
        if when == "NOTOK":
            _use_notify(dest, message, "@" in dest, f"SHOUT {dest}")
        elif when == "LATE":
            _apply_sla(plan, job, dest)
        else:
            _unmapped(f"SHOUT WHEN {when or '?'} DEST {dest}")


def _apply_forcejob(
    plan: TaskPlan,
    job: Job,
    ctx: RegistryContext,
    target: str,
    is_notok: bool,
    code: str,
) -> None:
    """ON ... DOFORCEJOB -> downstream TriggerDagRunOperator."""
    trigger_rule = "one_failed" if is_notok else "all_success"
    candidates = sorted(
        uid for uid in ctx.assignments if uid.rpartition("/")[2] == target
    )
    comments = [f"# ON code {code or '*'} DOFORCEJOB {target or '?'}"]
    if candidates:
        dag_id = ctx.assignments[candidates[0]]
    else:  # forced job outside this run's scope — best-effort literal dag id
        dag_id = snake_case(target)
        comments.append(
            f"# TODO forced job {target or '?'} not in this scope — verify dag id"
        )
        plan.diagnostics.append(
            (
                "warn",
                "FORCEJOB_UNRESOLVED",
                f"DOFORCEJOB target {target!r} not found in this scope; "
                f"emitted literal dag id {dag_id!r}",
                job.uid,
            )
        )
    plan.downstream.append(
        ExtraTask(
            base_id=f"force_{snake_case(target)}",
            operator="TriggerDagRunOperator",
            kwargs={"trigger_dag_id": dag_id, "trigger_rule": trigger_rule},
            imports=[
                "from airflow.operators.trigger_dagrun import TriggerDagRunOperator"
            ],
            comments=comments,
            relation="downstream",
        )
    )


def _apply_sla(plan: TaskPlan, job: Job, dest: str) -> None:
    """SHOUT WHEN LATE -> sla=timedelta(TIMETO - TIMEFROM), an approximation."""
    if job.timeto and job.timefrom and len(job.timeto) >= 4 and len(job.timefrom) >= 4:
        minutes = (_minutes(job.timeto) - _minutes(job.timefrom)) % 1440
        plan.comments.append(
            f"# SHOUT WHEN LATE ({dest}) -> sla approximation: TIMETO - TIMEFROM"
        )
        plan.kwargs["sla"] = Raw(f"timedelta(minutes={minutes})")
        plan.imports.append("from datetime import timedelta")
        plan.diagnostics.append(
            (
                "warn",
                "SLA_APPROX",
                f"SHOUT WHEN LATE approximated as sla=timedelta(minutes={minutes})",
                job.uid,
            )
        )
    else:
        plan.comments.append(
            "# TODO SHOUT WHEN LATE: no TIMETO window — sla not derivable"
        )
        plan.diagnostics.append(
            (
                "warn",
                "SLA_APPROX",
                "SHOUT WHEN LATE has no TIMETO window; sla not emitted",
                job.uid,
            )
        )


def apply_common_params(
    plan: TaskPlan, job: Job, ctx: RegistryContext, task_id: str
) -> TaskPlan:
    """Apply the param-by-param mapping that holds for EVERY task
    (docs/job-mapping-catalog.md §2). Mutates and returns *plan*."""
    # MAXRERUN -> retries; RERUNINTERVAL -> retry_delay
    if job.maxrerun > 0:
        plan.kwargs["retries"] = job.maxrerun
    if job.rerun_interval_minutes > 0:
        plan.kwargs["retry_delay"] = Raw(
            f"timedelta(minutes={job.rerun_interval_minutes})"
        )
        plan.imports.append("from datetime import timedelta")

    # PRIORITY ('AA' highest .. 'ZZ' lowest) + CRITICAL -> priority_weight
    weight = priority_weight_of(job.priority, job.critical)
    if weight is not None:
        if job.priority:
            plan.comments.append(
                f"# PRIORITY {job.priority} -> priority_weight {weight} "
                "(linear: AA=100 .. ZZ=1)"
            )
        if job.critical:
            plan.comments.append("# CRITICAL=1 -> priority_weight floored at 90")
        plan.kwargs["priority_weight"] = weight

    # QUANTITATIVE / CONTROL resources -> pool / pool_slots (+ pools.json)
    _apply_resources(plan, job, ctx)

    # VARIABLE -> task-level params (literal dict, sorted)
    if job.variables:
        plan.kwargs["params"] = {k: job.variables[k] for k in sorted(job.variables)}

    # DESCRIPTION -> doc_md
    if job.description:
        plan.kwargs["doc_md"] = job.description

    # ON/DO actions + SHOUTs (callback, email, force-job, outlets, sla)
    _apply_notifications(plan, job, ctx, task_id)

    # CONFIRM -> upstream manual approval gate (ctm_plugins sensor)
    if job.confirm:
        plan.upstream.append(
            ExtraTask(
                base_id=f"confirm_{task_id}",
                operator="CtmApprovalGateSensor",
                kwargs={"mode": "reschedule", "poke_interval": 60},
                imports=["from ctm_plugins.sensors import CtmApprovalGateSensor"],
                comments=[
                    "# CONFIRM=1 -> approval gate: set Airflow Variable "
                    "ctm_approve/<dag_id>/<task_id>/<ds> to 'yes'"
                ],
                relation="upstream",
            )
        )
    return plan
