r"""DAG file emitter — one Airflow 2.9+ (MWAA) file per DagSpec.

Implements the emit.py contract in docs/impl-contracts.md, the V2-3
operator-mapping delta, and the V3-3 registry integration
(docs/impl-contracts-v3.md): operator selection is delegated to the ordered
job-type registry in ``operator_registry.py`` (single source of truth, first
match wins, catch-all MANUAL stub last) and the full param-by-param Control-M
-> Airflow mapping (retries, retry_delay, doc_md, priority_weight, CONFIRM
gates, pools, params, ON/DO actions, SLA...) is applied to EVERY task via
``operator_registry.apply_common_params``. The user-facing description of the
whole mapping is docs/job-mapping-catalog.md.

V4-2 (docs/impl-contracts-v4.md): DATABASE jobs emit
``ctm_plugins.operators.CtmDatabaseJob(node=..., sql=...)`` (connection
resolved at parse time inside the operator — no conn literal in the file) and
every MANUAL row emits ``ctm_plugins.operators.CtmManualJob`` instead of the
old PythonOperator + module-level stub prelude. Everything else is v3 as-is:
command jobs stay plain SSHOperator/WinRMOperator with the common params
translated inline at codegen time.

Rendering: Jinja2 template (templates/dag.py.j2, FileSystemLoader relative to
this file), black formatting (fallback: raw render), py_compile syntax
validation (raises). Quantitative/control resources seen during a run are
collected into ``<scope>/config/pools.json`` next to the dags directory.

V5-1 (docs/impl-contracts-v5.md): the SAME TaskPlan pass that renders the
code also records a task-level plan of every generated DAG into
``<scope>/dag_plans.json`` (task kinds job/gate/confirm/wait/force/
folder_start/folder_end, source_uid, task_group, upstream edges mirroring the
emitted ``>>`` dependencies, outlets, external waits, schedule/dataset info).
Purely additive — the rendered .py output is byte-identical to v4.

Airflow is NEVER imported here — it only appears in the *generated* code text
(including imports from the write-once ``ctm_plugins`` package deployed to
MWAA as plugins.zip). All iteration orders are sorted; output is a pure
function of the inputs.
"""
from __future__ import annotations

import json
import keyword
import os
import py_compile
import tempfile
from pathlib import Path

import black
import yaml
from jinja2 import Environment, FileSystemLoader

from .desugar import END_JOB_NAME, START_JOB_NAME
from .model import (
    WIRE_DATASET,
    WIRE_PREV,
    WIRE_SENSOR,
    CrossLink,
    CtmGraph,
    DagSpec,
    Diagnostic,
    Job,
    PartitionConfig,
    PartitionResult,
)
from .operator_registry import (
    ExtraTask,
    Raw,
    RegistryContext,
    TaskPlan,
    apply_common_params,
    build_task_plan,
    render_value,
    snake_case,
)

try:  # canonical helper owned by the graph module (identical semantics)
    from .schedule import rel_minutes
except Exception:  # pragma: no cover — fallback until schedule.py lands

    def rel_minutes(hhmm: str, new_day: str) -> int:
        """ODATE clock: minutes since New Day time, (t - newday) % 1440."""

        def _mins(t: str) -> int:
            return int(t[:2]) * 60 + int(t[2:4])

        return (_mins(hhmm) - _mins(new_day)) % 1440


_DEFAULT_SENSOR_TIMEOUT = 21600  # 6h, when the consumer has no MAXWAIT


# ---------------------------------------------------------------- helpers


class _IdRegistry:
    """Deterministic unique-id allocator: base, base_2, base_3, ..."""

    def __init__(self) -> None:
        self._used: set[str] = set()

    def claim(self, base: str) -> str:
        candidate, n = base, 1
        while candidate in self._used:
            n += 1
            candidate = f"{base}_{n}"
        self._used.add(candidate)
        return candidate


def _var_name(task_id: str, used: set[str]) -> str:
    """A safe Python variable name for a task assignment."""
    v = task_id
    if not v.isidentifier() or keyword.iskeyword(v) or v in {"dag", "DAG"}:
        v = f"t_{v}"
    while v in used:
        v += "_"
    used.add(v)
    return v


def _job_of(graph: CtmGraph, uid: str) -> Job:
    job = graph.nodes.get(uid)
    if job is not None:
        return job
    folder, _, name = uid.partition("/")
    return Job(name=name or uid, folder=folder, task_type="Dummy", synthetic=True)


def _load_node_map(mapping_path: str | Path) -> dict[str, dict[str, str]]:
    """NODEID -> {"conn_id", "os", "type"} from mapping-config/nodes.yaml.

    Accepts both schemas (missing file -> {}):
    - v2: ``defaults: {os: ...}`` + ``nodes: {<id>: {conn_id: ..., os: ...,
      type: ...}}`` — ``type: db`` marks database endpoints (V3).
    - v1: flat ``<id>: <conn_id>`` entries at the top level.
    Entries missing ``os`` inherit ``defaults.os`` (ultimately ``linux``).
    """
    p = Path(mapping_path)
    if not p.exists():
        return {}
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return {}
    defaults = data.get("defaults")
    if not isinstance(defaults, dict):
        defaults = {}
    default_os = str(defaults.get("os", "linux")).strip().lower() or "linux"
    entries = data.get("nodes")
    if not isinstance(entries, dict):  # v1 flat file (ignore a stray defaults key)
        entries = {k: v for k, v in data.items() if k != "defaults"}
    node_map: dict[str, dict[str, str]] = {}
    for node in sorted(entries, key=str):
        value = entries[node]
        if isinstance(value, dict):
            conn = str(value.get("conn_id", f"ssh_{node}"))
            node_os = str(value.get("os", default_os)).strip().lower() or default_os
            node_type = str(value.get("type", "")).strip().lower()
        else:
            conn = str(value)
            node_os = default_os
            node_type = ""
        node_map[str(node)] = {"conn_id": conn, "os": node_os, "type": node_type}
    return node_map


def _add_diag(result: PartitionResult, level: str, code: str, message: str, subject: str) -> None:
    """Append a diagnostic once per (code, subject)."""
    for d in result.diagnostics:
        if d.code == code and d.subject == subject:
            return
    result.diagnostics.append(
        Diagnostic(level=level, code=code, message=message, subject=subject)
    )


def _merge_plan_diagnostics(result: PartitionResult, plan: TaskPlan) -> None:
    for level, code, message, subject in plan.diagnostics:
        _add_diag(result, level, code, message, subject)


# ---------------------------------------------------------------- planning


def _plan_dag(spec: DagSpec, graph: CtmGraph) -> dict:
    """Pre-compute task ids (and TaskGroup paths) for one DagSpec.

    Planned for EVERY dag before rendering any, because ExternalTaskSensor in
    a consumer dag needs the producer's final (group-prefixed) task id.
    """
    ids = _IdRegistry()
    jobs = {uid: _job_of(graph, uid) for uid in sorted(spec.jobs)}
    folders = sorted({j.folder for j in jobs.values()})
    use_groups = len(folders) > 1
    group_ids: dict[str, str] = {}
    if use_groups:
        gids = _IdRegistry()
        for folder in folders:
            group_ids[folder] = gids.claim(snake_case(folder))
    tasks: dict[str, dict] = {}
    for uid in sorted(jobs):
        job = jobs[uid]
        task_id = ids.claim(snake_case(job.name))
        group = group_ids.get(job.folder)
        tasks[uid] = {
            "task_id": task_id,
            "group": group,
            "full_id": f"{group}.{task_id}" if group else task_id,
        }
    return {
        "jobs": jobs,
        "folders": folders,
        "use_groups": use_groups,
        "group_ids": group_ids,
        "tasks": tasks,
        "ids": ids,
    }


# ------------------------------------------------- dag_plans.json recording
#
# V5-1: the render pass below records every task it declares (and every `>>`
# edge it emits) into a per-dag record; emit_dags serializes the records to
# <scope>/dag_plans.json. The record is produced by the SAME pass that writes
# the code — never re-derived — so it mirrors the .py files exactly.

# helper-task operator -> plan "kind" (anything else declared via ExtraTask
# is a gate-like structural task)
_EXTRA_TASK_KINDS = {
    "CtmApprovalGateSensor": "confirm",
    "TriggerDagRunOperator": "force",
}


def _job_kind(job: Job) -> str:
    """Plan kind of a graph-member task: job | folder_start | folder_end."""
    if job.synthetic:
        if job.name == START_JOB_NAME:
            return "folder_start"
        if job.name == END_JOB_NAME:
            return "folder_end"
    return "job"


def _new_dag_record(spec: DagSpec) -> dict:
    """Mutable per-dag recorder (finalized by _finalize_dag_record)."""
    return {
        "schedule": spec.schedule or None,
        "dataset_triggered": bool(spec.dataset_triggered),
        "datasets": sorted(set(spec.datasets)),
        "tasks": {},  # task_id -> {kind, operator, source_uid, task_group, upstream:set}
        "outlets": [],
        "external_waits": [],
    }


def _record_task(
    rec: dict,
    task_id: str,
    kind: str,
    operator: str,
    source_uid: str | None = None,
    task_group: str | None = None,
) -> None:
    rec["tasks"][task_id] = {
        "kind": kind,
        "operator": operator,
        "source_uid": source_uid,
        "task_group": task_group,
        "upstream": set(),
    }


def _record_edge(rec: dict, upstream_id: str, downstream_id: str) -> None:
    """Mirror one emitted ``upstream >> downstream`` dependency."""
    rec["tasks"][downstream_id]["upstream"].add(upstream_id)


def _finalize_dag_record(rec: dict) -> dict:
    """Freeze the recorder into the contract schema (sorted, JSON-ready)."""
    return {
        "schedule": rec["schedule"],
        "dataset_triggered": rec["dataset_triggered"],
        "datasets": rec["datasets"],
        "tasks": [
            {
                "task_id": task_id,
                "kind": t["kind"],
                "operator": t["operator"],
                "source_uid": t["source_uid"],
                "task_group": t["task_group"],
                "upstream": sorted(t["upstream"]),
            }
            for task_id, t in sorted(rec["tasks"].items())
        ],
        "outlets": sorted(
            rec["outlets"], key=lambda o: (o["task_id"], o["dataset"])
        ),
        "external_waits": sorted(
            rec["external_waits"],
            key=lambda w: (w["task_id"], w["external_dag_id"], w["external_task_id"]),
        ),
    }


# ---------------------------------------------------------------- rendering


def _docstring(spec: DagSpec, result: PartitionResult, plan: dict) -> str:
    lines = [
        f"DAG {spec.dag_id} — generated from a Control-M export "
        f"(strategy: {result.strategy}).",
        "",
        f"Source folders: {', '.join(spec.folders) or '-'}",
        "Source files: see ir.json / graph.json in the strategy output directory.",
        "Job provenance:",
    ]
    for uid in sorted(spec.jobs):
        job = plan["jobs"][uid]
        kind = "synthetic" if job.synthetic else (job.task_type or "Command")
        node = job.node_id or "-"
        lines.append(f"  - {uid} (task_type={kind}, node={node})")
    lines += ["", "Deterministic output — regenerate instead of editing by hand."]
    text = "\n".join(lines)
    # keep the generated file syntactically safe whatever the export contains
    return text.replace("\\", "\\\\").replace('"""', "'''")


def _schedule_repr(spec: DagSpec) -> str:
    if spec.dataset_triggered:
        uris = sorted(set(spec.datasets))
        if uris:
            return "[" + ", ".join(f'Dataset("{u}")' for u in uris) + "]"
        return "None"
    if spec.schedule:
        return f'"{spec.schedule}"'
    return "None"


def _render_call(operator: str, task_id: str, kwargs: dict) -> str:
    parts = [f'task_id="{task_id}"']
    parts.extend(f"{k}={render_value(v)}" for k, v in kwargs.items())
    return f"{operator}({', '.join(parts)})"


def _extra_task_lines(
    extra: ExtraTask,
    main_var: str,
    plan: dict,
    used_vars: set[str],
    imports: set[str],
    rec: dict,
    main_task_id: str,
    task_group: str | None,
) -> list[str]:
    """Declare one helper task (gate/sensor/trigger) and its edge to main."""
    eid = plan["ids"].claim(extra.base_id)
    evar = _var_name(eid, used_vars)
    lines = list(extra.comments)
    lines.append(f"{evar} = {_render_call(extra.operator, eid, extra.kwargs)}")
    _record_task(
        rec,
        eid,
        _EXTRA_TASK_KINDS.get(extra.operator, "gate"),
        extra.operator,
        None,
        task_group,
    )
    if extra.relation == "downstream":
        lines.append(f"{main_var} >> {evar}")
        _record_edge(rec, main_task_id, eid)
    else:
        lines.append(f"{evar} >> {main_var}")
        _record_edge(rec, eid, main_task_id)
    imports.update(extra.imports)
    return lines


def _task_lines(
    uid: str,
    plan: dict,
    var_of: dict[str, str],
    used_vars: set[str],
    ctx: RegistryContext,
    outlets: dict[str, list[str]],
    result: PartitionResult,
    imports: set[str],
    rec: dict,
) -> list[str]:
    """Code lines (unindented) declaring one task (+ helper tasks)."""
    job: Job = plan["jobs"][uid]
    info = plan["tasks"][uid]
    task_id = info["task_id"]
    var = _var_name(task_id, used_vars)
    var_of[uid] = var

    # registry consultation: operator selection + full param-by-param mapping
    tplan = build_task_plan(job, ctx)
    apply_common_params(tplan, job, ctx, task_id)
    _merge_plan_diagnostics(result, tplan)
    imports.update(tplan.imports)
    _record_task(rec, task_id, _job_kind(job), tplan.operator, uid, info["group"])

    # dataset outlets: cross-link producer outlets + DOCOND ADD extras
    uris = sorted(set(outlets.get(uid, [])) | set(tplan.outlets))
    if uris:
        imports.add("from airflow.datasets import Dataset")
        tplan.kwargs["outlets"] = Raw(
            "[" + ", ".join(f'Dataset("{u}")' for u in uris) + "]"
        )
        for u in uris:
            rec["outlets"].append({"task_id": task_id, "dataset": u})

    kind = "synthetic" if job.synthetic else (job.task_type or "Command")
    appl = f", appl_type={job.appl_type}" if job.appl_type else ""
    lines = [
        f"# Control-M job: {uid} (task_type={kind}{appl}) "
        f"[registry: {tplan.entry}, {tplan.status}]"
    ]
    lines.extend(tplan.comments)
    lines.append(f"{var} = {_render_call(tplan.operator, task_id, tplan.kwargs)}")
    for extra in tplan.upstream + tplan.downstream:
        lines.extend(
            _extra_task_lines(
                extra, var, plan, used_vars, imports, rec, task_id, info["group"]
            )
        )
    return lines


def _render_dag(
    spec: DagSpec,
    graph: CtmGraph,
    result: PartitionResult,
    config: PartitionConfig,
    ctx: RegistryContext,
    plans: dict[str, dict],
    template,
) -> tuple[str, dict]:
    """Render one DAG file; returns (code text, finalized dag_plans record)."""
    plan = plans[spec.dag_id]
    member = set(spec.jobs)
    imports: set[str] = set()
    var_of: dict[str, str] = {}
    used_vars: set[str] = set()
    rec = _new_dag_record(spec)

    # ---- dataset outlets (this dag is PRODUCER) / consumer-side sensor links
    outlets: dict[str, list[str]] = {}
    consumer_links: list[CrossLink] = []
    links = sorted(result.cross_links, key=lambda l: (l.source, l.target, l.kind))
    for link in links:
        src_dag = result.assignments.get(link.source)
        tgt_dag = result.assignments.get(link.target)
        if (
            src_dag == spec.dag_id
            and link.mechanism == WIRE_DATASET
            and tgt_dag != spec.dag_id
        ):
            uris = outlets.setdefault(link.source, [])
            for cond in sorted(link.conds):
                uri = f"ctrlm://cond/{cond}"
                if uri not in uris:
                    uris.append(uri)
        if tgt_dag == spec.dag_id and link.target in member:
            if link.mechanism == WIRE_PREV:
                consumer_links.append(link)  # prev-run gates are valid intra-DAG too
            elif link.mechanism in ("", WIRE_SENSOR) and src_dag != spec.dag_id:
                consumer_links.append(link)

    # ---- task declarations, grouped per folder when the dag spans >1 folder
    body: list[str] = []
    if plan["use_groups"]:
        imports.add("from airflow.utils.task_group import TaskGroup")
        for folder in plan["folders"]:
            gid = plan["group_ids"][folder]
            body.append(f'with TaskGroup(group_id="{gid}"):')
            for uid in sorted(u for u, j in plan["jobs"].items() if j.folder == folder):
                for line in _task_lines(
                    uid, plan, var_of, used_vars, ctx, outlets, result, imports, rec
                ):
                    body.append("    " + line)
            body.append("")
    else:
        for uid in sorted(plan["jobs"]):
            body.extend(
                _task_lines(
                    uid, plan, var_of, used_vars, ctx, outlets, result, imports, rec
                )
            )
        body.append("")

    # ---- intra-DAG dependencies from e_edges between members
    pairs = sorted(
        {
            (var_of[e.source], var_of[e.target])
            for e in graph.e_edges
            if e.source in member and e.target in member and e.source != e.target
        }
    )
    if pairs:
        body.append("# intra-DAG dependencies (Control-M same-ODATE conditions)")
        body.extend(f"{up} >> {down}" for up, down in pairs)
        body.append("")
    for e in graph.e_edges:  # record the same edges by task id (dedup via set)
        if e.source in member and e.target in member and e.source != e.target:
            _record_edge(
                rec,
                plan["tasks"][e.source]["task_id"],
                plan["tasks"][e.target]["task_id"],
            )

    # ---- time gates: members starting later than the dag anchor (ODATE clock)
    if spec.anchor:
        anchor_rel = rel_minutes(spec.anchor, config.new_day_time)
        for uid in sorted(spec.jobs):
            job = plan["jobs"][uid]
            if not job.timefrom or len(job.timefrom) < 4:
                continue
            if rel_minutes(job.timefrom, config.new_day_time) <= anchor_rel:
                continue
            imports.add("from airflow.sensors.date_time import DateTimeSensorAsync")
            gate_id = plan["ids"].claim(f"gate_{plan['tasks'][uid]['task_id']}")
            gate_var = _var_name(gate_id, used_vars)
            days = 1 if int(job.timefrom) < int(spec.anchor) else 0
            target = (
                "{{ (data_interval_end + macros.timedelta(days=%d))"
                ".replace(hour=%d, minute=%d) }}"
                % (days, int(job.timefrom[:2]), int(job.timefrom[2:4]))
            )
            body.append(f"# time gate: {uid} starts at {job.timefrom} (TIMEFROM)")
            body.append(
                "# gate target on the Control-M ODATE clock — same semantics as"
                " ctm_plugins._odate.gate_target"
            )
            body.append(
                f'{gate_var} = DateTimeSensorAsync(task_id="{gate_id}", '
                f'target_datetime="{target}")'
            )
            body.append(f"{gate_var} >> {var_of[uid]}")
            body.append("")
            _record_task(rec, gate_id, "gate", "DateTimeSensorAsync")
            _record_edge(rec, gate_id, plan["tasks"][uid]["task_id"])

    # ---- cross-DAG links where this dag is the CONSUMER (sensor mechanisms)
    for link in consumer_links:
        imports.add("from airflow.sensors.external_task import ExternalTaskSensor")
        src_dag = result.assignments.get(link.source, "")
        src_plan = plans.get(src_dag)
        if src_plan is not None and link.source in src_plan["tasks"]:
            src_info = src_plan["tasks"][link.source]
            ext_task, wait_base = src_info["full_id"], src_info["task_id"]
        else:  # producer dag was not emitted — best-effort deterministic id
            wait_base = snake_case(link.source.partition("/")[2] or link.source)
            ext_task = wait_base
        wait_id = plan["ids"].claim(f"wait_{wait_base}")
        wait_var = _var_name(wait_id, used_vars)
        consumer_job = plan["jobs"][link.target]
        timeout = (
            consumer_job.maxwait * 86400
            if consumer_job.maxwait > 0
            else _DEFAULT_SENSOR_TIMEOUT
        )
        body.append(
            f"# cross-DAG link ({link.kind or 'cut'}): "
            f"{link.source} -> {link.target} conds: {', '.join(sorted(link.conds))}"
        )
        if link.mechanism == WIRE_PREV:
            body.append("# TODO align to previous run (PREV-qualified condition)")
        body.append(
            f'{wait_var} = ExternalTaskSensor(task_id="{wait_id}", '
            f'external_dag_id="{src_dag}", external_task_id="{ext_task}", '
            f'mode="reschedule", timeout={timeout})'
        )
        body.append(f"{wait_var} >> {var_of[link.target]}")
        body.append("")
        _record_task(rec, wait_id, "wait", "ExternalTaskSensor")
        _record_edge(rec, wait_id, plan["tasks"][link.target]["task_id"])
        rec["external_waits"].append(
            {
                "task_id": wait_id,
                "external_dag_id": src_dag,
                "external_task_id": ext_task,
            }
        )

    while body and body[-1] == "":
        body.pop()

    # ---- imports: only what the generated file uses, deterministic order
    if spec.dataset_triggered and spec.datasets:
        imports.add("from airflow.datasets import Dataset")
    needs_timedelta = "from datetime import timedelta" in imports
    imports.discard("from datetime import timedelta")
    import_lines = ["from airflow import DAG"]
    import_lines += sorted(
        l for l in imports if l.startswith("from airflow") and l != "from airflow import DAG"
    )
    import_lines += sorted(l for l in imports if l.startswith("from ctm_plugins"))
    import_lines += sorted(
        l
        for l in imports
        if not l.startswith("from airflow") and not l.startswith("from ctm_plugins")
    )
    import_lines.append(
        "from datetime import datetime, timedelta"
        if needs_timedelta
        else "from datetime import datetime"
    )

    retries = max((plan["jobs"][uid].maxrerun for uid in spec.jobs), default=0)
    # APPLICATION / SUB_APPLICATION -> dag-level tags (deduped)
    app_tags = sorted(
        {f"app:{j.application}" for j in plan["jobs"].values() if j.application}
        | {
            f"subapp:{j.sub_application}"
            for j in plan["jobs"].values()
            if j.sub_application
        }
    )
    tags: list[str] = []
    for t in (
        ["ctrlm", f"strategy:{result.strategy}"]
        + [f"folder:{f}" for f in sorted(spec.folders)]
        + app_tags
    ):
        if t not in tags:
            tags.append(t)

    code = template.render(
        docstring=_docstring(spec, result, plan),
        imports=import_lines,
        dag_id=spec.dag_id,
        schedule_repr=_schedule_repr(spec),
        retries=retries,
        tags_repr=json.dumps(tags),
        body="\n".join(("    " + line) if line else "" for line in body) or "    pass",
    )
    return code, _finalize_dag_record(rec)


# ---------------------------------------------------------------- validation


def _check_syntax(path: Path) -> None:
    """py_compile the generated file; raise on syntax errors (contract)."""
    fd, tmp = tempfile.mkstemp(suffix=".pyc")
    os.close(fd)
    try:
        py_compile.compile(str(path), cfile=tmp, doraise=True)
    finally:
        try:
            os.remove(tmp)
        except OSError:  # pragma: no cover
            pass


# ---------------------------------------------------------------- public API


def emit_dags(
    graph: CtmGraph,
    result: PartitionResult,
    dags_dir: Path,
    config: PartitionConfig,
    mapping_path: str | Path = "mapping-config/nodes.yaml",
) -> list[Path]:
    """Render one dags/{dag_id}.py per DagSpec. Returns the written paths.

    Deterministic: dags rendered in sorted dag_id order; every inner loop is
    sorted. Appends registry/param-mapping diagnostics (UNMAPPED_NODE,
    UNRESOLVED_AUTOEDIT, UNSUPPORTED_TYPE, MULTI_RESOURCE, FORCEJOB_UNRESOLVED,
    SLA_APPROX, UNMAPPED_ACTION) to ``result.diagnostics`` as it goes, and
    writes the pools collected from QUANTITATIVE/CONTROL resources to
    ``<scope>/config/pools.json`` (sibling of the dags directory).

    V5-1: also writes ``<scope>/dag_plans.json`` — the task-level plan of
    every generated DAG (kinds, operators, source uids, task groups, upstream
    edges mirroring the emitted ``>>`` lines, outlets, external waits,
    schedule/dataset info) — recorded by the same pass that rendered the code.

    Because the pipeline serializes ``<scope>/partition.json`` BEFORE calling
    emit_dags, a pre-existing ``partition.json`` sibling of the dags directory
    is rewritten at the end with the updated result, so the emit-time
    diagnostics above are persisted on disk (and visible to the dashboard)
    instead of living only in the returned PartitionResult.
    """
    dags_dir = Path(dags_dir)
    dags_dir.mkdir(parents=True, exist_ok=True)
    ctx = RegistryContext(
        node_map=_load_node_map(mapping_path),
        config=config,
        scope=dags_dir.resolve().parent.name,
        assignments=dict(result.assignments),
    )

    env = Environment(
        loader=FileSystemLoader(str(Path(__file__).resolve().parent / "templates")),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    template = env.get_template("dag.py.j2")

    specs = sorted(result.dags, key=lambda d: d.dag_id)
    plans = {spec.dag_id: _plan_dag(spec, graph) for spec in specs}

    written: list[Path] = []
    dag_plans: dict[str, dict] = {}
    for spec in specs:
        code, dag_plan = _render_dag(spec, graph, result, config, ctx, plans, template)
        dag_plans[spec.dag_id] = dag_plan
        try:
            code = black.format_str(code, mode=black.Mode())
        except Exception:  # fallback: raw render (still py_compile-checked)
            pass
        path = dags_dir / f"{spec.dag_id}.py"
        path.write_text(code, encoding="utf-8", newline="\n")
        _check_syntax(path)
        written.append(path)

    # V5-1: <scope>/dag_plans.json — the task-level plan of every generated
    # DAG, from the SAME pass that rendered the code above (sorted keys/lists,
    # trailing newline, LF endings -> byte-stable across reruns).
    (dags_dir.parent / "dag_plans.json").write_text(
        json.dumps(dag_plans, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    if ctx.pools:  # <scope>/config/pools.json — import with `airflow pools import`
        config_dir = dags_dir.parent / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        pools = [
            {
                "name": name,
                "slots": ctx.pools[name]["slots"],
                "source": ctx.pools[name]["source"],
            }
            for name in sorted(ctx.pools)
        ]
        (config_dir / "pools.json").write_text(
            json.dumps(pools, indent=2) + "\n", encoding="utf-8", newline="\n"
        )

    # persist emit-time diagnostics: the pipeline writes <scope>/partition.json
    # before emit_dags runs, so the diagnostics appended above would otherwise
    # never reach disk. Rewrite the file (same serialization as pipeline.py)
    # when it already exists next to the dags directory.
    partition_path = dags_dir.parent / "partition.json"
    if partition_path.exists():
        partition_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return written
