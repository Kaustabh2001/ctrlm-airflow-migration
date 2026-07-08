"""Tests for ctrlm_core.operator_registry (V3-3) — pure logic, no Airflow.

Covers: registry order + first-match resolution, per-row TaskPlan contents,
the priority_weight formula, the param-by-param common mapping
(apply_common_params), value rendering, and the registry <-> catalog sync
test that keeps docs/job-mapping-catalog.md from drifting.
"""
from __future__ import annotations

import re
from pathlib import Path

from ctrlm_core.model import Job, OnAction, PartitionConfig, Resource
from ctrlm_core.operator_registry import (
    FULL,
    MANUAL,
    PARTIAL,
    REGISTRY,
    Raw,
    RegistryContext,
    apply_common_params,
    build_task_plan,
    priority_weight_of,
    render_value,
    resolve,
    snake_case,
)

REPO = Path(__file__).resolve().parents[1]

NODE_MAP = {
    "prdnode1": {"conn_id": "ssh_prdnode1", "os": "linux", "type": ""},
    "winnode1": {"conn_id": "winrm_winnode1", "os": "windows", "type": ""},
    "dbnode1": {"conn_id": "bank_dwh", "os": "linux", "type": "db"},
}


def _job(name="J", folder="F", **kw) -> Job:
    return Job(name=name, folder=folder, **kw)


def _ctx(**kw) -> RegistryContext:
    kw.setdefault("node_map", dict(NODE_MAP))
    kw.setdefault("config", PartitionConfig())
    return RegistryContext(**kw)


def _plan(job: Job, ctx: RegistryContext | None = None):
    ctx = ctx or _ctx()
    plan = build_task_plan(job, ctx)
    apply_common_params(plan, job, ctx, snake_case(job.name))
    return plan, ctx


# ---------------------------------------------------------------- structure


def test_registry_rows_and_order():
    names = [e.name for e in REGISTRY]
    assert names == [
        "dummy",
        "filewatch",
        "database",
        "file_transfer",
        "known_manual",
        "windows_command",
        "ssh_command",
        "unsupported",
    ]
    statuses = {e.name: e.status for e in REGISTRY}
    assert statuses["dummy"] == FULL
    assert statuses["file_transfer"] == MANUAL
    assert statuses["known_manual"] == MANUAL
    assert statuses["unsupported"] == MANUAL
    # v4 operator policy: custom operators ONLY where they add capability;
    # command jobs stay native SSH/WinRM, Dummy stays EmptyOperator.
    operators = {e.name: e.operator for e in REGISTRY}
    assert operators["dummy"] == "EmptyOperator"
    assert operators["filewatch"] == "CtmFileWatcherSensor"
    assert operators["database"] == "CtmDatabaseJob"
    assert operators["file_transfer"] == "CtmManualJob"
    assert operators["known_manual"] == "CtmManualJob"
    assert operators["windows_command"] == "WinRMOperator"
    assert operators["ssh_command"] == "SSHOperator"
    assert operators["unsupported"] == "CtmManualJob"


def test_catch_all_is_last_and_matches_anything():
    last = REGISTRY[-1]
    assert last.name == "unsupported"
    weird = _job(task_type="Detached", appl_type="MYSTERY_MIDDLEWARE")
    assert last.matches(weird, _ctx())
    assert resolve(weird, _ctx()).name == "unsupported"


# ---------------------------------------------------------------- resolution


def test_resolution_first_match():
    ctx = _ctx()
    cases = [
        (_job(task_type="Dummy"), "dummy"),
        (_job(task_type="Command", synthetic=True), "dummy"),
        (_job(task_type="FileWatch"), "filewatch"),
        (_job(task_type="Command", appl_type="FILEWATCH"), "filewatch"),
        (_job(task_type="Command", appl_type="DATABASE", node_id="dbnode1"), "database"),
        (_job(task_type="Job", appl_type="FILE_TRANS"), "file_transfer"),
        (_job(task_type="Job", appl_type="AFT"), "file_transfer"),
        (_job(task_type="Job", appl_type="MFT"), "file_transfer"),
        (_job(task_type="Job", appl_type="SAP"), "known_manual"),
        (_job(task_type="Job", appl_type="INFORMATICA"), "known_manual"),
        (_job(task_type="Job", appl_type="HADOOP"), "known_manual"),
        (_job(task_type="Command", node_id="winnode1", command="run.bat"), "windows_command"),
        (_job(task_type="Command", node_id="prdnode1", command="powershell -File a.ps1"), "windows_command"),
        (_job(task_type="Command", node_id="prdnode1", command="x.sh"), "ssh_command"),
        (_job(task_type="Command", appl_type="OS", command="x.sh"), "ssh_command"),
        (_job(task_type="Command", appl_type="FOOBAR"), "unsupported"),
        (_job(task_type="Detached"), "unsupported"),
    ]
    for job, expected in cases:
        assert resolve(job, ctx).name == expected, (job.task_type, job.appl_type)


def test_dummy_row_wins_over_everything():
    # synthetic FILE_TRANS-flavoured node still becomes an EmptyOperator
    job = _job(task_type="Dummy", appl_type="DATABASE")
    assert resolve(job, _ctx()).name == "dummy"


# ---------------------------------------------------------------- row plans


def test_database_plan_mapped_conn():
    job = _job(
        name="BANK_BAL_CHECK",
        task_type="Command",
        appl_type="DATABASE",
        node_id="dbnode1",
        command="SELECT COUNT(*) FROM balances WHERE ds='%%ODATE'",
    )
    plan, _ = _plan(job)
    assert plan.entry == "database"
    assert plan.status == FULL
    assert plan.operator == "CtmDatabaseJob"
    # connection resolution moved to parse time (V4-2): the plan carries the
    # NODEID only — no conn_id literal reaches the generated file.
    assert plan.kwargs["node"] == "dbnode1"
    assert "conn_id" not in plan.kwargs
    assert "{{ ds_nodash }}" in plan.kwargs["sql"]  # AUTOEDIT applied to SQL
    assert "from ctm_plugins.operators import CtmDatabaseJob" in plan.imports
    assert not plan.diagnostics


def test_database_plan_unmapped_conn_is_partial():
    job = _job(task_type="Command", appl_type="DATABASE", node_id="ghost", command="SELECT 1")
    plan, _ = _plan(job)
    assert plan.status == PARTIAL
    assert plan.kwargs["node"] == "ghost"  # emitted anyway; resolved at parse time
    assert "conn_id" not in plan.kwargs
    assert ("warn", "UNMAPPED_NODE") in {(d[0], d[1]) for d in plan.diagnostics}
    assert any("nodes.yaml" in c for c in plan.comments)  # TODO comment


def test_filewatch_plan():
    job = _job(
        task_type="FileWatch",
        command="/in/feed_%%ODATE.csv",
        maxwait=2,
    )
    plan, _ = _plan(job)
    assert plan.operator == "CtmFileWatcherSensor"
    assert plan.kwargs["path"] == "/in/feed_{{ ds_nodash }}.csv"
    assert plan.kwargs["mode"] == "reschedule"
    assert plan.kwargs["poke_interval"] == 60
    assert plan.kwargs["timeout"] == 2 * 86400  # MAXWAIT days
    assert "from ctm_plugins.sensors import CtmFileWatcherSensor" in plan.imports


def test_filewatch_default_timeout_and_missing_path():
    plan, _ = _plan(_job(task_type="FileWatch", command=""))
    assert plan.kwargs["timeout"] == 21600
    assert any("TODO FILEWATCH" in c for c in plan.comments)


def test_manual_stub_plans_name_original_types():
    for appl, entry in (("FILE_TRANS", "file_transfer"), ("SAP", "known_manual")):
        plan, _ = _plan(_job(name="X_JOB", task_type="Job", appl_type=appl))
        assert plan.entry == entry
        assert plan.status == MANUAL
        assert plan.operator == "CtmManualJob"
        assert "from ctm_plugins.operators import CtmManualJob" in plan.imports
        assert plan.kwargs["ctm_task_type"] == "Job"
        assert plan.kwargs["ctm_appl_type"] == appl
        assert plan.kwargs["ctm_job"] == "X_JOB"
        codes = {d[1] for d in plan.diagnostics}
        assert "UNSUPPORTED_TYPE" in codes


def test_unsupported_catch_all_stub_defaults_dashes():
    plan, _ = _plan(_job(name="ODD", task_type="Detached"))
    assert plan.entry == "unsupported"
    assert plan.operator == "CtmManualJob"
    assert plan.kwargs["ctm_task_type"] == "Detached"
    assert plan.kwargs["ctm_appl_type"] == "-"  # no APPL_TYPE in the export
    assert plan.kwargs["ctm_job"] == "ODD"


def test_file_transfer_comments_name_source_target_hints():
    job = _job(
        task_type="Job",
        appl_type="FILE_TRANS",
        node_id="prdnode1",
        description="from /bank/out to sftp://partner/inbound",
        variables={"FT-DEST": "partner"},
    )
    plan, _ = _plan(job)
    text = "\n".join(plan.comments)
    assert "NODEID=prdnode1" in text
    assert "from /bank/out to sftp://partner/inbound" in text
    assert "FT-DEST=partner" in text


def test_ssh_and_winrm_plans_keep_v2_semantics():
    ssh, _ = _plan(_job(task_type="Command", node_id="prdnode1", command="run.sh %%ODATE"))
    assert ssh.operator == "SSHOperator"
    assert ssh.kwargs["ssh_conn_id"] == "ssh_prdnode1"
    assert "{{ ds_nodash }}" in ssh.kwargs["command"]
    win, _ = _plan(_job(task_type="Command", node_id="winnode1", command="run.bat"))
    assert win.operator == "WinRMOperator"
    assert win.kwargs["ssh_conn_id"] == "winrm_winnode1"
    assert any("microsoft-winrm" in c for c in win.comments)
    unmapped, _ = _plan(_job(task_type="Command", node_id="ghost", command="a.sh"))
    assert unmapped.kwargs["ssh_conn_id"] == "ssh_ghost"
    assert ("UNMAPPED_NODE", "ghost") in {(d[1], d[3]) for d in unmapped.diagnostics}


# ---------------------------------------------------------------- priorities


def test_priority_weight_formula():
    assert priority_weight_of("AA", False) == 100
    assert priority_weight_of("ZZ", False) == 1
    assert priority_weight_of("MM", False) == 52  # idx 324 -> 100 - round(47.52)
    assert priority_weight_of("", False) is None
    assert priority_weight_of("50", False) == 50
    assert priority_weight_of("0", False) == 1  # numeric clamp
    assert priority_weight_of("999", False) == 100


def test_critical_floors_at_90():
    assert priority_weight_of("", True) == 90
    assert priority_weight_of("ZZ", True) == 90
    assert priority_weight_of("AA", True) == 100  # floor never lowers


def test_priority_and_critical_in_plan():
    plan, _ = _plan(_job(task_type="Command", priority="AA", critical=True, command="x"))
    assert plan.kwargs["priority_weight"] == 100
    text = "\n".join(plan.comments)
    assert "PRIORITY AA" in text and "AA=100 .. ZZ=1" in text
    assert "CRITICAL=1" in text


# ---------------------------------------------------------------- common params


def test_retries_retry_delay_params_doc_md():
    job = _job(
        task_type="Command",
        command="x.sh",
        maxrerun=2,
        rerun_interval_minutes=10,
        variables={"B": "2", "A": "1"},
        description="Nightly load",
    )
    plan, _ = _plan(job)
    assert plan.kwargs["retries"] == 2
    assert plan.kwargs["retry_delay"] == Raw("timedelta(minutes=10)")
    assert "from datetime import timedelta" in plan.imports
    assert plan.kwargs["params"] == {"A": "1", "B": "2"}
    assert list(plan.kwargs["params"]) == ["A", "B"]  # sorted
    assert plan.kwargs["doc_md"] == "Nightly load"


def test_zero_maxrerun_omits_task_retries():
    plan, _ = _plan(_job(task_type="Command", command="x.sh"))
    assert "retries" not in plan.kwargs
    assert "retry_delay" not in plan.kwargs


def test_confirm_adds_upstream_approval_gate():
    plan, _ = _plan(_job(name="PAY_RUN", task_type="Command", command="x", confirm=True))
    assert len(plan.upstream) == 1
    gate = plan.upstream[0]
    assert gate.base_id == "confirm_pay_run"
    assert gate.operator == "CtmApprovalGateSensor"
    assert gate.relation == "upstream"
    assert "from ctm_plugins.sensors import CtmApprovalGateSensor" in gate.imports
    assert any("ctm_approve/" in c for c in gate.comments)


def test_quantitative_resource_pool_and_pools_collection():
    ctx = _ctx()
    job = _job(
        task_type="Command",
        command="x",
        resources=[Resource(name="SETTLE_SLOTS", kind="quantitative", quant=3)],
    )
    plan = build_task_plan(job, ctx)
    apply_common_params(plan, job, ctx, "settle")
    assert plan.kwargs["pool"] == "SETTLE_SLOTS"
    assert plan.kwargs["pool_slots"] == 3
    assert ctx.pools == {"SETTLE_SLOTS": {"slots": 3, "source": "quantitative"}}
    # a second job with a smaller quant: pools.json keeps the max
    job2 = _job(
        name="J2",
        task_type="Command",
        command="y",
        resources=[Resource(name="SETTLE_SLOTS", kind="quantitative", quant=2)],
    )
    plan2 = build_task_plan(job2, ctx)
    apply_common_params(plan2, job2, ctx, "j2")
    assert ctx.pools["SETTLE_SLOTS"]["slots"] == 3
    assert not plan.diagnostics and not plan2.diagnostics


def test_control_exclusive_resource_is_a_one_slot_pool():
    ctx = _ctx()
    job = _job(
        task_type="Command",
        command="x",
        resources=[Resource(name="GL_LOCK", kind="control", control_type="E")],
    )
    plan = build_task_plan(job, ctx)
    apply_common_params(plan, job, ctx, "j")
    assert plan.kwargs["pool"] == "GL_LOCK"
    assert "pool_slots" not in plan.kwargs  # 1 slot is the Airflow default
    assert ctx.pools == {"GL_LOCK": {"slots": 1, "source": "control"}}


def test_multi_resource_partial_diagnostic():
    job = _job(
        task_type="Command",
        command="x",
        resources=[
            Resource(name="A_SLOTS", kind="quantitative", quant=2),
            Resource(name="B_SLOTS", kind="quantitative", quant=1),
        ],
    )
    plan, ctx = _plan(job)
    assert plan.kwargs["pool"] == "A_SLOTS"  # first resource wins
    assert ("MULTI_RESOURCE", job.uid) in {(d[1], d[3]) for d in plan.diagnostics}
    assert set(ctx.pools) == {"A_SLOTS", "B_SLOTS"}  # both still land in pools.json


def test_domail_notok_callback_and_email():
    job = _job(
        task_type="Command",
        command="x",
        on_actions=[
            OnAction(
                stmt="*",
                code="NOTOK",
                actions=[
                    {"type": "DOMAIL", "DEST": "ops@corp.com", "MESSAGE": "it broke"}
                ],
            )
        ],
    )
    plan, _ = _plan(job)
    cb = plan.kwargs["on_failure_callback"]
    assert isinstance(cb, Raw)
    assert "ctm_shout(dest='ops@corp.com', message='it broke')" in str(cb)
    assert plan.kwargs["email"] == ["ops@corp.com"]
    assert plan.kwargs["email_on_failure"] is True
    assert "from ctm_plugins.callbacks import ctm_shout" in plan.imports


def test_doshout_and_shout_notok_callback_without_email():
    for job in (
        _job(
            task_type="Command",
            command="x",
            on_actions=[
                OnAction(code="NOTOK", actions=[{"type": "DOSHOUT", "DEST": "OPS"}])
            ],
        ),
        _job(
            task_type="Command",
            command="x",
            shouts=[{"when": "NOTOK", "dest": "OPS", "message": "boom"}],
        ),
    ):
        plan, _ = _plan(job)
        assert "ctm_shout(dest='OPS'" in str(plan.kwargs["on_failure_callback"])
        assert "email" not in plan.kwargs


def test_second_notok_notification_is_flagged_unmapped():
    job = _job(
        task_type="Command",
        command="x",
        on_actions=[
            OnAction(
                code="NOTOK",
                actions=[
                    {"type": "DOMAIL", "DEST": "a@corp.com"},
                    {"type": "DOSHOUT", "DEST": "OPS"},
                ],
            )
        ],
    )
    plan, _ = _plan(job)
    assert plan.kwargs["email"] == ["a@corp.com"]  # first one won
    assert "UNMAPPED_ACTION" in {d[1] for d in plan.diagnostics}


def test_doforcejob_resolved_via_assignments():
    ctx = _ctx(assignments={"BANK_EOD/BANK_RECON": "bank_eod", "F/J": "d"})
    job = _job(
        task_type="Command",
        command="x",
        on_actions=[
            OnAction(code="NOTOK", actions=[{"type": "DOFORCEJOB", "JOBNAME": "BANK_RECON"}])
        ],
    )
    plan = build_task_plan(job, ctx)
    apply_common_params(plan, job, ctx, "j")
    assert len(plan.downstream) == 1
    force = plan.downstream[0]
    assert force.base_id == "force_bank_recon"
    assert force.operator == "TriggerDagRunOperator"
    assert force.kwargs["trigger_dag_id"] == "bank_eod"
    assert force.kwargs["trigger_rule"] == "one_failed"  # NOTOK code
    assert force.relation == "downstream"
    assert "FORCEJOB_UNRESOLVED" not in {d[1] for d in plan.diagnostics}


def test_doforcejob_ok_code_uses_all_success():
    job = _job(
        task_type="Command",
        command="x",
        on_actions=[
            OnAction(code="OK", actions=[{"type": "DOFORCEJOB", "JOBNAME": "NEXT_JOB"}])
        ],
    )
    plan, _ = _plan(job, _ctx(assignments={"F/NEXT_JOB": "next"}))
    assert plan.downstream[0].kwargs["trigger_rule"] == "all_success"


def test_doforcejob_unresolved_falls_back_to_snake_case():
    job = _job(
        task_type="Command",
        command="x",
        on_actions=[
            OnAction(code="NOTOK", actions=[{"type": "DOFORCEJOB", "JOBNAME": "OTHER_SCOPE_JOB"}])
        ],
    )
    plan, _ = _plan(job)  # empty assignments
    assert plan.downstream[0].kwargs["trigger_dag_id"] == "other_scope_job"
    assert ("FORCEJOB_UNRESOLVED", job.uid) in {(d[1], d[3]) for d in plan.diagnostics}


def test_docond_add_becomes_dataset_outlet():
    job = _job(
        task_type="Command",
        command="x",
        on_actions=[
            OnAction(
                code="OK",
                actions=[
                    {"type": "DOCOND", "NAME": "EXTRA-OK", "SIGN": "ADD"},
                    {"type": "DOCOND", "NAME": "GONE", "SIGN": "DEL"},
                ],
            )
        ],
    )
    plan, _ = _plan(job)
    assert plan.outlets == ["ctrlm://cond/EXTRA-OK"]
    assert "UNMAPPED_ACTION" in {d[1] for d in plan.diagnostics}  # the DEL


def test_shout_late_sla_approximation():
    job = _job(
        task_type="Command",
        command="x",
        timefrom="1900",
        timeto="2300",
        shouts=[{"when": "LATE", "dest": "OPS", "message": "late"}],
    )
    plan, _ = _plan(job)
    assert plan.kwargs["sla"] == Raw("timedelta(minutes=240)")
    assert ("SLA_APPROX", job.uid) in {(d[1], d[3]) for d in plan.diagnostics}


def test_shout_late_without_timeto_flags_only():
    job = _job(
        task_type="Command",
        command="x",
        timefrom="1900",
        shouts=[{"when": "LATE", "dest": "OPS", "message": "late"}],
    )
    plan, _ = _plan(job)
    assert "sla" not in plan.kwargs
    assert "SLA_APPROX" in {d[1] for d in plan.diagnostics}
    assert any("TODO SHOUT WHEN LATE" in c for c in plan.comments)


def test_unmapped_do_actions_get_todo_and_diagnostic():
    job = _job(
        task_type="Command",
        command="x",
        on_actions=[
            OnAction(code="NOTOK", actions=[{"type": "DOACTION", "ACTION": "STOPCYCLIC"}])
        ],
    )
    plan, _ = _plan(job)
    assert any("# TODO unmapped ON/DO action" in c for c in plan.comments)
    assert ("UNMAPPED_ACTION", job.uid) in {(d[1], d[3]) for d in plan.diagnostics}


# ---------------------------------------------------------------- rendering


def test_render_value():
    assert render_value(Raw("timedelta(minutes=5)")) == "timedelta(minutes=5)"
    assert render_value("abc") == "'abc'"
    assert render_value(3) == "3"
    assert render_value(True) == "True"
    assert render_value(["a", Raw("X")]) == "['a', X]"
    assert render_value({"k": "v"}) == "{'k': 'v'}"


def test_plans_are_deterministic():
    job = _job(
        task_type="Command",
        command="x %%ODATE",
        priority="BB",
        confirm=True,
        resources=[Resource(name="R", kind="quantitative", quant=2)],
        on_actions=[OnAction(code="NOTOK", actions=[{"type": "DOMAIL", "DEST": "a@b.c"}])],
    )
    a, _ = _plan(job)
    b, _ = _plan(job)
    assert a == b


# ---------------------------------------------------------------- catalog sync


def _catalog_text() -> str:
    return (REPO / "docs" / "job-mapping-catalog.md").read_text(encoding="utf-8")


def test_registry_and_catalog_stay_in_sync():
    """Every RegistryEntry.name appears verbatim in the catalog, and the
    catalog's marker list names exactly the registry rows (no drift)."""
    text = _catalog_text()
    marker = re.search(r"<!-- registry-names\n(.*?)\n-->", text, re.S)
    assert marker, "docs/job-mapping-catalog.md must carry the registry-names marker"
    doc_names = [l.strip() for l in marker.group(1).splitlines() if l.strip()]
    registry_names = [e.name for e in REGISTRY]
    assert doc_names == registry_names  # same rows, same order
    body = text.replace(marker.group(0), "")  # names must ALSO appear in prose
    for name in registry_names:
        assert f"`{name}`" in body, f"registry row {name!r} missing from the catalog"


def test_catalog_documents_the_four_sections():
    text = _catalog_text()
    assert "## 1. Job types" in text
    assert "## 2. Parameters" in text
    assert "## 3. Custom components" in text
    assert "## 4. Configuration files" in text


def test_catalog_documents_playbook_and_roadmap():
    """V4-2: the extension playbook and the application-type roadmap."""
    text = _catalog_text()
    assert "Adding a new job type" in text and "playbook" in text
    assert "Application-type roadmap" in text
    # the playbook decision tree is provider-first
    playbook = text[text.index("Adding a new job type"):]
    assert playbook.index("provider") < playbook.index("Ctm*")
    assert "MANUAL" in playbook
    # every roadmap application type from the contract is present
    for app in (
        "SAP",
        "Informatica",
        "Hadoop / Spark",
        "AWS (Lambda / Batch / Step Functions)",
        "Azure (Data Factory / Functions)",
        "Databricks",
        "Snowflake",
        "Kubernetes",
        "AFT / MFT",
        "Web Services / REST",
        "Java",
        "IBM i (OS/400)",
        "z/OS members",
    ):
        assert app in text, f"roadmap entry {app!r} missing from the catalog"
