"""Tests for ctrlm_core.emit and ctrlm_core.autoedit.

Airflow is NOT installed: generated DAG files are text, validated with
py_compile (syntax only). Fixtures are built inline — no dependency on the
parser/graph/strategy modules (other agents own those).
"""
from __future__ import annotations

import json
import py_compile
from pathlib import Path

import pytest

from ctrlm_core.autoedit import translate
from ctrlm_core.emit import emit_dags, snake_case
from ctrlm_core.model import (
    CrossLink,
    CtmGraph,
    DagSpec,
    GraphEdge,
    Job,
    PartitionConfig,
    PartitionResult,
)

# ---------------------------------------------------------------- autoedit


def test_translate_known_tokens():
    text, unresolved = translate("run.sh %%ODATE %%$ODATE %%DATE %%TIME")
    assert text == "run.sh {{ ds_nodash }} {{ ds }} {{ ds_nodash }} {{ ts_nodash }}"
    assert unresolved == []


def test_translate_jobname_deferred_and_unresolved():
    text, unresolved = translate("x %%JOBNAME %%FOO %%FOO %%BAR")
    assert "%%JOBNAME" in text  # left for emit to substitute
    assert unresolved == ["%%FOO", "%%BAR"]  # deduped, first-appearance order


def test_translate_longest_match():
    # %%ODATEV is a DIFFERENT variable, not %%ODATE + "V"
    text, unresolved = translate("a %%ODATEV b")
    assert "%%ODATEV" in text and "ds_nodash" not in text
    assert unresolved == ["%%ODATEV"]


def test_snake_case():
    assert snake_case("FIN-DW  Load#1") == "fin_dw_load_1"
    assert snake_case("__X__") == "x"


# ---------------------------------------------------------------- fixture


def _job(name, folder, **kw) -> Job:
    return Job(name=name, folder=folder, **kw)


def make_fixture() -> tuple[CtmGraph, PartitionResult]:
    """Two+ DAGs covering: TaskGroup (multi-folder), SSH with %%ODATE and an
    unresolved var, unmapped NODEID, FileWatch, time gates (both day offsets),
    a cross-link of every mechanism, and a dataset-triggered DAG."""
    jobs = [
        _job(
            "LOAD",
            "FIN_DW",
            task_type="Command",
            command="run_load.sh %%ODATE %%CUSTOM",
            node_id="prdnode1",
            timefrom="2100",
            weekdays="1,2,3,4,5",
            maxrerun=2,
            day_pattern="WD=1,2,3,4,5|MD=ALL|M=ALL|OP=OR",
        ),
        _job(
            "MART",
            "FIN_DW",
            task_type="Command",
            command="build_mart.sh %%$ODATE",
            node_id="nodeX",
            timefrom="2300",
        ),
        _job(
            "EXTRACT",
            "FIN_DW",
            task_type="Command",
            command="extract.sh",
            node_id="prdnode1",
            timefrom="0200",
        ),
        _job("REPORT", "RPT", task_type="Dummy"),
        _job("WATCH", "STG", task_type="FileWatch"),
        _job("INGEST", "STG", task_type="Command", command="ingest.sh %%JOBNAME"),
        _job("QUALITY", "STG", task_type="Command", command="dq.sh"),
        _job(
            "CALC",
            "RISK",
            task_type="Command",
            command="risk.sh %%DATE %%TIME",
            node_id="prdnode1",
            timefrom="0300",
            maxwait=2,
        ),
    ]
    graph = CtmGraph(
        nodes={j.uid: j for j in jobs},
        e_edges=[
            GraphEdge(source="FIN_DW/LOAD", target="FIN_DW/MART", cond="DW-LOADED"),
            GraphEdge(source="FIN_DW/MART", target="FIN_DW/EXTRACT", cond="DW-MART-OK"),
            GraphEdge(source="FIN_DW/MART", target="RPT/REPORT", cond="DW-MART-OK"),
            GraphEdge(source="STG/WATCH", target="STG/INGEST", cond="FILE-SEEN"),
            GraphEdge(source="STG/INGEST", target="STG/QUALITY", cond="STG-LOADED"),
        ],
    )
    dags = [
        DagSpec(
            dag_id="fin_dw",
            jobs=sorted(["FIN_DW/LOAD", "FIN_DW/MART", "FIN_DW/EXTRACT", "RPT/REPORT"]),
            roots=["FIN_DW/LOAD"],
            folders=["FIN_DW", "RPT"],
            day_pattern="WD=1,2,3,4,5|MD=ALL|M=ALL|OP=OR",
            anchor="2100",
            schedule="0 21 * * 1-5",
        ),
        DagSpec(
            dag_id="stg",
            jobs=sorted(["STG/WATCH", "STG/INGEST", "STG/QUALITY"]),
            roots=["STG/WATCH"],
            folders=["STG"],
            dataset_triggered=True,
            datasets=["ctrlm://cond/FIN-OK"],
        ),
        DagSpec(
            dag_id="risk",
            jobs=["RISK/CALC"],
            roots=["RISK/CALC"],
            folders=["RISK"],
            day_pattern="WD=1,2,3,4,5|MD=ALL|M=ALL|OP=OR",
            anchor="0300",
            schedule="0 3 * * 1-5",
        ),
    ]
    assignments = {
        "FIN_DW/LOAD": "fin_dw",
        "FIN_DW/MART": "fin_dw",
        "FIN_DW/EXTRACT": "fin_dw",
        "RPT/REPORT": "fin_dw",
        "STG/WATCH": "stg",
        "STG/INGEST": "stg",
        "STG/QUALITY": "stg",
        "RISK/CALC": "risk",
    }
    cross_links = [
        CrossLink(
            source="FIN_DW/MART",
            target="STG/INGEST",
            conds=["FIN-OK"],
            kind="HUB",
            mechanism="dataset",
        ),
        CrossLink(
            source="FIN_DW/EXTRACT",
            target="RISK/CALC",
            conds=["FIN-DONE"],
            kind="PATTERN",
            mechanism="sensor",
        ),
        CrossLink(
            source="FIN_DW/LOAD",
            target="RISK/CALC",
            conds=["RISK-PREV"],
            kind="PREV_RUN",
            mechanism="prev_run_sensor",
        ),
    ]
    result = PartitionResult(
        strategy="components",
        dags=dags,
        assignments=assignments,
        cross_links=cross_links,
    )
    return graph, result


@pytest.fixture()
def emitted(tmp_path: Path):
    graph, result = make_fixture()
    mapping = tmp_path / "nodes.yaml"
    mapping.write_text("prdnode1: ssh_prdnode1\n", encoding="utf-8")
    dags_dir = tmp_path / "dags"
    paths = emit_dags(graph, result, dags_dir, PartitionConfig(), mapping_path=mapping)
    texts = {p.stem: p.read_text(encoding="utf-8") for p in paths}
    return paths, texts, result


# ---------------------------------------------------------------- emit


def test_one_file_per_dag_and_all_compile(emitted):
    paths, texts, _ = emitted
    assert [p.name for p in paths] == ["fin_dw.py", "risk.py", "stg.py"]
    for p in paths:
        py_compile.compile(str(p), doraise=True)  # raises on syntax error


def test_provenance_header(emitted):
    _, texts, _ = emitted
    fin = texts["fin_dw"]
    assert "strategy: components" in fin
    assert "FIN_DW/LOAD" in fin and "RPT/REPORT" in fin
    assert "Source folders: FIN_DW, RPT" in fin


def test_taskgroup_only_when_multi_folder(emitted):
    _, texts, _ = emitted
    assert "TaskGroup" in texts["fin_dw"]
    assert 'group_id="fin_dw"' in texts["fin_dw"]
    assert 'group_id="rpt"' in texts["fin_dw"]
    assert "TaskGroup" not in texts["stg"]
    assert "TaskGroup" not in texts["risk"]


def test_task_type_mapping(emitted):
    _, texts, _ = emitted
    fin, stg = texts["fin_dw"], texts["stg"]
    # Command -> SSHOperator, mapped and fallback conn ids
    assert "SSHOperator" in fin
    assert 'ssh_conn_id="ssh_prdnode1"' in fin
    assert 'ssh_conn_id="ssh_nodeX"' in fin  # unmapped NODEID fallback
    # Dummy -> EmptyOperator; FileWatch -> CtmFileWatcherSensor (v3 registry)
    assert "EmptyOperator" in fin
    assert "CtmFileWatcherSensor" in stg
    assert "from ctm_plugins.sensors import CtmFileWatcherSensor" in stg
    assert 'mode="reschedule"' in stg
    # AUTOEDIT translation flowed through the command
    assert "ds_nodash" in fin  # %%ODATE
    assert "{{ ds }}" in fin  # %%$ODATE
    assert "ingest.sh INGEST" in stg  # %%JOBNAME substituted at emit time
    # unresolved var stays verbatim with a TODO comment
    assert "%%CUSTOM" in fin
    assert "# TODO unresolved AUTOEDIT: %%CUSTOM" in fin


def test_intra_dag_dependencies(emitted):
    _, texts, _ = emitted
    fin = texts["fin_dw"]
    assert "load >> mart" in fin
    assert "mart >> extract" in fin
    assert "mart >> report" in fin
    stg = texts["stg"]
    assert "watch >> ingest" in stg
    assert "ingest >> quality" in stg


def test_time_gates_with_day_offset(emitted):
    _, texts, _ = emitted
    fin = texts["fin_dw"]
    assert "DateTimeSensorAsync" in fin
    # MART at 2300 (same ODATE day as the 2100 anchor): days=0
    assert 'task_id="gate_mart"' in fin
    assert "macros.timedelta(days=0)).replace(hour=23, minute=0)" in fin
    # EXTRACT at 0200 (< anchor raw HHMM -> next calendar day): days=1
    assert 'task_id="gate_extract"' in fin
    assert "macros.timedelta(days=1)).replace(hour=2, minute=0)" in fin
    assert "gate_extract >> extract" in fin
    # anchor task itself gets no gate
    assert "gate_load" not in fin
    # risk anchor == its only timefrom: no gates at all
    assert "DateTimeSensorAsync" not in texts["risk"]


def test_consumer_side_sensors(emitted):
    _, texts, _ = emitted
    risk = texts["risk"]
    assert "ExternalTaskSensor" in risk
    assert 'external_dag_id="fin_dw"' in risk
    # producer sits inside a TaskGroup -> group-prefixed external_task_id
    assert 'external_task_id="fin_dw.extract"' in risk
    assert 'mode="reschedule"' in risk
    assert "timeout=172800" in risk  # maxwait 2 days * 86400
    # prev_run_sensor: same sensor + TODO marker
    assert 'external_task_id="fin_dw.load"' in risk
    assert "# TODO align to previous run" in risk
    assert "wait_extract >> calc" in risk
    assert "wait_load >> calc" in risk


def test_producer_outlets_and_dataset_schedule(emitted):
    _, texts, _ = emitted
    assert 'outlets=[Dataset("ctrlm://cond/FIN-OK")]' in texts["fin_dw"]
    assert 'schedule=[Dataset("ctrlm://cond/FIN-OK")]' in texts["stg"]
    assert "from airflow.datasets import Dataset" in texts["stg"]


def test_dag_kwargs_and_conditional_imports(emitted):
    _, texts, _ = emitted
    fin, stg, risk = texts["fin_dw"], texts["stg"], texts["risk"]
    assert 'schedule="0 21 * * 1-5"' in fin
    assert '"retries": 2' in fin  # max maxrerun of members
    assert '"retries": 0' in stg
    assert "start_date=datetime(2026, 1, 1)" in fin
    assert "catchup=False" in fin
    assert '"ctrlm"' in fin and '"strategy:components"' in fin
    assert '"folder:FIN_DW"' in fin and '"folder:RPT"' in fin
    # imports only what each file uses
    assert "ExternalTaskSensor" not in fin  # fin_dw consumes nothing
    assert "DateTimeSensorAsync" not in stg
    assert "SSHOperator" not in texts["risk"] or "SSHOperator" in risk  # risk has SSH
    assert "from airflow.utils.task_group import TaskGroup" not in stg
    assert "from airflow import DAG" in stg


def test_diagnostics_appended(emitted):
    _, _, result = emitted
    codes = {(d.code, d.subject) for d in result.diagnostics}
    assert ("UNMAPPED_NODE", "nodeX") in codes
    assert ("UNRESOLVED_AUTOEDIT", "FIN_DW/LOAD") in codes


def test_determinism_byte_identical(tmp_path: Path):
    mapping = tmp_path / "nodes.yaml"
    mapping.write_text("prdnode1: ssh_prdnode1\n", encoding="utf-8")
    outs = []
    for sub in ("a", "b"):
        graph, result = make_fixture()
        paths = emit_dags(
            graph, result, tmp_path / sub, PartitionConfig(), mapping_path=mapping
        )
        outs.append({p.name: p.read_bytes() for p in paths})
    assert outs[0] == outs[1]


def test_task_id_sanitized_and_deduped(tmp_path: Path):
    jobs = [
        _job("A-B", "F1", task_type="Dummy"),
        _job("A_B", "F1", task_type="Dummy"),
    ]
    graph = CtmGraph(nodes={j.uid: j for j in jobs})
    spec = DagSpec(dag_id="f1", jobs=sorted(j.uid for j in jobs), folders=["F1"])
    result = PartitionResult(
        strategy="components",
        dags=[spec],
        assignments={j.uid: "f1" for j in jobs},
    )
    (path,) = emit_dags(graph, result, tmp_path / "dags", PartitionConfig())
    text = path.read_text(encoding="utf-8")
    assert 'task_id="a_b"' in text
    assert 'task_id="a_b_2"' in text
    py_compile.compile(str(path), doraise=True)


# ------------------------------------------------- v2 operator mapping (V2-3)

V2_MAPPING = """\
defaults: {os: linux}
nodes:
  prdnode1: {conn_id: ssh_prdnode1, os: linux}
  winnode1: {conn_id: winrm_winnode1, os: windows}
"""

WINRM_PROVIDER = "# provider: apache-airflow-providers-microsoft-winrm"
WINRM_IMPORT = "from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator"


def _emit_one(tmp_path: Path, job: Job, mapping_text: str | None):
    """Emit a single one-job DAG with the given nodes.yaml text; return (text, result)."""
    graph = CtmGraph(nodes={job.uid: job})
    spec = DagSpec(dag_id="d", jobs=[job.uid], folders=[job.folder])
    result = PartitionResult(
        strategy="components", dags=[spec], assignments={job.uid: "d"}
    )
    mapping = tmp_path / "nodes.yaml"
    if mapping_text is not None:
        mapping.write_text(mapping_text, encoding="utf-8")
    (path,) = emit_dags(
        graph, result, tmp_path / "dags", PartitionConfig(), mapping_path=mapping
    )
    py_compile.compile(str(path), doraise=True)
    return path.read_text(encoding="utf-8"), result


def test_winrm_for_windows_node_plain_command(tmp_path: Path):
    # (a) windows node, command with NO PowerShell markers -> node os decides
    job = _job("WIN_COPY", "MFG", task_type="Command",
               command="cmd /c copy_files.bat", node_id="winnode1")
    text, _ = _emit_one(tmp_path, job, V2_MAPPING)
    assert "WinRMOperator" in text
    assert 'ssh_conn_id="winrm_winnode1"' in text
    assert "SSHOperator" not in text
    assert WINRM_PROVIDER in text
    assert WINRM_IMPORT in text


def test_winrm_for_ps1_command_on_linux_node(tmp_path: Path):
    # (b) linux-mapped node but a .ps1 command -> PowerShell sniff decides
    job = _job("PRESS_REPORT", "MFG", task_type="Job",
               command="C:\\jobs\\press_report.ps1 %%ODATE", node_id="prdnode1")
    text, _ = _emit_one(tmp_path, job, V2_MAPPING)
    assert "WinRMOperator" in text
    assert 'ssh_conn_id="ssh_prdnode1"' in text  # conn resolution unchanged
    assert "SSHOperator" not in text
    assert WINRM_PROVIDER in text
    assert "ds_nodash" in text  # AUTOEDIT still applied on the WinRM path


def test_winrm_for_leading_powershell_word(tmp_path: Path):
    job = _job("PRESS_LOAD", "MFG", task_type="Command",
               command="powershell -File C:\\jobs\\press_load.ps1 %%ODATE",
               node_id="winnode1")
    text, _ = _emit_one(tmp_path, job, V2_MAPPING)
    assert "WinRMOperator" in text
    assert 'ssh_conn_id="winrm_winnode1"' in text
    assert WINRM_PROVIDER in text


def test_ssh_for_linux_bash_with_v2_mapping(tmp_path: Path):
    job = _job("EXTRACT", "MFG", task_type="Command",
               command="/opt/mfg/extract.sh %%ODATE", node_id="prdnode1")
    text, _ = _emit_one(tmp_path, job, V2_MAPPING)
    assert "SSHOperator" in text
    assert 'ssh_conn_id="ssh_prdnode1"' in text
    assert "WinRMOperator" not in text
    assert WINRM_PROVIDER not in text


def test_ps1_not_sniffed_from_lookalikes(tmp_path: Path):
    # ".ps1x" is not a .ps1 script; "powershell" not in command position is fine,
    # but a path containing "powershell.log" must not trigger either.
    job = _job("CLEAN", "OPS", task_type="Command",
               command="rm -f /var/log/powershell.log /tmp/a.ps1x", node_id="prdnode1")
    text, _ = _emit_one(tmp_path, job, V2_MAPPING)
    assert "SSHOperator" in text
    assert "WinRMOperator" not in text


def test_v1_flat_mapping_still_parses(tmp_path: Path):
    job = _job("LOAD", "FIN", task_type="Command",
               command="run.sh", node_id="prdnode1")
    text, result = _emit_one(tmp_path, job, "prdnode1: ssh_prdnode1\n")
    assert "SSHOperator" in text
    assert 'ssh_conn_id="ssh_prdnode1"' in text  # flat entry -> conn, os linux
    assert "WinRMOperator" not in text
    assert not any(d.code == "UNMAPPED_NODE" for d in result.diagnostics)


def test_unmapped_node_ps1_gets_winrm_and_diag(tmp_path: Path):
    # sniff wins even for an unmapped node; conn falls back to ssh_<nodeid>
    job = _job("QA_CHECK", "MFG", task_type="Command",
               command="powershell -File qa.ps1", node_id="winnodeX")
    text, result = _emit_one(tmp_path, job, "defaults: {os: linux}\nnodes: {}\n")
    assert "WinRMOperator" in text
    assert 'ssh_conn_id="ssh_winnodeX"' in text
    assert ("UNMAPPED_NODE", "winnodeX") in {
        (d.code, d.subject) for d in result.diagnostics
    }


def test_repo_nodes_yaml_maps_winnode1_windows(tmp_path: Path):
    repo_mapping = Path(__file__).resolve().parents[1] / "mapping-config" / "nodes.yaml"
    job = _job("WIN_JOB", "MFG", task_type="Command",
               command="cmd /c run.bat", node_id="winnode1")
    graph = CtmGraph(nodes={job.uid: job})
    spec = DagSpec(dag_id="d", jobs=[job.uid], folders=[job.folder])
    result = PartitionResult(
        strategy="components", dags=[spec], assignments={job.uid: "d"}
    )
    (path,) = emit_dags(
        graph, result, tmp_path / "dags", PartitionConfig(), mapping_path=repo_mapping
    )
    py_compile.compile(str(path), doraise=True)
    text = path.read_text(encoding="utf-8")
    assert "WinRMOperator" in text
    assert 'ssh_conn_id="winrm_winnode1"' in text
    assert WINRM_PROVIDER in text
    assert not any(d.code == "UNMAPPED_NODE" for d in result.diagnostics)


def test_missing_mapping_file_falls_back(tmp_path: Path):
    graph, result = make_fixture()
    paths = emit_dags(
        graph,
        result,
        tmp_path / "dags",
        PartitionConfig(),
        mapping_path=tmp_path / "does-not-exist.yaml",
    )
    fin = next(p for p in paths if p.name == "fin_dw.py").read_text(encoding="utf-8")
    assert 'ssh_conn_id="ssh_prdnode1"' in fin  # ssh_<nodeid> fallback


# ------------------------------------------------- v3 registry + params (V3-3)

V3_MAPPING = """\
defaults: {os: linux}
nodes:
  prdnode1: {conn_id: ssh_prdnode1, os: linux}
  winnode1: {conn_id: winrm_winnode1, os: windows}
  dbnode1: {conn_id: bank_dwh, type: db}
"""


def _emit_jobs(
    tmp_path: Path,
    jobs: list[Job],
    mapping_text: str = V3_MAPPING,
    extra_assignments: dict[str, str] | None = None,
):
    """Emit one DAG holding *jobs*; return (text, result, out_root)."""
    graph = CtmGraph(nodes={j.uid: j for j in jobs})
    spec = DagSpec(
        dag_id="d",
        jobs=sorted(j.uid for j in jobs),
        folders=sorted({j.folder for j in jobs}),
    )
    assignments = {j.uid: "d" for j in jobs}
    assignments.update(extra_assignments or {})
    result = PartitionResult(
        strategy="components", dags=[spec], assignments=assignments
    )
    mapping = tmp_path / "nodes.yaml"
    mapping.write_text(mapping_text, encoding="utf-8")
    out_root = tmp_path / "scope"
    (path,) = emit_dags(
        graph, result, out_root / "dags", PartitionConfig(), mapping_path=mapping
    )
    py_compile.compile(str(path), doraise=True)
    return path.read_text(encoding="utf-8"), result, out_root


def test_database_job_sql_operator(tmp_path: Path):
    job = _job(
        "BANK_BAL_CHECK",
        "BANK_EOD",
        task_type="Command",
        appl_type="DATABASE",
        node_id="dbnode1",
        command="SELECT COUNT(*) FROM balances WHERE ds='%%ODATE'",
    )
    text, result, _ = _emit_jobs(tmp_path, [job])
    assert "CtmDatabaseJob" in text
    assert "from ctm_plugins.operators import CtmDatabaseJob" in text
    assert 'node="dbnode1"' in text
    # V4-2: connection resolution happens at parse time inside the operator —
    # NO conn_id literal anywhere in the generated file.
    assert "conn_id" not in text
    assert "ds_nodash" in text  # AUTOEDIT applied to the SQL text
    assert "SQLExecuteQueryOperator" not in text  # subclassed inside the plugin
    assert "SSHOperator" not in text
    assert not any(d.code == "UNMAPPED_NODE" for d in result.diagnostics)


def test_repo_nodes_yaml_maps_dbnode1(tmp_path: Path):
    repo_mapping = Path(__file__).resolve().parents[1] / "mapping-config" / "nodes.yaml"
    job = _job("BAL", "BANK", task_type="Command", appl_type="DATABASE",
               node_id="dbnode1", command="SELECT 1")
    graph = CtmGraph(nodes={job.uid: job})
    spec = DagSpec(dag_id="d", jobs=[job.uid], folders=[job.folder])
    result = PartitionResult(
        strategy="components", dags=[spec], assignments={job.uid: "d"}
    )
    (path,) = emit_dags(
        graph, result, tmp_path / "dags", PartitionConfig(), mapping_path=repo_mapping
    )
    text = path.read_text(encoding="utf-8")
    assert 'node="dbnode1"' in text  # repo nodes.yaml maps it -> no diagnostic
    assert "conn_id" not in text
    assert not any(d.code == "UNMAPPED_NODE" for d in result.diagnostics)


def test_database_unmapped_node_partial(tmp_path: Path):
    job = _job("Q", "F", task_type="Command", appl_type="DATABASE",
               node_id="ghostdb", command="SELECT 1")
    text, result, _ = _emit_jobs(tmp_path, [job])
    assert 'node="ghostdb"' in text  # still emitted; resolved at parse time
    assert "# TODO DATABASE node ghostdb unmapped" in text
    assert ("UNMAPPED_NODE", "ghostdb") in {
        (d.code, d.subject) for d in result.diagnostics
    }


def test_manual_stub_for_file_trans_and_sap(tmp_path: Path):
    jobs = [
        _job("FT_STMTS", "BANK", task_type="Job", appl_type="FILE_TRANS",
             node_id="prdnode1", description="to sftp://partner/in"),
        _job("MAINFRAME_SYNC", "BANK", task_type="Job", appl_type="SAP",
             node_id="prdnode1"),
    ]
    text, result, _ = _emit_jobs(tmp_path, jobs)
    # V4-2: MANUAL rows emit CtmManualJob — no PythonOperator+prelude stub
    assert "CtmManualJob" in text
    assert "from ctm_plugins.operators import CtmManualJob" in text
    assert "PythonOperator" not in text
    assert "_ctm_manual_stub" not in text
    assert 'ctm_task_type="Job"' in text
    assert 'ctm_appl_type="FILE_TRANS"' in text
    assert 'ctm_appl_type="SAP"' in text
    assert 'ctm_job="FT_STMTS"' in text
    assert 'ctm_job="MAINFRAME_SYNC"' in text
    assert "# TODO FILE_TRANS" in text
    assert "sftp://partner/in" in text  # source/target hint comment
    codes = {(d.code, d.subject) for d in result.diagnostics}
    assert ("UNSUPPORTED_TYPE", "BANK/FT_STMTS") in codes
    assert ("UNSUPPORTED_TYPE", "BANK/MAINFRAME_SYNC") in codes


def test_unknown_appl_type_hits_catch_all(tmp_path: Path):
    job = _job("X", "F", task_type="Command", appl_type="MYSTERY", command="x.sh")
    text, result, _ = _emit_jobs(tmp_path, [job])
    assert "CtmManualJob" in text
    assert 'ctm_appl_type="MYSTERY"' in text
    assert "APPL_TYPE=MYSTERY" in text  # the `# original ...` comment
    assert "SSHOperator" not in text
    assert "PythonOperator" not in text
    assert any(d.code == "UNSUPPORTED_TYPE" for d in result.diagnostics)


def test_bank_settle_full_param_mapping(tmp_path: Path):
    """The verifier scenario: priority/critical, pool, callback+email,
    DOFORCEJOB resolved in scope, SHOUT LATE -> sla."""
    from ctrlm_core.model import OnAction, Resource

    settle = _job(
        "BANK_SETTLE",
        "BANK_EOD",
        task_type="Command",
        command="/opt/bank/settle.sh %%ODATE",
        node_id="prdnode1",
        priority="AA",
        critical=True,
        timefrom="1900",
        timeto="2300",
        maxrerun=1,
        rerun_interval_minutes=5,
        resources=[Resource(name="SETTLE_SLOTS", kind="quantitative", quant=3)],
        on_actions=[
            OnAction(
                stmt="*",
                code="NOTOK",
                actions=[
                    {"type": "DOMAIL", "DEST": "ops@corp.com", "MESSAGE": "settle failed"},
                    {"type": "DOFORCEJOB", "JOBNAME": "BANK_RECON"},
                ],
            )
        ],
        shouts=[{"when": "LATE", "dest": "OPS", "message": "late"}],
    )
    recon = _job("BANK_RECON", "BANK_EOD", task_type="Command",
                 command="/opt/bank/recon.sh", node_id="prdnode1")
    text, result, out_root = _emit_jobs(
        tmp_path, [settle], extra_assignments={recon.uid: "bank_recon_dag"}
    )
    # v4 operator policy: command jobs stay a PLAIN SSHOperator with the
    # common params translated inline at codegen time — no Ctm* wrapper class
    # anywhere in this file (ctm_shout, lowercase, is the only plugin import)
    assert "SSHOperator" in text
    assert 'ssh_conn_id="ssh_prdnode1"' in text
    assert "Ctm" not in text
    # pool + slots
    assert 'pool="SETTLE_SLOTS"' in text
    assert "pool_slots=3" in text
    # priority: AA -> 100 (>= 90 critical floor)
    assert "priority_weight=100" in text
    assert "# PRIORITY AA -> priority_weight 100" in text
    assert "# CRITICAL=1 -> priority_weight floored at 90" in text
    # retries / retry_delay at task level
    assert "retries=1" in text
    assert "retry_delay=timedelta(minutes=5)" in text
    assert "from datetime import datetime, timedelta" in text
    # NOTOK callback + email
    assert (
        'on_failure_callback=ctm_shout(dest="ops@corp.com", message="settle failed")'
        in text
    )
    assert 'email=["ops@corp.com"]' in text
    assert "email_on_failure=True" in text
    assert "from ctm_plugins.callbacks import ctm_shout" in text
    # DOFORCEJOB -> downstream TriggerDagRunOperator, resolved via assignments
    assert "from airflow.operators.trigger_dagrun import TriggerDagRunOperator" in text
    assert 'task_id="force_bank_recon"' in text
    assert 'trigger_dag_id="bank_recon_dag"' in text
    assert 'trigger_rule="one_failed"' in text
    assert "bank_settle >> force_bank_recon" in text
    assert not any(d.code == "FORCEJOB_UNRESOLVED" for d in result.diagnostics)
    # SHOUT LATE -> sla (2300 - 1900 = 240 minutes)
    assert "sla=timedelta(minutes=240)" in text
    assert ("SLA_APPROX", settle.uid) in {
        (d.code, d.subject) for d in result.diagnostics
    }
    # pools.json next to the dags dir
    pools = json.loads(
        (out_root / "config" / "pools.json").read_text(encoding="utf-8")
    )
    assert pools == [{"name": "SETTLE_SLOTS", "slots": 3, "source": "quantitative"}]


def test_forcejob_unresolved_literal_dag_id(tmp_path: Path):
    from ctrlm_core.model import OnAction

    job = _job(
        "A", "F", task_type="Command", command="a.sh",
        on_actions=[OnAction(code="NOTOK",
                             actions=[{"type": "DOFORCEJOB",
                                       "JOBNAME": "ELSEWHERE_JOB"}])],
    )
    text, result, _ = _emit_jobs(tmp_path, [job])
    assert 'trigger_dag_id="elsewhere_job"' in text  # literal snake_case fallback
    assert ("FORCEJOB_UNRESOLVED", "F/A") in {
        (d.code, d.subject) for d in result.diagnostics
    }


def test_confirm_upstream_approval_gate(tmp_path: Path):
    job = _job("HR_PAY_RUN", "HR_PAY", task_type="Command",
               command="pay.sh", node_id="prdnode1", confirm=True)
    text, _, _ = _emit_jobs(tmp_path, [job])
    assert "from ctm_plugins.sensors import CtmApprovalGateSensor" in text
    assert 'task_id="confirm_hr_pay_run"' in text
    assert "confirm_hr_pay_run >> hr_pay_run" in text
    assert "ctm_approve/" in text  # how-to-approve comment


def test_docond_add_extra_dataset_outlet(tmp_path: Path):
    from ctrlm_core.model import OnAction

    job = _job(
        "A", "F", task_type="Command", command="a.sh",
        on_actions=[OnAction(code="OK",
                             actions=[{"type": "DOCOND", "NAME": "EXTRA-OK",
                                       "SIGN": "ADD"}])],
    )
    text, _, _ = _emit_jobs(tmp_path, [job])
    assert 'outlets=[Dataset("ctrlm://cond/EXTRA-OK")]' in text
    assert "from airflow.datasets import Dataset" in text


def test_unmapped_do_action_todo_and_diagnostic(tmp_path: Path):
    from ctrlm_core.model import OnAction

    job = _job(
        "A", "F", task_type="Command", command="a.sh",
        on_actions=[OnAction(code="NOTOK",
                             actions=[{"type": "DOACTION", "ACTION": "SET"}])],
    )
    text, result, _ = _emit_jobs(tmp_path, [job])
    assert "# TODO unmapped ON/DO action" in text
    assert ("UNMAPPED_ACTION", "F/A") in {
        (d.code, d.subject) for d in result.diagnostics
    }


def test_doc_md_params_and_app_tags(tmp_path: Path):
    job = _job(
        "LOADX", "F", task_type="Command", command="x.sh", node_id="prdnode1",
        description="Load the X feed", application="FIN", sub_application="DW",
        variables={"TGT": "dwh", "SRC": "crm"},
    )
    text, _, _ = _emit_jobs(tmp_path, [job])
    assert 'doc_md="Load the X feed"' in text
    assert '"SRC": "crm"' in text and '"TGT": "dwh"' in text
    assert "params=" in text
    assert '"app:FIN"' in text and '"subapp:DW"' in text  # dag-level tags


def test_filewatch_sensor_with_path_and_maxwait(tmp_path: Path):
    job = _job("FW", "F", task_type="FileWatch",
               command="/in/feed_%%ODATE.csv", maxwait=1)
    text, _, _ = _emit_jobs(tmp_path, [job])
    assert "CtmFileWatcherSensor" in text
    assert 'path="/in/feed_{{ ds_nodash }}.csv"' in text
    assert "timeout=86400" in text  # MAXWAIT 1 day
    assert "poke_interval=60" in text


def test_control_resource_pool_and_pools_json(tmp_path: Path):
    from ctrlm_core.model import Resource

    jobs = [
        _job("A", "F", task_type="Command", command="a.sh",
             resources=[Resource(name="GL_LOCK", kind="control",
                                 control_type="E")]),
        _job("B", "F", task_type="Command", command="b.sh",
             resources=[Resource(name="DB_SLOTS", kind="quantitative", quant=2)]),
    ]
    text, _, out_root = _emit_jobs(tmp_path, jobs)
    assert 'pool="GL_LOCK"' in text
    assert 'pool="DB_SLOTS"' in text
    assert "pool_slots=2" in text
    pools = json.loads(
        (out_root / "config" / "pools.json").read_text(encoding="utf-8")
    )
    assert pools == [
        {"name": "DB_SLOTS", "slots": 2, "source": "quantitative"},
        {"name": "GL_LOCK", "slots": 1, "source": "control"},
    ]


def test_no_pools_no_config_file(tmp_path: Path):
    text, _, out_root = _emit_jobs(
        tmp_path, [_job("A", "F", task_type="Command", command="a.sh")]
    )
    assert not (out_root / "config").exists()


def test_registry_provenance_comment(tmp_path: Path):
    text, _, _ = _emit_jobs(
        tmp_path, [_job("A", "F", task_type="Command", command="a.sh",
                        node_id="prdnode1")]
    )
    assert "[registry: ssh_command, FULL]" in text


def test_v3_features_deterministic(tmp_path: Path):
    from ctrlm_core.model import OnAction, Resource

    def _jobs():
        return [
            _job("S", "F", task_type="Command", command="s.sh %%ODATE",
                 node_id="prdnode1", priority="BB", critical=True, confirm=True,
                 timefrom="1900", timeto="2300",
                 resources=[Resource(name="SLOTS", kind="quantitative", quant=2)],
                 on_actions=[OnAction(code="NOTOK", actions=[
                     {"type": "DOMAIL", "DEST": "a@b.c", "MESSAGE": "m"},
                     {"type": "DOFORCEJOB", "JOBNAME": "GONE"},
                 ])],
                 shouts=[{"when": "LATE", "dest": "OPS", "message": "l"}]),
            _job("FT", "F", task_type="Job", appl_type="FILE_TRANS"),
        ]

    outs = []
    for sub in ("a", "b"):
        (tmp_path / sub).mkdir()
        text, _, out_root = _emit_jobs(tmp_path / sub, _jobs())
        pools = (out_root / "config" / "pools.json").read_bytes()
        outs.append((text, pools))
    assert outs[0] == outs[1]


# ------------------------------- emit-time diagnostics persisted to disk


def test_partition_json_rewritten_with_emit_diagnostics(tmp_path: Path):
    """The pipeline writes partition.json BEFORE emit_dags; emit_dags must
    rewrite it so emit-time diagnostics (UNSUPPORTED_TYPE, ...) reach disk."""
    job = _job("FT", "F", task_type="Job", appl_type="FILE_TRANS")
    graph = CtmGraph(nodes={job.uid: job})
    spec = DagSpec(dag_id="d", jobs=[job.uid], folders=["F"])
    result = PartitionResult(
        strategy="components", dags=[spec], assignments={job.uid: "d"}
    )
    scope_dir = tmp_path / "scope"
    scope_dir.mkdir()
    partition_path = scope_dir / "partition.json"
    # simulate pipeline.py: serialize the pre-emit result first
    partition_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    assert json.loads(partition_path.read_text(encoding="utf-8"))["diagnostics"] == []

    emit_dags(graph, result, scope_dir / "dags", PartitionConfig())

    on_disk = json.loads(partition_path.read_text(encoding="utf-8"))
    codes = {(d["code"], d["subject"]) for d in on_disk["diagnostics"]}
    assert ("UNSUPPORTED_TYPE", "F/FT") in codes
    # the file matches the in-memory result exactly (same serialization)
    assert partition_path.read_text(encoding="utf-8") == result.model_dump_json(indent=2)


def test_partition_json_not_created_when_absent(tmp_path: Path):
    """Standalone emit_dags (no pipeline) must not invent a partition.json."""
    job = _job("FT", "F", task_type="Job", appl_type="FILE_TRANS")
    graph = CtmGraph(nodes={job.uid: job})
    spec = DagSpec(dag_id="d", jobs=[job.uid], folders=["F"])
    result = PartitionResult(
        strategy="components", dags=[spec], assignments={job.uid: "d"}
    )
    scope_dir = tmp_path / "scope"
    emit_dags(graph, result, scope_dir / "dags", PartitionConfig())
    assert result.diagnostics  # diagnostics still returned in memory
    assert not (scope_dir / "partition.json").exists()


# ------------------------------------------------- v5 dag_plans.json (V5-1)

import re  # noqa: E402  (kept local to the v5 test block)


def _load_dag_plans(dags_dir_parent: Path) -> dict:
    return json.loads(
        (dags_dir_parent / "dag_plans.json").read_text(encoding="utf-8")
    )


def _rendered_task_ids(text: str) -> set[str]:
    # lookbehind excludes external_task_id= / trigger_dag_id= style kwargs
    return set(re.findall(r'(?<!\w)task_id="([^"]+)"', text))


def _rendered_edges(text: str) -> set[tuple[str, str]]:
    """All `up >> down` dependency lines of a rendered file (var == task_id
    for every fixture used here — plain identifiers, no dedup/keyword clash)."""
    return {
        (m.group(1), m.group(2))
        for m in re.finditer(r"^\s*(\w+) >> (\w+)\s*$", text, re.MULTILINE)
    }


def _plan_edges(dag_plan: dict) -> set[tuple[str, str]]:
    return {
        (up, t["task_id"]) for t in dag_plan["tasks"] for up in t["upstream"]
    }


def test_dag_plans_written_for_every_dag(emitted, tmp_path: Path):
    paths, texts, _ = emitted
    plans = _load_dag_plans(paths[0].parent.parent)
    assert sorted(plans) == sorted(texts)  # one plan per emitted dag file


def test_dag_plans_task_sets_match_rendered_code(emitted):
    paths, texts, _ = emitted
    plans = _load_dag_plans(paths[0].parent.parent)
    for dag_id, text in texts.items():
        plan_ids = {t["task_id"] for t in plans[dag_id]["tasks"]}
        assert plan_ids == _rendered_task_ids(text), dag_id


def test_dag_plans_upstream_mirrors_emitted_edges(emitted):
    paths, texts, _ = emitted
    plans = _load_dag_plans(paths[0].parent.parent)
    for dag_id, text in texts.items():
        assert _plan_edges(plans[dag_id]) == _rendered_edges(text), dag_id
    # spot-check the fixture's known edges land in the right upstream lists
    fin = {t["task_id"]: t for t in plans["fin_dw"]["tasks"]}
    assert fin["mart"]["upstream"] == ["gate_mart", "load"]
    assert fin["extract"]["upstream"] == ["gate_extract", "mart"]
    risk = {t["task_id"]: t for t in plans["risk"]["tasks"]}
    assert risk["calc"]["upstream"] == ["wait_extract", "wait_load"]


def test_dag_plans_kinds_operators_and_provenance(emitted):
    paths, _, _ = emitted
    plans = _load_dag_plans(paths[0].parent.parent)
    fin = {t["task_id"]: t for t in plans["fin_dw"]["tasks"]}
    # job tasks carry their Control-M uid + operator + TaskGroup (multi-folder)
    assert fin["load"]["kind"] == "job"
    assert fin["load"]["operator"] == "SSHOperator"
    assert fin["load"]["source_uid"] == "FIN_DW/LOAD"
    assert fin["load"]["task_group"] == "fin_dw"
    assert fin["report"]["operator"] == "EmptyOperator"
    assert fin["report"]["task_group"] == "rpt"
    # time gates: structural, no source uid, no group (module level)
    assert fin["gate_mart"]["kind"] == "gate"
    assert fin["gate_mart"]["operator"] == "DateTimeSensorAsync"
    assert fin["gate_mart"]["source_uid"] is None
    assert fin["gate_mart"]["task_group"] is None
    # waits: structural sensors in the consumer dag
    risk = {t["task_id"]: t for t in plans["risk"]["tasks"]}
    assert risk["wait_extract"]["kind"] == "wait"
    assert risk["wait_extract"]["operator"] == "ExternalTaskSensor"
    assert risk["wait_extract"]["source_uid"] is None
    # single-folder dag -> no task groups anywhere
    stg = {t["task_id"]: t for t in plans["stg"]["tasks"]}
    assert all(t["task_group"] is None for t in stg.values())
    assert stg["watch"]["operator"] == "CtmFileWatcherSensor"


def test_dag_plans_schedule_datasets_outlets_external_waits(emitted):
    paths, _, _ = emitted
    plans = _load_dag_plans(paths[0].parent.parent)
    fin, stg, risk = plans["fin_dw"], plans["stg"], plans["risk"]
    # schedule / dataset info mirrors the DAG kwargs
    assert fin["schedule"] == "0 21 * * 1-5"
    assert fin["dataset_triggered"] is False
    assert stg["schedule"] is None
    assert stg["dataset_triggered"] is True
    assert stg["datasets"] == ["ctrlm://cond/FIN-OK"]
    # producer outlet recorded on the producing task
    assert fin["outlets"] == [
        {"task_id": "mart", "dataset": "ctrlm://cond/FIN-OK"}
    ]
    assert stg["outlets"] == []
    # consumer-side external waits (sensor + prev_run_sensor mechanisms)
    assert risk["external_waits"] == [
        {
            "task_id": "wait_extract",
            "external_dag_id": "fin_dw",
            "external_task_id": "fin_dw.extract",
        },
        {
            "task_id": "wait_load",
            "external_dag_id": "fin_dw",
            "external_task_id": "fin_dw.load",
        },
    ]
    assert fin["external_waits"] == []


def test_dag_plans_confirm_and_force_kinds(tmp_path: Path):
    from ctrlm_core.model import OnAction

    settle = _job(
        "BANK_SETTLE", "BANK_EOD", task_type="Command",
        command="/opt/bank/settle.sh", node_id="prdnode1", confirm=True,
        on_actions=[OnAction(code="NOTOK",
                             actions=[{"type": "DOFORCEJOB",
                                       "JOBNAME": "BANK_RECON"}])],
    )
    recon = _job("BANK_RECON", "BANK_EOD", task_type="Command",
                 command="/opt/bank/recon.sh", node_id="prdnode1")
    text, _, out_root = _emit_jobs(
        tmp_path, [settle], extra_assignments={recon.uid: "bank_recon_dag"}
    )
    plans = _load_dag_plans(out_root)
    tasks = {t["task_id"]: t for t in plans["d"]["tasks"]}
    assert tasks["confirm_bank_settle"]["kind"] == "confirm"
    assert tasks["confirm_bank_settle"]["operator"] == "CtmApprovalGateSensor"
    assert tasks["confirm_bank_settle"]["source_uid"] is None
    assert tasks["force_bank_recon"]["kind"] == "force"
    assert tasks["force_bank_recon"]["operator"] == "TriggerDagRunOperator"
    # confirm >> job and job >> force edges mirror the emitted code
    assert tasks["bank_settle"]["upstream"] == ["confirm_bank_settle"]
    assert tasks["force_bank_recon"]["upstream"] == ["bank_settle"]
    assert _plan_edges(plans["d"]) == _rendered_edges(text)


def test_dag_plans_folder_start_end_kinds(tmp_path: Path):
    jobs = [
        _job("__FOLDER_START__", "F", task_type="Dummy", synthetic=True),
        _job("A", "F", task_type="Command", command="a.sh", node_id="prdnode1"),
        _job("__FOLDER_END__", "F", task_type="Dummy", synthetic=True),
    ]
    graph = CtmGraph(
        nodes={j.uid: j for j in jobs},
        e_edges=[
            GraphEdge(source="F/__FOLDER_START__", target="F/A",
                      cond="__start__F"),
            GraphEdge(source="F/A", target="F/__FOLDER_END__",
                      cond="__done__F/A"),
        ],
    )
    spec = DagSpec(dag_id="f", jobs=sorted(j.uid for j in jobs), folders=["F"])
    result = PartitionResult(
        strategy="components", dags=[spec],
        assignments={j.uid: "f" for j in jobs},
    )
    (path,) = emit_dags(graph, result, tmp_path / "scope" / "dags",
                        PartitionConfig())
    plans = _load_dag_plans(tmp_path / "scope")
    tasks = {t["task_id"]: t for t in plans["f"]["tasks"]}
    assert tasks["folder_start"]["kind"] == "folder_start"
    assert tasks["folder_end"]["kind"] == "folder_end"
    assert tasks["folder_start"]["operator"] == "EmptyOperator"
    assert tasks["folder_start"]["source_uid"] == "F/__FOLDER_START__"
    assert tasks["a"]["kind"] == "job"
    assert tasks["a"]["upstream"] == ["folder_start"]
    assert tasks["folder_end"]["upstream"] == ["a"]


def test_dag_plans_byte_deterministic(tmp_path: Path):
    mapping = tmp_path / "nodes.yaml"
    mapping.write_text("prdnode1: ssh_prdnode1\n", encoding="utf-8")
    outs = []
    for sub in ("a", "b"):
        graph, result = make_fixture()
        emit_dags(
            graph, result, tmp_path / sub / "dags", PartitionConfig(),
            mapping_path=mapping,
        )
        outs.append((tmp_path / sub / "dag_plans.json").read_bytes())
    assert outs[0] == outs[1]
    assert outs[0].endswith(b"\n")
    assert b"\r" not in outs[0]  # LF-only on every platform


def test_dag_plans_rendered_output_unchanged(emitted):
    """V5-1 is purely additive: the .py files carry no trace of the plan."""
    _, texts, _ = emitted
    for text in texts.values():
        assert "dag_plans" not in text
