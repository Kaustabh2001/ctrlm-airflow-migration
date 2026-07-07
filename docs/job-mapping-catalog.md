# Control-M → Airflow job-mapping catalog

The complete, param-by-param mapping of Control-M job definitions to Airflow
(2.9+/MWAA), exactly as implemented by the job-type registry
(`core/ctrlm_core/operator_registry.py`) and applied by the DAG emitter
(`core/ctrlm_core/emit.py`). Custom components are written ONCE in the
`plugins/ctm_plugins` package and reused by every generated DAG.

This document is kept in sync with the code by `tests/test_registry.py`: the
registry row names in the marker block below must match
`operator_registry.REGISTRY` exactly.

<!-- registry-names
dummy
filewatch
database
file_transfer
known_manual
windows_command
ssh_command
unsupported
-->

Statuses: **FULL** = automatic, faithful mapping · **PARTIAL** = automatic
with caveats (a WARN diagnostic names them) · **MANUAL** = a placeholder stub
is generated (`PythonOperator` raising `NotImplementedError`) and a human must
migrate the job.

## 1. Job types (the registry, in resolution order — first match wins)

| # | Registry row | Control-M type matched | Airflow operator | Status | Notes |
|---|---|---|---|---|---|
| 1 | `dummy` | `TASKTYPE=Dummy` or synthetic `__FOLDER_START__` / `__FOLDER_END__` | `EmptyOperator` | FULL | Synthetic nodes carry folder schedule / fan-in semantics. |
| 2 | `filewatch` | `TASKTYPE=FileWatch` or `APPL_TYPE=FILEWATCH` | `ctm_plugins.sensors.CtmFileWatcherSensor(path=<command>, mode="reschedule", poke_interval=60, timeout=<MAXWAIT>)` | FULL | Path taken from the job command (AUTOEDIT-translated); supports local paths, `s3://`, `sftp://`. A TODO comment is emitted when the export carries no path. |
| 3 | `database` | `APPL_TYPE=DATABASE` | `SQLExecuteQueryOperator(conn_id=<nodes.yaml conn>, sql=<command>)` | FULL | Provider `apache-airflow-providers-common-sql`. SQL goes through the AUTOEDIT translator (`%%ODATE` → `{{ ds_nodash }}`). PARTIAL (diagnostic `UNMAPPED_NODE`, placeholder conn `db_<nodeid>`) when the NODEID is not in `nodes.yaml` — map it with `type: db`. |
| 4 | `file_transfer` | `APPL_TYPE` in `FILE_TRANS`, `AFT`, `MFT` | MANUAL stub (`PythonOperator` → `NotImplementedError`) | MANUAL | Transfer direction and endpoints need humans. The stub is preceded by comments naming the source/target hints (NODEID, RUN_AS, DESCRIPTION, variables). Diagnostic `UNSUPPORTED_TYPE`. |
| 5 | `known_manual` | `APPL_TYPE` in `SAP`, `INFORMATICA`, `HADOOP`, `PEOPLESOFT`, `WEBSERVICES`, `JAVA`, `MQ`, `EMR` | MANUAL stub | MANUAL | Recognized application integrations that map to dedicated Airflow providers — pick and configure the provider manually. Diagnostic `UNSUPPORTED_TYPE`. |
| 6 | `windows_command` | `TASKTYPE=Command`/`Job` on a node with `os: windows` in `nodes.yaml`, **or** a PowerShell command (`\.ps1\b` or leading `powershell`) | `WinRMOperator(ssh_conn_id=<conn>, command=<translated>)` | FULL | Provider `apache-airflow-providers-microsoft-winrm` (its connection kwarg really is `ssh_conn_id`). |
| 7 | `ssh_command` | `TASKTYPE=Command`/`Job` (plain OS, default) | `SSHOperator(ssh_conn_id=<conn>, command=<translated>)` | FULL | Provider `apache-airflow-providers-ssh`. Unmapped NODEID → conn `ssh_<nodeid>` + diagnostic `UNMAPPED_NODE`. |
| 8 | `unsupported` | anything else (catch-all, always last) | MANUAL stub | MANUAL | The stub's `NotImplementedError` and the preceding comment name the original `TASKTYPE` / `APPL_TYPE`. Diagnostic `UNSUPPORTED_TYPE`. |

## 2. Parameters (applied to EVERY task, regardless of operator)

| Control-M | Airflow | Notes |
|---|---|---|
| `JOBNAME` | `task_id` | Sanitized (`snake_case`), deduplicated with `_2`, `_3`, ... |
| `DESCRIPTION` | `doc_md` | Rendered in the Airflow UI task docs. |
| `APPLICATION` / `SUB_APPLICATION` | DAG-level `tags` (`app:<X>`, `subapp:<Y>`) | Deduped; alongside `ctrlm`, `strategy:<s>`, `folder:<f>` tags. |
| `NODEID` | connection id via `mapping-config/nodes.yaml` | See §4; unmapped → fallback conn + `UNMAPPED_NODE` diagnostic. |
| `MAXRERUN` | task-level `retries` (and DAG `default_args.retries` = max over members) | Only emitted per task when > 0. |
| `RERUNINTERVAL` | `retry_delay=timedelta(minutes=<n>)` | |
| `MAXWAIT` (days) | sensor `timeout` (`MAXWAIT * 86400` s, default 21600) | Applies to `ExternalTaskSensor` waits and `CtmFileWatcherSensor`. |
| `TIMEFROM` | upstream `DateTimeSensorAsync` time gate | Gate targets are computed on the Control-M ODATE clock (New Day = `0600` by default) — same semantics as `ctm_plugins._odate.gate_target`; a comment in the generated code references it. |
| `PRIORITY` (`AA` highest .. `ZZ` lowest) | `priority_weight` | Formula: `idx = (c0-'A')*26 + (c1-'A')` (AA=0 .. ZZ=675), `priority_weight = 100 - round(idx * 99 / 675)` — linear onto 1..100, `AA`→100, `ZZ`→1. Purely numeric priorities are clamped into 1..100. |
| `CRITICAL=1` | `priority_weight` floored at 90 | A comment marks the floor; combines with `PRIORITY` (max wins). |
| `CONFIRM=1` | upstream `ctm_plugins.sensors.CtmApprovalGateSensor` task `confirm_<task_id>` | Approve a run by setting Airflow Variable `ctm_approve/<dag_id>/<task_id>/<ds>` to `yes`. |
| `QUANTITATIVE NAME=<R> QUANT=<n>` | `pool="<R>", pool_slots=<n>` | First resource maps to the task's pool; extra resources → WARN diagnostic `MULTI_RESOURCE` (Airflow tasks have one pool). All pools are collected into `<scope>/config/pools.json` (slots = max quant seen; `source: "quantitative"`). |
| `CONTROL NAME=<R> TYPE=E` | `pool="<R>"` with 1 slot | Exclusive control = a 1-slot pool (`source: "control"` in pools.json). Shared (`TYPE=S`) controls get a NOTE comment only. |
| `VARIABLE NAME=... VALUE=...` | task-level `params={...}` | Literal dict, sorted by name; folder variables were already merged in (job wins). |
| `ON CODE=NOTOK` + `DOMAIL` / `DOSHOUT`, or `SHOUT WHEN=NOTOK` | `on_failure_callback=ctm_shout(dest=..., message=...)` | From `ctm_plugins.callbacks`; destination resolution via `mapping-config/notify.yaml`. A `DOMAIL` DEST containing `@` additionally sets `email=[...]` and `email_on_failure=True`. Only the first NOTOK notification becomes the callback; extras get a TODO + `UNMAPPED_ACTION`. |
| `ON ... DOFORCEJOB JOBNAME=<J>` | downstream `TriggerDagRunOperator` task `force_<j>` | `trigger_rule="one_failed"` for NOTOK codes, `"all_success"` for OK. `trigger_dag_id` resolved through this run's assignments when `<J>` is in scope; otherwise the literal `snake_case(<J>)` plus a WARN diagnostic `FORCEJOB_UNRESOLVED`. |
| `ON ... DOCOND NAME=<C> SIGN=ADD` | extra Dataset outlet `ctrlm://cond/<C>` | Merged with the cross-link outlets on the task. `SIGN=DEL` is unmapped (TODO + `UNMAPPED_ACTION`). |
| `SHOUT WHEN=LATE` | `sla=timedelta(TIMETO − TIMEFROM)` | An approximation, flagged `SLA_APPROX`; when the job has no `TIMETO` window the SLA cannot be derived (TODO comment + `SLA_APPROX`). |
| any other `ON`/`DO` action (`DOSTOPCYCLIC`, `DO_IFRERUN`, `DOACTION`, ...) | `# TODO unmapped ON/DO action ...` comment | WARN diagnostic `UNMAPPED_ACTION`. |
| `CMDLINE` / `MEMLIB/MEMNAME` `%%`-variables | AUTOEDIT translation | `%%ODATE`→`{{ ds_nodash }}`, `%%$ODATE`→`{{ ds }}`, `%%DATE`→`{{ ds_nodash }}`, `%%TIME`→`{{ ts_nodash }}`, `%%JOBNAME`→ literal job name; unknown tokens stay verbatim (TODO + `UNRESOLVED_AUTOEDIT`). |

Scheduling (`WEEKDAYS`/`DAYS`/`MONTHS` → cron, cyclic jobs, dataset-triggered
DAGs, cross-DAG sensors/datasets) is unchanged from v1/v2 — see
`docs/operator-mapping.md`.

## 3. Custom components (`plugins/ctm_plugins`, written once, reused everywhere)

| Component | Maps | Notes |
|---|---|---|
| `ctm_plugins.sensors.CtmApprovalGateSensor` | `CONFIRM=1` approval gates | Pokes Airflow Variable `ctm_approve/<dag_id>/<task_id>/<ds>` == `yes`; mode `reschedule`, poke 60 s. |
| `ctm_plugins.sensors.CtmFileWatcherSensor` | `FILEWATCH` jobs | Path scheme decides the check: local path / `s3://` (S3Hook) / `sftp://` (SFTPHook). |
| `ctm_plugins.callbacks.ctm_shout` | `DOMAIL` / `DOSHOUT` / `SHOUT` notifications | Destination resolution via `mapping-config/notify.yaml` (email / sns / log; a dest containing `@` is email directly). Usable as `on_failure_callback`, `on_success_callback`, `sla_miss_callback`. |
| `ctm_plugins._odate.ctm_odate` / `gate_target` | Control-M ODATE semantics | The run's order date (fire time before New Day time belongs to the previous day) and time-gate targeting; registered as the `ctm_odate` macro. |
| `ctm_plugins.timetables.CtmCalendarTimetable` | Control-M calendar schedules | Fires on exactly the dates listed in `mapping-config/calendars.yaml`. |
| `plugins/ctm_plugin.py` (`CtmPlugin`) | Airflow plugin registration | Registers the macro and the timetable. |

**Deploy note:** zip the `ctm_plugins` package together with `ctm_plugin.py`
into `plugins.zip` and upload it to the MWAA environment (see
`plugins/README.md`). Generated DAGs import from `ctm_plugins...` and will not
parse on a scheduler without the plugins installed.

## 4. Configuration files

### `mapping-config/nodes.yaml` — NODEID → connection

```yaml
defaults: {os: linux}            # os used when a node entry omits it
nodes:
  prdnode1: {conn_id: ssh_prdnode1, os: linux}       # -> SSHOperator
  winnode1: {conn_id: winrm_winnode1, os: windows}   # -> WinRMOperator
  dbnode1:  {conn_id: bank_dwh, type: db}            # -> SQLExecuteQueryOperator conn
```

v1 flat entries (`<nodeid>: <conn_id>`) are still accepted (os =
`defaults.os`, ultimately `linux`). The optional `type: db` (v3) marks a
database endpoint used by `APPL_TYPE=DATABASE` jobs. Unmapped NODEIDs fall
back to `ssh_<nodeid>` (or `db_<nodeid>` for DATABASE jobs) with an
`UNMAPPED_NODE` diagnostic.

### `mapping-config/notify.yaml` — SHOUT/DOMAIL destinations

```yaml
OPS: {type: log}                          # log-only destination
DBA: {type: email, target: dba@corp.com}  # SES email via Airflow send_email
PAGER: {type: sns, target: "arn:aws:sns:..."}  # SNS publish
```

Unknown destinations degrade to log-only; a destination that itself contains
`@` (e.g. `ops@corp.com`) is treated as an email address directly.

### `mapping-config/calendars.yaml` — Control-M calendars

```yaml
BANK_BUS_DAYS: ["2026-01-02", "2026-01-05", "2026-01-06"]
```

Consumed by `CtmCalendarTimetable`; one key per calendar, listing the exact
fire dates.

### `<scope>/config/pools.json` — generated OUTPUT (per scope)

```json
[
  {"name": "SETTLE_SLOTS", "slots": 3, "source": "quantitative"}
]
```

Collected from every `QUANTITATIVE` (slots = max quant seen) and exclusive
`CONTROL` (1 slot) resource in the scope. Create the pools in Airflow before
enabling the DAGs, e.g. `airflow pools import pools.json` (adapt to the
CLI's expected format) or via the MWAA UI.
