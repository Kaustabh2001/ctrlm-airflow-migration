# Control-M → Airflow job-mapping catalog

The complete, param-by-param mapping of Control-M job definitions to Airflow
(3.x / MWAA), exactly as implemented by the job-type registry
(`core/ctrlm_core/operator_registry.py`) and applied by the DAG emitter
(`core/ctrlm_core/emit.py`). Custom components are written ONCE in the
`plugins/ctm_plugins` package and reused by every generated DAG.

**Airflow 3 target (v6).** Generated DAGs use the Airflow 3 authoring style: a
`@dag(...)`-decorated function per DAG (function name = dag_id) plus a
module-bottom call — operators are still instantiated as objects inside the
function (`@task` is only for Python callables, which converted Control-M jobs
are not). Authoring imports come from `airflow.sdk` (`dag`, `TaskGroup`,
`Asset`); `EmptyOperator`, `TriggerDagRunOperator`, `ExternalTaskSensor` and
`DateTimeSensorAsync` import from their `airflow.providers.standard.*` paths.
Datasets were renamed **Assets** in 3.0 (`airflow.datasets` is gone), and the
task-level `sla` parameter was **removed** — see the `SHOUT WHEN=LATE` row
in §2.

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
is generated (`ctm_plugins.operators.CtmManualJob`, whose `execute()` raises
`NotImplementedError` naming the original job) and a human must migrate the
job.

Operator policy (v4): custom `Ctm*` operators exist ONLY where they add real
capability (connection resolution, watcher/gate logic, loud manual stubs).
Plain command jobs stay native `SSHOperator`/`WinRMOperator` and Dummy stays
`EmptyOperator`, with every common Control-M param translated inline at
CODEGEN time (§2) — no blanket wrapper classes.

## 1. Job types (the registry, in resolution order — first match wins)

| # | Registry row | Control-M type matched | Airflow operator | Status | Notes |
|---|---|---|---|---|---|
| 1 | `dummy` | `TASKTYPE=Dummy` or synthetic `__FOLDER_START__` / `__FOLDER_END__` | `EmptyOperator` (`airflow.providers.standard.operators.empty`) | FULL | Synthetic nodes carry folder schedule / fan-in semantics. |
| 2 | `filewatch` | `TASKTYPE=FileWatch` or `APPL_TYPE=FILEWATCH` | `ctm_plugins.sensors.CtmFileWatcherSensor(path=<command>, mode="reschedule", poke_interval=60, timeout=<MAXWAIT>)` | FULL | Path taken from the job command (AUTOEDIT-translated); supports local paths, `s3://`, `sftp://`. A TODO comment is emitted when the export carries no path. |
| 3 | `database` | `APPL_TYPE=DATABASE` | `ctm_plugins.operators.CtmDatabaseJob(node=<NODEID>, sql=<command>)` | FULL | Custom operator (subclass of `SQLExecuteQueryOperator`, provider `apache-airflow-providers-common-sql`): `node` resolves to the Airflow connection at PARSE time via the `nodes.yaml` shipped in `plugins.zip` (entry should carry `type: db`; an explicit `conn_id` kwarg overrides) — no connection literal is baked into the DAG file. SQL goes through the AUTOEDIT translator (`%%ODATE` → `{{ ds_nodash }}`). PARTIAL (diagnostic `UNMAPPED_NODE`) when the NODEID is not in the codegen-side `nodes.yaml` — map it with `type: db`. Common params (§2) are still translated inline at codegen time; the operator only owns connectivity. |
| 4 | `file_transfer` | `APPL_TYPE` in `FILE_TRANS`, `AFT`, `MFT` | MANUAL stub (`ctm_plugins.operators.CtmManualJob(ctm_task_type=..., ctm_appl_type=..., ctm_job=...)` → `NotImplementedError`) | MANUAL | Transfer direction and endpoints need humans. The stub is preceded by comments naming the source/target hints (NODEID, RUN_AS, DESCRIPTION, variables). Diagnostic `UNSUPPORTED_TYPE`. See §6 for the AFT/MFT roadmap. |
| 5 | `known_manual` | `APPL_TYPE` in `SAP`, `INFORMATICA`, `HADOOP`, `PEOPLESOFT`, `WEBSERVICES`, `JAVA`, `MQ`, `EMR` | MANUAL stub (`CtmManualJob`) | MANUAL | Recognized application integrations that map to dedicated Airflow providers — pick and configure the provider manually (see §5 playbook and §6 roadmap). Diagnostic `UNSUPPORTED_TYPE`. |
| 6 | `windows_command` | `TASKTYPE=Command`/`Job` on a node with `os: windows` in `nodes.yaml`, **or** a PowerShell command (`\.ps1\b` or leading `powershell`) | `WinRMOperator(ssh_conn_id=<conn>, command=<translated>)` | FULL | Provider `apache-airflow-providers-microsoft-winrm` (its connection kwarg really is `ssh_conn_id`). |
| 7 | `ssh_command` | `TASKTYPE=Command`/`Job` (plain OS, default) | `SSHOperator(ssh_conn_id=<conn>, command=<translated>)` | FULL | Provider `apache-airflow-providers-ssh`. Unmapped NODEID → conn `ssh_<nodeid>` + diagnostic `UNMAPPED_NODE`. |
| 8 | `unsupported` | anything else (catch-all, always last) | MANUAL stub (`CtmManualJob`) | MANUAL | The stub's `NotImplementedError` and the preceding comment name the original `TASKTYPE` / `APPL_TYPE`. Diagnostic `UNSUPPORTED_TYPE`. Extend via §5 when a type shows up often. |

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
| `TIMEFROM` | upstream `DateTimeSensorAsync` time gate (`airflow.providers.standard.sensors.date_time`, kwarg `target_time`) | Gate targets are computed on the Control-M ODATE clock (New Day = `0600` by default) — same semantics as `ctm_plugins._odate.gate_target`; a comment in the generated code references it. |
| `PRIORITY` (`AA` highest .. `ZZ` lowest) | `priority_weight` | Formula: `idx = (c0-'A')*26 + (c1-'A')` (AA=0 .. ZZ=675), `priority_weight = 100 - round(idx * 99 / 675)` — linear onto 1..100, `AA`→100, `ZZ`→1. Purely numeric priorities are clamped into 1..100. |
| `CRITICAL=1` | `priority_weight` floored at 90 | A comment marks the floor; combines with `PRIORITY` (max wins). |
| `CONFIRM=1` | upstream `ctm_plugins.sensors.CtmApprovalGateSensor` task `confirm_<task_id>` | Approve a run by setting Airflow Variable `ctm_approve/<dag_id>/<task_id>/<ds>` to `yes`. |
| `QUANTITATIVE NAME=<R> QUANT=<n>` | `pool="<R>", pool_slots=<n>` | First resource maps to the task's pool; extra resources → WARN diagnostic `MULTI_RESOURCE` (Airflow tasks have one pool). All pools are collected into `<scope>/config/pools.json` (slots = max quant seen; `source: "quantitative"`). |
| `CONTROL NAME=<R> TYPE=E` | `pool="<R>"` with 1 slot | Exclusive control = a 1-slot pool (`source: "control"` in pools.json). Shared (`TYPE=S`) controls get a NOTE comment only. |
| `VARIABLE NAME=... VALUE=...` | task-level `params={...}` | Literal dict, sorted by name; folder variables were already merged in (job wins). |
| `ON CODE=NOTOK` + `DOMAIL` / `DOSHOUT`, or `SHOUT WHEN=NOTOK` | `on_failure_callback=ctm_shout(dest=..., message=...)` | From `ctm_plugins.callbacks`; destination resolution via `mapping-config/notify.yaml`. A `DOMAIL` DEST containing `@` additionally sets `email=[...]` and `email_on_failure=True`. Only the first NOTOK notification becomes the callback; extras get a TODO + `UNMAPPED_ACTION`. |
| `ON ... DOFORCEJOB JOBNAME=<J>` | downstream `TriggerDagRunOperator` task `force_<j>` (`airflow.providers.standard.operators.trigger_dagrun`) | `trigger_rule="one_failed"` for NOTOK codes, `"all_success"` for OK. `trigger_dag_id` resolved through this run's assignments when `<J>` is in scope; otherwise the literal `snake_case(<J>)` plus a WARN diagnostic `FORCEJOB_UNRESOLVED`. |
| `ON ... DOCOND NAME=<C> SIGN=ADD` | extra Asset outlet `ctrlm://cond/<C>` | Merged with the cross-link outlets on the task (`outlets=[Asset(...)]`, `from airflow.sdk import Asset`). `SIGN=DEL` is unmapped (TODO + `UNMAPPED_ACTION`). |
| `SHOUT WHEN=LATE` | `# TODO Airflow 3 removed SLAs; map to Deadline Alerts (3.1+): late after <n>m` comment | Airflow 3.0 removed the task-level `sla` parameter, so nothing executable is emitted. `<n>` = `TIMETO − TIMEFROM` minutes (the old sla approximation); when the job has no `TIMETO` the window is reported as not derivable. Always flagged with a PARTIAL diagnostic `SLA_AF3_REMOVED` naming the job and the late window. |
| any other `ON`/`DO` action (`DOSTOPCYCLIC`, `DO_IFRERUN`, `DOACTION`, ...) | `# TODO unmapped ON/DO action ...` comment | WARN diagnostic `UNMAPPED_ACTION`. |
| `CMDLINE` / `MEMLIB/MEMNAME` `%%`-variables | AUTOEDIT translation | `%%ODATE`→`{{ ds_nodash }}`, `%%$ODATE`→`{{ ds }}`, `%%DATE`→`{{ ds_nodash }}`, `%%TIME`→`{{ ts_nodash }}`, `%%JOBNAME`→ literal job name; unknown tokens stay verbatim (TODO + `UNRESOLVED_AUTOEDIT`). |

Scheduling (`WEEKDAYS`/`DAYS`/`MONTHS` → cron, cyclic jobs, asset-triggered
DAGs `schedule=[Asset(...)]`, cross-DAG sensors/assets) is unchanged from
v1/v2 in substance — see `docs/operator-mapping.md` (which still uses the 2.x
`Dataset` spelling; emitted code says `Asset`). `ExternalTaskSensor` waits
import from `airflow.providers.standard.sensors.external_task` and keep the
default logical-date alignment (`execution_delta`/`execution_date_fn` are
unchanged in Airflow 3).

## 3. Custom components (`plugins/ctm_plugins`, written once, reused everywhere)

| Component | Maps | Notes |
|---|---|---|
| `ctm_plugins.operators.CtmDatabaseJob` | `APPL_TYPE=DATABASE` jobs | Subclass of `SQLExecuteQueryOperator`; resolves `node` → Airflow connection at parse time via the `nodes.yaml` shipped in `plugins.zip` (explicit `conn_id` overrides). The single place for future DB-specific behavior (stored procs, output capture). |
| `ctm_plugins.operators.CtmManualJob` | every MANUAL registry row (`file_transfer`, `known_manual`, `unsupported`) | `execute()` raises `NotImplementedError` naming `ctm_task_type`, `ctm_appl_type` and `ctm_job` — loud, actionable, impossible to mistake for success. |
| `ctm_plugins.sensors.CtmApprovalGateSensor` | `CONFIRM=1` approval gates | Pokes Airflow Variable `ctm_approve/<dag_id>/<task_id>/<ds>` == `yes`; mode `reschedule`, poke 60 s. |
| `ctm_plugins.sensors.CtmFileWatcherSensor` | `FILEWATCH` jobs | Path scheme decides the check: local path / `s3://` (S3Hook) / `sftp://` (SFTPHook). |
| `ctm_plugins.callbacks.ctm_shout` | `DOMAIL` / `DOSHOUT` / `SHOUT` notifications | Destination resolution via `mapping-config/notify.yaml` (email / sns / log; a dest containing `@` is email directly). Usable as `on_failure_callback`, `on_success_callback` (Airflow 3 removed `sla_miss_callback`). |
| `ctm_plugins._odate.ctm_odate` / `gate_target` | Control-M ODATE semantics | The run's order date (fire time before New Day time belongs to the previous day) and time-gate targeting; registered as the `ctm_odate` macro. |
| `ctm_plugins.timetables.CtmCalendarTimetable` | Control-M calendar schedules | Fires on exactly the dates listed in `mapping-config/calendars.yaml`. |
| `plugins/ctm_plugin.py` (`CtmPlugin`) | Airflow plugin registration | Registers the macro and the timetable. |

**Deploy note:** zip the `ctm_plugins` package together with `ctm_plugin.py`
AND `mapping-config/nodes.yaml` into `plugins.zip` and upload it to the MWAA
environment (see `plugins/README.md`) — `CtmDatabaseJob` resolves `node` from
that file at parse time. Generated DAGs import from `ctm_plugins...` and will
not parse on a scheduler without the plugins installed.

## 4. Configuration files

### `mapping-config/nodes.yaml` — NODEID → connection

```yaml
defaults: {os: linux}            # os used when a node entry omits it
nodes:
  prdnode1: {conn_id: ssh_prdnode1, os: linux}       # -> SSHOperator
  winnode1: {conn_id: winrm_winnode1, os: windows}   # -> WinRMOperator
  dbnode1:  {conn_id: bank_dwh, type: db}            # -> CtmDatabaseJob conn
```

v1 flat entries (`<nodeid>: <conn_id>`) are still accepted (os =
`defaults.os`, ultimately `linux`). The optional `type: db` (v3) marks a
database endpoint used by `APPL_TYPE=DATABASE` jobs. Unmapped NODEIDs fall
back to `ssh_<nodeid>` with an `UNMAPPED_NODE` diagnostic. The file is read
TWICE: at codegen time (SSH/WinRM connection literals, os-based operator
choice, diagnostics) and at DAG-parse time by `CtmDatabaseJob` (which is why
it must ship inside `plugins.zip`).

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

## 5. Adding a new job type — the playbook

Control-M's job-type universe is open-ended (Application Integrator lets every
site define its own types), so the converter is built to be EXTENDED, not
complete. When a new `TASKTYPE`/`APPL_TYPE` shows up in your export (it lands
in the `unsupported` catch-all with an `UNSUPPORTED_TYPE` diagnostic, so it is
impossible to miss), walk this decision tree — **provider first**:

1. **An Airflow provider already exists for the target system** (Databricks,
   Snowflake, `KubernetesPodOperator`, the AWS/Azure/Google provider packages,
   HTTP/REST, SFTP, ...): add a registry row in
   `core/ctrlm_core/operator_registry.py` that maps the job's IR params onto
   that provider operator's kwargs. Write a custom `Ctm*` operator ONLY if the
   param/connection translation is complex enough to earn it (the
   `CtmDatabaseJob` bar: parse-time connection resolution, or translation
   logic that should ship once via `plugins.zip` instead of being baked into
   every generated DAG). Plain kwarg renaming does NOT earn a class.
2. **No provider exists**: write a `Ctm*` operator in `plugins/ctm_plugins/`
   that wraps the system's API once (hook/requests-based `execute()`), and add
   the registry row emitting it.
3. **Not automatable** (endpoints, credentials or semantics need humans, e.g.
   file-transfer endpoint pairs): leave it to the MANUAL rows — the
   `CtmManualJob` stub plus the `UNSUPPORTED_TYPE` diagnostic is the
   deliberate, loud safety net.

Every new registry row ships as a four-part change (the sync test
`tests/test_registry.py::test_registry_and_catalog_stay_in_sync` enforces the
first two):

- the `RegistryEntry` (ordered — insert BEFORE the rows it would shadow, and
  always before the `unsupported` catch-all);
- a row in this catalog **and** its name in the `registry-names` marker block;
- a sample-XML job exercising the type (under `examples/exports/`) so the
  end-to-end run covers it;
- an emit test in `tests/test_emit.py` asserting the generated task text
  (operator, kwargs, imports, diagnostics).

Common params (§2) come for free: `apply_common_params` runs for every row,
so a new operator only has to own what is genuinely type-specific.

## 6. Application-type roadmap

Known Control-M application/job types NOT yet auto-converted. Today every one
of them lands in a MANUAL row (`file_transfer`, `known_manual`, or the
`unsupported` catch-all) and emits a `CtmManualJob` stub + `UNSUPPORTED_TYPE`
diagnostic — this table is the honest answer to "what about all the other
types" and the suggested extension order via the §5 playbook.

| Application / job type | Likely target in Airflow | IR data it would need | Status today |
|---|---|---|---|
| SAP (R/3 jobs, BW process chains) | `Ctm*` custom operator over an RFC/BAPI hook (no first-class SAP provider); PARTIAL fallback: SSH to an SAP host running `sapevt`/RFC CLI | ABAP program/variant, BW chain id, SAP system/client, credentials conn | MANUAL (`known_manual`) |
| Informatica (PowerCenter workflows) | `Ctm*` custom operator wrapping the Informatica REST/`pmcmd` API | repository/folder/workflow name, run parameters, endpoint conn | MANUAL (`known_manual`) |
| Hadoop / Spark (incl. EMR) | Provider: `apache-airflow-providers-apache-spark` (`SparkSubmitOperator`) or `amazon` (`EmrAddStepsOperator`) | jar/py file, class, spark args, cluster/EMR id | MANUAL (`known_manual`) |
| AWS (Lambda / Batch / Step Functions) | Provider: `apache-airflow-providers-amazon` (`LambdaInvokeFunctionOperator`, `BatchOperator`, `StepFunctionStartExecutionOperator`) | function/job-queue/state-machine ARN, payload, region, AWS conn | MANUAL (catch-all) |
| Azure (Data Factory / Functions) | Provider: `apache-airflow-providers-microsoft-azure` (`AzureDataFactoryRunPipelineOperator`, Functions via HTTP) | pipeline name, resource group, factory, parameters, Azure conn | MANUAL (catch-all) |
| Databricks | Provider: `apache-airflow-providers-databricks` (`DatabricksRunNowOperator` / `DatabricksSubmitRunOperator`) | job id or notebook/task spec, cluster spec, workspace conn | MANUAL (catch-all) |
| Snowflake | Provider: `apache-airflow-providers-snowflake` (or route through `database` row with a Snowflake conn) | SQL text, warehouse/database/schema/role, Snowflake conn | MANUAL (catch-all) |
| Kubernetes | Provider: `apache-airflow-providers-cncf-kubernetes` (`KubernetesPodOperator`) | image, command/args, namespace, resources, cluster conn | MANUAL (catch-all) |
| File Transfer (AFT / MFT / FILE_TRANS) | Provider `sftp`/`ftp`/`amazon` (S3) transfer operators, or a `Ctm*` transfer operator once endpoint conventions are known | source/target endpoints + credentials, direction, path patterns, post-transfer actions | MANUAL (`file_transfer`) |
| Web Services / REST (`WEBSERVICES`) | Provider: `apache-airflow-providers-http` (`HttpOperator`) | URL/endpoint, method, payload template, auth conn | MANUAL (`known_manual`) |
| Java (standalone JVM jobs) | `SSHOperator` running `java -jar ...` on the target node (registry row, no custom class needed) | jar/classpath, main class, JVM args, node | MANUAL (`known_manual`) |
| IBM i (OS/400) | `Ctm*` custom operator over JT400/ODBC, or SSH to the IBM i host (`QSH`/`SBMJOB`) | library/program/command, job description, host conn | MANUAL (catch-all) |
| z/OS members (JCL) | `Ctm*` custom operator over FTP-JES / Zowe CLI | member/PDS, JES job card, completion-code rules, host conn | MANUAL (catch-all) |

Statuses update as rows are implemented; a type graduates out of this table by
following the §5 playbook (registry row + catalog row + sample XML + emit
test).
