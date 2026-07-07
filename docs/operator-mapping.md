# Control-M → Airflow operator mapping

> **v3 note — partially superseded.** The authoritative, registry-synced
> catalog is now [`job-mapping-catalog.md`](job-mapping-catalog.md)
> (job-type table, param-by-param mapping, `ctm_plugins` components,
> config schemas). In particular, since v3: `TASKTYPE=FileWatch` emits a real
> `ctm_plugins.sensors.CtmFileWatcherSensor` (no longer an `EmptyOperator` +
> TODO), `CONFIRM` emits an upstream `CtmApprovalGateSensor`, APPL_TYPE
> `DATABASE` emits `SQLExecuteQueryOperator`, and unknown/manual types emit
> `NotImplementedError` stubs. Where this document disagrees with the catalog,
> the catalog wins. The v2 material below (SSH vs WinRM selection, AUTOEDIT
> translation, cross-link mechanisms, time gates) is still accurate.

This document describes exactly what the DAG emitter (`core/ctrlm_core/emit.py`)
generates for each Control-M construct, and how you configure it. Generated
code targets Airflow 2.9+ (MWAA). The output is deterministic: regenerate
instead of editing DAG files by hand.

## Task types

| Control-M | Condition | Airflow operator | Provider package | Notes |
|---|---|---|---|---|
| `TASKTYPE=Command` / `Job` | node os `linux` and command is not PowerShell | `SSHOperator(task_id=..., ssh_conn_id=<conn>, command=<translated>)` | `apache-airflow-providers-ssh` | Command passed through the AUTOEDIT translator (table below). |
| `TASKTYPE=Command` / `Job` | node os `windows` **or** the command sniffs PowerShell | `WinRMOperator(task_id=..., ssh_conn_id=<conn>, command=<translated>)` | `apache-airflow-providers-microsoft-winrm` | The task is preceded by a `# provider: apache-airflow-providers-microsoft-winrm` comment. Yes, the WinRM connection kwarg really is named `ssh_conn_id` in the provider. |
| `TASKTYPE=Dummy` | — | `EmptyOperator(task_id=...)` | core | |
| Synthetic `__FOLDER_START__` / `__FOLDER_END__` | added by the desugar phase for folder-level conditions | `EmptyOperator(task_id=...)` | core | Carry the folder's schedule / fan-in semantics. |
| `TASKTYPE=FileWatch` | — | `EmptyOperator(task_id=...)` + `# TODO FileWatch -> SFTPSensor` | core (TODO: `apache-airflow-providers-sftp`) | Path mapping is site-specific; convert the TODO manually. |

**PowerShell sniffing** (case-insensitive, applied to the raw `CMDLINE` /
`MEMLIB/MEMNAME`): the command references a `.ps1` script (`\.ps1\b`) **or**
its first word is `powershell`. Either match forces `WinRMOperator`, even for
a node mapped as `linux` (a `.ps1` cannot run under a plain POSIX shell).

## Connection resolution (`mapping-config/nodes.yaml`)

v2 schema:

```yaml
defaults: {os: linux}          # os used when a node entry omits it
nodes:
  prdnode1: {conn_id: ssh_prdnode1, os: linux}
  winnode1: {conn_id: winrm_winnode1, os: windows}
```

v1 flat entries are still accepted (and may be mixed with a `defaults:` key);
they resolve with `os` = `defaults.os` (ultimately `linux`):

```yaml
prdnode1: ssh_prdnode1
```

Resolution order for a job's `NODEID`:

1. `nodes.<NODEID>.conn_id` (v2) or the flat `NODEID: <conn_id>` value (v1);
   `os` from the entry, else `defaults.os`, else `linux`.
2. Unmapped `NODEID` → connection `ssh_<nodeid>`, os `linux`, plus a WARN
   diagnostic `UNMAPPED_NODE` in `partition.json`.
3. No `NODEID` at all → connection `ssh_default`, os `linux` (no diagnostic).

The resolved connection id is emitted verbatim as the operator's
`ssh_conn_id` (both SSH and WinRM operators use that kwarg name). Create the
matching Airflow connections (type `ssh` / type `winrm`) in MWAA.

## AUTOEDIT variable translation

Applied to every Command/Job command string (`core/ctrlm_core/autoedit.py`):

| Control-M | Airflow replacement |
|---|---|
| `%%ODATE` | `{{ ds_nodash }}` |
| `%%$ODATE` | `{{ ds }}` |
| `%%DATE` | `{{ ds_nodash }}` |
| `%%TIME` | `{{ ts_nodash }}` |
| `%%JOBNAME` | the literal job name, substituted at generation time |
| any other `%%NAME` | left verbatim; the task gets a `# TODO unresolved AUTOEDIT: ...` comment and an `UNRESOLVED_AUTOEDIT` WARN diagnostic |

Matching is longest-token: `%%ODATEV` is a distinct (unresolved) variable,
not `%%ODATE` + `V`.

## Cross-DAG link mechanisms

Condition edges that end up between two DAGs are realized as:

| Mechanism | When | Generated code |
|---|---|---|
| `sensor` | consumer DAG is time-scheduled | `ExternalTaskSensor(task_id="wait_<producer task>", external_dag_id=..., external_task_id=..., mode="reschedule", timeout=<see below>)` upstream of the consumer task. `REVIEW`-kind links also use this plus a WARN diagnostic. |
| `dataset` | consumer DAG has no schedule of its own | Producer task gets `outlets=[Dataset("ctrlm://cond/<COND>")]`; the consumer DAG is created with `schedule=[Dataset("ctrlm://cond/<COND>"), ...]`. |
| `prev_run_sensor` | the in-condition was `ODATE="PREV"` | Same `ExternalTaskSensor` as `sensor`, plus a `# TODO align to previous run` comment — review the execution-date offset manually. |

Every cross link is also listed in `partition.json` (`cross_links`) and
`cluster-map.yaml` with its cause (`HUB`, `PATTERN`, `AUTO_RESOLVED`,
`ANCHOR`, `OWNER_SPLIT`, `PREV_RUN`, `REVIEW`, ...).

## Time gates (TIMEFROM inside one DAG)

A member whose `TIMEFROM` (on the Control-M ODATE clock, New Day = `0600` by
default) is later than the DAG's anchor gets an upstream
`DateTimeSensorAsync(task_id="gate_<task>", target_datetime="{{ (data_interval_end
+ macros.timedelta(days=<0|1>)).replace(hour=<H>, minute=<M>) }}")` — day
offset `1` when the gate's wall-clock `HHMM` is earlier than the anchor's
(i.e. it fires after midnight of the next calendar day).

## Retries, timeouts, schedules

| Control-M | Airflow |
|---|---|
| `MAXRERUN` | DAG-level `default_args={"retries": max(MAXRERUN of members)}` |
| `MAXWAIT` (days) | `ExternalTaskSensor timeout = MAXWAIT * 86400` seconds; `21600` (6 h) when unset |
| Folder/job `WEEKDAYS` / `DAYS` / `MONTHS` + earliest root `TIMEFROM` | DAG `schedule="<cron>"` (`DAYS_AND_OR=AND` is approximated, flagged `CRON_AND_APPROX`) |
| No day pattern (purely condition-driven) | dataset-triggered DAG (`schedule=[Dataset(...)]`) |
| `CYCLIC=1` + `INTERVAL` | own single-job DAG with a `*/N`-style cron limited to the `TIMEFROM`–`TIMETO` window |
| `CONFIRM=1` | flagged `CONFIRM_JOB` (manual-confirmation semantics are not auto-generated) |

## Diagnostics emitted by this stage

`UNMAPPED_NODE` (warn), `UNRESOLVED_AUTOEDIT` (warn) — appended to
`partition.json` `diagnostics`. Every generated `dags/*.py` is validated with
`py_compile`; generation fails loudly on a syntax error.
