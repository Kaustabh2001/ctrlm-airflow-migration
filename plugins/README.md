# plugins/ — write-once Control-M compatibility components for Airflow (MWAA)

Custom operators/sensors/timetables/callbacks written ONCE and imported by
every generated DAG (`from ctm_plugins... import ...`). Deployed to MWAA as
`plugins.zip`. See `docs/job-mapping-catalog.md` for the full job-type and
param-by-param mapping this package backs — including the "Adding a new job
type" playbook and the application-type roadmap.

**Airflow version target (v6): Airflow 3 on MWAA, with 2.x kept working.**
`ctm_plugins/_compat.py` is a dual-compat import shim: every airflow base
class whose canonical path moved in Airflow 3 (`BaseOperator`,
`BaseSensorOperator`, `Variable` — all exported from `airflow.sdk` per
AIP-72) is imported there via try-Airflow-3-path / `except ImportError` →
fall back to the 2.x path, so ONE plugins.zip deploys unchanged on either
major version. Paths verified UNCHANGED in Airflow 3 (no shim needed):
`airflow.plugins_manager.AirflowPlugin`, `airflow.timetables.base.Timetable`,
`SQLExecuteQueryOperator` (common.sql), `S3Hook` (amazon), `SFTPHook` (sftp).
Note: `Variable.get`'s default kwarg was renamed in 3.0 (`default_var=` →
`default=`); the sensors pass the default positionally, valid on both.

**Operator policy (v4):** custom operators exist ONLY where they add real
capability (connection resolution, translation logic, watchers, gates, loud
manual stubs). Command/Job tasks stay plain `SSHOperator`/`WinRMOperator` and
`Dummy` stays `EmptyOperator`; their common Control-M params
(`priority_weight`, `pool`/`pool_slots`, callbacks, `email`, retries — but no
longer `sla`, which Airflow 3 removed; see the `SLA_AF3_REMOVED` diagnostic)
are translated at CODEGEN time by `core/ctrlm_core/operator_registry.py` —
which is also the single home of the PRIORITY/CRITICAL -> `priority_weight`
formula (`AA`=100 .. `ZZ`=1, CRITICAL floors at 90).

## Components

| Component | File | Maps (Control-M) | Notes |
|---|---|---|---|
| `CtmDatabaseJob(node=..., sql=..., ...)` | `ctm_plugins/operators.py` | `APPL_TYPE=DATABASE` | `SQLExecuteQueryOperator` subclass (provider `common-sql`). `node` resolves to `conn_id` at DAG-parse time via `mapping-config/nodes.yaml` (`type: db` entries; unmapped nodes fall back to `ssh_<node>`); an explicit `conn_id` kwarg overrides the lookup. All standard operator kwargs pass through. Also the single landing place for future DB-specific behavior (stored-proc handling, output capture). |
| `CtmManualJob(ctm_task_type=..., ctm_appl_type=..., ctm_job=..., ...)` | `ctm_plugins/operators.py` | job types with no automatic mapping (FILE_TRANS, SAP, ...) | `BaseOperator` stub replacing the old emitted `PythonOperator`+prelude pattern; `execute()` raises `NotImplementedError` naming `ctm_job`, `ctm_task_type` and `ctm_appl_type` — loud, actionable, impossible to mistake for success. |
| `resolve_node(node)` | `ctm_plugins/_params.py` | NODEID -> Airflow connection | Pure (airflow-free, unit-tested) `{conn_id, os, type}` lookup behind `CtmDatabaseJob`; reads nodes.yaml (v2 and v1 flat schemas), fallback `ssh_<node>`. |
| `ctm_odate(logical_dt, new_day_hhmm="0600", fmt="%Y%m%d")` | `ctm_plugins/_odate.py` | `%%ODATE` semantics under a non-midnight New Day time | Pure function, registered as a template macro: `{{ macros.ctm_odate(data_interval_end) }}`. A 05:00 fire with New Day 06:00 yields the PREVIOUS calendar date. |
| `gate_target(logical_dt, gate_hhmm, new_day_hhmm="0600")` | `ctm_plugins/_odate.py` | `TIMEFROM` gates that cross midnight within one ODATE | Returns the occurrence of `gate_hhmm` belonging to the SAME odate (fire 22:00, gate 02:00 → next morning). Also a macro. |
| `ctm_shout(dest, message="", when="NOTOK")` | `ctm_plugins/callbacks.py` | `SHOUT` / `ON ... DOMAIL` / `DOSHOUT` | Returns a callable for `on_failure_callback` / `on_success_callback` (on Airflow 2.x also usable as `sla_miss_callback`; Airflow 3 removed SLAs). Destination resolved via `mapping-config/notify.yaml`; a dest containing `@` is emailed directly; unknown dests log only. Email/SNS sends are late-imported inside the callable. |
| `CtmApprovalGateSensor` | `ctm_plugins/sensors.py` | `CONFIRM=1` (manual confirmation) | Waits (mode `reschedule`, poke 60s) for Airflow Variable `ctm_approve/<dag_id>/<task_id>/<ds>` == `yes`. Approve in the UI (Admin → Variables) or `airflow variables set`. |
| `CtmFileWatcherSensor(path=...)` | `ctm_plugins/sensors.py` | `FILEWATCH` jobs / `TASKTYPE=FileWatch` | Scheme dispatch: local path exists / `s3://bucket/key` via S3Hook / `sftp://host/path` via SFTPHook. |
| `CtmCalendarTimetable(calendar_name, anchor_hhmm)` | `ctm_plugins/timetables.py` | periodic/user calendars (DAYSCAL/WEEKCAL) | Fires at `anchor_hhmm` UTC on exactly the dates listed in `mapping-config/calendars.yaml`. |
| `CtmPlugin` | `ctm_plugin.py` | — | `AirflowPlugin` registering the two macros and the timetable (the `Ctm*` operators and sensors are imported directly by generated DAGs; no registration needed on Airflow 2 or 3). |
| `_compat` shim | `ctm_plugins/_compat.py` | — | Airflow 3 / 2.x dual-compat imports: `BaseOperator`, `BaseSensorOperator`, `Variable` tried from `airflow.sdk` first (3.x canonical), 2.x paths on `ImportError`. Airflow-importing wrapper; py_compile-checked only in this repo. |

Import discipline (this repo runs on Windows, where Airflow cannot be
installed): `_odate.py`, `_params.py` and `callbacks.py` are airflow-free at
import time and fully unit-tested; `timetables.py` guards its airflow import
so its pure date-selection helper (`select_next_fire`) is unit-tested too;
`operators.py`, `sensors.py`, `_compat.py` and `ctm_plugin.py` import airflow
at module level and are only syntax-checked (`py_compile`) by
`tests/test_plugins.py` (which also asserts the `_compat.py` shim pattern
textually).

## Config files (ship them with the zip or point env vars at them)

- `mapping-config/notify.yaml` — `{dest: {type: email|sns|log, target: ...}}`;
  override location with env var `CTM_NOTIFY_CONFIG`.
- `mapping-config/calendars.yaml` — `{calendar_name: ["YYYY-MM-DD", ...]}`;
  override with `CTM_CALENDARS_CONFIG`.
- `mapping-config/nodes.yaml` — NODEID -> Airflow connection mapping
  (`defaults: {os: ...}` + `nodes: {<id>: {conn_id: ..., os: ..., type: ...}}`;
  v1 flat `<id>: <conn_id>` entries also accepted); override with
  `CTM_NODES_CONFIG`. **Since v4 this file MUST ship in plugins.zip** —
  `CtmDatabaseJob` resolves `node` -> `conn_id` at DAG-parse time on MWAA
  (unmapped nodes degrade to the `ssh_<node>` fallback).

Lookup: an explicit path argument, or failing that the env var, is
authoritative (a missing file then just means log-only shouts / empty
calendars). Without either, the search order is `mapping-config/` next to the
deployed `ctm_plugins` package (i.e. inside the extracted plugins.zip) →
repo-root `mapping-config/` (dev layout) → `mapping-config/` under the
current working directory. Missing/malformed files degrade gracefully — they
never crash the scheduler or a worker.

## Building plugins.zip

`ctm_plugin.py` and the `ctm_plugins/` package must sit at the ZIP ROOT
(MWAA puts that root on `sys.path`). Bundle `mapping-config/` alongside them
so the default config lookup finds it.

PowerShell (from the repo root):

```powershell
Compress-Archive -Force `
  -Path plugins\ctm_plugin.py, plugins\ctm_plugins, mapping-config `
  -DestinationPath plugins.zip
```

bash/zip (from the repo root):

```bash
(cd plugins && zip -r ../plugins.zip ctm_plugin.py ctm_plugins -x "*__pycache__*")
zip -r plugins.zip mapping-config
```

Resulting layout:

```
plugins.zip
├── ctm_plugin.py
├── ctm_plugins/
│   ├── __init__.py
│   ├── _compat.py          # Airflow 3 / 2.x import shim (v6)
│   ├── _odate.py
│   ├── _params.py
│   ├── callbacks.py
│   ├── operators.py
│   ├── sensors.py
│   └── timetables.py
└── mapping-config/
    ├── notify.yaml
    ├── calendars.yaml
    └── nodes.yaml          # REQUIRED since v4 (Ctm* node resolution)
```

## Deploying to MWAA

1. Upload: `aws s3 cp plugins.zip s3://<mwaa-bucket>/plugins.zip`.
2. In the MWAA environment (console or `aws mwaa update-environment`), set
   **Plugins file** to `plugins.zip` and pin the new **S3 object version**
   (MWAA requires versioned plugin objects).
3. Ensure `requirements.txt` for the environment includes the providers the
   generated DAGs and sensors use: `apache-airflow-providers-ssh`,
   `apache-airflow-providers-microsoft-winrm`,
   `apache-airflow-providers-sftp`, `apache-airflow-providers-amazon`,
   `apache-airflow-providers-common-sql` (boto3 ships with MWAA).
4. Apply the update and wait for the environment to cycle; verify in the UI
   under Admin → Plugins that `ctm_plugin` is listed, then trigger a DAG that
   uses `{{ macros.ctm_odate(...) }}`.
5. Every plugins.zip change requires another environment update (MWAA
   installs plugins only at worker/scheduler startup).

Local smoke-test alternative: [aws-mwaa-local-runner] with the same
`plugins.zip`, `requirements.txt`, and the generated `dags/` folder.

[aws-mwaa-local-runner]: https://github.com/aws/aws-mwaa-local-runner
