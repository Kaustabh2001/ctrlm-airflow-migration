# plugins/ — write-once Control-M compatibility components for Airflow (MWAA)

Custom operators/sensors/timetables/callbacks written ONCE and imported by
every generated DAG (`from ctm_plugins... import ...`). Deployed to MWAA as
`plugins.zip`. See `docs/job-mapping-catalog.md` for the full job-type and
param-by-param mapping this package backs.

## Components

| Component | File | Maps (Control-M) | Notes |
|---|---|---|---|
| `ctm_odate(logical_dt, new_day_hhmm="0600", fmt="%Y%m%d")` | `ctm_plugins/_odate.py` | `%%ODATE` semantics under a non-midnight New Day time | Pure function, registered as a template macro: `{{ macros.ctm_odate(data_interval_end) }}`. A 05:00 fire with New Day 06:00 yields the PREVIOUS calendar date. |
| `gate_target(logical_dt, gate_hhmm, new_day_hhmm="0600")` | `ctm_plugins/_odate.py` | `TIMEFROM` gates that cross midnight within one ODATE | Returns the occurrence of `gate_hhmm` belonging to the SAME odate (fire 22:00, gate 02:00 → next morning). Also a macro. |
| `ctm_shout(dest, message="", when="NOTOK")` | `ctm_plugins/callbacks.py` | `SHOUT` / `ON ... DOMAIL` / `DOSHOUT` | Returns a callable for `on_failure_callback` / `on_success_callback` / `sla_miss_callback`. Destination resolved via `mapping-config/notify.yaml`; a dest containing `@` is emailed directly; unknown dests log only. Email/SNS sends are late-imported inside the callable. |
| `CtmApprovalGateSensor` | `ctm_plugins/sensors.py` | `CONFIRM=1` (manual confirmation) | Waits (mode `reschedule`, poke 60s) for Airflow Variable `ctm_approve/<dag_id>/<task_id>/<ds>` == `yes`. Approve in the UI (Admin → Variables) or `airflow variables set`. |
| `CtmFileWatcherSensor(path=...)` | `ctm_plugins/sensors.py` | `FILEWATCH` jobs / `TASKTYPE=FileWatch` | Scheme dispatch: local path exists / `s3://bucket/key` via S3Hook / `sftp://host/path` via SFTPHook. |
| `CtmCalendarTimetable(calendar_name, anchor_hhmm)` | `ctm_plugins/timetables.py` | periodic/user calendars (DAYSCAL/WEEKCAL) | Fires at `anchor_hhmm` UTC on exactly the dates listed in `mapping-config/calendars.yaml`. |
| `CtmPlugin` | `ctm_plugin.py` | — | `AirflowPlugin` registering the two macros and the timetable. |

Import discipline (this repo runs on Windows, where Airflow cannot be
installed): `_odate.py` and `callbacks.py` are airflow-free at import time and
fully unit-tested; `timetables.py` guards its airflow import so its pure
date-selection helper (`select_next_fire`) is unit-tested too; `sensors.py`
and `ctm_plugin.py` import airflow at module level and are only
syntax-checked (`py_compile`) by `tests/test_plugins.py`.

## Config files (ship them with the zip or point env vars at them)

- `mapping-config/notify.yaml` — `{dest: {type: email|sns|log, target: ...}}`;
  override location with env var `CTM_NOTIFY_CONFIG`.
- `mapping-config/calendars.yaml` — `{calendar_name: ["YYYY-MM-DD", ...]}`;
  override with `CTM_CALENDARS_CONFIG`.

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
│   ├── _odate.py
│   ├── callbacks.py
│   ├── sensors.py
│   └── timetables.py
└── mapping-config/
    ├── notify.yaml
    └── calendars.yaml
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
