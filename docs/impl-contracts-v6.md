# Implementation contracts — v6 delta (Airflow 3 authoring style)

Delta on v1–v5. USER DECISION: generated DAGs must use the Airflow 3
decorator-based authoring style (@dag) and Airflow 3 APIs. Single target —
no dual 2.x/3.x emission mode (2.x style lives in git history; a config knob
can be added later if ever needed).

What "Airflow 3 style" means here, precisely:
- `@dag(...)`-decorated function per DAG + a module-bottom call, replacing
  `with DAG(...) as dag:`. Classic operator tasks are still instantiated as
  operator objects inside the function (correct TaskFlow idiom — `@task` is
  for Python callables, which converted Control-M jobs are not).
- Authoring imports from the `airflow.sdk` namespace: `dag`, `TaskGroup`,
  `Asset` (Datasets were renamed Assets in 3.0; `airflow.datasets` is gone).
- Core operators moved to the standard provider in 3.x: EmptyOperator,
  TriggerDagRunOperator, ExternalTaskSensor, DateTimeSensorAsync now import
  from `airflow.providers.standard.*` paths. SSH / WinRM / common.sql
  provider paths are unchanged.
- The `sla` parameter was REMOVED in Airflow 3.0. SHOUT WHEN LATE therefore
  can no longer emit `sla=`; see V6-1.
- VERIFY exact import paths against the current Airflow 3 docs (you have
  WebFetch/WebSearch — use them for airflow.apache.org) rather than guessing;
  record any correction as a code comment.

## V6-1: emit + registry + docs (emit agent)

Owner files: `core/ctrlm_core/emit.py`, `core/ctrlm_core/operator_registry.py`,
`core/ctrlm_core/templates/dag.py.j2`, `docs/job-mapping-catalog.md`,
`README.md` + `CLAUDE.md` (only the lines saying generated DAGs target
Airflow 2.9+ — change to Airflow 3 / MWAA), `tests/test_emit.py`,
`tests/test_registry.py`.

- Template: emit
  ```python
  @dag(
      dag_id="fin_eod",
      schedule="0 6 * * 1,2,3,4,5",        # or [Asset(...)] or None
      start_date=datetime(2026, 1, 1),
      catchup=False,
      default_args={"retries": 0},
      tags=[...],
  )
  def fin_eod():
      ...tasks, groups, dependencies...

  fin_eod()
  ```
  Function name = dag_id (already a valid identifier); dag_id still passed
  explicitly. TaskGroups, gates, sensors, force tasks, provenance headers,
  black formatting, py_compile validation, determinism: all unchanged in
  substance.
- Imports: `from airflow.sdk import dag` (+ `TaskGroup`, `Asset` when used);
  standard-provider paths for Empty/TriggerDagRun/ExternalTaskSensor/
  DateTimeSensorAsync; unchanged provider paths for SSHOperator,
  WinRMOperator; ctm_plugins imports unchanged. No `from airflow import DAG`,
  no `airflow.datasets` anywhere in emitted code.
- Assets: outlets `[Asset("ctrlm://cond/<name>")]`; asset-triggered DAGs
  `schedule=[Asset(...), ...]`.
- SLA removal: do NOT emit `sla=`. For SHOUT WHEN LATE emit
  `# TODO Airflow 3 removed SLAs; map to Deadline Alerts (3.1+): late after <n>m`
  on the task and a PARTIAL diagnostic `SLA_AF3_REMOVED` (message naming the
  job and the late window). Keep the late window in dag_plans.json untouched
  (schema unchanged — the dashboard must not need changes).
- `execution_date_fn`/logical-date handling on ExternalTaskSensor: verify the
  Airflow 3 signature in the docs; emit whatever is current with a comment.
- Catalog: update the imports/param rows (Dataset->Asset, sla row ->
  SLA_AF3_REMOVED path), add an "Airflow 3 target" note; keep the
  registry<->catalog sync test green.
- Tests: assert `@dag(` + module-bottom call in emitted files and that
  `with DAG(` / `airflow.datasets` / `sla=` no longer appear; Asset outlets;
  standard-provider import lines; everything still py_compiles; determinism.

## V6-2: plugins Airflow-3 compatibility (plugins agent)

Owner files: `plugins/ctm_plugins/operators.py`, `plugins/ctm_plugins/sensors.py`,
`plugins/ctm_plugins/timetables.py`, `plugins/ctm_plugin.py`,
`plugins/README.md`, `tests/test_plugins.py` (additive).

- Update airflow imports to Airflow 3 paths with a 2.x fallback shim so the
  package deploys on either (try new path, except ImportError -> old path,
  in ONE `_compat.py`-style block per module or a tiny shared helper you own):
  BaseOperator / BaseSensorOperator (verify 3.x canonical paths — airflow.sdk
  exposes BaseOperator; sensors' base moved), SQLExecuteQueryOperator
  (common.sql — likely unchanged), Timetable base + AirflowPlugin
  (plugins_manager — verify), hooks used by CtmFileWatcherSensor.
- Pure modules (_odate, _params, callbacks resolution) are untouched.
- plugins/README.md: note the dual-compat shim and the Airflow 3 target.
- Tests: py_compile still green for all wrapper modules; pure tests untouched
  and passing; add a test asserting the compat shim pattern exists (textual).

## Integration + verification (single agent, combined)

Full pytest green (fix fallout anywhere); regenerate output/components,
output/single_entry, dashboard; verify on real artifacts: every dags/*.py
contains `@dag(` and a module-bottom `<dag_id>()` call, zero occurrences of
`with DAG(`, `airflow.datasets`, or `sla=` across all generated files; Asset
outlets on the FILE-ARRIVED producer and asset-triggered STG schedule;
standard-provider imports present; BANK_SETTLE has the SLA_AF3_REMOVED
diagnostic + TODO comment; dag_plans.json schema unchanged and dashboard
builds with the new outputs; per-scope byte-determinism; run_all.ps1
-SkipPush green end to end. Do NOT git commit.
