# Implementation contracts — v3 delta (job-type registry, plugins package, full param mapping)

Delta on docs/impl-contracts.md + docs/impl-contracts-v2.md (still authoritative
where not overridden). Goal, in the user's words: a mapping of ALL possible
Control-M job types to Airflow, param-by-param, with custom operators written
ONCE and reused. Three artifacts enforce that:

1. `core/ctrlm_core/operator_registry.py` — declarative registry, the single
   source of truth the emitter consults.
2. `plugins/ctm_plugins/` — the write-once custom components package
   (deployable to MWAA as plugins.zip); generated DAGs import from it.
3. `docs/job-mapping-catalog.md` — the user-facing catalog, kept in sync with
   the registry by a test.

`core/ctrlm_core/model.py` gained `Job.appl_type`, `Job.priority`,
`Job.critical` (already done — never modify model.py or pipeline.py).

## V3-2: Plugins package (plugins agent)

Owner files: `plugins/ctm_plugins/__init__.py`, `plugins/ctm_plugins/_odate.py`,
`plugins/ctm_plugins/callbacks.py`, `plugins/ctm_plugins/sensors.py`,
`plugins/ctm_plugins/timetables.py`, `plugins/ctm_plugin.py` (AirflowPlugin
registration), `plugins/README.md`, `tests/test_plugins.py`.

DESIGN RULE: pure logic lives in airflow-free modules (unit-testable here,
where Airflow is NOT installed); anything importing airflow is a thin wrapper
that is only syntax-checked (py_compile) by tests.

- `_odate.py` (PURE python): `ctm_odate(logical_dt, new_day_hhmm="0600",
  fmt="%Y%m%d") -> str` — the run's Control-M order date: calendar date of the
  fire time, shifted back one day when fire time-of-day < New Day time.
  Plus `gate_target(logical_dt, gate_hhmm, new_day_hhmm)` -> datetime of the
  next occurrence of gate_hhmm belonging to the SAME odate. Exhaustive unit
  tests (2200 fire / 0200 gate; 0500 fire before New Day 0600; midnight edges).
- `callbacks.py`: `ctm_shout(dest: str, message: str = "", when: str = "NOTOK")`
  returns a callback callable for on_failure_callback / on_success_callback /
  sla_miss_callback. Destination resolution: reads
  `mapping-config/notify.yaml` ({dest: {type: email|sns|log, target: ...}});
  unknown dest -> log-only. The airflow-touching send paths (SES email via
  airflow.utils.email.send_email, SNS via boto3) are late imports inside the
  callable so the module itself imports airflow-free; resolution + message
  formatting are pure and unit-tested.
- `sensors.py` (airflow imports at module level — syntax-check only):
  `CtmApprovalGateSensor(BaseSensorOperator)` — pokes an Airflow Variable
  `ctm_approve/<dag_id>/<task_id>/<ds>` == "yes" (mode reschedule, poke 60s);
  maps Control-M CONFIRM. `CtmFileWatcherSensor(BaseSensorOperator)` — path by
  scheme: local path exists / `s3://` via S3Hook / `sftp://` via SFTPHook;
  maps FILEWATCH jobs.
- `timetables.py` (airflow imports — syntax-check only):
  `CtmCalendarTimetable(Timetable)` — reads
  `mapping-config/calendars.yaml` ({calendar_name: ["YYYY-MM-DD", ...]}) and
  fires at a fixed anchor time on exactly the listed dates. Date-selection
  logic delegated to a pure helper in the same file, unit-tested via that
  helper. Ship a small example calendars.yaml entry (BANK_BUS_DAYS) in
  mapping-config/ (create the file).
- `plugins/ctm_plugin.py`: `class CtmPlugin(AirflowPlugin)` registering macros
  {"ctm_odate": ...} and the timetable. Syntax-check only.
- `plugins/README.md`: what each component maps, how to build plugins.zip
  (zip the ctm_plugins package + ctm_plugin.py), MWAA deploy note.
- `mapping-config/notify.yaml` NEW (you own it): example entries
  (OPS: {type: log}, ops@corp.com passthrough rule: a dest containing "@" is
  treated as email directly).

## V3-3: Registry + emit integration (registry agent)

Owner files: `core/ctrlm_core/operator_registry.py`, `core/ctrlm_core/emit.py`,
`core/ctrlm_core/templates/dag.py.j2`, `docs/job-mapping-catalog.md`,
`tests/test_emit.py`, `tests/test_registry.py`.

- `operator_registry.py`: ordered list of entries
  `RegistryEntry(name, matches(job)->bool, status: FULL|PARTIAL|MANUAL,
  imports: list[str], build(job, ctx)->TaskPlan)` where TaskPlan carries
  operator class name, kwargs (as literal-renderable values), provider comment,
  extra upstream tasks (gates/sensors), extra imports. ctx gives node mapping,
  config, scope info. Resolution = first match; LAST entry is the catch-all
  MANUAL stub (PythonOperator raising NotImplementedError with the original
  TASKTYPE/APPL_TYPE named, status MANUAL, diagnostic code UNSUPPORTED_TYPE).
- Registry rows (order matters):
  1. Dummy / synthetic -> EmptyOperator (FULL)
  2. FILEWATCH (task_type FileWatch or appl_type FILEWATCH) ->
     `ctm_plugins.sensors.CtmFileWatcherSensor(path=command, deferrable-safe
     defaults)` (FULL)
  3. DATABASE (appl_type DATABASE) -> `SQLExecuteQueryOperator` from
     airflow.providers.common.sql.operators.sql, `conn_id` from nodes.yaml
     entry (v2 schema gains optional `type: db`), `sql` = command through
     autoedit (FULL, PARTIAL if conn unmapped)
  4. FILE_TRANS / AFT / MFT (appl_type FILE_TRANS) -> MANUAL stub with comment
     naming source/target (MANUAL — transfer direction/endpoints need humans)
  5. SAP / INFORMATICA / HADOOP / other known appl_types -> MANUAL stub
  6. Command/Job on windows-or-PowerShell -> WinRMOperator (existing v2 rule,
     moved into the registry) (FULL)
  7. Command/Job otherwise -> SSHOperator (FULL)
- Param mapping applied by emit for EVERY task regardless of operator (this IS
  the param-by-param contract; also documented in the catalog):
  JOBNAME->task_id (sanitized) · DESCRIPTION->doc_md · APPLICATION/
  SUB_APPLICATION->tags (dag-level, deduped) · NODEID->conn via nodes.yaml ·
  MAXRERUN->retries · RERUNINTERVAL->retry_delay=timedelta(minutes=n) ·
  MAXWAIT->existing sensor timeouts · TIMEFROM->existing time gates but NOW
  emitted via `{{ ctm_odate }}`-consistent gate targeting (keep current
  DateTimeSensorAsync approach; add comment referencing ctm_plugins._odate) ·
  PRIORITY (AA highest..ZZ) -> priority_weight (map: two-letter code ->
  int 1..100, e.g. 'AA'->100 linear down; document formula) · CRITICAL ->
  priority_weight floor 90 + comment · CONFIRM -> upstream
  `CtmApprovalGateSensor` task `confirm_<task_id>` (FULL now, replaces
  nothing) · QUANTITATIVE -> `pool="<resource name>", pool_slots=<quant>`
  (first resource; extra resources -> PARTIAL diagnostic MULTI_RESOURCE) +
  collect all pools into `<scope>/config/pools.json` [{name, slots: max seen
  quant, source: "quantitative"}] ; CONTROL type E -> pool `<name>` slots 1 ·
  VARIABLE -> task-level `params` dict (literal) · ON code NOTOK + DOMAIL or
  SHOUT WHEN NOTOK -> `on_failure_callback=ctm_shout(dest=..., message=...)`
  + import from ctm_plugins.callbacks; DOMAIL dest with "@" also sets
  `email=[...]`, `email_on_failure=True` · ON ... DOFORCEJOB ->
  TriggerDagRunOperator task `force_<target>` downstream with
  trigger_rule="one_failed" for NOTOK codes / "all_success" for OK
  (trigger_dag_id resolved via assignments when the forced job is in this
  run's scope, else literal snake_case(jobname) + PARTIAL diagnostic
  FORCEJOB_UNRESOLVED) · ON ... DOCOND ADD -> extra Dataset outlet
  `ctrlm://cond/<name>` · SHOUT WHEN LATE -> `sla=timedelta(...)` when TIMETO
  present (sla = timeto - timefrom) + PARTIAL diagnostic SLA_APPROX ·
  remaining DO types (DOSTOPCYCLIC, DO_IFRERUN, DOACTION...) -> comment
  `# TODO unmapped ON/DO action ...` + PARTIAL diagnostic UNMAPPED_ACTION.
- Generated files import from `ctm_plugins...` (plugins are on the Airflow
  image path via plugins.zip; for py_compile that is irrelevant). Keep every
  emitted file py_compile-clean and black-formatted; determinism unchanged.
- `docs/job-mapping-catalog.md` (user-facing, the deliverable the user asked
  for): section 1 job-type table (every registry row: Control-M type ->
  operator -> status -> notes); section 2 the param-by-param table above;
  section 3 custom components inventory (what plugins/ctm_plugins provides,
  what each maps, deploy note); section 4 config files (nodes.yaml,
  notify.yaml, calendars.yaml) with schemas.
- `tests/test_registry.py` MUST include the sync test: every RegistryEntry.name
  appears verbatim in docs/job-mapping-catalog.md (and vice versa via a
  marker list in the doc), so catalog and code cannot drift.

## V3-4: Parser + sample (parser agent)

Owner files: `core/ctrlm_core/parser.py`, NEW `examples/exports/sample_bank.xml`,
`tests/test_parser.py` (additive).

- Parser: read `APPL_TYPE`, `PRIORITY`, `CRITICAL` ("0"/"1") into the new Job
  fields. Unknown appl_types pass through verbatim.
- NEW `sample_bank.xml` (do not touch other samples): FOLDER `BANK_EOD`
  (datacenter CTM_PROD): `BANK_BAL_CHECK` (APPL_TYPE DATABASE, NODEID dbnode1,
  CMDLINE `SELECT COUNT(*) FROM balances WHERE ds='%%ODATE'`, weekdays 1-5,
  TIMEFROM 1800) OUTCOND `BANK-BAL-OK`; `BANK_SETTLE` (Command, NODEID
  prdnode1, in `BANK-BAL-OK`, PRIORITY "AA", CRITICAL 1, QUANTITATIVE
  SETTLE_SLOTS 3, ON STMT * CODE NOTOK with DOMAIL DEST ops@corp.com +
  DOFORCEJOB JOBNAME BANK_RECON, SHOUT WHEN LATE) OUTCOND `BANK-SETTLED`;
  `BANK_RECON` (Command, prdnode1, no schedule, in `BANK-SETTLED` OR-logic
  alt: keep simple AND); `BANK_FT_STATEMENTS` (APPL_TYPE FILE_TRANS, in
  `BANK-SETTLED`) — the MANUAL-stub showcase; `BANK_MAINFRAME_SYNC`
  (APPL_TYPE SAP) — second MANUAL showcase.
- nodes.yaml: registry agent owns adding `dbnode1: {conn_id: bank_dwh,
  type: db}` (coordinate: parser agent does NOT touch nodes.yaml).
- Tests: appl_type/priority/critical parsed; sample_bank job count; other
  samples unchanged.

## Integration + verification

- Integrator: full pytest green (job-count assertions will shift with the new
  sample — fix them); run both strategies + dashboard on all 5 XMLs; confirm
  per the Verifier list; README pointer to docs/job-mapping-catalog.md and
  plugins/README.md; scripts/run_all.ps1 still works.
- Verifier (real outputs): bank scope exists; BANK_BAL_CHECK emitted as
  SQLExecuteQueryOperator with conn bank_dwh and translated sql (ds_nodash);
  BANK_SETTLE has pool="SETTLE_SLOTS", pool_slots=3, priority_weight >= 90,
  on_failure_callback=ctm_shout(...), email=["ops@corp.com"], a downstream
  force_bank_recon TriggerDagRunOperator with trigger_rule="one_failed";
  `<scope>/config/pools.json` lists SETTLE_SLOTS(3); FILE_TRANS + SAP jobs
  emitted as MANUAL stubs raising NotImplementedError + UNSUPPORTED_TYPE
  diagnostics; HR_PAY_RUN (existing hr sample: CONFIRM + QUANTITATIVE DB_SLOTS
  + ON NOTOK DOMAIL) now gains confirm_ sensor upstream, pool, callback;
  registry<->catalog sync test passes; every dags/*.py py_compiles; plugins
  pure-logic tests pass (odate edges); per-scope determinism still
  byte-identical; full pytest green.
