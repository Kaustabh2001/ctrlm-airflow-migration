# Implementation contracts — v4 delta (REVISED: targeted custom operators + extension playbook)

Delta on v1–v3. USER DECISIONS driving this revision:
1. Custom operators ONLY where they add real capability (connectivity,
   translation logic). Wrapping SSHOperator/WinRMOperator in rename-classes is
   explicitly rejected: Command/Job tasks stay plain SSHOperator/WinRMOperator
   and Dummy stays EmptyOperator, with common params (priority_weight, pool,
   callbacks, email, sla, retries) translated at CODEGEN time exactly as v3
   already does (HEAD state — do not change that machinery).
2. Control-M's job-type universe is open-ended (Application Integrator lets
   sites define types), so the deliverables are (a) targeted operators where
   value exists today, (b) a documented EXTENSION PLAYBOOK for adding types,
   (c) a roadmap table of known application types, (d) the existing loud
   MANUAL catch-all as the safety net.

LEFTOVER STATE: an earlier (abandoned) v4 attempt part-wrote
`plugins/ctm_plugins/operators.py`, `plugins/ctm_plugins/_params.py` and
modified `plugins/ctm_plugins/__init__.py`, `plugins/ctm_plugin.py`,
`plugins/README.md`, `tests/test_plugins.py`. The plugins agent OWNS all of
these and must rework them to this revised contract — in particular DELETE any
CtmCommandJob / CtmPowerShellJob / CtmDummy / CtmJobMixin blanket-wrapper code.

## V4-1 (revised): targeted operators (plugins agent)

Owner files: `plugins/ctm_plugins/operators.py`, `plugins/ctm_plugins/_params.py`,
`plugins/ctm_plugins/__init__.py`, `plugins/ctm_plugin.py`, `plugins/README.md`,
`tests/test_plugins.py`.

- `_params.py` (PURE, airflow-free): ONLY `resolve_node(node: str) -> dict`
  ({conn_id, os, type}) reading `mapping-config/nodes.yaml` with the same
  resolution order as notify.yaml (env var CTM_NODES_CONFIG → plugins-zip root
  → repo root → cwd), `ssh_<node>`/linux fallback. The priority formula STAYS
  in core/ctrlm_core/operator_registry.py (v3, already single-sourced) — do
  NOT duplicate it here.
- `operators.py` (airflow imports — py_compile only): exactly TWO classes:
  - `CtmDatabaseJob(SQLExecuteQueryOperator)`: kwargs `node` (resolved to
    `conn_id` at parse time via _params.resolve_node; explicit `conn_id`
    kwarg overrides), `sql`, plus passthrough of all standard BaseOperator
    kwargs. This is the class that EARNS its existence: connection resolution
    + a place for future DB-specific behavior (stored-proc handling, output
    capture) to land once, not per-DAG.
  - `CtmManualJob(BaseOperator)`: kwargs `ctm_task_type`, `ctm_appl_type`,
    `ctm_job`; `execute()` raises NotImplementedError naming all three.
    Replaces the emitted PythonOperator+prelude stub (cleaner generated code,
    identical loud-failure behavior).
- `__init__.py` re-exports the two classes + existing public API; `ctm_plugin.py`
  registration updated; `plugins/README.md`: document both operators, note
  nodes.yaml must ship in plugins.zip, and REMOVE any blanket-wrapper text.
- `tests/test_plugins.py`: resolve_node unit tests (v2 nodes.yaml schema, v1
  flat compat, fallback, env-var override), py_compile for operators.py, and
  removal of any tests referencing deleted wrapper classes. All existing
  passing tests must stay passing.

## V4-2 (revised): registry/emit touch-up + catalog playbook (registry agent)

Owner files: `core/ctrlm_core/operator_registry.py`, `core/ctrlm_core/emit.py`,
`core/ctrlm_core/templates/dag.py.j2` (only if needed),
`docs/job-mapping-catalog.md`, `tests/test_registry.py`, `tests/test_emit.py`.

- Registry changes are SURGICAL — everything else stays v3 (HEAD):
  - DATABASE row: emit `CtmDatabaseJob(task_id=..., node="<nodeid>", sql=...)`
    (import from ctm_plugins.operators). Codegen-translated common params
    (pool, priority_weight, callbacks, ...) still applied inline as for any
    task — the operator only owns connectivity.
  - file_transfer / known_manual / unsupported catch-all rows: emit
    `CtmManualJob(task_id=..., ctm_task_type=..., ctm_appl_type=...,
    ctm_job=...)` instead of the PythonOperator stub prelude; drop the
    prelude def from the template. Diagnostics (UNSUPPORTED_TYPE) unchanged.
  - NO other row changes: Command/Job keep plain SSHOperator/WinRMOperator,
    Dummy keeps EmptyOperator, FILEWATCH keeps CtmFileWatcherSensor.
- `docs/job-mapping-catalog.md` gains two sections (and updated rows for the
  two changed operators; keep the registry<->catalog sync test green):
  - **"Adding a new job type — the playbook"**: the decision tree —
    (1) an Airflow provider exists for the target system (Databricks,
    Snowflake, KubernetesPodOperator, AWS/Azure providers, HTTP, ...) →
    add a registry row mapping IR params to that operator; write a Ctm*
    operator ONLY if param/connection translation is complex enough to earn
    it; (2) no provider → write a Ctm* operator in plugins/ctm_plugins
    wrapping the system's API once; (3) not automatable → leave it to the
    MANUAL catch-all. Each new row needs: registry entry + catalog row (sync
    test enforces) + a sample-XML job exercising it + emit test.
  - **"Application-type roadmap"**: a table of common Control-M
    application/job types NOT yet auto-converted — SAP (R/3, BW), Informatica,
    Hadoop/Spark, AWS (Lambda/Batch/Step Functions), Azure (ADF/Functions),
    Databricks, Snowflake, Kubernetes, File Transfer (AFT/MFT), Web Services/
    REST, Java, IBM i (OS/400), z/OS members — each with: likely target
    (provider operator or Ctm* custom), what IR data it would need, and
    current status = MANUAL (catch-all). This table answers "what about all
    the other types" honestly and gives the extension order.
- Tests: bank scope BANK_BAL_CHECK emits CtmDatabaseJob(node="dbnode1", sql
  with ds_nodash) and NO conn_id literal in the file; FILE_TRANS/SAP jobs emit
  CtmManualJob (no PythonOperator prelude anywhere); BANK_SETTLE still emits a
  plain SSHOperator WITH inline priority_weight/pool/pool_slots/callback/email/
  sla exactly as v3; py_compile; determinism.

## Integration + verification

- Integrator: full pytest green; regenerate output/components,
  output/single_entry, dashboard; README bullet: operator policy = "custom
  operators only where they add capability (DB connectivity, watchers, gates,
  manual stubs); command jobs stay native SSH/WinRM"; run_all.ps1 green;
  do NOT git commit.
- Verifier (fresh runs): every V4-2 test bullet on real outputs; no
  CtmCommandJob/CtmPowerShellJob/CtmDummy/CtmJobMixin string anywhere in the
  repo (code, tests, docs, generated output); v1–v3 regression spot-checks
  (invariants per scope, WinRM in mfg, cross_scope_links, pools.json content,
  byte-determinism, catalog<->registry sync); playbook + roadmap sections
  present in the catalog; full pytest green.
