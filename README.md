# ctrlm-airflow-migration

Converts Control-M job definition XML exports into Airflow DAGs (targeting AWS MWAA),
implementing **two partitioning strategies** side by side, plus a comparison dashboard.

- `strategy_components/` — connected components of the condition graph + principled
  cuts (hubs, day-pattern conflicts, anchor spread). See `DESIGN.md` §5.
- `strategy_single_entry/` — directed ownership propagation from root jobs;
  convergence points become new roots (every DAG single-entry).
- `docs/partition-algorithm.md` — the exact algorithm spec.
- `docs/impl-contracts.md` — module contracts for this implementation
  (`docs/impl-contracts-v2.md` — the v2 delta: scopes, nested folders, operators;
  `docs/impl-contracts-v3.md` — the v3 delta: job-type registry, plugins package,
  full param mapping).
- **`docs/job-mapping-catalog.md`** — the authoritative (v3) Control-M → Airflow
  catalog: every job type → operator (with FULL/PARTIAL/MANUAL status), the
  param-by-param mapping table (PRIORITY/CRITICAL → priority_weight, CONFIRM →
  approval sensor, QUANTITATIVE → pools, ON/DO → callbacks/TriggerDagRun/SLA, …),
  the custom components inventory, and config-file schemas. Kept in sync with
  `core/ctrlm_core/operator_registry.py` by a test.
- **`plugins/README.md`** — the write-once `ctm_plugins` package generated DAGs
  import from (ODATE macro/helpers, approval-gate + file-watcher sensors,
  notify callbacks, calendar timetable) and how to build/deploy `plugins.zip`
  to MWAA.
- `docs/operator-mapping.md` — the v2 operator-mapping notes (SSH vs WinRM
  selection, connection resolution, AUTOEDIT variables, cross-link mechanisms,
  time gates, retries/timeouts); superseded where it overlaps by
  `docs/job-mapping-catalog.md`.

## Quickstart

Everything in one go (both strategies + dashboard):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_all.ps1
```

Or step by step:

```powershell
# one-time setup (already done if venv/ exists)
python -m venv venv
venv\Scripts\python -m pip install -r requirements.txt
venv\Scripts\python -m pip install -e core

# run both strategies on the sample exports
venv\Scripts\python strategy_components\run.py examples\exports -o output\components
venv\Scripts\python strategy_single_entry\run.py examples\exports -o output\single_entry

# build the comparison dashboard, then open it in a browser
venv\Scripts\python dashboard\build.py --a output\components --b output\single_entry -o output\dashboard\index.html
```

The dashboard is one self-contained offline HTML file with a **scope selector**
(all views render per selected XML scope) and a **run overview** tab showing
per-scope stats, cross-scope condition links, and dag_id collisions.

## Outputs (per scope)

**Scope rule (v2): each input XML file is an independent conversion scope.**
Conditions only match within their own XML; matches that would cross files are
NOT wired — they are reported for human review instead, and show up inside each
scope as orphan / dead-end conditions.

Each strategy's output directory is a tree of per-scope subdirectories (scope
name = XML file stem) plus one run-level summary:

```
output/<strategy>/
  scopes.json                  # run-level: per-scope stats, cross_scope_links,
                               # dag_id_collisions (cross-scope dag_id renames)
  <scope>/ir.json              # typed IR for that XML
  <scope>/graph.json           # condition graph
  <scope>/partition.json       # DAG assignments + cross-DAG links + stats
  <scope>/cluster-map.yaml
  <scope>/dags/*.py            # generated Airflow DAG files, syntax-validated
  <scope>/config/pools.json    # Airflow pools from QUANTITATIVE/CONTROL resources
                               # (only when non-empty; `airflow pools import`)
```

dag_ids stay unique ACROSS scopes (one Airflow instance): a collision keeps the
first scope's name and renames later ones to `<scope>__<dag_id>`, recorded in
`scopes.json` under `dag_id_collisions`.

Note: generated DAGs target Airflow 2.9+ / MWAA. Airflow itself is not installed
here (no native Windows support) — DagBag import validation happens later on
`aws-mwaa-local-runner`. Sample XMLs in `examples/exports/` are synthetic, written
in a classic DEFTABLE dialect; the parser gets adjusted against real exports in P0.

## Tests

```powershell
venv\Scripts\python -m pytest tests -q
```

## Determinism

Output is a pure function of `(export files, PartitionConfig, cluster map)`:
every iteration that affects output is sorted, min-cut inputs are canonically
ordered, no wall-clock or randomness. Running a pipeline twice yields
byte-identical `partition.json`.

## Notable semantics

- **Nested folders** (v2): `SUB_FOLDER` (and nested `FOLDER`/`SMART_FOLDER`)
  elements are accepted at arbitrary depth and flattened into full slash-path
  folder names (`MFG_NIGHT/PRESS_SHOP/QA`); each folder records its parent
  path. Desugar cascades deepest-first: a child folder's start node (or, if it
  has none, its entry jobs) is gated on the nearest ancestor's synthetic
  `__start__` condition, end nodes mirror this, and variables cascade
  ancestor → child → job (nearest wins). Day-attribute inheritance walks the
  parent chain to the nearest ancestor folder that has day attributes.
- **Operator selection** (v3): a declarative job-type registry
  (`core/ctrlm_core/operator_registry.py`) resolves every job first-match:
  FILEWATCH → `ctm_plugins.sensors.CtmFileWatcherSensor`, APPL_TYPE DATABASE →
  `SQLExecuteQueryOperator` (conn from `mapping-config/nodes.yaml` `type: db`
  entries), FILE_TRANS/SAP/other known-manual types → PythonOperator stubs
  raising `NotImplementedError` (+ `UNSUPPORTED_TYPE` diagnostic), and
  Command/Job → `WinRMOperator` when the node's OS is `windows` in
  `mapping-config/nodes.yaml` or the command sniffs as PowerShell
  (`.ps1` / leading `powershell`), otherwise `SSHOperator`.
  Full table: `docs/job-mapping-catalog.md`.
- **Param mapping** (v3): PRIORITY (`AA`..`ZZ`) → `priority_weight` 100..1
  (CRITICAL floors it at 90), CONFIRM → upstream `CtmApprovalGateSensor`,
  QUANTITATIVE/CONTROL → `pool`/`pool_slots` + `<scope>/config/pools.json`,
  ON NOTOK DOMAIL/DOSHOUT → `on_failure_callback=ctm_shout(...)` (+ email),
  DOFORCEJOB → downstream `TriggerDagRunOperator`, SHOUT WHEN LATE → `sla`.
  Generated DAGs import the write-once `ctm_plugins` package
  (see `plugins/README.md` for the MWAA plugins.zip build).
- **Schedule cascade**: a job with ANY scheduling attribute of its own — day
  fields (WEEKDAYS/DAYS/MONTHS) *or* a TIMEFROM/TIMETO window — does NOT
  inherit folder-level day attributes. A gated-but-unscheduled job (e.g.
  `FIN_EOD/FIN_EXTRACT`, TIMEFROM only) therefore stays day-pattern-less and
  acts as an "unscheduled middle": day-pattern conflicts through it are
  resolved transitively by min-cut (`AUTO_RESOLVED`), not by the direct
  `PATTERN` cut.
- `__`-prefixed synthetic conditions (folder start/end) are exempt from hub cuts.
- Cyclic jobs always get their own DAG with an interval cron
  (`*/15 6-19 * * *` style).
