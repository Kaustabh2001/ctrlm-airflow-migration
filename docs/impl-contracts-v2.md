# Implementation contracts — v2 delta (per-XML scope, nested folders, operator mapping)

Delta on top of docs/impl-contracts.md (still authoritative for anything not
mentioned here). `core/ctrlm_core/model.py` (FolderDef gained `parent: str`)
and `core/ctrlm_core/pipeline.py` (rewritten for per-XML scopes) are already
updated — read both, never modify them. Strategy partitioners are UNCHANGED.

## V2-1: Scope = one XML file  (pipeline — already implemented)

`run_pipeline` now parses each XML separately, partitions and emits per scope,
writes `<out>/<scope>/{ir,graph,partition,cluster-map,dags/}` plus a run-level
`<out>/scopes.json` with `scopes` (per-scope stats), `cross_scope_links`
(condition matches severed by the scope rule), and `dag_id_collisions`
(cross-scope renames to `<scope>__<dag_id>`). Returns `dict[scope, PartitionResult]`.

## V2-2: Nested folders (parser agent)

Owner files: `core/ctrlm_core/parser.py`, `core/ctrlm_core/desugar.py`,
`core/ctrlm_core/schedule.py`, NEW `examples/exports/sample_mfg.xml`,
`tests/test_parser.py`, `tests/test_desugar.py`, `tests/test_schedule.py`.

- Parser: accept `SUB_FOLDER` (and nested `FOLDER`/`SMART_FOLDER`, any tag of
  the folder family) nested inside a folder element, at ARBITRARY depth.
  Flatten into `Deftable.folders` entries where `FolderDef.name` is the full
  slash path (`"MFG_NIGHT/PRESS_SHOP/QA"`) and `FolderDef.parent` is the parent
  path (`""` for top level). `Job.folder` = its immediate folder's full path
  (so `Job.uid` stays globally unique). Folder-level INCOND/OUTCOND/VARIABLE
  still recognized before the first JOB/sub-folder child. Sub-folders are
  `smart=True` iff their tag says so; they may carry their own scheduling attrs.
- Desugar cascade, deepest-first:
  - a folder with folder-level conds (or `folder_start_always`) gets synthetic
    `__FOLDER_START__`/`__FOLDER_END__` exactly as v1, named under its own path;
  - if a CHILD folder has a start node and its parent (transitively nearest
    ancestor) also has one, the child's start gains in_cond `__start__{parent}`;
  - if a child has NO start node but a (transitively nearest) ancestor does,
    the child's ENTRY jobs gain that ancestor's `__start__` in_cond;
  - end nodes mirror this: parent end waits on child ends / terminal jobs;
  - variables cascade ancestor -> child -> job (nearest wins).
- schedule.normalize_jobs: day-attr cascade walks the PARENT CHAIN — a job (or
  sub-folder acting for its jobs) with NO scheduling attrs of its own inherits
  from the nearest ancestor folder that has day attrs. "Own scheduling attrs"
  keeps the v1 meaning (docs/impl-contracts.md §schedule): day fields OR an own
  TIMEFROM/TIMETO window both block the cascade, so `MFG_NIGHT/MFG_EXTRACT`
  (TIMEFROM 2100, no day fields) stays day-pattern-less / condition-driven —
  exactly like v1's `FIN_EOD/FIN_LOAD_GL` ("unscheduled middle", Phase 6).
  Only the WALK is new in v2, not the blocking rule.
- `sample_mfg.xml` (NEW — do not touch the other three samples): datacenter
  CTM_PROD. FOLDER `MFG_IN`: job `MFG_SENSORS` (TASKTYPE FileWatch, weekdays
  ALL, TIMEFROM 0500) OUTCOND `MFG-PLANT-READY`. SMART_FOLDER `MFG_NIGHT`
  (weekdays ALL, folder INCOND `MFG-PLANT-READY`, folder OUTCOND
  `MFG-NIGHT-DONE`): job `MFG_EXTRACT` (NODEID prdnode2, TIMEFROM 2100,
  CMDLINE `/opt/mfg/extract.sh %%ODATE`) OUTCOND `MFG-EXTRACTED`; SUB_FOLDER
  `PRESS_SHOP` (folder INCOND `MFG-EXTRACTED`) containing job `PRESS_LOAD`
  (NODEID winnode1, CMDLINE `powershell -File C:\jobs\press_load.ps1 %%ODATE`)
  OUTCOND `PRESS-LOADED`; job `PRESS_REPORT` (NODEID winnode1,
  MEMNAME `press_report.ps1`, MEMLIB `C:\jobs`, INCOND `PRESS-LOADED`); and a
  nested SUB_FOLDER `QA` (no folder conds) containing job `QA_CHECK`
  (NODEID winnode1, no conds, no schedule — the deep false-root test).
- Test assertions: depth-3 flattening with correct name/parent paths;
  `QA_CHECK` is NOT a root after desugar (gains `__start__MFG_NIGHT/PRESS_SHOP`);
  `PRESS_SHOP` start gains `__start__MFG_NIGHT`; day-attr inheritance walks to
  `MFG_NIGHT` for jobs in `QA`; existing 3 samples still parse identically.

## V2-3: Operator mapping (emit agent)

Owner files: `core/ctrlm_core/emit.py`, `core/ctrlm_core/autoedit.py`,
`core/ctrlm_core/templates/dag.py.j2`, `mapping-config/nodes.yaml`,
NEW `docs/operator-mapping.md`, `tests/test_emit.py`.

- `mapping-config/nodes.yaml` v2 format (keep v1 flat `node: conn_id` entries
  working, defaulting to os linux):
  ```yaml
  defaults: {os: linux}
  nodes:
    prdnode1: {conn_id: ssh_prdnode1, os: linux}
    winnode1: {conn_id: winrm_winnode1, os: windows}
  ```
- Operator selection for task_type Command/Job:
  1. command sniffs PowerShell (regex: `\.ps1\b` or leading `powershell`) OR
     node os == windows  ->  `WinRMOperator` from
     `airflow.providers.microsoft.winrm.operators.winrm`
     (`WinRMOperator(task_id=..., ssh_conn_id=<conn>, command=...)` — verify the
     provider's current kwarg names via docs if possible; whatever you emit
     must py_compile and carry a `# provider: apache-airflow-providers-microsoft-winrm`
     comment);
  2. else `SSHOperator` (unchanged v1 path).
  Unmapped NODEID fallback: `ssh_<nodeid>` (linux) / warn diagnostic UNMAPPED_NODE.
- NEW `docs/operator-mapping.md`: the full Control-M -> Airflow mapping table
  as implemented and configurable: task types (Command/Job linux, Command/Job
  windows/ps1, Dummy, synthetic, FileWatch), conn resolution order, autoedit
  variable table, cross-link mechanisms (sensor / dataset / prev-run), time
  gates, retries/timeouts. This document is user-facing.
- Tests: WinRM chosen for (a) windows node with plain cmd, (b) linux node with
  .ps1 command; SSH for linux bash; v1 flat nodes.yaml still parses; generated
  files py_compile.

## V2-4: Dashboard scopes (dashboard agent)

Owner files: `dashboard/*`, `tests/test_dashboard.py`.

- CLI unchanged (`--a <components out> --b <single_entry out> -o index.html`)
  but the out dirs are now scope trees: read `<dir>/scopes.json` and each
  `<dir>/<scope>/{graph,partition,ir}.json`.
- UI: a SCOPE SELECTOR (one entry per XML scope); all five views render for
  the selected scope. Add a run-level panel (visible regardless of scope):
  cross_scope_links table (cond, producer scope/job, consumer scope/job) and
  dag_id_collisions, plus a per-scope stats overview table.
- Still ONE self-contained offline html. Tests: fixtures with two scopes;
  assert selector present, per-scope data embedded, cross-scope table present.

## Integration + verification

- Integrator: fix any fallout anywhere (strategy tests parse merged files —
  partition() is scope-agnostic so they should survive; pipeline-level tests
  and run.py summaries must match the new per-scope outputs). Update README
  (outputs per scope; scopes.json; operator mapping doc pointer; nested folder
  support). Re-run: full pytest, both runners, dashboard build. Keep
  scripts/run_all.ps1 working.
- Verifier checks (real outputs): 4 scopes in scopes.json; cross_scope_links
  contains BATCH-OPEN ops->finance (producer OPS/OPS_OPEN_BATCH, consumer
  FIN_EOD/__FOLDER_START__); finance scope orphans include BATCH-OPEN;
  depth-3 nesting parsed (folders MFG_NIGHT/PRESS_SHOP and
  MFG_NIGHT/PRESS_SHOP/QA exist in mfg ir.json; QA_CHECK not a root);
  WinRMOperator emitted for PRESS_LOAD and PRESS_REPORT, SSHOperator for
  MFG_EXTRACT; every dags/*.py py_compiles; per-scope determinism (second run
  byte-identical partition.json per scope); dashboard has the scope selector
  and cross-scope panel; full pytest green.
