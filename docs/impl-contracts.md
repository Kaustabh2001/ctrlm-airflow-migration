# Implementation contracts — v1 two-strategy build

Read this WHOLE file before writing code. Also read `DESIGN.md` (§4–§6) and
`docs/partition-algorithm.md` (the exact algorithm). The pydantic models in
`core/ctrlm_core/model.py` and the pipeline in `core/ctrlm_core/pipeline.py`
already exist and ARE the contract — code against them, never modify them.

## Environment

- Windows 11, Python 3.10 venv at `venv/`. Interpreter: `E:/ctrlm-airflow-migration/venv/Scripts/python.exe`
- `ctrlm-core` is pip-installed editable: `import ctrlm_core` works everywhere.
- Run tests: `E:/ctrlm-airflow-migration/venv/Scripts/python.exe -m pytest tests/<your files> -q` from repo root.
- Airflow is NOT installed (it does not run on native Windows). Generated DAG files
  are validated with `py_compile` (syntax) only; DagBag validation happens later on
  `aws-mwaa-local-runner`. Never `import airflow` in tool code — only in *generated* code text.

## File ownership (write ONLY your files)

| Module | Owner agent | Files |
|---|---|---|
| Parser + samples | parser | `core/ctrlm_core/parser.py`, `core/ctrlm_core/desugar.py`, `examples/exports/sample_finance.xml`, `sample_ops.xml`, `sample_hr.xml`, `tests/test_parser.py`, `tests/test_desugar.py` |
| Graph + schedule + cuts + stats | graph | `core/ctrlm_core/schedule.py`, `core/ctrlm_core/graph.py`, `core/ctrlm_core/cuts.py`, `core/ctrlm_core/stats.py`, `tests/test_schedule.py`, `tests/test_graph.py` |
| Emit | emit | `core/ctrlm_core/emit.py`, `core/ctrlm_core/autoedit.py`, `core/ctrlm_core/templates/dag.py.j2`, `tests/test_emit.py` |
| Dashboard | dashboard | `dashboard/build.py`, `dashboard/*.j2` or assets, `tests/test_dashboard.py` |
| Components strategy | components | `strategy_components/partitioner.py`, `strategy_components/run.py`, `tests/test_components.py` |
| Single-entry strategy | single-entry | `strategy_single_entry/partitioner.py`, `strategy_single_entry/run.py`, `tests/test_single_entry.py` |
| Integration | integrator | may touch anything; owns `README.md`, `scripts/run_all.ps1` |

## Data flow

```
examples/exports/*.xml
  -> parser.parse_files(files) -> Deftable
  -> desugar.desugar(deftable, config)          (mutates: synthetic nodes, folder conds)
  -> schedule.normalize_jobs(deftable, config)  (mutates: Job.day_pattern, folder sched cascade)
  -> graph.build_graph(deftable, config) -> CtmGraph
  -> <strategy>.partition(graph, config) -> PartitionResult
  -> emit.emit_dags(graph, result, dags_dir, config)
outputs per strategy dir: ir.json, graph.json, partition.json, cluster-map.yaml, dags/*.py
  -> dashboard/build.py reads both output dirs -> output/dashboard/index.html
```

## Module contracts (exact signatures)

### parser.py
```python
def parse_files(files: list[Path]) -> Deftable
```
- Parses the DEFTABLE XML dialect below with `xml.etree.ElementTree`.
- Elements: root `DEFTABLE`; children `FOLDER` / `SMART_FOLDER` (also accept legacy
  `TABLE` / `SMART_TABLE`) with attrs `FOLDER_NAME` (or `TABLE_NAME`), `DATACENTER`,
  optional folder-level scheduling attrs (same names as jobs) and folder-level
  `INCOND`/`OUTCOND` children **that appear before the first JOB element**.
- `JOB` attrs: `JOBNAME, DESCRIPTION, APPLICATION, SUB_APPLICATION, TASKTYPE,
  CMDLINE, MEMNAME, MEMLIB, NODEID, RUN_AS, WEEKDAYS, DAYS (-> monthdays), MONTHS,
  DAYS_AND_OR, TIMEFROM, TIMETO, TIMEZONE, CYCLIC ("0"/"1"), INTERVAL (e.g. "15M",
  "2H" -> minutes), MAXWAIT, MAXRERUN, RERUNINTERVAL, CONFIRM ("0"/"1")`.
  `command` = CMDLINE if present else `MEMLIB/MEMNAME`.
- `JOB` children: `INCOND(NAME, ODATE, AND_OR)`, `OUTCOND(NAME, ODATE, SIGN)` where
  SIGN in {ADD, DEL, +, -} (normalize +/- to ADD/DEL), `VARIABLE(NAME, VALUE)`,
  `QUANTITATIVE(NAME, QUANT)`, `CONTROL(NAME, TYPE)`,
  `ON(STMT, CODE)` containing `DOMAIL/DOSHOUT/DOFORCEJOB/DOCOND/DOACTION` (store
  each child as `{"type": tag, **attrib}`), `SHOUT(WHEN, DEST, MESSAGE)`.
- Unknown elements/attrs: never crash; count them into `Deftable`-level warnings via
  print or ignore, but do not lose jobs.
- Missing attrs -> model defaults. Whitespace-trim everything.

### desugar.py
```python
def desugar(deftable: Deftable, config: PartitionConfig) -> None
```
Implements Phase −1 of docs/partition-algorithm.md exactly:
- For each folder with folder-level in/out conds (or every smart folder when
  `config.folder_start_always`): create synthetic Jobs
  `name="__FOLDER_START__"` / `"__FOLDER_END__"`, `synthetic=True`, `task_type="Dummy"`,
  same `folder`. Start: carries the folder in_conds + out_cond
  `Condition(name=f"__start__{folder}")`; every ENTRY job (no in_cond produced by a
  job of the same folder) gains in_cond `__start__{folder}`. End: carries folder
  out_conds + in_cond `__done__{folder}/{job}` for each TERMINAL job (no out_cond
  consumed within the folder), and each terminal job gains that out_cond.
  Start node inherits the folder-level scheduling attrs (weekdays/monthdays/months/
  days_and_or/timezone).
- Folder variables merge into each job's variables (job value wins).
- Append synthetic jobs to `folder.jobs`.

### schedule.py
```python
def normalize_jobs(deftable: Deftable, config: PartitionConfig) -> None
def day_pattern_of(job: Job) -> str | None          # helper used by normalize
def rel_minutes(hhmm: str, new_day: str) -> int     # ODATE clock: (t - newday) % 1440
def cron_for(day_pattern: str | None, anchor_hhmm: str) -> str | None
def cyclic_cron(job: Job) -> str                    # "*/15 6-19 * * *" style
```
- Cascade: a job with NO scheduling attrs of its own, inside a folder that HAS
  folder-level scheduling attrs, inherits them (fills the raw fields) BEFORE
  computing day_pattern. Jobs with own attrs keep them.
- `day_pattern` canonical string: `None` when weekdays==monthdays==months=="".
  Else `"WD={wd}|MD={md}|M={m}|OP={AND|OR}"` where each part is `ALL` or a
  sorted comma list of ints (normalize `weekdays="ALL"` and every-day sets to `ALL`).
- `cron_for`: anchor "2230" -> minute 30 hour 22. WD only -> dow field `1-5`-style
  (cron 1=Mon..7=Sun; emit sorted comma list, collapse full set to `*`), dom `*`.
  MD only -> dom list. Both + OP=OR -> set both fields (cron's native OR). Both +
  OP=AND -> set dom, and the caller must add a WARN diagnostic (cron cannot AND).
  Months -> month field.

### graph.py
```python
def build_graph(deftable: Deftable, config: PartitionConfig) -> CtmGraph
```
Phase 0 of the spec: hash-join producers/consumers by condition name.
- ODAT<->ODAT -> `e_edges` kind `E`; consumer PREV -> `w_edges` kind `PREV_RUN`;
  anything else -> `w_edges` kind `REVIEW` + flag.
- Self-edges dropped + flag. DEL-sign out-conds never make edges; flag each.
- Orphans/dead-ends: conditions consumed-but-never-produced / produced-but-never-
  consumed, EXCLUDING synthetic `__`-prefixed conditions. Fill `orphan_conds` /
  `dead_end_conds`.
- Deterministic: iterate sorted condition names, sorted producer/consumer uids.

### cuts.py  (shared phases — both strategies call these in order)
```python
def extract_cyclic(graph: CtmGraph) -> list[str]        # Phase 1; returns cyclic uids
def hub_cuts(graph: CtmGraph, config) -> None           # Phase 2 (skip "__" conds!)
def pattern_cuts(graph: CtmGraph) -> None               # Phase 3 (components only)
```
Each moves edges from `e_edges` to `w_edges` with the right kind, deterministically.

### stats.py
```python
def compute_stats(graph: CtmGraph, dags: list[DagSpec], cross_links: list[CrossLink]) -> dict
```
Keys (dashboard + verifiers rely on EXACTLY these):
`n_jobs, n_dags, n_cross_links, cross_links_by_kind, cross_links_by_mechanism,
single_job_dags, multi_root_dags, dataset_triggered_dags, largest_dag,
size_histogram` (buckets `"1","2-5","6-15","16-50","51+"`).

### strategy_components/partitioner.py  — "components" strategy
```python
def partition(graph: CtmGraph, config: PartitionConfig) -> PartitionResult   # strategy="components"
```
Implement Phases 1–9 of docs/partition-algorithm.md via cuts.py + your code:
union-find components; singleton coalescing per (folder, day_pattern) when
`config.coalesce_singletons`; transitive pattern conflicts resolved by MIN EDGE CUT
(networkx max-flow: undirected -> DiGraph both directions capacity=1 per parallel
cond pair, super source/sink capacity 10**9 to terminal groups, cut = E-edges
crossing the reachable partition; canonical edge order for determinism), kind
`AUTO_RESOLVED`; ADDITIONALLY anchor purity: after pattern purity, while scheduled
ROOTS of a cluster have rel_minutes(timefrom) spread > anchor_spread_hours*60,
min-cut rarest anchor-bucket roots vs rest, kind `ANCHOR`. Size guardrail: warn
diagnostic `OVERSIZED` when len > max_tasks (do NOT split). Schedule per Phase 8
(anchor = earliest scheduled-root rel time; cron via schedule.cron_for; pattern-less
cluster -> dataset_triggered with inbound dataset URIs `ctrlm://cond/{cond}`).
Cyclic jobs: own DAG each, schedule = schedule.cyclic_cron(job). dag_id =
snake_case(modal folder), tie lexicographic, collision `_2`,`_3` in canonical order.
Wiring: build cross_links from final w_edges whose endpoints exist, dedupe by
(source, target, kind) merging conds; mechanism: PREV_RUN->prev_run_sensor; else
consumer's DAG time-scheduled -> sensor, else dataset. `REVIEW` links -> mechanism
sensor + warn diagnostic. Fill stats via stats.compute_stats.

### strategy_single_entry/partitioner.py — "single_entry" strategy
```python
def partition(graph: CtmGraph, config: PartitionConfig) -> PartitionResult   # strategy="single_entry"
```
Shared cuts FIRST: extract_cyclic + hub_cuts (NOT pattern_cuts). Then ownership
propagation on directed e_edges in Kahn topological order:
- roots (no incoming e_edge) own themselves.
- node n with owner-set O = {owner(p) for pred p}:
  new owner (its own group root) iff len(O) >= 2, OR n.day_pattern is not None and
  n.day_pattern != day_pattern of its sole owner root (treat owner pattern None as
  mismatch), else owner(n) = sole element.
- every e_edge whose endpoints end up with different owners -> w_edges kind
  `OWNER_SPLIT`.
- leftover nodes (condition cycles, unreachable) -> weakly-connected fallback groups,
  owner = lexicographically smallest uid, diagnostic `CYCLE_FALLBACK` (warn).
- NO singleton coalescing (that is the point of comparison); count them in stats.
- Group schedule = owner root's day_pattern + timefrom anchor; condition-driven root
  -> dataset-triggered. dag_id = snake_case(owner job name; folder-start synthetic ->
  folder name), collisions suffixed canonically. Wiring + stats same rules as above.

### Both run.py files
```python
# python strategy_components/run.py examples/exports -o output/components
# python strategy_single_entry/run.py examples/exports -o output/single_entry
```
argparse: positional `inputs` (nargs="+", files or dirs), `-o/--out` (defaults above).
Import sibling `partitioner` (same dir), call
`ctrlm_core.pipeline.run_pipeline(<name>, partition, inputs, out, PartitionConfig())`.

### emit.py
```python
def emit_dags(graph: CtmGraph, result: PartitionResult, dags_dir: Path,
              config: PartitionConfig, mapping_path: str | Path = "mapping-config/nodes.yaml") -> list[Path]
```
One file per DagSpec: `dags/{dag_id}.py`, generated from `templates/dag.py.j2`, then
`black.format_str` (fallback: raw) and `py_compile.compile` (raise on syntax error).
Generated code targets Airflow 2.9+ (MWAA):
- header docstring: strategy, source folders, source files, job provenance.
- imports only what the file uses: `from airflow import DAG`,
  `from airflow.operators.empty import EmptyOperator`,
  `from airflow.providers.ssh.operators.ssh import SSHOperator`,
  `from airflow.sensors.external_task import ExternalTaskSensor`,
  `from airflow.sensors.date_time import DateTimeSensorAsync`,
  `from airflow.datasets import Dataset`, `from datetime import datetime, timedelta`.
- `with DAG(dag_id=..., start_date=datetime(2026, 1, 1), catchup=False,
  schedule=<cron str | [Dataset(...)] | None>, default_args={"retries": <max maxrerun>},
  tags=["ctrlm", "strategy:<s>", "folder:<f>..."]) as dag:`
- task per job: task_id = sanitized job name (lowercase, [a-z0-9_], dedupe with
  suffix). Dummy/synthetic -> EmptyOperator; Command/Job -> SSHOperator
  (ssh_conn_id from nodes.yaml else `ssh_{node_id or 'default'}`, command through
  autoedit.translate); FileWatch -> EmptyOperator + `# TODO FileWatch -> SFTPSensor`.
  TaskGroup per folder when the DAG spans >1 folder.
- intra-DAG deps from e_edges between members: `up >> down`.
- time gates: member with rel_minutes(timefrom) > dag anchor -> insert
  `DateTimeSensorAsync(task_id=f"gate_{task}", target_datetime="{{ (data_interval_end
  + macros.timedelta(days=%d)).replace(hour=%d, minute=%d) }}")` upstream of it
  (day offset 1 when the gate's raw HHMM < anchor's raw HHMM).
- cross_links where this DAG is the CONSUMER: sensor ->
  `ExternalTaskSensor(task_id=f"wait_{...}", external_dag_id=..., external_task_id=...,
  mode="reschedule", timeout=<maxwait days*86400 or 21600>)` upstream of the target;
  prev_run_sensor -> same + `# TODO align to previous run` comment.
- cross_links where this DAG is the PRODUCER with mechanism dataset: producer task
  gets `outlets=[Dataset("ctrlm://cond/<cond>")]`.
- dataset-triggered DAGs: `schedule=[Dataset(...), ...]`.

### autoedit.py
```python
def translate(command: str) -> tuple[str, list[str]]   # (translated, unresolved %%vars)
```
Map: `%%ODATE -> {{ ds_nodash }}`, `%%$ODATE -> {{ ds }}`, `%%DATE -> {{ ds_nodash }}`,
`%%TIME -> {{ ts_nodash }}`, `%%JOBNAME -> <literal job name at emit time>` (leave
`%%JOBNAME` for emit to substitute). Any other `%%NAME` -> left verbatim, returned in
unresolved (emit adds `# TODO unresolved AUTOEDIT: ...` comment + diagnostic).

### dashboard/build.py
```
venv python dashboard/build.py --a output/components --b output/single_entry -o output/dashboard/index.html
```
Reads graph.json (from --a), both partition.json (+ ir.json for folder metadata).
Produces ONE self-contained offline HTML (no CDN). Preferred: inline vis-network
(read the bundled lib from the installed pyvis package) once + custom JS; fallback:
pyvis-generated per-view HTML files in `output/dashboard/views/` iframed by index.
Five views (tabs): (1) Control-M structure — nodes colored/grouped by folder, E
edges; (2) Strategy A: components — nodes colored by dag_id, intra edges solid,
cross_links dashed with arrows + kind on hover; (3) Strategy B: single-entry — same;
(4) Full condition graph — ALL edges (E + wiring), edge color by kind, legend;
(5) Comparison — side-by-side stats table (all stats.py keys), size-histogram bars,
divergence table (each components-DAG -> which single-entry DAGs its jobs landed in,
and counts: jobs co-grouped in both / split), diagnostics lists per strategy.
Node tooltip: job name, folder, task_type, day_pattern, timefrom, cyclic, dag under
each strategy. Search box filtering/highlighting nodes by substring. Dark theme,
readable in a browser opened from disk (file://). Physics on but stabilized;
graphs up to a few hundred nodes must stay responsive.

## Sample XML content requirements (parser agent authors these)

The three files must jointly produce these scenarios (tests in ALL modules rely on them):
- `sample_finance.xml`: FOLDER `FIN_DW`: `DW_LOAD_CUSTOMERS`, `DW_LOAD_ORDERS`,
  `DW_LOAD_PRODUCTS` (weekdays 1-5, TIMEFROM 2100, no in-conds) each OUTCOND
  `DW-<X>-LOADED`; `DW_BUILD_MART` (no schedule; AND in-conds all three) OUTCOND
  `DW-MART-OK`; `DW_PUBLISH` (no schedule, in `DW-MART-OK`). SMART_FOLDER `FIN_EOD`
  (folder weekdays 1-5, folder INCOND `BATCH-OPEN`, folder OUTCOND `FIN-EOD-DONE`):
  `FIN_LOAD_GL` (TIMEFROM 2200, NO own in-conds -> false-root test) OUTCOND
  `FIN-GL-LOADED`; `FIN_POST_GL` (in `FIN-GL-LOADED`) OUTCOND `FIN-POSTED`;
  `FIN_EXTRACT` (TIMEFROM 0200, in `FIN-POSTED`) OUTCOND `FIN-DONE`. FOLDER `RISK`:
  `RISK_CALC` (weekdays 1-5, TIMEFROM 0300, in `FIN-DONE`). FOLDER `RPT`:
  `RPT_WEEKLY_PACK` (WEEKDAYS "1" Monday-only, in `FIN-DONE`) -> transitive conflict.
- `sample_ops.xml`: FOLDER `OPS`: `OPS_OPEN_BATCH` (weekdays ALL, TIMEFROM 2000)
  OUTCOND `BATCH-OPEN`; `OPS_APP01..OPS_APP10` (weekdays 1-5, TIMEFROM 2100, each
  in `BATCH-OPEN`, no out-conds) -> hub + singleton-coalescing comparison;
  `OPS_FS_POLL` (CYCLIC=1 INTERVAL 15M, TIMEFROM 0600 TIMETO 2000) OUTCOND
  `FILE-ARRIVED`. FOLDER `STG`: `STG_INGEST` (no schedule, in `FILE-ARRIVED`)
  OUTCOND `STG-LOADED`; `STG_QUALITY` (no schedule, in `STG-LOADED`).
- `sample_hr.xml`: FOLDER `HR_IN`: `HR_FW` (TASKTYPE FileWatch, weekdays ALL,
  TIMEFROM 0700) OUTCOND `HR-FILES-READY`. SMART_FOLDER `HR_PAY` (folder DAYS
  "1,15", folder INCOND `HR-FILES-READY`, folder OUTCOND `HR-PAY-DONE`): `HR_CALC`
  (no own conds/schedule; also give it INCOND `HR-PAY-DONE` with ODATE="PREV" ->
  PREV_RUN test) OUTCOND `HR-CALC-OK`; `HR_PAY_RUN` (in `HR-CALC-OK`, CONFIRM=1,
  QUANTITATIVE DB_SLOTS 2, ON NOTOK DOMAIL) OUTCOND `HR-PAY-OK`; `HR_REPORT`
  (in `HR-PAY-OK`, SHOUT NOTOK). Plus one job consuming orphan `EXT-FEED-OK`
  (never produced) and one OUTCOND with SIGN="DEL" somewhere.

## Expected outcomes (write your tests against these)

- components: all 5 `FIN_DW` jobs in ONE dag; single_entry: `DW_BUILD_MART` NOT in
  the same dag as any loader; single_entry n_dags > components n_dags overall.
- Both: no dag contains `OPS/OPS_OPEN_BATCH` together with any `BATCH-OPEN` consumer
  (hub cut, fan-out 12 >= 10).
- components: `OPS_APP01..10` coalesce into ONE dag; single_entry: 10 separate dags.
- components: `RPT/RPT_WEEKLY_PACK` in a different dag than `FIN_EOD` chain, with an
  `AUTO_RESOLVED` cross_link; `RISK/RISK_CALC` in the SAME dag as the FIN_EOD chain.
- `HR_PAY/HR_CALC` has an incoming edge from `HR_PAY/__FOLDER_START__` (not a root);
  a `PREV_RUN` cross_link exists for `HR-PAY-DONE`.
- `OPS/OPS_FS_POLL` is a single-job dag in BOTH strategies; `STG` chain is
  dataset-triggered in both.
- graph.json orphans include `EXT-FEED-OK`; every emitted dags/*.py passes py_compile;
  determinism: running a pipeline twice yields byte-identical partition.json.

## Conventions

- Every loop over dict/set that affects output: sort first. No wall-clock, no random.
- snake_case(): lowercase, non-alphanumeric -> `_`, collapse repeats, strip `_`.
- Diagnostics codes used so far: `OVERSIZED, CYCLE_FALLBACK, UNMAPPED_NODE,
  UNRESOLVED_AUTOEDIT, CRON_AND_APPROX, REVIEW_QUALIFIER, DEL_CONDITION,
  SELF_CONDITION, CONFIRM_JOB, SINGLE_JOB_DAGS`.
- Keep functions small and typed; docstrings state which spec phase they implement.
