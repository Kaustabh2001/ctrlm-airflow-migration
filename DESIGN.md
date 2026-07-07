# Control-M → Airflow (MWAA) Migration Converter — Design

Status: **v3 — designed and implemented** (updated 2026-07-07; v1 draft 2026-07-06)
Decisions locked: **AWS MWAA target · generated Python DAGs · one-time conversion · scope = one XML file per conversion (v2)**

**DAG-boundary strategy — two implementations, decision deliberately open:**
`strategy_components/` implements this document's §5 algorithm (connected
components + principled cuts); `strategy_single_entry/` implements ownership
propagation from roots (every DAG single-entry; spec in the final section of
`docs/partition-algorithm.md`). Both run on every conversion and the comparison
dashboard settles the default once real exports are available.

**Doc map** (reading order for a new agent): `CLAUDE.md` (onboarding +
commands) → this file (architecture + rationale) → `docs/partition-algorithm.md`
(exact algorithm spec) → `docs/job-mapping-catalog.md` (job/param → Airflow
mapping as implemented; test-synced to `core/ctrlm_core/operator_registry.py`)
→ `docs/impl-contracts{,-v2,-v3}.md` (module contracts, accurate historical
deltas) → `plugins/README.md` (the write-once `ctm_plugins` package).

**Design-only — NOT yet implemented** (everything else in this doc is built):
the standalone HTML gap report of §9 (diagnostics currently live in
`partition.json` and the dashboard); the `cluster-map.yaml` pin/override
read-back of §5 (the file is written as a report, but re-runs do not yet honor
human edits); `overrides.yaml` (§6); validation levels L1/L3 of §8 (need
`aws-mwaa-local-runner`); the calendar timetable runs on example data pending a
real calendar export; RUN_AS credential policy (§6) is still an open decision.

---

## 1. Goal

Convert Control-M job definition XML exports into:

1. Readable, idiomatic Airflow DAG `.py` files that run on AWS MWAA.
2. The supporting environment artifacts: plugins (timetables, macros, callbacks), `requirements.txt`, pools/variables manifests, connection manifest, bootstrap script.
3. A per-job **gap report** stating exactly what converted FULL / PARTIAL / MANUAL, with reason codes. This report drives the human workstream and is a first-class output.

### Non-goals

- No runtime coexistence bridge (Airflow ↔ Control-M condition sync).
- No continuous re-sync after cutover — the generated Python becomes the maintained source of truth.
- No credential migration — secrets are provisioned out of band (Secrets Manager); we emit *manifests* naming what must exist, never secret values.

---

## 2. Architecture

A staged pipeline with inspectable intermediate artifacts between every stage. Nothing maps XML directly to Python.

```
XML exports ──► ingest ──► IR (typed, JSON-serializable)
                              │
                              ▼
                          analyze     condition graph · schedule normalization ·
                              │       job classification · calendar resolution
                              ▼
                         partition    clusters → DAG boundaries  (cluster-map.yaml)
                              │
                              ▼
                            map       IR constructs → Airflow constructs  (mapping-config/, overrides.yaml)
                              │
                              ▼
                            emit      dags/*.py · plugins/ · requirements.txt · pools/vars/conn manifests
                              │
                              ▼
                    validate + report DagBag import · graph equivalence · gap report (HTML/CSV)
```

### Repo layout (as built)

```
ctrlm-airflow-migration/
├── core/ctrlm_core/          # shared core (pip-installed editable as ctrlm-core)
│   ├── model.py              #   THE contract: typed IR + partition models
│   ├── parser.py             #   DEFTABLE XML → IR (nested folders any depth, dialect-tolerant)
│   ├── desugar.py            #   folder-level conds/vars → synthetic start/end nodes (recursive)
│   ├── schedule.py           #   day-pattern normalization, ODATE clock, cron helpers
│   ├── graph.py / cuts.py    #   condition matching → CtmGraph; shared cut phases
│   ├── operator_registry.py  #   declarative job-type → operator registry (v3)
│   ├── emit.py / templates/  #   Jinja2 + black codegen; full param mapping
│   ├── stats.py / autoedit.py
│   └── pipeline.py           #   per-XML-scope orchestration; scopes.json; pools.json
├── strategy_components/      # strategy A: components + cuts (this doc §5)
├── strategy_single_entry/    # strategy B: ownership propagation (single-entry DAGs)
├── plugins/ctm_plugins/      # write-once custom components → MWAA plugins.zip
├── dashboard/                # offline comparison dashboard (scope selector, 5 views)
├── mapping-config/           # nodes.yaml (conn/os/type), notify.yaml, calendars.yaml
├── examples/exports/         # 5 synthetic sample XMLs (tests assert their content)
├── docs/                     # this file + algorithm spec + contracts + catalog
├── scripts/run_all.ps1       # both strategies + dashboard in one command
├── tests/                    # 285 tests; conftest.py puts repo root on sys.path
└── output/<strategy>/<scope>/  # generated (gitignored): ir/graph/partition/dags/pools
```

Each XML file is one **conversion scope** (v2): the pipeline above runs once per
file; cross-file condition matches are reported in `scopes.json`, never wired.

### Tech stack

Python 3.11 (matches MWAA runtime) · `lxml` (parse) · Pydantic v2 (IR) · `networkx` (graph) · Jinja2 (codegen) · `black` (format emitted code) · Typer (CLI) · pytest (golden files). Local validation via `aws-mwaa-local-runner` pinned to the environment's Airflow version.

---

## 3. Intermediate representation (IR)

Typed Pydantic models, serializable to `build/ir.json`. Everything downstream consumes IR, never XML. Key models:

- **Job**: name, folder, application/sub-application, task type, command/script (memname/memlib), node id, run-as, priority/critical, confirm flag, cyclic (+interval), maxwait, maxrerun/rerun-interval, timezone, time window (from/to).
- **Schedule**: raw month-days/weekdays/months + and/or logic, calendars (days/weeks/conf + shift), retro, specific dates; plus the *resolved* form after analysis (cron string or calendar-spec).
- **Condition**: in-conditions (name, date qualifier, and/or), out-conditions (name, date qualifier, add/delete sign).
- **OnDoAction**: trigger (statement/code pattern) → list of actions (mail, shout, force-job, condition add/del, set-variable, ok/notok, stop-cyclic, rerun).
- **Resource**: quantitative (name, units) and control (name, exclusive/shared).
- **Variable**: AUTOEDIT assignments; global (`%%\`) vs local.
- **Shout**: when (ok/notok/late/…), destination, message.

Ingest normalizes Control-M dialect differences (v8 `TABLE`/`GROUP` vs v9+ `FOLDER`/`SUB_APPLICATION`, attribute spelling drift) into one IR. Unknown attributes are preserved in a `raw` bag and counted in the inventory report so nothing is silently dropped.

---

## 4. Semantic analysis

### 4.0 Folder-level desugaring (runs first)

SMART folders and sub-folders carry their own scheduling criteria, in/out conditions, variables, and ON/DO actions that cascade to their jobs. A desugaring pass rewrites these into job-level IR before any graph is built:

- A folder carrying folder-level conditions gets a synthetic **folder-start** node (holds the folder's in-conditions; every entry job of the folder depends on it) and a synthetic **folder-end** node (depends on every terminal job; holds the folder's out-conditions). Synthetic nodes participate in the condition graph exactly like jobs and are emitted as the TaskGroup's boundary `EmptyOperator`s.
- Without this pass, a job with no conditions of its own inside a gated SMART folder would masquerade as a root — corrupting partitioning *and* silently dropping the folder-level cross-folder edges.
- Folder-level scheduling and rule-based calendars resolve into per-job effective schedules (§4.2); variables and ON/DO actions cascade nearest-ancestor-wins; sub-folders nest recursively (job ← sub-folder ← folder).
- Synthetic conditions are exempt from hub cuts — a folder with many entry jobs must not have its own start node severed.
- Config knob `folder_start_always`: when on, every SMART folder gets a start node even without folder-level conditions, pulling each folder's jobs into one cluster (more folder-shaped boundaries). Off by default — boundaries stay purely dependency-driven.

### 4.1 Condition graph

Directed graph spanning every folder **within one conversion scope** — since v2 one XML file (user decision; originally the whole export set): node = job; edge = producer `OUTCOND(add)` → consumer `INCOND` matched on condition name + date qualifier. Matches that would cross XML files are reported in `scopes.json` (`cross_scope_links`) and surface per scope as orphans — never silently wired, never silently dropped. Only same-run `ODAT↔ODAT` matches become clustering edges; `PREV`-qualified in-conditions are cross-*run* dependencies and go straight to the wiring set as previous-run gates; `STAT`/literal-date qualifiers are flagged for review (they usually encode flags or locks, not dataflow).

Also recorded:
- **Orphan in-conditions** — consumed but never produced in the export: either set externally (`ctmcontb`, other datacenters, manual ops) or the export is incomplete. Each needs a config decision: external-trigger (map to a Dataset a human/system updates), always-true, or error.
- **Dead-end out-conditions** — produced but never consumed: usually no-ops; listed for review.
- **Negative conditions** (delete sign) — no direct Airflow equivalent. The classifier attempts pattern detection (mutual-exclusion pairs → pools; self-cleanup → no-op); everything else → MANUAL.

### 4.2 Schedule normalization

Per job (after applying SMART-folder scheduling and rule-based calendars, which resolve to per-job effective schedules):

- Plain weekday/monthday/month combos → **cron**.
- Anything touching calendars (DAYSCAL/WEEKSCAL/CONFCAL, SHIFT rules, periodic calendars) → **calendar-spec** consumed by a custom `CtmCalendarTimetable` plugin fed from an exported calendar file (`mapping-config/calendars.yaml`).
- Expected split for typical estates is ~80% cron / 20% timetable; the inventory report gives the real numbers.

### 4.3 Job classification

command / script-on-agent / dummy / file-watcher / database / cyclic / confirm-gated / unsupported-application (SAP, Informatica, etc. — anything without a mapped operator becomes a MANUAL stub, never a guess).

---

## 5. Partitioning: clusters → DAGs

This section specifies the **components strategy** (`strategy_components/`):
DAG per dependency cluster (connected component of the condition graph), not
per folder. Folders are preserved *inside* DAGs as TaskGroups and tags, so
teams keep their familiar grouping visually. The alternative **single-entry
strategy** (`strategy_single_entry/` — ownership propagation from roots;
convergence points become new roots, every DAG has exactly one entry/anchor) is
specified in the final section of `docs/partition-algorithm.md`; both run on
every conversion for comparison.

### Algorithm

Inputs: the analyzed job set — each job carrying its normalized **day-pattern** (which days it is ordered, from weekdays/monthdays/calendars) and optional **start-time gate** (TIMEFROM) — plus the directed condition-edge multigraph from §4.1. Outputs: a job→DAG assignment (`cluster-map.yaml`), the cross-DAG wiring set, a schedule spec per DAG, and diagnostics.

1. **Extract cyclic jobs** — each becomes its own DAG (it needs its own run cadence); all its condition edges move to the wiring set.
2. **Cut hub conditions** — any condition with fan-in ≥ N or fan-out ≥ N (default 10), or spanning ≥ H folders (default 3): all its edges move to the wiring set. Broadcast events like `DAILY_LOADS_OK` must not fuse the estate into one DAG.
3. **Cut day-pattern conflicts** — an edge whose producer and consumer are *both* day-scheduled with *different* normalized day-patterns moves to the wiring set. TIMEFROM differences do **not** cut: within one day-pattern they become in-DAG time gates (deferrable `DateTimeSensor` targeting ODATE + configured time, New-Day-aware).
4. **Apply manual cuts** listed in `cluster-map.yaml`.
5. **Connected components** (union-find) of the undirected projection of the remaining edges = candidate DAGs. Jobs left with no edges (**singletons**) are coalesced into one DAG per (folder, day-pattern) group — otherwise unconditioned jobs would explode the DAG count. Manual merges are applied here (refused if day-patterns differ).
6. **Resolve transitive conflicts** — a component can still hold two day-patterns connected through unscheduled middle jobs (daily → unscheduled → weekly), which step 3 cannot see. Compute the **minimum edge cut** (max-flow) between the two pattern groups — unscheduled middles are interior nodes and fall on whichever side minimizes severed edges. Move the cut edges to the wiring set, record them in `cluster-map.yaml` flagged `auto_resolved`, re-split, and repeat while any component holds more than one pattern (rarest group first; deterministic tie-breaks).
7. **Anchor purity** (run-context rule, implemented as kind `ANCHOR`): while a component's scheduled *roots* spread more than `anchor_spread_hours` (default 6) apart on the ODATE clock, min-cut the rarest anchor bucket away, same machinery as step 6. Roots at 21:00 and 23:00 share one overnight DAG; roots at 06:00 and 20:00 would stretch every run toward its schedule period and operationally couple unrelated chains — those split, wired back with sensors.
8. **Size guardrail**: component > 150 tasks → generate anyway, but flag loudly with suggested further cuts (highest-betweenness condition edges) in the report.
9. **Schedule + name**: the DAG's day-pattern is the component's (now unique) pattern, time-anchored at the earliest root TIMEFROM; later TIMEFROMs become time gates. Pattern-less components are Dataset-triggered. `dag_id` = snake_cased modal folder name (tie → lexicographic first; collision → deterministic numeric suffix; cross-scope collisions renamed `<scope>__<dag_id>` by the pipeline).

The exact implementable form of this algorithm — pseudocode, invariants, and tie-break rules — is in `docs/partition-algorithm.md`.

### `cluster-map.yaml` — the human control surface

The partition stage *writes* this file (job → dag_id, plus the cut list); subsequent runs *honor* it. Humans edit it to rename DAGs, force splits (`cut: [COND_NAME]`), or force merges — without touching code. This keeps dev-iteration re-runs deterministic even in one-time mode, and makes DAG boundaries a reviewed, sign-off-able artifact.

### Cross-DAG edge wiring rule

For every cut or cross-cluster condition edge:

- **Consumer is time-scheduled** (has its own cron/calendar and *waits* for the condition) → **gate**: `ExternalTaskSensor` in reschedule/deferrable mode, `execution_date_fn` aligning same-ODATE runs, `timeout` = MAXWAIT converted to seconds.
- **Consumer is purely condition-driven** → **trigger**: producer task gets `outlets=[Dataset("ctrlm://cond/<NAME>")]`; consumer DAG `schedule=[...datasets]`. AND of conditions = the list (Airflow requires all); OR requires conditional dataset expressions (Airflow ≥ 2.9) — on older MWAA, fall back to sensor gates.
- Caveat baked into the report: Dataset triggers carry no date semantics, so `ODAT`-strict same-day matching relies on daily cadence; non-ODAT qualifiers (PREV, STAT, literal dates) → PARTIAL flag.

---

## 6. Mapping rules

**Implemented form (v3):** the declarative registry `core/ctrlm_core/operator_registry.py` (job-type rows, first-match, MANUAL catch-all stub) plus the per-task param mapping in `emit.py`; the authoritative user-facing version is `docs/job-mapping-catalog.md`, kept in sync with the registry by a test. The table below is the original design-level view (`overrides.yaml` is still design-only). Driven by `mapping-config/` (environment facts). Core table:

| Control-M construct | Airflow / MWAA mechanism | Status |
|---|---|---|
| Folder / SMART folder | TaskGroup within DAG + `ctrlm:<folder>` tag | FULL |
| Command / script job | `SSHOperator` to the mapped agent host (see NODEID note) | FULL |
| Dummy job | `EmptyOperator` | FULL |
| File watcher | `SFTPSensor` / `S3KeySensor` (deferrable) per path mapping | FULL/PARTIAL |
| DB job / stored proc | SQL-provider operator per `mapping-config` | PARTIAL |
| Unsupported app types (SAP, …) | MANUAL stub task raising `NotImplementedError` + report entry | MANUAL |
| In/out conditions (intra-DAG) | `>>` dependencies; AND default; OR → join `EmptyOperator` with `trigger_rule=one_success` | FULL |
| Conditions (cross-DAG) | Dataset outlet/inlet or `ExternalTaskSensor` per wiring rule §5 | FULL/PARTIAL |
| Negative (delete) conditions | Pattern-detect: mutual exclusion → 1-slot pool; else MANUAL | PARTIAL/MANUAL |
| Cyclic job (interval, window) | Own DAG: `*/N` cron restricted to window, `max_active_runs=1`, `catchup=False` | FULL |
| DO STOPCYCLIC | MANUAL (pause via API is operational, not structural) | MANUAL |
| MAXWAIT | Sensor `timeout` / `dagrun_timeout` | FULL |
| MAXRERUN + rerun interval | `retries` + `retry_delay` | FULL |
| ON code → DOMAIL / DOSHOUT | `on_failure_callback` / `on_success_callback` → SNS helper (plugins); SES SMTP alternative | FULL |
| ON code → DOFORCEJOB | `TriggerDagRunOperator` | FULL |
| ON code → DOCOND | Dataset outlet (cross-DAG) or no-op if the edge already exists intra-DAG | FULL |
| ON specific exit-code branches | `BranchPythonOperator` on return code where pattern is simple; else MANUAL | PARTIAL |
| DO IFRERUN FROM \<step\> | MANUAL — no step-level restart in Airflow | MANUAL |
| CONFIRM flag | Approval-gate helper (deferrable sensor polling an Airflow Variable / SQS) from plugins | PARTIAL |
| Quantitative resource | Pool (`slots` = resource quantity; task `pool_slots` = units consumed) → `pools.json` | FULL |
| Control resource (exclusive) | 1-slot pool | FULL |
| Control resource (shared) | PARTIAL — reader/writer semantics don't map to pools cleanly; report | PARTIAL |
| PRIORITY / CRITICAL | `priority_weight`; CRITICAL noted in report (no true resource pre-allocation) | PARTIAL |
| AUTOEDIT variables | Jinja translation table (`%%ODATE` → `{{ ctm.odate }}`, etc.); unresolved `%%` functions → PARTIAL | FULL/PARTIAL |
| Global variables (`%%\`) | Airflow Variables → `variables.json` manifest | FULL |
| SHOUT WHEN LATE | `sla` + `sla_miss_callback` → SNS; limitations documented (SLA only evaluated on scheduled runs) | PARTIAL |
| Time window FROM/TO | FROM = schedule time; TO = "don't start after" guard task + `dagrun_timeout` | PARTIAL |
| Calendars / CONFCAL+SHIFT | `CtmCalendarTimetable` plugin reading exported calendar data | FULL |
| TIMEZONE | DAG-level pendulum timezone | FULL |
| RETRO | Default `catchup=False`; RETRO jobs flagged for explicit decision | MANUAL |

### The ODATE convention (subtle, load-bearing)

Control-M's order date is not the wall-clock date: jobs running after midnight but before **New Day time** belong to the *previous* ODATE. Convention:

> A run's ODATE = calendar date of `data_interval_end` in the DAG's timezone, shifted back one day when the fire time is before the configured New Day time.

Shipped as a `{{ ctm.odate }}` macro in the plugins (plus format variants), configured with `new_day_time` from `mapping-config`. All `%%ODATE`-family substitutions route through it. Getting this wrong corrupts every date-parameterized script downstream — it gets dedicated unit tests against known Control-M behavior.

### NODEID → connection mapping (mandatory, not optional)

MWAA workers are ephemeral Fargate containers: **no business workload runs locally**. Every command/script job executes remotely via SSH to the surviving agent host. `mapping-config/nodes.yaml` maps NODEID/node-group → `ssh_conn_id`; RUN_AS policy (one service account per host vs per-user keys) is a config decision that shapes the connection manifest. Unmapped NODEIDs fail generation loudly.

---

## 7. Code generation & output artifacts

- One file per DAG: `output/dags/<dag_id>.py`, black-formatted, with a provenance header (source folders, input file hashes, tool version) and per-task comments carrying the original JOBNAME/MEMNAME.
- `task_id` = sanitized JOBNAME (Airflow charset, ≤250 chars, collision-suffixed deterministically).
- `output/plugins/`: `ctm_timetables.py`, `ctm_macros.py` (odate), `ctm_callbacks.py` (SNS/SES shout), `approval_sensor.py`; zipped into `plugins.zip` by the build script.
- `output/requirements.txt`: pinned to the MWAA constraints file for the environment's Airflow version (providers: `amazon`, `ssh`, `sftp`, `common-sql`).
- `output/config/`: `pools.json`, `variables.json`, `connections-manifest.yaml` (ids/hosts/types only — no secrets), plus `bootstrap.py` that provisions pools/variables through the MWAA-exposed Airflow REST API (no CLI access on MWAA).
- Generation is deterministic: same inputs + config → byte-identical output (dev re-runs diff cleanly).

### MWAA-specific constraints honored by design

- Custom timetables/macros/callbacks must ship via `plugins.zip` — kept dependency-light; validated in `aws-mwaa-local-runner` at the exact Airflow version.
- Confirm the environment's Airflow version early: conditional dataset scheduling (OR-conditions) needs ≥ 2.9; older → sensor fallback path.
- Connections/Variables via the Secrets Manager backend (recommended); manifests name the required secret keys.
- Network prerequisite: MWAA VPC must reach agent hosts on 22/tcp — validate with one connection before bulk generation.
- Environment sizing (class, workers, DAG-parse budget) reviewed after inventory gives real DAG/task counts.

---

## 8. Validation

| Level | Check |
|---|---|
| L0 | Golden-file tests: XML fixture → expected `.py` (byte-exact) |
| L1 | DagBag import test at the pinned MWAA Airflow version (no import errors, parse time budget) |
| L2 | **Graph equivalence**: introspect generated DAGs, recompute the dependency edge set, assert it equals the IR condition graph modulo recorded cuts; reconcile counts (every job = exactly one task or one reported skip) |
| L3 | Pilot cluster on staging MWAA with `--dry-run` emit mode (SSH commands wrapped in `echo`) to prove orchestration/timing/notifications without executing workloads |

---

## 9. Gap report

`build/gap-report.html` + CSV. Per job: folder, dag_id, task_id, operator, status (FULL/PARTIAL/MANUAL), reason codes, cross-DAG links. Rollups: % by status per folder/application, top blockers ranked by job count, orphan conditions, oversized clusters, unmapped NODEIDs. CSV imports straight into the migration team's tracker — the MANUAL rows *are* the human work queue.

---

## 10. Delivery phases

| Phase | Deliverable | Exit criterion |
|---|---|---|
| **P0 — Inventory** | Parser + `ctrlm2af inventory`: counts by task type, schedule complexity, ON/DO usage, cyclic, resources, condition stats — run on real exports | Inventory reviewed; parser survives the real XML dialect |
| **P1 — Boundaries** | Condition graph + partition + cluster report | `cluster-map.yaml` reviewed/signed off; no unexplained mega-cluster |
| **P2 — Core conversion** | Mapping + emit for the dominant cases (command/dummy, cron schedules, plain conditions); L0–L2 validation green | Majority of jobs generate FULL |
| **P3 — Long tail** | Calendars/timetable plugin, cyclic, watchers, ON/DO, resources→pools, notifications, ODATE macro | PARTIAL/MANUAL rate at agreed target |
| **P4 — Pilot & handoff** | One real cluster on staging MWAA (dry-run then live), bootstrap script, runbook, final gap report | Pilot signed off; conversion of remaining estate is mechanical |

P0 is deliberately tiny and first: it validates every assumption in this document against the real exports before any mapping code exists.

**Implementation status (2026-07-07):** the machinery of P0–P3 is built ahead of
schedule against synthetic samples (parser + IR + graph + both partitioners +
registry-driven emit + plugins package + comparison dashboard; 285 tests,
deterministic). What remains is exactly the *real-data* half of each phase:
running the parser against real exports (P0 exit), reviewing real cluster
boundaries (P1 exit), the calendar export for the timetable, and the P4 pilot
on staging MWAA with `aws-mwaa-local-runner` validation.

---

## 11. Risks

- **Hairball graph** (one giant component) — mitigated by hub cuts + P1 human gate; worst case the cluster-map forces boundaries.
- **XML dialect surprises** — P0 runs the parser on real exports before anything else is built.
- **MWAA → agent SSH reachability** — infra prerequisite; test with one host in week one, not at pilot time.
- **Calendar semantics** (SHIFT/business-day rules) — timetable gets unit tests against truth tables exported from Control-M.
- **ODATE/New Day mistakes** — dedicated tests; single macro chokepoint so a fix lands everywhere.
- **RUN_AS credential sprawl** — decide the SSH account policy early; it sets the size of the connection/secret manifest.

---

## 12. Inputs needed (blocking P0)

1. **Sample XML exports** — a representative set of folders → `examples/exports/`. Include at least one SMART folder, one cyclic job, one file watcher, calendars in use, and cross-folder conditions.
2. **Control-M version** and export method (Desktop/ctm XML vs Automation API JSON).
3. **Calendar definitions export** + the system **New Day time** + timezone(s) in use.
4. **Global variables export** (`%%\` variables).
5. Rough scale: number of folders/jobs (or I compute it from full exports at P0).
6. **MWAA facts**: environment Airflow version + class; VPC connectivity to agent hosts; Secrets Manager backend enabled?
7. **Post-migration execution model**: do current agent hosts remain as SSH targets, or does any workload move (containers/AWS Batch) during migration?
