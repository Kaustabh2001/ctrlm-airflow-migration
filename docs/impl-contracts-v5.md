# Implementation contracts — v5 delta (dashboard: level-wise structure view + per-DAG graph view)

Delta on v1–v4. User request: (1) the Control-M structure view must be a
hierarchical, LEVEL-WISE layout (Backstage-catalog style: roots left, each
dependency rank one layer right) instead of force-directed; (2) each strategy's
output must be viewable as per-DAG task graphs like Airflow's Graph view,
including generated structural tasks (gates, waits, confirm, force, folder
start/end), with cross-DAG links shown as external stubs.

## V5-1: dag_plans.json (emit agent)

Owner files: `core/ctrlm_core/emit.py`, `tests/test_emit.py` (additive).

- `emit_dags` additionally writes `<scope>/dag_plans.json` — the task-level
  plan of every generated DAG, produced from the SAME TaskPlan pass that
  renders the code (never re-derived elsewhere). Deterministic (sorted keys,
  sorted lists). Schema:
  ```json
  {
    "<dag_id>": {
      "schedule": "cron or null",
      "dataset_triggered": false,
      "datasets": ["ctrlm://cond/..."],
      "tasks": [
        {"task_id": "...",
         "kind": "job|gate|confirm|wait|force|folder_start|folder_end",
         "operator": "SSHOperator|WinRMOperator|CtmDatabaseJob|CtmManualJob|CtmFileWatcherSensor|EmptyOperator|DateTimeSensorAsync|CtmApprovalGateSensor|ExternalTaskSensor|TriggerDagRunOperator",
         "source_uid": "FOLDER/JOB or null for structural tasks",
         "task_group": "group id or null",
         "upstream": ["task_id", ...]}
      ],
      "outlets": [{"task_id": "...", "dataset": "..."}],
      "external_waits": [{"task_id": "wait_...", "external_dag_id": "...", "external_task_id": "..."}]
    }
  }
  ```
- `upstream` must reproduce exactly the dependencies emitted in the .py file
  (including gate->job, confirm->job, job->force edges).
- Tests: plan exists for every dag file; task sets match between plan and
  rendered code (parse the rendered file text for task_id= occurrences);
  upstream edges match the emitted `>>` lines for a fixture; byte-determinism.

## V5-2: dashboard views (dashboard agent)

Owner files: `dashboard/build.py`, `dashboard/template.html` (or assets),
`tests/test_dashboard.py`.

If a `dataviz` skill is available to you, load it before writing view code;
otherwise keep the existing dark theme and colorblind-safe palette.

- **Control-M structure tab — level-wise view (replaces force layout):**
  - Compute per-node `level` = longest-path depth from roots over the scope's
    ORIGINAL dependency graph (graph.json e_edges + w_edges; the pipeline
    writes graph.json before strategy cuts, so this IS the original job graph).
    Cycle guard: nodes on cycles get level = max(level of non-cycle preds)+1,
    flagged in tooltip.
  - Render with vis-network hierarchical layout, direction LR, physics off,
    explicit node.level driving the ranks (deterministic), straight/smooth
    edges with arrows. Node color = folder (existing palette), shape/icon by
    task type, synthetic folder nodes visually muted. Keep tooltips + search.
  - Add a layout toggle (Level-wise | Force) defaulting to Level-wise.
- **Strategy tabs (both) — add a sub-mode switch: "Partition overview"
  (existing colored graph, unchanged) | "DAG graph" (new):**
  - DAG selector dropdown (dag_ids of the selected scope, with task counts,
    sorted). Renders the selected DAG's task graph from dag_plans.json:
    hierarchical LR, levels from longest-path over `upstream` edges.
  - Task styling by `kind`: job tasks = solid boxes labeled task_id with
    operator name as sub-label; gate/wait/confirm = distinct shape (e.g.
    diamond/ellipse) with dashed border; force = distinct color; folder
    start/end = small muted dots. Legend for kinds.
  - Cross-DAG context as external stubs: for each entry in `external_waits`,
    a ghost node "<external_dag_id>.<external_task_id>" feeding the wait task;
    for each `outlets` entry, a ghost dataset node downstream of its task; for
    dataset-triggered DAGs, ghost dataset nodes feeding the roots. Ghost nodes
    visually distinct (dotted outline) and non-interactive except tooltip.
  - Tooltip on job tasks: original Control-M job (source_uid), operator,
    upstream count; clicking a job task highlights the same job in the
    structure view is NOT required (nice-to-have only if trivial).
- build.py: read `<scope>/dag_plans.json` from both strategy dirs and embed
  per scope; fail with a clear message if missing (regenerate outputs).
- Keep: one self-contained offline HTML, scope selector, run overview tab,
  comparison tab, determinism. Tests: two-scope fixtures now include
  dag_plans.json; assert level-wise container + layout toggle present, DAG
  selector present per strategy, ghost/external stub markers present, kinds
  legend present, still zero external http(s) refs.

## V5.1 (view refinements — user decisions)

- Synthetic folder start/end nodes are hidden in EVERY dashboard view by
  default; dependencies are transitively contracted through them (tooltip/
  legend notes the folder gate). One global "Show folder nodes" toggle
  restores them everywhere. Display-level only: the generated DAG .py files
  keep their EmptyOperator gate tasks (they carry folder-level condition
  wiring at runtime).
- Strategy "Partition overview" views default to the hierarchical level-wise
  layout (same style as the structure view), with Force as the toggle.

## Integration + verification (single agent, combined)

Run full pytest and fix fallout; regenerate output/components,
output/single_entry, output/dashboard/index.html; verify on the real dashboard
HTML: level-wise structure view data present (node levels embedded), strategy
DAG selector lists e.g. fin_eod with its task count, fin_eod DAG graph includes
gate_ tasks and wait_/external stubs per its dag_plans.json, bank scope DAG
graph shows force_bank_recon and confirm-style tasks where applicable, offline
self-containment intact; per-scope byte-determinism of dag_plans.json on
rerun; run_all.ps1 green. Do NOT git commit.
