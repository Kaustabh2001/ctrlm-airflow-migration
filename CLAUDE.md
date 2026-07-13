# CLAUDE.md — agent onboarding

Converts Control-M job-definition XML exports into Airflow DAGs (target:
Airflow 3 on AWS MWAA; `@dag` authoring style, `airflow.sdk` imports, Assets),
with TWO partitioning strategies run side by side plus an offline
comparison dashboard. Everything is **deterministic**: same input + config →
byte-identical output. No AI/LLM runs at conversion time.

## Commands (Windows; venv at `venv/`)

```powershell
venv\Scripts\python -m pytest tests -q                                        # 326 tests
venv\Scripts\python strategy_components\run.py examples\exports -o output\components
venv\Scripts\python strategy_single_entry\run.py examples\exports -o output\single_entry
venv\Scripts\python dashboard\build.py --a output\components --b output\single_entry -o output\dashboard\index.html
powershell -ExecutionPolicy Bypass -File scripts\run_all.ps1                  # all of the above
```

Setup from scratch: `python -m venv venv`, `venv\Scripts\python -m pip install
-r requirements.txt`, `venv\Scripts\python -m pip install -e core`.

## Reading order

1. `DESIGN.md` — architecture, rationale, MWAA constraints, what is design-only.
2. `docs/partition-algorithm.md` — exact spec of BOTH partitioning strategies.
3. `docs/job-mapping-catalog.md` — job/param → Airflow operator mapping (synced
   to `core/ctrlm_core/operator_registry.py` by a test in `tests/test_registry.py`).
4. `docs/impl-contracts.md`, `-v2.md`, `-v3.md` — module contracts (historical
   deltas; still accurate descriptions of module boundaries).
5. `plugins/README.md` — the write-once `ctm_plugins` package (MWAA plugins.zip).

## Hard rules (violating these breaks tests or trust)

- `core/ctrlm_core/model.py` and `core/ctrlm_core/pipeline.py` are the contract
  between all modules — change them only with a deliberate, documented reason.
- **Determinism**: every iteration that affects output must be sorted; no
  wall-clock, no randomness. Verified by byte-identical rerun tests.
- **Scope = one XML file** (user decision, v2): conditions never wire across
  files; cross-file matches are *reported* in `scopes.json`.
- **Airflow is NOT installed** and cannot be (native Windows): generated DAGs
  and `plugins/` airflow-importing files are `py_compile`-checked only; pure
  logic (e.g. `plugins/ctm_plugins/_odate.py`) lives in airflow-free modules
  with real unit tests. Never `import airflow` in tool code or tests.
- `examples/exports/sample_*.xml` contents are asserted by many tests — treat
  them as fixtures, extend by adding a NEW file (job-count assertions in
  strategy tests will need updating).
- Unknown/unsupported job types must FAIL LOUDLY (MANUAL stub raising
  `NotImplementedError` + `UNSUPPORTED_TYPE` diagnostic) — never guess silently.
- `docs/job-mapping-catalog.md` and the operator registry must stay in sync —
  a test enforces it bidirectionally.

## Current state / what's next

Built and verified: parser (nested folders any depth), folder desugaring
(synthetic start/end nodes), condition graph, both partitioners, registry-driven
emit (SSH/WinRM/SQL, pools, priorities, callbacks, approval gates), ctm_plugins,
per-scope pipeline, dashboard. Design-only so far: standalone gap report,
cluster-map pin read-back, overrides.yaml, mwaa-local-runner validation.
**Blocked on the user's real Control-M exports** (drop into `examples/exports/`
or a new input dir), the real calendar export + New Day time, and MWAA
environment facts. The DAG-boundary default (components vs single-entry) is
deliberately open until the dashboard compares both on real data.
