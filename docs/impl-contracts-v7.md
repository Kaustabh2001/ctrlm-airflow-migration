# Implementation contracts — v7 (PROPOSED: the decision layer)

**STATUS: DESIGN FOR REVIEW — nothing here is implemented. No code until the user
approves this document.**

## Purpose

Some conversion decisions cannot be enumerated by rules: which cyclic jobs are
observers, what a MANUAL-stubbed job should become, which CTM-utility jobs to
eliminate, whether two DAGs belong together. v7 adds a **decision layer**: the
deterministic pipeline surfaces the ambiguous residue; an AI agent (or human)
proposes decisions into a reviewable file; the deterministic pipeline applies them.

## Guardrails (non-negotiable)

1. **No AI at conversion time.** Conversion stays a pure function of
   `(XML, config, decisions.yaml)` — byte-identical reruns, testable.
2. **Every decision is a recorded artifact**: decision + rationale + provenance
   (`proposed_by: ai | human`) + approval state. Humans can veto any line.
3. **Every applied AI decision is visible**: diagnostic `AI_DECISION_APPLIED`
   per application; eliminated jobs leave a tombstone entry in partition.json
   (never silently vanish). Config `apply_unapproved_ai: true|false`
   (proposed default: **true**, with the diagnostic trail; human entries always apply).
4. **Advisor agents run on Sonnet/Haiku, never Fable** (project golden rule).

## Data flow

```
pipeline run 1 ──► outputs + <scope>/decision_points.json     (ambiguous residue + evidence)
                            │
        ADVISOR (agent or human) reads points + ir.json ──► mapping-config/decisions.yaml
                            │
pipeline run 2 ──► decisions applied deterministically; diagnostics record each application
```

## Decision domains (v7 scope)

| Domain | Subject | Decisions | Applied where |
|---|---|---|---|
| `cyclic_mode` | job uid | `observer_sensor` \| `event_source_dag` \| `workload_dag` | partitioner (un-extract observer; it clusters normally, emit swaps its operator to `CtmCyclicObserverSensor`) |
| `manual_job` | job uid | `operator_override {operator_import, kwargs}` \| `eliminate {reason}` \| `stay_manual` | emit (registry consults decisions as highest-precedence row) |
| `utility_job` | job uid | `translate {to: trigger_dag\|variable_set\|callback\|asset_event, params}` \| `eliminate {reason}` \| `stay_manual` | emit |
| `orphan_condition` | condition name | `external_asset {uri}` \| `ignore` \| `error` | partitioner/emit (Asset gate/schedule) |
| `cut_edge` (HUMAN-only) | (source, target, cond) | force cut | partitioner (existing MANUAL cut kind, finally wired) |
| `rename_dag` (HUMAN-only) | dag_id | new name | partitioner naming (implements cluster-map pin read-back) |

**No free-form merge domain (user decision, 2026-07-09).** Merging happens only as a
CONSEQUENCE of reclassification, never as a command: when `cyclic_mode:
observer_sensor` applies, the job is no longer extracted into its own DAG — it
clusters normally and the fragmented component reunites with the observer inside
as a sensor task; when a utility job between two chains is `eliminate`d, edges
contract through it and the pieces rejoin naturally. The partitioning algorithm
remains the only authority on DAG boundaries. Consequently the AI advisor may
propose ONLY reclassification domains (`cyclic_mode`, `manual_job`,
`utility_job`, `orphan_condition`); structural entries (`cut_edge`,
`rename_dag`) are accepted from humans only — AI-proposed structural entries
are rejected at load with a diagnostic.

## Deterministic classifier (runs first; only the residue reaches the advisor)

Auto-decided, no decision point emitted:
- cyclic + FileWatch → observer_sensor
- cyclic + ON OK DOSTOPCYCLIC(self) → observer_sensor
- cyclic + out-conds consumed only by condition-driven consumers → event_source_dag
- cyclic + no out-conds consumed → workload_dag
- command matches a **fixed-translation utility** (per docs/coverage-matrix.md Class 6:
  ctmorder→trigger_dag, ctmvar→variable_set, ctmshout→callback, ctmcontb -ADD→asset_event;
  ctmldnrs/ctmudly/ctmagcln/ctmruninf/ctmlog/start_ctm-family → eliminate)

Emitted as decision points (`decision_points.json`):
- cyclic + no in-conds + out-cond consumed by time-scheduled chain (observer candidate)
- every job resolving to the MANUAL catch-all (full IR context attached)
- utility commands with ambiguous semantics (ctmcontb -DELETE, ctmpsm, ctmkilljob,
  ctmcreate, ctmdefine, unknown `ctm*` wrappers)
- orphan conditions
- (informational) oversized clusters and AUTO_RESOLVED cuts, as merge/cut review candidates

## Schemas

`<scope>/decision_points.json` (emitted, deterministic):
```json
[{"id": "cyclic:OPS/WH_STOCK_CHECK", "domain": "cyclic_mode",
  "subject": "OPS/WH_STOCK_CHECK", "current_default": "workload_dag",
  "evidence": {"interval": "10M", "in_conds": [], "consumers": ["WH/WH_REPORT (weekdays 0900)"]},
  "ir": { ...trimmed Job model... }}]
```

`mapping-config/decisions.yaml` (input, tracked in git):
```yaml
decisions:
  - id: cyclic:OPS/WH_STOCK_CHECK          # matches a decision point id, or free-form for merges/renames
    domain: cyclic_mode
    subject: OPS/WH_STOCK_CHECK
    decision: observer_sensor
    rationale: "polls stock table until rows appear; WH_REPORT chain waits on it"
    proposed_by: ai
    approved: false                         # applied anyway iff apply_unapproved_ai
```

## New components

- `core/ctrlm_core/decisions.py` — schema (pydantic), loader, matcher; NOT in model.py.
- `plugins/ctm_plugins/sensors.py` + `CtmCyclicObserverSensor` — deferrable/reschedule;
  runs the job's command via SSH/WinRM per poke (exit 0 = found);
  `poke_interval` = INTERVAL, `timeout` = TIMEFROM–TIMETO window else MAXWAIT.
  Config fallback `cyclic_observer_style: sensor|retries` (retries variant documented
  with its failure-semantics caveat).
- `docs/advisor.md` — the advisor protocol: prompt spec for running an agent (Sonnet)
  over decision_points + ir.json to produce decisions.yaml proposals; includes the
  review workflow (human flips `approved`, re-run pipeline).
- Emit application for `translate`: trigger_dag → TriggerDagRunOperator task;
  variable_set → @task calling Variable.set; callback → notification-only task or
  absorbed into neighbor's callback; asset_event → Asset outlet on predecessor or
  EmptyOperator with outlet. `eliminate` → job dropped from DAG, edges contracted
  through it (same contraction rule as the dashboard's folder-gate hiding),
  tombstone diagnostic `ELIMINATED {reason}`.

## Out of scope for v7 (explicit)

- Provider-mappable registry expansion (Class 2 of the coverage matrix) — lazy,
  after real-inventory numbers exist.
- Dashboard decision-panel (optional v7.1: show decision points + applied decisions).
- Auto-drafting DAG code for stay-manual jobs (possible v8: agent drafts into
  `manual_drafts/`, never into dags/).

## Settled decisions (user, 2026-07-09)

1. `apply_unapproved_ai` = **true**: AI proposals apply immediately, with the
   `AI_DECISION_APPLIED` diagnostic trail; humans veto by editing decisions.yaml.
2. `eliminate` = the job **vanishes entirely** from the generated DAG (edges
   contracted through it); the tombstone lives only in partition.json/diagnostics.
3. **No free-form merges** — merge-as-consequence only (see the domain table
   note). AI advisor is restricted to reclassification domains.
4. Samples: the user cannot supply real exports; add a **complex synthetic**
   sample exercising observer-cyclics, utility jobs, and orphans — and
   additionally hunt public GitHub/community sources for a genuinely complex
   real-world DEFTABLE export to include (license permitting) for parser
   hardening.

## Verification plan (when approved)

Golden tests for classifier tiers; decision application tests per domain
(observer un-extraction, eliminate contraction, translate emissions, merge/rename);
decision_points determinism; end-to-end: run 1 → advisor fixture decisions.yaml →
run 2 asserts applied outcomes + diagnostics; full-suite green; dashboards build.
