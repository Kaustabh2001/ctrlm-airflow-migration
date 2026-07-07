# Partition algorithm — exact specification

The implementable form of DESIGN.md §5. Pseudocode is Python-ish; every iteration
order is canonical (sorted) so the output is a pure function of
`(export, config, cluster-map.yaml)`.

```python
# ============================================================
# INPUT
#   jobs : set of Job
#     Job = (name, folder,
#            day_pattern,     # normalized "which days is it ordered":
#                             #   cron day-fields | calendar-spec | None (condition-driven only)
#            timefrom,        # optional time-of-day start gate
#            cyclic: bool, maxwait,
#            in_conds : [(cond, qualifier, and_or)],
#            out_conds: [(cond, qualifier, sign)])
#   config      : N = 10 (hub fan threshold), H = 3 (hub folder spread),
#                 MAX_TASKS = 150, new_day_time, site_default_timefrom
#   cluster_map : prior pins + manual cuts/merges (may be empty)
#
# OUTPUT
#   dag_of      : job -> dag_id            (a partition: every job in exactly one DAG)
#   E           : directed edges realized as `>>` inside a DAG
#   W           : wiring set — entries (p, s, conds, kind) realized cross-DAG
#   schedule_of : dag_id -> cron | calendar-timetable | dataset-trigger
#   diagnostics : conflicts, oversize warnings, flagged constructs
# ============================================================

# ---------- Phase -1: folder-level desugaring ----------
# SMART folders / sub-folders carry their own in/out conditions, schedules,
# variables, ON/DO actions. Cascade them into job-level IR BEFORE any graph
# work, recursing through sub-folders (job <- sub-folder <- folder).
# Pure IR rewrite: only adds synthetic jobs and conditions, so Phase 0's
# generic matching builds the edges with no special cases.
for f in sorted(folders) if f has folder-level conditions:
    start_f = synthetic_job(folder=f, day_pattern=f.day_pattern,
                            in_conds=f.in_conds,
                            out_conds=[("__start__" + f, ODAT, ADD)])
    end_f   = synthetic_job(folder=f, out_conds=f.out_conds,
                            in_conds=[("__done__" + j, ODAT) for j in terminals(f)])
    for j in entry_jobs(f):     j.in_conds  += [("__start__" + f, ODAT)]
    for j in terminals(f):      j.out_conds += [("__done__" + j, ODAT, ADD)]
# entry_jobs(f)  = jobs of f with no intra-folder upstream
# terminals(f)   = jobs of f with no intra-folder downstream
# Synthetic nodes are ordinary nodes everywhere below; they emit as the
# TaskGroup's boundary EmptyOperators. Without them, a job with no own
# conditions inside a gated folder would masquerade as a ROOT.
# NOTE: "__"-prefixed synthetic conditions are EXEMPT from Phase 2 hub cuts
# (intra-folder by construction; a 20-entry folder must not lose its start).
# Folder schedules/RBCs resolve into each job's day_pattern; folder variables
# and ON/DO cascade nearest-ancestor-wins.
# Config: folder_start_always=True adds a start node to EVERY smart folder,
# pulling each folder's jobs into one cluster (more folder-shaped boundaries).

# ---------- Phase 0: build the condition graph ----------
producers = multimap()          # cond -> jobs having out_cond(cond, sign=ADD)
consumers = multimap()          # cond -> jobs having in_cond(cond)

E, W = [], []
for cond in sorted(all_condition_names):
    # Implementation note: hub statistics (Phase 2) are computed BEFORE
    # materializing pairs, so a 1000-consumer hub never expands to
    # fan_in x fan_out edges in memory.
    for p in producers[cond]:
        for s in consumers[cond]:
            if p == s:
                flag(p, "self-condition")               # dropped, reported
            elif qualifier(s, cond) == PREV:
                W.add((p, s, cond, PREV_RUN))           # cross-RUN, never clusters
            elif qualifiers are ODAT <-> ODAT:
                E.add((p, s, cond))                     # the only clustering edges
            else:                                       # STAT / literal dates
                W.add((p, s, cond, REVIEW))             # wired + PARTIAL flag
# out_conds with sign=DEL never create edges; they are collected for
# mutual-exclusion pattern detection (-> 1-slot pools) or MANUAL review.
# in_conds with no producer anywhere  -> orphan list (external trigger decision).
# out_conds with no consumer anywhere -> dead-end list (report only).

# ---------- Phase 1: extract cyclic jobs ----------
for j in sorted(jobs) if j.cyclic:
    dag_of[j] = fresh_dag(j)                # own DAG, own run cadence
    move all E-edges touching j into W (kind=CYCLIC)

# ---------- Phase 2: cut hub conditions ----------
for cond in sorted(all_condition_names):
    fan_in  = len(producers[cond]); fan_out = len(consumers[cond])
    spread  = len({j.folder for j in producers[cond] + consumers[cond]})
    if fan_in >= N or fan_out >= N or spread >= H:
        move all E-edges labeled cond into W (kind=HUB)

# ---------- Phase 3: cut direct day-pattern conflicts ----------
for (p, s, cond) in E:
    if p.day_pattern and s.day_pattern and p.day_pattern != s.day_pattern:
        move edge into W (kind=PATTERN)
# timefrom plays NO role here: same days + different hours stays ONE dag
# (the later job gets an in-DAG time gate in Phase 8).

# ---------- Phase 4: manual cuts ----------
move E-edges matching cluster_map.cuts into W (kind=MANUAL)
# cuts are addressed by condition name or by (producer, consumer) pair

# ---------- Phase 5: connected components ----------
uf = UnionFind(jobs not yet assigned)       # cyclic jobs already excluded
for (p, s, _) in E:
    uf.union(p, s)                          # UNDIRECTED: direction is ignored
clusters = uf.groups()                      # for grouping, never for wiring

# Singleton coalescing: a size-1 component has no surviving edges.
merge all singletons sharing (folder, day_pattern) into one cluster each.

# Manual merges from cluster_map: union the named clusters,
# REFUSED with an error if their day-patterns differ.

# ---------- Phase 6: transitive pattern conflicts ----------
# Phase 3 cannot see conflicts hidden behind unscheduled middles:
#   daily A -> (None) B -> weekly C   still lands in one component.
queue = clusters
final = []
while queue:
    C = queue.pop()
    pats = {j.day_pattern for j in C if j.day_pattern}
    if len(pats) <= 1:
        final.append(C); continue
    G_minor = jobs of the rarest pattern in C     # tie -> lexicographically first pattern
    G_rest  = jobs of every other pattern in C
    cut = MIN_EDGE_CUT(C, source=G_minor, sink=G_rest)
    #   max-flow / min-cut with the pattern groups as terminal sets.
    #   Unscheduled jobs are interior nodes: the cut decides which side
    #   keeps them. Minimizing severed edges minimizes later sensors.
    #   Determinism: edges fed to max-flow in canonical (p, s, cond) order.
    move cut edges into W (kind=AUTO_RESOLVED)    # + recorded in cluster_map
    queue.extend(resplit(C))                      # parts re-checked until pure

clusters = final

# ---------- Phase 7: size guardrail (report-only) ----------
for C in clusters if len(C) > MAX_TASKS:
    warn(C, suggestions = top_k_edge_betweenness(C))
    # highest-betweenness edges are the natural bottlenecks — offered as
    # manual cut candidates; the cluster is still generated as-is.

# ---------- Phase 8: schedule, anchor, name, pin ----------
rel = lambda t: (t - new_day_time) % 24h    # times compared on the ODATE clock,
                                            # so 02:00 sorts AFTER 22:00
for C in clusters:
    pattern = the unique day_pattern of C, or None
    roots   = {j in C with no incoming E-edge from inside C}
    if pattern is None:
        schedule_of[C] = dataset_trigger(C's inbound W edges)   # see Phase 9
    else:
        anchored = scheduled roots, else all scheduled jobs of C
        anchor   = min(rel(j.timefrom or site_default_timefrom) for j in anchored)
        schedule_of[C] = cron_or_calendar_timetable(pattern, at=anchor)
        for j in C if rel(j.timefrom) > anchor:
            attach time_gate(j)             # deferrable DateTimeSensor:
                                            # ODATE + timefrom, New-Day aware
    dag_of[C] = pinned name from cluster_map if present
                else snake_case(modal folder of C)
                     # tie -> lexicographic min folder
                     # collision -> numeric suffix, assigned in canonical
                     #              cluster order (sorted by min job name)

write cluster_map.yaml:
    every job -> dag_id, every cut (MANUAL and AUTO_RESOLVED), names.
    On re-runs: existing assignments are honored verbatim; new jobs are
    assigned by the algorithm and appended; manual edits always win.

# ---------- Phase 9: realize the wiring set ----------
for (p, s, conds, kind) in dedupe(W, key=(p, s, kind)):   # cond names merged,
    if kind == PREV_RUN:                                  # listed in comments
        gate s with a sensor on p's PREVIOUS logical run   # valid intra-DAG too
    elif dag_of[s] is time-scheduled:
        gate s with ExternalTaskSensor(dag_of[p], task=p,
             execution_date_fn=same_ODATE, timeout=s.maxwait, deferrable=True)
    else:
        p.outlets += Dataset("ctrlm://cond/" + cond)      # one Dataset per
        schedule_of[dag_of[s]] = ALL inbound Datasets     # condition, shared
        # AND semantics by default (matches Control-M); OR-groups need
        # conditional dataset expressions (Airflow >= 2.9), else fall back
        # to sensor gates.
```

## Invariants

- **I1 — Partition.** Every non-flagged job maps to exactly one `dag_id`; no job is duplicated or silently dropped.
- **I2 — Edge conservation.** Every matched producer→consumer pair is realized exactly once: as an intra-DAG `>>` edge or as one wiring-set entry per `(p, s, kind)`. Negatives, self-conditions, orphans, and dead-ends are explicitly flagged, never silently discarded.
- **I3 — Cadence purity.** After Phase 6, all day-scheduled jobs within a cluster share one day-pattern (guaranteed by the worklist loop, regardless of how conflicts nest).
- **I4 — Determinism.** Output is a pure function of `(export, config, cluster-map.yaml)`: all iterations are sorted, min-cut input is canonically ordered, name collisions resolve in canonical cluster order.
- **I5 — Pin stability.** A re-run never reassigns a job that `cluster-map.yaml` already places; human edits always win over the algorithm.

## Complexity

Condition matching is a hash join over condition names (hub detection runs on counts before pair expansion, so broadcast conditions never materialize `fan_in × fan_out` edges). Components are union-find (near-linear). The only two nontrivial graph computations are confined: **min-cut** runs only on pattern-conflicted clusters (small, and terminals shrink the problem), and **edge betweenness** runs only on clusters exceeding `MAX_TASKS` — both off the hot path.
