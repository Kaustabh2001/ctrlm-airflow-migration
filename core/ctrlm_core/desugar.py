"""Phase -1: folder-level desugaring (docs/partition-algorithm.md, Phase -1;
nested-folder cascade per docs/impl-contracts-v2.md §V2-2).

Rewrites folder-level conditions, schedules and variables into job-level IR
BEFORE any graph work, by adding synthetic ``__FOLDER_START__`` /
``__FOLDER_END__`` Dummy jobs plus ``__``-prefixed synthetic conditions.
Without this pass, a job with no conditions of its own inside a gated folder
would masquerade as a ROOT.

Entry/terminal detection is strictly INTRA-folder (direct jobs of one
FolderDef only) and only considers real same-run dataflow: PREV-qualified
in-conditions never count as upstream and DEL-signed out-conditions never
count as producing/feeding downstream.

Nested folders (v2) cascade recursively, deepest-first:
- every folder with folder-level conds (or ``folder_start_always`` on smart
  folders) gets its own start/end pair exactly as v1, named under its full
  slash path;
- a child folder WITH a start node whose nearest ancestor also has one: the
  child's start gains in_cond ``__start__{ancestor}``;
- a child folder WITHOUT a start node under such an ancestor: its ENTRY jobs
  gain the ancestor's ``__start__`` in_cond instead;
- end nodes mirror this: the nearest ancestor end waits on the child's end
  (``__done__{child}/__FOLDER_END__``) or, when the child has no end node,
  directly on the child's TERMINAL jobs;
- variables cascade ancestor -> child -> job, nearest wins.
"""
from __future__ import annotations

from .model import Condition, Deftable, FolderDef, Job, PartitionConfig

START_JOB_NAME = "__FOLDER_START__"
END_JOB_NAME = "__FOLDER_END__"


def start_cond_name(folder_name: str) -> str:
    return f"__start__{folder_name}"


def done_cond_name(job_uid: str) -> str:
    return f"__done__{job_uid}"


def _entry_jobs(folder: FolderDef) -> list[Job]:
    """Real jobs with no intra-folder upstream: no in-cond (PREV excluded)
    whose name is produced (sign=ADD) by a job of the same folder."""
    real = [j for j in folder.jobs if not j.synthetic]
    produced = {c.name for j in real for c in j.out_conds if c.sign == "ADD"}
    return [
        j for j in real
        if not any(c.name in produced and c.odate != "PREV" for c in j.in_conds)
    ]


def _terminal_jobs(folder: FolderDef) -> list[Job]:
    """Real jobs with no intra-folder downstream: no ADD out-cond consumed
    (PREV excluded) by a job of the same folder."""
    real = [j for j in folder.jobs if not j.synthetic]
    consumed = {c.name for j in real for c in j.in_conds if c.odate != "PREV"}
    return [
        j for j in real
        if not any(c.name in consumed for c in j.out_conds if c.sign == "ADD")
    ]


# ------------------------------------------------------------------ helpers

def _find_synthetic(folder: FolderDef, name: str) -> Job | None:
    for job in folder.jobs:
        if job.synthetic and job.name == name:
            return job
    return None


def _ancestors(folder: FolderDef, by_name: dict[str, FolderDef]) -> list[FolderDef]:
    """Ancestor chain, nearest first. Missing parents end the walk."""
    chain: list[FolderDef] = []
    parent = folder.parent
    while parent:
        anc = by_name.get(parent)
        if anc is None:
            break
        chain.append(anc)
        parent = anc.parent
    return chain


def _nearest_with_synthetic(
    folder: FolderDef, by_name: dict[str, FolderDef], synth_name: str
) -> tuple[FolderDef, Job] | None:
    """(ancestor, its synthetic node) for the nearest ancestor that has one."""
    for anc in _ancestors(folder, by_name):
        node = _find_synthetic(anc, synth_name)
        if node is not None:
            return anc, node
    return None


def _ensure_in_cond(job: Job, name: str) -> None:
    if not any(c.name == name for c in job.in_conds):
        job.in_conds.append(Condition(name=name))


def _ensure_out_cond(job: Job, name: str) -> None:
    if not any(c.name == name for c in job.out_conds):
        job.out_conds.append(Condition(name=name, sign="ADD"))


# ------------------------------------------------------------------ variables

def _merge_folder_variables(folder: FolderDef, by_name: dict[str, FolderDef]) -> None:
    """Ancestor -> child -> job variable cascade; the nearest value wins."""
    merged_folder: dict[str, str] = {}
    for anc in reversed([folder] + _ancestors(folder, by_name)):  # root first
        merged_folder.update(anc.variables)
    if not merged_folder:
        return
    for job in folder.jobs:
        if job.synthetic:
            continue
        merged = dict(merged_folder)
        merged.update(job.variables)
        job.variables = merged


# ------------------------------------------------------------------ synthetics

def _desugar_folder(folder: FolderDef) -> None:
    # entries/terminals computed on the ORIGINAL job list, before synthetics
    entries = sorted(_entry_jobs(folder), key=lambda j: j.name)
    terminals = sorted(_terminal_jobs(folder), key=lambda j: j.name)

    start = Job(
        name=START_JOB_NAME,
        folder=folder.name,
        task_type="Dummy",
        synthetic=True,
        # start node inherits the folder-level scheduling attributes
        weekdays=folder.weekdays,
        monthdays=folder.monthdays,
        months=folder.months,
        days_and_or=folder.days_and_or,
        timezone=folder.timezone,
        in_conds=[c.model_copy(deep=True) for c in folder.in_conds],
        out_conds=[Condition(name=start_cond_name(folder.name))],
    )
    end = Job(
        name=END_JOB_NAME,
        folder=folder.name,
        task_type="Dummy",
        synthetic=True,
        in_conds=[Condition(name=done_cond_name(t.uid)) for t in terminals],
        out_conds=[c.model_copy(deep=True) for c in folder.out_conds],
    )

    for job in entries:
        job.in_conds.append(Condition(name=start_cond_name(folder.name)))
    for job in terminals:
        job.out_conds.append(Condition(name=done_cond_name(job.uid), sign="ADD"))

    folder.jobs.append(start)
    folder.jobs.append(end)


# ------------------------------------------------------------------ cascade

def _cascade_start(folder: FolderDef, by_name: dict[str, FolderDef]) -> None:
    hit = _nearest_with_synthetic(folder, by_name, START_JOB_NAME)
    if hit is None:
        return
    ancestor, _ = hit
    cond = start_cond_name(ancestor.name)
    own_start = _find_synthetic(folder, START_JOB_NAME)
    if own_start is not None:
        _ensure_in_cond(own_start, cond)
    else:
        for job in sorted(_entry_jobs(folder), key=lambda j: j.name):
            _ensure_in_cond(job, cond)


def _cascade_end(folder: FolderDef, by_name: dict[str, FolderDef]) -> None:
    hit = _nearest_with_synthetic(folder, by_name, END_JOB_NAME)
    if hit is None:
        return
    _, ancestor_end = hit
    own_end = _find_synthetic(folder, END_JOB_NAME)
    if own_end is not None:
        done = done_cond_name(own_end.uid)
        _ensure_out_cond(own_end, done)
        _ensure_in_cond(ancestor_end, done)
    else:
        for job in sorted(_terminal_jobs(folder), key=lambda j: j.name):
            done = done_cond_name(job.uid)
            _ensure_out_cond(job, done)
            _ensure_in_cond(ancestor_end, done)


# ------------------------------------------------------------------ entry point

def desugar(deftable: Deftable, config: PartitionConfig) -> None:
    """Mutate the Deftable in place; deterministic (sorted folder order),
    idempotent (guarded appends + synthetic guard)."""
    by_name = {f.name: f for f in deftable.folders}

    # pass 1: variables cascade + per-folder synthetics (v1 semantics per folder)
    for folder in sorted(deftable.folders, key=lambda f: f.name):
        _merge_folder_variables(folder, by_name)
        gated = bool(folder.in_conds or folder.out_conds)
        forced = folder.smart and config.folder_start_always
        if not (gated or forced):
            continue
        if any(j.synthetic for j in folder.jobs):
            continue  # already desugared (idempotence guard)
        _desugar_folder(folder)

    # pass 2: recursive start/end cascade across the ancestor chain, deepest-first
    for folder in sorted(deftable.folders, key=lambda f: (-f.name.count("/"), f.name)):
        if not folder.parent:
            continue
        _cascade_start(folder, by_name)
        _cascade_end(folder, by_name)
