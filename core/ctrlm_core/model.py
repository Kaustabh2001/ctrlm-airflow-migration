"""Typed IR and partition models — the CONTRACT between all modules.

Field names are load-bearing: the parser fills raw fields, schedule.normalize_jobs
fills day_pattern, desugar adds synthetic jobs, strategies fill PartitionResult.
Do not change anything here without updating docs/impl-contracts.md and every consumer.
"""
from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------- conditions

class Condition(BaseModel):
    name: str
    odate: str = "ODAT"        # ODAT | PREV | STAT | explicit date string
    and_or: str = "AND"        # meaningful on in-conditions: AND | OR
    sign: str = "ADD"          # meaningful on out-conditions: ADD | DEL


class OnAction(BaseModel):
    stmt: str = "*"
    code: str = "NOTOK"
    actions: list[dict] = Field(default_factory=list)   # {"type": "DOMAIL", ...attrs}


class Resource(BaseModel):
    name: str
    kind: str                   # "quantitative" | "control"
    quant: int = 1              # units consumed (quantitative)
    control_type: str = "E"     # E(xclusive) | S(hared)


# ---------------------------------------------------------------- jobs / folders

class Job(BaseModel):
    name: str
    folder: str
    application: str = ""
    sub_application: str = ""
    description: str = ""
    task_type: str = "Command"  # Command | Job | Dummy | FileWatch
    command: str = ""           # CMDLINE, or MEMLIB/MEMNAME joined for script jobs
    node_id: str = ""           # Control-M agent host (NODEID)
    run_as: str = ""
    # raw scheduling attributes, exactly as parsed (normalized later):
    weekdays: str = ""          # "1,2,3,4,5" | "ALL" | ""     (1=Mon .. 7=Sun)
    monthdays: str = ""         # "1,15" | "ALL" | ""          (attr DAYS)
    months: str = ""            # "1,2,12" | "ALL" | ""
    days_and_or: str = "OR"
    timefrom: str = ""          # "HHMM" | ""
    timeto: str = ""
    timezone: str = ""
    cyclic: bool = False
    interval_minutes: int = 0
    maxwait: int = 0            # days
    maxrerun: int = 0
    rerun_interval_minutes: int = 0
    confirm: bool = False
    appl_type: str = ""         # APPL_TYPE: OS | DATABASE | FILE_TRANS | SAP | ... ("" = plain OS)
    priority: str = ""          # Control-M priority code (e.g. "AA".."ZZ" or numeric)
    critical: bool = False
    in_conds: list[Condition] = Field(default_factory=list)
    out_conds: list[Condition] = Field(default_factory=list)
    variables: dict[str, str] = Field(default_factory=dict)
    resources: list[Resource] = Field(default_factory=list)
    on_actions: list[OnAction] = Field(default_factory=list)
    shouts: list[dict] = Field(default_factory=list)     # {"when","dest","message"}
    synthetic: bool = False     # True for folder-start / folder-end nodes
    # derived by ctrlm_core.schedule.normalize_jobs():
    day_pattern: str | None = None   # canonical day pattern; None = condition-driven only

    @property
    def uid(self) -> str:
        """Globally unique node id (job names may repeat across folders)."""
        return f"{self.folder}/{self.name}"


class FolderDef(BaseModel):
    name: str                   # full path for nested folders: "PARENT/CHILD/GRANDCHILD"
    datacenter: str = ""
    smart: bool = False
    parent: str = ""            # full path of the parent folder ("" = top level)
    # folder-level scheduling criteria (cascade to jobs in normalize)
    weekdays: str = ""
    monthdays: str = ""
    months: str = ""
    days_and_or: str = "OR"
    timezone: str = ""
    in_conds: list[Condition] = Field(default_factory=list)   # folder-level conditions
    out_conds: list[Condition] = Field(default_factory=list)
    variables: dict[str, str] = Field(default_factory=dict)
    jobs: list[Job] = Field(default_factory=list)


class Deftable(BaseModel):
    folders: list[FolderDef] = Field(default_factory=list)
    source_files: list[str] = Field(default_factory=list)

    def all_jobs(self):
        for f in self.folders:
            yield from f.jobs

    def job_index(self) -> dict[str, Job]:
        return {j.uid: j for j in self.all_jobs()}


# ---------------------------------------------------------------- graph

EDGE_INTRA = "E"                # candidate intra-DAG edge (ODAT <-> ODAT)
# wiring-set kinds (why the edge is cross-DAG):
EDGE_PREV_RUN = "PREV_RUN"      # consumer qualifier PREV — previous-run gate
EDGE_REVIEW = "REVIEW"          # STAT / literal-date qualifiers — flagged
EDGE_CYCLIC = "CYCLIC"          # endpoint is a cyclic job (own DAG)
EDGE_HUB = "HUB"                # broadcast condition cut
EDGE_PATTERN = "PATTERN"        # direct day-pattern conflict cut
EDGE_MANUAL = "MANUAL"          # cluster-map manual cut
EDGE_AUTO = "AUTO_RESOLVED"     # transitive pattern conflict min-cut (components)
EDGE_ANCHOR = "ANCHOR"          # root anchor-time spread min-cut (components)
EDGE_OWNER = "OWNER_SPLIT"      # ownership convergence split (single-entry)

WIRE_SENSOR = "sensor"
WIRE_DATASET = "dataset"
WIRE_PREV = "prev_run_sensor"


class GraphEdge(BaseModel):
    source: str                 # Job.uid of producer
    target: str                 # Job.uid of consumer
    cond: str
    kind: str = EDGE_INTRA


class CtmGraph(BaseModel):
    """Post-desugar, post-normalize condition graph. graph.json serializes this."""
    nodes: dict[str, Job] = Field(default_factory=dict)       # uid -> Job
    e_edges: list[GraphEdge] = Field(default_factory=list)    # candidate intra-DAG
    w_edges: list[GraphEdge] = Field(default_factory=list)    # wiring set (kind != E)
    orphan_conds: list[dict] = Field(default_factory=list)    # {"cond": str, "consumers": [uid]}
    dead_end_conds: list[dict] = Field(default_factory=list)  # {"cond": str, "producers": [uid]}
    flags: list[dict] = Field(default_factory=list)           # {"level","code","message","subject"}


# ---------------------------------------------------------------- partition

class DagSpec(BaseModel):
    dag_id: str
    jobs: list[str] = Field(default_factory=list)    # member uids, sorted
    roots: list[str] = Field(default_factory=list)   # uids with no intra-DAG upstream
    folders: list[str] = Field(default_factory=list) # distinct member folders, sorted
    day_pattern: str | None = None
    anchor: str = ""             # "HHMM" — earliest root time (ODATE clock)
    schedule: str | None = None  # cron string; None when dataset-triggered or undefined
    dataset_triggered: bool = False
    datasets: list[str] = Field(default_factory=list)  # inbound dataset URIs


class CrossLink(BaseModel):
    source: str                  # producer uid
    target: str                  # consumer uid
    conds: list[str] = Field(default_factory=list)
    kind: str = ""               # the GraphEdge kind that caused it
    mechanism: str = ""          # WIRE_SENSOR | WIRE_DATASET | WIRE_PREV


class Diagnostic(BaseModel):
    level: str = "info"          # info | warn | error
    code: str = ""
    message: str = ""
    subject: str = ""            # uid / dag_id / condition name


class PartitionResult(BaseModel):
    strategy: str                # "components" | "single_entry"
    dags: list[DagSpec] = Field(default_factory=list)
    assignments: dict[str, str] = Field(default_factory=dict)   # uid -> dag_id
    cross_links: list[CrossLink] = Field(default_factory=list)
    diagnostics: list[Diagnostic] = Field(default_factory=list)
    stats: dict = Field(default_factory=dict)   # see ctrlm_core.stats.compute_stats


class PartitionConfig(BaseModel):
    hub_fan: int = 10               # fan-in/out >= N -> hub cut
    hub_spread: int = 3             # distinct folders >= H -> hub cut
    max_tasks: int = 150            # size guardrail (warn only)
    new_day_time: str = "0600"      # Control-M New Day (ODATE clock zero)
    default_timefrom: str = "0600"  # anchor for scheduled jobs with no TIMEFROM
    anchor_spread_hours: float = 6.0    # components: root anchor spread split threshold
    folder_start_always: bool = False   # desugar: start node for every smart folder
    coalesce_singletons: bool = True    # components: bundle edge-less jobs per (folder, pattern)
