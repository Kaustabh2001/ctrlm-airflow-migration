"""Targeted Control-M operators — ONLY where they add real capability
(contract V4-1, REVISED).

AIRFLOW IMPORTS AT MODULE LEVEL — on the Windows dev box (no Airflow) this
file is only syntax-checked with py_compile; it is imported for real on MWAA
via plugins.zip. Do NOT import this module from tool code or tests.

USER POLICY (revised V4): wrapping SSHOperator/WinRMOperator/EmptyOperator in
rename-classes is explicitly rejected. Command/Job tasks stay plain
SSHOperator/WinRMOperator and Dummy stays EmptyOperator, with common params
(priority_weight, pool, callbacks, email, sla, retries) translated at CODEGEN
time exactly as v3 does. This module therefore ships exactly TWO classes:

- :class:`CtmDatabaseJob`  APPL_TYPE=DATABASE — earns its existence through
  parse-time node -> conn_id resolution (nodes.yaml shipped in plugins.zip)
  and as the single place for future DB-specific behavior (stored-proc
  handling, output capture) to land once, not per-DAG.
- :class:`CtmManualJob`    jobs with NO automatic mapping — a loud stub that
  replaces the emitted PythonOperator+prelude pattern; running it raises
  NotImplementedError naming the original Control-M job.

Graph-structural semantics remain separate visible tasks — time gates
(``gate_*``), approvals (``confirm_*`` CtmApprovalGateSensor), force-job
(``force_*``), cross-DAG waits (``wait_*``), folder start/end EmptyOperators.
"""
from __future__ import annotations

from typing import Any

# Airflow 3 / 2.x dual-compat: BaseOperator comes via the _compat shim
# (airflow.sdk on 3.x, airflow.models.baseoperator on 2.x). See _compat.py.
from ._compat import BaseOperator

# Provider path verified UNCHANGED in Airflow 3 (common.sql provider).
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from ._params import resolve_node


class CtmDatabaseJob(SQLExecuteQueryOperator):
    """Control-M APPL_TYPE=DATABASE -> SQLExecuteQueryOperator
    (provider: apache-airflow-providers-common-sql).

    ``node`` is resolved to ``conn_id`` at PARSE TIME via
    ``ctm_plugins._params.resolve_node`` (mapping-config/nodes.yaml, shipped
    in plugins.zip; the node entry should carry ``type: db``; unmapped nodes
    fall back to ``ssh_<node>``). An explicit ``conn_id`` kwarg overrides the
    lookup entirely. ``sql`` and all standard BaseOperator /
    SQLExecuteQueryOperator kwargs pass straight through.
    """

    def __init__(
        self,
        *,
        node: str = "",
        conn_id: str | None = None,
        sql: Any = "",
        **kwargs: Any,
    ) -> None:
        if conn_id is None:
            conn_id = resolve_node(node)["conn_id"]
        super().__init__(conn_id=conn_id, sql=sql, **kwargs)
        # keep the raw Control-M node for introspection; log once at parse time
        self.ctm_node = node
        self.log.info(
            "CtmDatabaseJob %s: node %r -> conn_id %r (parse time)",
            self.task_id, node, conn_id,
        )


class CtmManualJob(BaseOperator):
    """Placeholder for Control-M jobs with NO automatic Airflow mapping
    (FILE_TRANS/AFT/MFT, SAP, INFORMATICA, ... and the catch-all row).

    Replaces the emitted ``PythonOperator`` + ``_ctm_manual_stub`` prelude:
    identical loud-failure behavior, cleaner generated code. Running it
    raises NotImplementedError naming the original job so the failure is
    actionable and impossible to mistake for success.
    """

    ui_color = "#e76f51"  # loud: a human must migrate this job

    def __init__(
        self,
        *,
        ctm_task_type: str = "",
        ctm_appl_type: str = "",
        ctm_job: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ctm_task_type = ctm_task_type
        self.ctm_appl_type = ctm_appl_type
        self.ctm_job = ctm_job

    def execute(self, context: Any) -> None:
        raise NotImplementedError(
            f"MANUAL migration required: Control-M job {self.ctm_job!r} "
            f"(TASKTYPE={self.ctm_task_type or '-'}/"
            f"APPL_TYPE={self.ctm_appl_type or '-'}) has no automatic "
            "Airflow mapping."
        )


__all__ = ["CtmDatabaseJob", "CtmManualJob"]
