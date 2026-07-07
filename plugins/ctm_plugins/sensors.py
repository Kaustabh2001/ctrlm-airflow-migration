"""Control-M CONFIRM / FILEWATCH -> Airflow sensors (contract V3-2).

AIRFLOW IMPORTS AT MODULE LEVEL — on the Windows dev box (no Airflow) this
file is only syntax-checked with py_compile; it is imported for real on MWAA
via plugins.zip. Do NOT import this module from tool code or tests.

- CtmApprovalGateSensor: maps Control-M CONFIRM=1. The run waits until an
  operator sets Airflow Variable ``ctm_approve/<dag_id>/<task_id>/<ds>`` to
  "yes" (UI: Admin -> Variables, or ``airflow variables set``).
- CtmFileWatcherSensor: maps FILEWATCH jobs. Scheme-dispatched path check:
  local path / ``s3://bucket/key`` via S3Hook / ``sftp://host/path`` via
  SFTPHook.
"""
from __future__ import annotations

import os
from typing import Any

from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator

# One Control-M day by default: an unanswered CONFIRM should not hang forever.
DEFAULT_CONFIRM_TIMEOUT = 24 * 3600


class CtmApprovalGateSensor(BaseSensorOperator):
    """Manual-approval gate replacing Control-M CONFIRM.

    Pokes (mode="reschedule", every 60s by default — deferrable-safe: no slot
    is held between pokes) until Variable ``ctm_approve/<dag_id>/<task_id>/<ds>``
    equals "yes" (case-insensitive).
    """

    ui_color = "#f4a261"

    def __init__(
        self,
        *,
        poke_interval: float = 60,
        mode: str = "reschedule",
        timeout: float = DEFAULT_CONFIRM_TIMEOUT,
        **kwargs: Any,
    ) -> None:
        super().__init__(poke_interval=poke_interval, mode=mode, timeout=timeout, **kwargs)

    def approval_key(self, context: dict) -> str:
        dag_id = context["dag"].dag_id
        return f"ctm_approve/{dag_id}/{self.task_id}/{context['ds']}"

    def poke(self, context: dict) -> bool:
        key = self.approval_key(context)
        value = Variable.get(key, default_var="")
        approved = str(value).strip().lower() == "yes"
        if not approved:
            self.log.info(
                "waiting for CONFIRM: set Airflow Variable %r to 'yes' to release", key
            )
        return approved


class CtmFileWatcherSensor(BaseSensorOperator):
    """File-arrival gate replacing Control-M FILEWATCH jobs.

    ``path`` schemes:
    - ``s3://bucket/key``   -> S3Hook.check_for_key (conn_id or "aws_default")
    - ``sftp://host/path``  -> SFTPHook.path_exists (conn_id or "sftp_<host>")
    - anything else         -> os.path.exists on the worker's filesystem
    """

    template_fields = ("path",)
    ui_color = "#2a9d8f"

    def __init__(
        self,
        *,
        path: str,
        conn_id: str | None = None,
        poke_interval: float = 60,
        mode: str = "reschedule",
        **kwargs: Any,
    ) -> None:
        super().__init__(poke_interval=poke_interval, mode=mode, **kwargs)
        self.path = path
        self.conn_id = conn_id

    def poke(self, context: dict) -> bool:
        path = self.path
        if path.startswith("s3://"):
            # LATE import: amazon provider is only needed for s3:// watches.
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook(aws_conn_id=self.conn_id or "aws_default")
            bucket, key = hook.parse_s3_url(path)
            self.log.info("poking s3 bucket=%s key=%s", bucket, key)
            return hook.check_for_key(key, bucket_name=bucket)
        if path.startswith("sftp://"):
            # LATE import: sftp provider only needed for sftp:// watches.
            from airflow.providers.sftp.hooks.sftp import SFTPHook

            rest = path[len("sftp://"):]
            host, _, remote = rest.partition("/")
            hook = SFTPHook(ssh_conn_id=self.conn_id or f"sftp_{host}")
            self.log.info("poking sftp host=%s path=/%s", host, remote)
            return hook.path_exists("/" + remote)
        self.log.info("poking local path %s", path)
        return os.path.exists(path)
