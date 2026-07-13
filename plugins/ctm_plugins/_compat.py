"""Airflow 3 / Airflow 2.x import-compatibility shim (contract V6-2).

AIRFLOW IMPORTS AT MODULE LEVEL — like operators.py / sensors.py this file is
only syntax-checked with py_compile on the Windows dev box (Airflow cannot be
installed here); it is imported for real on MWAA via plugins.zip. Do NOT
import this module from tool code or tests.

Canonical Airflow 3 paths (verified against airflow.apache.org — "Public
Interface for Airflow 3.0+" and the Task SDK API reference, AIP-72):
``BaseOperator``, ``BaseSensorOperator`` and ``Variable`` are exported from
``airflow.sdk``. Each block below tries the 3.x path first and falls back to
the 2.x path on ImportError, so ONE plugins.zip deploys unchanged on either
major version (target: Airflow 3 on MWAA; 2.x kept working as fallback).

Verified UNCHANGED in Airflow 3 (no shim needed; recorded here so the next
reader does not have to re-verify):

- ``airflow.plugins_manager.AirflowPlugin``            (ctm_plugin.py)
- ``airflow.timetables.base.Timetable`` + DagRunInfo/DataInterval/
  TimeRestriction                                      (timetables.py)
- ``airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator``
                                                       (operators.py)
- ``airflow.providers.amazon.aws.hooks.s3.S3Hook``     (CtmFileWatcherSensor)
- ``airflow.providers.sftp.hooks.sftp.SFTPHook``       (CtmFileWatcherSensor)

NOTE on ``Variable.get``: Airflow 3 renamed the default-value keyword
(2.x ``default_var=`` -> 3.x ``default=``). Callers in this package must pass
the default POSITIONALLY — it is the second positional parameter in BOTH
signatures — to stay dual-compatible.
"""
from __future__ import annotations

try:  # Airflow 3 canonical path (Task SDK, AIP-72)
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2.x fallback
    from airflow.models.baseoperator import BaseOperator

try:  # Airflow 3 canonical path (Task SDK, AIP-72)
    from airflow.sdk import BaseSensorOperator
except ImportError:  # Airflow 2.x fallback
    from airflow.sensors.base import BaseSensorOperator

try:  # Airflow 3 canonical path (Task SDK, AIP-72)
    from airflow.sdk import Variable
except ImportError:  # Airflow 2.x fallback
    from airflow.models import Variable

__all__ = ["BaseOperator", "BaseSensorOperator", "Variable"]
