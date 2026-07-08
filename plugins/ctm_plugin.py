"""AirflowPlugin registration for the Control-M compatibility package.

Lives at the top level of plugins.zip so MWAA's plugin manager discovers it
(contracts V3-2 / V4-1). AIRFLOW IMPORT AT MODULE LEVEL — syntax-checked only
in this repo; executed for real on MWAA.

Registers:
- macros ``ctm_odate`` / ``gate_target`` -> usable in templates as
  ``{{ macros.ctm_odate(data_interval_end) }}`` and
  ``{{ macros.gate_target(data_interval_end, "0200") }}``;
- the ``CtmCalendarTimetable`` (required for DAG-serialization round-trips).

The targeted operators (CtmDatabaseJob, CtmManualJob), the sensors
(CtmApprovalGateSensor, CtmFileWatcherSensor) and the callbacks (ctm_shout)
need no plugin-manager registration in Airflow 2: generated DAGs import them
directly from ``ctm_plugins.operators`` / ``ctm_plugins.sensors`` /
``ctm_plugins.callbacks``. Importing them here still validates at plugin-load
time that the whole ctm_plugins package (and its provider dependencies) is
importable, and keeps a single inventory of everything the package ships.
"""
from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from ctm_plugins._odate import ctm_odate, gate_target
from ctm_plugins.operators import CtmDatabaseJob, CtmManualJob
from ctm_plugins.sensors import CtmApprovalGateSensor, CtmFileWatcherSensor
from ctm_plugins.timetables import CtmCalendarTimetable


class CtmPlugin(AirflowPlugin):
    name = "ctm_plugin"
    # exposed as macros.ctm_odate / macros.gate_target in all templates
    macros = [ctm_odate, gate_target]
    timetables = [CtmCalendarTimetable]
    # Inventory of the operator/sensor family (informational in Airflow 2 —
    # DAGs import these classes directly; listing them keeps the shipped
    # surface visible in one place and fails fast on import errors).
    ctm_operators = [CtmDatabaseJob, CtmManualJob]
    ctm_sensors = [CtmApprovalGateSensor, CtmFileWatcherSensor]
