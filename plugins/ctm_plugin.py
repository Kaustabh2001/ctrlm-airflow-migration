"""AirflowPlugin registration for the Control-M compatibility package.

Lives at the top level of plugins.zip so MWAA's plugin manager discovers it
(contract V3-2). AIRFLOW IMPORT AT MODULE LEVEL — syntax-checked only in this
repo; executed for real on MWAA.

Registers:
- macros ``ctm_odate`` / ``gate_target`` -> usable in templates as
  ``{{ macros.ctm_odate(data_interval_end) }}`` and
  ``{{ macros.gate_target(data_interval_end, "0200") }}``;
- the ``CtmCalendarTimetable`` (required for DAG-serialization round-trips).

The sensors (CtmApprovalGateSensor, CtmFileWatcherSensor) and callbacks
(ctm_shout) need no registration: generated DAGs import them directly from
``ctm_plugins.sensors`` / ``ctm_plugins.callbacks``.
"""
from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from ctm_plugins._odate import ctm_odate, gate_target
from ctm_plugins.timetables import CtmCalendarTimetable


class CtmPlugin(AirflowPlugin):
    name = "ctm_plugin"
    # exposed as macros.ctm_odate / macros.gate_target in all templates
    macros = [ctm_odate, gate_target]
    timetables = [CtmCalendarTimetable]
