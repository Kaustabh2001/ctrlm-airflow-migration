"""ctm_plugins — write-once Control-M compatibility components for Airflow/MWAA.

Deployed to MWAA as plugins.zip (see plugins/README.md); generated DAGs import
from this package. Import discipline (contracts V3-2 / V4-1):

- AIRFLOW-FREE at import time: ``_odate``, ``callbacks``, ``timetables``,
  ``_params`` (pure helpers; timetables guards its airflow import) — safe to
  import in tool code and tests. Their public API is re-exported eagerly.
- AIRFLOW-REQUIRED: ``sensors`` and ``operators`` (module-level airflow
  imports) — only importable inside an Airflow runtime; in this repo they are
  syntax-checked only. The two operator classes (``CtmDatabaseJob``,
  ``CtmManualJob``) are re-exported LAZILY via module ``__getattr__`` so that
  ``from ctm_plugins import CtmDatabaseJob`` works on MWAA while importing
  this package stays airflow-free on the dev box.
"""
from __future__ import annotations

from typing import Any

from ._odate import ctm_odate, gate_target, odate_date, parse_hhmm
from ._params import resolve_node
from .callbacks import ctm_shout, resolve_dest

# airflow-requiring re-exports, resolved on first attribute access (PEP 562)
_LAZY_OPERATOR_EXPORTS = ("CtmDatabaseJob", "CtmManualJob")


def __getattr__(name: str) -> Any:
    if name in _LAZY_OPERATOR_EXPORTS:
        from . import operators  # airflow import — MWAA runtime only

        return getattr(operators, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ctm_odate",
    "gate_target",
    "odate_date",
    "parse_hhmm",
    "ctm_shout",
    "resolve_dest",
    "resolve_node",
    "CtmDatabaseJob",
    "CtmManualJob",
]

__version__ = "4.0.0"
