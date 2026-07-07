"""ctm_plugins — write-once Control-M compatibility components for Airflow/MWAA.

Deployed to MWAA as plugins.zip (see plugins/README.md); generated DAGs import
from this package. Import discipline (contract V3-2):

- AIRFLOW-FREE at import time: ``_odate``, ``callbacks``, ``timetables``
  (pure helpers; timetables guards its airflow import) — safe to import in
  tool code and tests.
- AIRFLOW-REQUIRED: ``sensors`` (module-level airflow imports) — never import
  it outside an Airflow runtime; it is only syntax-checked in this repo.

This ``__init__`` therefore re-exports only the airflow-free API.
"""
from __future__ import annotations

from ._odate import ctm_odate, gate_target, odate_date, parse_hhmm
from .callbacks import ctm_shout, resolve_dest

__all__ = [
    "ctm_odate",
    "gate_target",
    "odate_date",
    "parse_hhmm",
    "ctm_shout",
    "resolve_dest",
]

__version__ = "3.0.0"
