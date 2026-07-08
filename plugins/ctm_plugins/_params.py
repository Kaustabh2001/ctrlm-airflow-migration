"""Control-M NODEID -> Airflow connection resolution — PURE python, no airflow.

Contract V4-1 (REVISED): this module's public API is ONLY :func:`resolve_node`
(the nodes.yaml loader below is a private helper). It backs the one operator
that earns a parse-time node lookup, ``ctm_plugins.operators.CtmDatabaseJob``.

What deliberately does NOT live here (user policy, revised V4):

- The PRIORITY/CRITICAL -> ``priority_weight`` formula stays single-sourced in
  ``core/ctrlm_core/operator_registry.py`` (v3). Do not duplicate it here.
- There is no blanket Control-M kwarg surface: Command/Job tasks stay plain
  SSHOperator/WinRMOperator (and Dummy stays EmptyOperator) with common params
  (priority_weight, pool, callbacks, email, sla, retries) translated at
  CODEGEN time exactly as v3 already does.

``mapping-config/nodes.yaml`` is resolved with the same order as notify.yaml
(see ``ctm_plugins.callbacks``): explicit path argument > env var
``CTM_NODES_CONFIG`` > plugins.zip root > repo root > cwd. Both schemas are
accepted (v2 ``defaults:``/``nodes:`` and v1 flat ``<id>: <conn_id>``);
unmapped nodes fall back to ``ssh_<node>`` / os linux, exactly like the code
generator. Missing/malformed config degrades to fallbacks and never raises —
connection lookup must not break DAG parsing.

Everything here is deterministic (sorted iteration, no wall clock, no
randomness) and unit-tested on the Windows dev box where Airflow cannot be
installed. Do NOT import airflow from this module.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Mapping

import yaml

log = logging.getLogger("ctm_plugins._params")

NODES_ENV_VAR = "CTM_NODES_CONFIG"
_NODES_BASENAME = Path("mapping-config") / "nodes.yaml"


def _candidate_paths(explicit: str | Path | None) -> list[Path]:
    """Candidate locations for nodes.yaml (first existing wins).

    An explicit path argument, or failing that the CTM_NODES_CONFIG env var,
    is authoritative: when set, no fallback search happens (a missing file
    then means ssh_<node> fallbacks everywhere). Otherwise search the
    deployed-plugins.zip layout, this repo's dev layout, then the current
    working directory — the same order notify.yaml / calendars.yaml use.
    """
    if explicit:
        return [Path(explicit)]
    env = os.environ.get(NODES_ENV_VAR, "")
    if env:
        return [Path(env)]
    here = Path(__file__).resolve()
    return [
        # deployed plugins.zip layout: <plugins>/ctm_plugins/_params.py
        #                              <plugins>/mapping-config/nodes.yaml
        here.parents[1] / _NODES_BASENAME,
        # this repo's dev layout: <repo>/plugins/ctm_plugins/_params.py
        #                         <repo>/mapping-config/nodes.yaml
        here.parents[2] / _NODES_BASENAME,
        Path.cwd() / _NODES_BASENAME,
    ]


def _load_node_map(path: str | Path | None = None) -> dict[str, dict[str, str]]:
    """Load nodes.yaml -> {NODEID: {"conn_id", "os", "type"}}.

    Accepts both schemas (same parse rules as the code generator):
    - v2: ``defaults: {os: ...}`` + ``nodes: {<id>: {conn_id: ..., os: ...,
      type: ...}}`` — ``type: db`` marks database endpoints;
    - v1: flat ``<id>: <conn_id>`` entries at the top level.
    Entries missing ``os`` inherit ``defaults.os`` (ultimately ``linux``).

    Missing/unreadable file or malformed content degrades to {} (every node
    then resolves to its fallback connection), never raises.
    """
    for candidate in _candidate_paths(path):
        try:
            if not candidate.is_file():
                continue
            data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        except Exception:  # malformed yaml / IO error -> fallbacks only
            log.warning("could not read nodes config %s", candidate, exc_info=True)
            return {}
        if not isinstance(data, dict):
            return {}
        defaults = data.get("defaults")
        if not isinstance(defaults, dict):
            defaults = {}
        default_os = str(defaults.get("os", "linux")).strip().lower() or "linux"
        entries = data.get("nodes")
        if not isinstance(entries, dict):  # v1 flat file (ignore a stray defaults key)
            entries = {k: v for k, v in data.items() if k != "defaults"}
        node_map: dict[str, dict[str, str]] = {}
        for node in sorted(entries, key=str):
            value = entries[node]
            if isinstance(value, dict):
                conn = str(value.get("conn_id", f"ssh_{node}"))
                node_os = str(value.get("os", default_os)).strip().lower() or default_os
                node_type = str(value.get("type", "")).strip().lower()
            else:
                conn = str(value)
                node_os = default_os
                node_type = ""
            node_map[str(node)] = {"conn_id": conn, "os": node_os, "type": node_type}
        return node_map
    return {}


def resolve_node(
    node: str,
    node_map: Mapping[str, Mapping[str, str]] | None = None,
    path: str | Path | None = None,
) -> dict[str, str]:
    """Resolve a Control-M NODEID to ``{"conn_id", "os", "type"}``.

    PURE given ``node_map``; loads nodes.yaml (see :func:`_load_node_map`)
    only when a map is not supplied. Unmapped (or empty) NODEIDs fall back to
    ``ssh_<node or 'default'>`` / os linux — the same ``ssh_<node>`` fallback
    the code generator uses.
    """
    node = (node or "").strip()
    if node_map is None:
        node_map = _load_node_map(path)
    entry = node_map.get(node) if node else None
    if entry is not None:
        return {
            "conn_id": str(entry.get("conn_id") or f"ssh_{node}"),
            "os": str(entry.get("os", "linux")) or "linux",
            "type": str(entry.get("type", "") or ""),
        }
    return {"conn_id": f"ssh_{node or 'default'}", "os": "linux", "type": ""}
