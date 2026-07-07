"""Control-M SHOUT / DOMAIL -> Airflow callbacks (contract V3-2).

``ctm_shout(dest, message, when)`` returns a callable suitable for
``on_failure_callback`` / ``on_success_callback`` / ``sla_miss_callback``.

DESIGN RULE: this module imports NO airflow at module level. Destination
resolution and message formatting are pure (unit-tested on Windows where
Airflow cannot be installed). The actual send paths (SES email via
``airflow.utils.email.send_email``, SNS via ``boto3``) are late imports INSIDE
the returned callable, so they only execute on an Airflow worker.

Destination resolution order (``resolve_dest``):
1. dest containing "@" -> treated as an email address directly (passthrough);
2. dest found in mapping-config/notify.yaml -> its ``{type, target}`` entry;
3. anything else -> log-only (``{"type": "log", "target": dest}``).

notify.yaml schema: ``{dest: {type: email|sns|log, target: ...}}``.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Callable

import yaml

log = logging.getLogger("ctm_plugins.callbacks")

NOTIFY_ENV_VAR = "CTM_NOTIFY_CONFIG"
_NOTIFY_BASENAME = Path("mapping-config") / "notify.yaml"


# --------------------------------------------------------------- config load

def _candidate_paths(explicit: str | Path | None) -> list[Path]:
    """Candidate locations for notify.yaml (first existing wins).

    An explicit path argument, or failing that the CTM_NOTIFY_CONFIG env var,
    is authoritative: when set, no fallback search happens (a missing file
    then means log-only behaviour). Otherwise search the deployed-plugins.zip
    layout, this repo's dev layout, then the current working directory.
    """
    if explicit:
        return [Path(explicit)]
    env = os.environ.get(NOTIFY_ENV_VAR, "")
    if env:
        return [Path(env)]
    here = Path(__file__).resolve()
    return [
        # deployed plugins.zip layout: <plugins>/ctm_plugins/callbacks.py
        #                              <plugins>/mapping-config/notify.yaml
        here.parents[1] / _NOTIFY_BASENAME,
        # this repo's dev layout: <repo>/plugins/ctm_plugins/callbacks.py
        #                         <repo>/mapping-config/notify.yaml
        here.parents[2] / _NOTIFY_BASENAME,
        Path.cwd() / _NOTIFY_BASENAME,
    ]


def load_notify_map(path: str | Path | None = None) -> dict[str, dict]:
    """Load notify.yaml -> {dest: {"type": ..., "target": ...}}.

    Missing/unreadable file or malformed content degrades to {} (log-only
    behaviour), never raises: notification config must not break a DAG.
    """
    for candidate in _candidate_paths(path):
        try:
            if not candidate.is_file():
                continue
            raw = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        except Exception:  # malformed yaml / IO error -> log-only
            log.warning("could not read notify config %s", candidate, exc_info=True)
            return {}
        if not isinstance(raw, dict):
            return {}
        out: dict[str, dict] = {}
        for dest in sorted(raw):
            entry = raw[dest]
            if isinstance(entry, dict) and entry.get("type"):
                out[str(dest)] = {
                    "type": str(entry["type"]).strip().lower(),
                    "target": str(entry.get("target", "") or ""),
                }
        return out
    return {}


# ---------------------------------------------------------- pure resolution

def resolve_dest(dest: str, notify_map: dict[str, dict] | None = None) -> dict:
    """Resolve a Control-M SHOUT/DOMAIL destination to a delivery spec.

    Returns ``{"type": "email"|"sns"|"log", "target": str}``. PURE given
    ``notify_map``; loads the default map only when one is not supplied.
    """
    dest = (dest or "").strip()
    if "@" in dest:  # e-mail address passthrough rule
        return {"type": "email", "target": dest}
    if notify_map is None:
        notify_map = load_notify_map()
    entry = notify_map.get(dest)
    if entry and entry.get("type") in ("email", "sns", "log"):
        return {"type": entry["type"], "target": entry.get("target", "") or dest}
    return {"type": "log", "target": dest}


def summarize_context(context: Any) -> dict[str, str]:
    """Extract dag/task/run identifiers from an Airflow callback context.

    PURE and defensive: accepts the Airflow context dict, plain dicts (tests),
    or anything else (sla_miss_callback passes positional args, not a dict).
    """
    out: dict[str, str] = {}
    if not isinstance(context, dict):
        if context is not None:
            dag_id = getattr(context, "dag_id", None)
            if dag_id:
                out["dag_id"] = str(dag_id)
        return out
    dag = context.get("dag")
    if dag is not None:
        out["dag_id"] = str(getattr(dag, "dag_id", dag))
    ti = context.get("task_instance") or context.get("ti")
    if ti is not None:
        out["task_id"] = str(getattr(ti, "task_id", ti))
    for key in ("run_id", "ds"):
        value = context.get(key)
        if value:
            out[key] = str(value)
    return out


def format_message(dest: str, message: str, when: str, summary: dict[str, str]) -> str:
    """Render the notification text. PURE; fixed key order for determinism."""
    parts = [f"[CTM SHOUT when={when} dest={dest}]"]
    if message:
        parts.append(message)
    details = [f"{k}={summary[k]}" for k in ("dag_id", "task_id", "run_id", "ds") if k in summary]
    if details:
        parts.append("(" + ", ".join(details) + ")")
    return " ".join(parts)


# ------------------------------------------------------------ the callback

def ctm_shout(
    dest: str,
    message: str = "",
    when: str = "NOTOK",
    notify_path: str | Path | None = None,
) -> Callable[..., None]:
    """Build a callback callable for a Control-M SHOUT / DOMAIL action.

    Usage in generated DAGs::

        from ctm_plugins.callbacks import ctm_shout
        task = SSHOperator(..., on_failure_callback=ctm_shout("ops@corp.com"))

    The callable tolerates every Airflow callback signature:
    ``on_*_callback(context)`` and
    ``sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)``.
    Send failures are logged, never raised (a broken notification must not
    take the worker down).
    """

    def _shout_callback(context: Any = None, *args: Any, **kwargs: Any) -> None:
        spec = resolve_dest(dest, load_notify_map(notify_path))
        summary = summarize_context(context)
        text = format_message(dest, message, when, summary)
        subject = f"Control-M shout ({when}): {summary.get('dag_id', dest)}"
        try:
            if spec["type"] == "email":
                # LATE import — only available on an Airflow worker.
                from airflow.utils.email import send_email

                send_email(to=[spec["target"]], subject=subject, html_content=text)
            elif spec["type"] == "sns":
                # LATE import — boto3 ships with MWAA.
                import boto3

                boto3.client("sns").publish(
                    TopicArn=spec["target"], Subject=subject[:100], Message=text
                )
            else:
                if when in ("NOTOK", "LATE"):
                    log.warning("%s", text)
                else:
                    log.info("%s", text)
        except Exception:
            log.exception("ctm_shout delivery to %s (%s) failed", dest, spec["type"])

    # introspection hooks (used by tests and debuggers)
    _shout_callback.ctm_dest = dest  # type: ignore[attr-defined]
    _shout_callback.ctm_message = message  # type: ignore[attr-defined]
    _shout_callback.ctm_when = when  # type: ignore[attr-defined]
    return _shout_callback
