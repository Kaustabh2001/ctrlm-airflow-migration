"""AUTOEDIT variable translation (DESIGN.md §6 mapping table).

Maps the %%-variable family to Airflow Jinja macros. %%JOBNAME is left
verbatim — emit.py substitutes the literal job name at generation time.
Unknown %%NAME tokens stay verbatim and are reported so emit.py can add a
`# TODO unresolved AUTOEDIT` comment plus an UNRESOLVED_AUTOEDIT diagnostic.
"""
from __future__ import annotations

import re

# Exact-token translation table. Longest-match is guaranteed by the token
# regex (a token is the whole %%\$?NAME run), so %%ODATEV is NOT %%ODATE + V.
_MAP: dict[str, str] = {
    "%%ODATE": "{{ ds_nodash }}",
    "%%$ODATE": "{{ ds }}",
    "%%DATE": "{{ ds_nodash }}",
    "%%TIME": "{{ ts_nodash }}",
}

# Tokens handled later in the pipeline (emit substitutes the literal value).
_DEFERRED: frozenset[str] = frozenset({"%%JOBNAME"})

_TOKEN = re.compile(r"%%\$?[A-Za-z_][A-Za-z0-9_]*")


def translate(command: str) -> tuple[str, list[str]]:
    """Translate AUTOEDIT %%-variables in *command*.

    Returns ``(translated, unresolved)`` where *unresolved* lists the unknown
    ``%%NAME`` tokens (deduped, in first-appearance order — deterministic).
    """
    unresolved: list[str] = []

    def _repl(match: re.Match[str]) -> str:
        token = match.group(0)
        if token in _MAP:
            return _MAP[token]
        if token in _DEFERRED:
            return token
        if token not in unresolved:
            unresolved.append(token)
        return token

    return _TOKEN.sub(_repl, command), unresolved
