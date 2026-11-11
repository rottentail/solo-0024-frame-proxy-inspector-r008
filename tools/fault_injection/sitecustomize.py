"""Auto-imported fault injection hook for the persistence verification suite.

The production service never references this module.  The verification driver
spawns the real ``python -m frame_proxy serve`` subprocess with this
directory on ``PYTHONPATH``; Python then imports ``sitecustomize`` once,
before any application module is loaded.

A single primitive is wrapped -- the process-global :func:`os.replace`, which
is the sole publication mechanism used by ``storage/atomic.py``.  Every
durable publication (clip media, clip record, contact-sheet artifact, sheet
record states) crosses this primitive, so wrapping it gives deterministic
fault points without modifying any production source file.

Fault rules are read from the JSON file named by
``FRAME_PROXY_FAULT_CONFIG``::

    {
      "rules": [
        {
          "id": "clip-media-replace",
          "path_endswith": "clips/<clip-id>/clip.avi",
          "occurrence": 1,
          "when": "before_replace" | "after_replace",
          "mode": "raise" | "exit"
        }
      ],
      "event_file": "/abs/path/optional-events.jsonl"
    }

``path_endswith`` pins a rule to one concrete object path (the tail is
relative to the data root), so keeper and neighbor objects that merely share
a file name never trigger it.  ``occurrence`` counts ``os.replace`` calls for
that exact destination, starting at one; this matters because a workflow may
rewrite the same record file several times.

``raise`` propagates an :class:`OSError` (the same family a real disk or
filesystem fault would surface) into the application thread, exercising the
production exception and cleanup path.  ``exit`` sends ``SIGKILL`` to the
current process, simulating a sudden process death with no ``finally``,
``atexit``, or socket shutdown running.
"""

from __future__ import annotations

import errno
import json
import os
import signal
from typing import Any


def _load_config() -> dict[str, Any] | None:
    config_path = os.environ.get("FRAME_PROXY_FAULT_CONFIG")
    if not config_path:
        return None
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(config, dict) or not isinstance(config.get("rules"), list):
        return None
    return config


def _matches(rule: dict[str, Any], destination: str) -> bool:
    # Exact tail match takes precedence and pins the rule to one concrete
    # object path so keeper/neighbor writes with the same file names never
    # trigger it.
    exact_tail = rule.get("path_endswith")
    if exact_tail:
        return destination.endswith(exact_tail)
    fragment = rule.get("path_contains")
    if fragment and fragment not in destination:
        return False
    suffix = rule.get("path_suffix")
    if suffix and not destination.endswith(suffix):
        return False
    return True


def _record_event(event_file: str | None, event: dict[str, Any]) -> None:
    if not event_file:
        return
    try:
        with open(event_file, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
    except OSError:
        # Fault injection must never mask itself through a logging failure.
        pass


def install() -> None:
    config = _load_config()
    if config is None:
        return
    rules = config["rules"]
    event_file = config.get("event_file")
    # Occurrence counts are scoped to a concrete destination path, matching
    # how a workflow rewrites the same record file several times.
    counts: dict[str, int] = {}
    real_replace = os.replace

    def injected_replace(src: Any, dst: Any) -> Any:
        destination = os.fspath(dst)
        for rule in rules:
            if not _matches(rule, destination):
                continue
            occurrence = counts.get(destination, 0) + 1
            counts[destination] = occurrence
            if occurrence != int(rule.get("occurrence", 1)):
                break
            mode = rule.get("mode", "raise")
            when = rule.get("when", "before_replace")
            event = {
                "rule": rule.get("id"),
                "pid": os.getpid(),
                "destination": destination,
                "occurrence": occurrence,
                "when": when,
                "mode": mode,
            }
            if mode == "exit":
                if when == "after_replace":
                    real_replace(src, dst)
                _record_event(
                    event_file,
                    {**event, "action": "sigkill"},
                )
                # Flush the event line first so the driver never has to infer
                # whether the fault point was reached.
                os.kill(os.getpid(), signal.SIGKILL)
                # SIGKILL cannot return; raise to satisfy static control flow.
                raise SystemExit(137)
            _record_event(event_file, {**event, "action": "raise"})
            raise PermissionError(
                errno.EACCES,
                f"injected storage fault at {destination} (rule {rule.get('id')})",
            )
        return real_replace(src, dst)

    os.replace = injected_replace


install()
