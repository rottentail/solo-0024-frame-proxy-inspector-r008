# Persistence fault verification

This document describes the fault verification suite that proves how the file
persistence layer behaves under two fundamentally different failure classes:

- **ordinary exceptions** (`raise`) — an in-process `OSError` at a storage
  boundary. The interpreter, worker thread, and socket stay alive; every
  production `except`, rollback, and failure-finalization path runs.
- **sudden process exit** (`exit`) — `SIGKILL` of the whole service process.
  Nothing in the interpreter runs: no `finally`, no `atexit`, no socket
  shutdown, no cleanup.

The key rule the suite enforces is that **client-visible status alone cannot
establish commit outcome**. A complete file on disk does not prove a commit,
and a client error does not prove the absence of one. Every scenario is
reconciled through three independent views that must tell one consistent
story.

## Running

```bash
PYTHONPATH=src:. python3 tools/check_persistence_faults.py
```

The suite writes a structured reconciliation report to
`persistence-fault-report.json` in the working directory and exits non-zero
if any expectation is violated. Set `FRAME_PROXY_KEEP_WORKSPACE=1` to retain
the per-scenario data roots and service logs for manual inspection.

## How faults are injected (without touching production code)

No production module imports or references the test harness. The driver
launches the real production program —
`python -m frame_proxy serve` — as an independent subprocess and puts
`tools/fault_injection` on `PYTHONPATH`. Python then auto-imports
`sitecustomize.py` once at interpreter startup, before any application code.

The hook wraps exactly one primitive, the process-global `os.replace`, which
is the sole publication mechanism used by `storage/atomic.py`. Every durable
publication (clip media, clip record, contact-sheet artifact, sheet record
state) crosses it, so wrapping this one function yields deterministic fault
points across both workflows. A rule pins:

- an exact destination path tail, scoped to one concrete object id so keeper
  and neighbor objects never trigger it;
- an occurrence (records are rewritten several times within a workflow);
- a moment (`before_replace` or `after_replace`);
- an action (`raise` an `OSError`, or `SIGKILL` the process).

A JSONL event file records every fired rule, proving the fault point was
actually reached rather than the test passing because injection never
happened. The restarted generation is always started **without** the fault
environment variable, so reconciliation never re-arms a rule.

## The three reconciled views

For each scenario the driver captures and cross-checks:

1. **Client result** — an HTTP response with status and stable error code, or
   a connection-level failure (`RemoteDisconnected`) when the process died.
2. **Restarted normal reads** — a fresh process, no fault rule, same data
   root, exercised only through the production read APIs (`GET clip`,
   `GET sheet`, artifact fetch, identifier reuse).
3. **Independent disk evidence** — files read directly from the data root
   without going through the service: presence, size, SHA-256 digest, record
   state, and surviving temporary files.

A verdict is accepted only when all three views agree.

## Controls

Two classes of pre-existing and in-flight objects prove that failure
handling never crosses object boundaries:

- **Keepers** — a fully registered clip, an indexed inspection, and a ready
  contact sheet created before the fault. After restart the suite asserts they
  remain readable through the API and byte-identical on disk (record, media,
  and artifact digests).
- **Adjacent concurrent request** — a different object (different clip id or
  sheet id) driven concurrently with the faulted request behind the same
  `ThreadingHTTPServer`. Under `raise` it must fully commit while the target
  fails, demonstrating per-key isolation. Under `exit` the process-wide
  kill may interrupt it at any publication step; its actual outcome is
  reconciled against the disk rather than assumed.

## Verdicts

| Verdict | Meaning |
| --- | --- |
| `NOT_COMMITTED` | No operation output is visible after the fault and every byte attributable to the attempt has been removed by production cleanup. |
| `COMMITTED` | The output is durably present, the restarted service reads the exact record/digest the operation computed, and reusing the identifier conflicts. |
| `INDETERMINATE` | Surviving evidence cannot be resolved into either verdict using only mechanisms the specification promises. |

## Scenario matrix

### R0 — clean restart baseline (no fault)

Before any fault is injected, the suite creates a full set of legal objects
(registered clip, indexed inspection, ready contact sheet plus its PPM),
restarts the service with no fault configuration against the same data root,
and asserts that every object reads identically and that the set and digest
of persisted domain files is unchanged. This isolates restart durability from
failure handling and proves the reconciliation harness reads a healthy data
root the same way across process generations.

### Clip intake (`PUT /v1/clips/{id}`)

| Scenario | Fault point | Action | Verdict |
| --- | --- | --- | --- |
| C1 | before media `os.replace` | raise | `NOT_COMMITTED` |
| C2 | before record `os.replace` (media already durable) | raise | `NOT_COMMITTED` |
| C3 | after media `os.replace` | SIGKILL | `INDETERMINATE` |
| C4 | after record `os.replace` | SIGKILL | `COMMITTED` |

- **C2** specifically proves the production rollback that deletes the already
  durable `clip.avi` when the record publication raises. A mutation test that
  disables this rollback makes C2 fail.
- **C4** proves the inverse rule: the client receives a connection error even
  though the commit completed, and after restart the clip reads back with the
  original digest while a repeated upload returns `409`.

### Contact sheet generation (`POST /v1/inspections/{id}/contact-sheets`)

| Scenario | Fault point | Action | Verdict |
| --- | --- | --- | --- |
| S1 | before artifact `os.replace` | raise | `NOT_COMMITTED` |
| S2 | before ready-record `os.replace`, 3rd record write (artifact already durable) | raise | `NOT_COMMITTED` |
| S3 | after artifact `os.replace` | SIGKILL | `INDETERMINATE` |
| S4 | after ready-record `os.replace`, 3rd record write | SIGKILL | `COMMITTED` |

- **S1/S2** prove failure finalization: the sheet record ends in the terminal
  `failed` state with a stable error code, the artifact (including one already
  published in S2) is removed, the artifact endpoint refuses with `409`, and a
  fresh sheet id still composes.
- **S4** proves a ready commit can survive a client-visible crash: the served
  PPM digest matches the ready record and the on-disk bytes.

## Explicitly undecidable ranges

The baseline specification defines **no startup reconciliation, no
temporary-file reaping, and no automatic failure finalization after a process
death**. The suite therefore does not add recovery behavior and does not
weaken assertions to force a clean verdict. Two states are recorded as
`INDETERMINATE` with the exact open question documented:

1. **Orphan clip media (C3).** After SIGKILL following media publication, the
   data root contains a complete `clip.avi` whose digest equals the upload but
   no `record.json`, plus a stale lock file. The service correctly reports
   `404` (media without a record is not a registered clip) and the stale lock
   does not block identifier reuse. What cannot be decided through promised
   mechanisms is whether that orphan media should have been committed or
   discarded. The suite neither reaps nor commits it.
2. **Stuck non-terminal contact sheet (S3).** After SIGKILL following artifact
   publication, a valid `sheet.ppm` exists beside a record still in
   `compositing` whose `artifact_digest` is null. On restart the service does
   not invent a terminal state: the sheet reads back as `compositing`, the
   artifact endpoint refuses with `409`, the identifier stays reserved, and
   other sheets still compose. Whether the orphan PPM is usable cannot be
   decided through promised mechanisms.

Additionally, under SIGKILL a concurrent neighbor may be interrupted at any
publication step. The suite reconciles every physically possible neighbor
state — nothing published, orphan media/record only, a stuck non-terminal
job, or a full commit — and requires the restarted API view to match the disk
view. Surviving `.tmp` files after SIGKILL are reported as process-death
evidence; they are only treated as a cleanup failure when an ordinary
exception (which runs every cleanup path) leaves one behind.

## What this suite does not do

- It does not add recovery, startup sweep, or auto-finalization capability;
  production code is unchanged.
- It does not assert outcomes the specification leaves open; those are
  reported as `INDETERMINATE` with the undecidable question stated.
- It does not mock the storage layer or run the service in-process; every
  fault executes in a real subprocess against an independent, isolated data
  root that is discarded after the run.
