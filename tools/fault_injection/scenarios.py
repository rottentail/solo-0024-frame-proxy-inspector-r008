"""Declarative fault scenario matrix.

A scenario pins one publication primitive (``os.replace`` inside
``storage/atomic.py``), one moment relative to it, and one fault class:

* ``raise`` -- an ordinary in-process :class:`OSError`, so the service stays
  alive and its normal exception/cleanup path runs;
* ``exit``  -- ``SIGKILL`` of the whole subprocess, so nothing in the
  interpreter gets a chance to clean up.

Each scenario also fixes the verdict the evidence must support:

* ``NOT_COMMITTED``   -- no operation output is visible after the fault, and
  all bytes attributable to the attempt are gone;
* ``COMMITTED``       -- the operation output is durably present and the
  restarted service reads exactly the record/digest the operation computed;
* ``INDETERMINATE``   -- the surviving evidence cannot be reconciled into
  either verdict using the mechanisms the specification promises.

``INDETERMINATE`` rows intentionally assert boundaries rather than invent a
recovery path: the baseline spec defines no startup reconciliation, no
temporary-file reaping, and no automatic failure finalization, so the suite
records precisely what an operator cannot decide and confirms the service
remains usable for other objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


FaultMode = Literal["raise", "exit"]
FaultWhen = Literal["before_replace", "after_replace"]
Verdict = Literal["NOT_COMMITTED", "COMMITTED", "INDETERMINATE"]
Workflow = Literal["clip_intake", "contact_sheet"]


@dataclass(frozen=True, slots=True)
class FaultRule:
    rule_id: str
    # Tail relative to the data root, with a "{id}" placeholder replaced by
    # the concrete target object id per run, e.g. clips/{id}/clip.avi.
    path_template: str
    occurrence: int
    when: FaultWhen
    mode: FaultMode

    def render(self, target_id: str) -> dict[str, Any]:
        return {
            "id": self.rule_id,
            "path_endswith": self.path_template.format(id=target_id),
            "occurrence": self.occurrence,
            "when": self.when,
            "mode": self.mode,
        }

    def as_json(self) -> dict[str, Any]:
        return {
            "id": self.rule_id,
            "path_template": self.path_template,
            "occurrence": self.occurrence,
            "when": self.when,
            "mode": self.mode,
        }


@dataclass(frozen=True, slots=True)
class FaultScenario:
    scenario_id: str
    workflow: Workflow
    summary: str
    rule: FaultRule
    verdict: Verdict
    # What the requesting client must observe.
    client_expectation: str
    # What the restarted service must answer for the target object.
    restart_expectation: str
    # What an independent disk inspection must find.
    disk_expectation: str
    # The exact question the current specification gives no mechanism to
    # answer.  Empty for rows that are fully decidable.
    undecidable_scope: str = ""
    tags: frozenset[str] = field(default_factory=frozenset)


def _clip_media_rule(when: FaultWhen, mode: FaultMode) -> FaultRule:
    return FaultRule(
        rule_id=f"clip-media-{when}-{mode}",
        path_template="clips/{id}/clip.avi",
        occurrence=1,
        when=when,
        mode=mode,
    )


def _clip_record_rule(when: FaultWhen, mode: FaultMode) -> FaultRule:
    return FaultRule(
        rule_id=f"clip-record-{when}-{mode}",
        path_template="clips/{id}/record.json",
        occurrence=1,
        when=when,
        mode=mode,
    )


def _sheet_artifact_rule(when: FaultWhen, mode: FaultMode) -> FaultRule:
    return FaultRule(
        rule_id=f"sheet-artifact-{when}-{mode}",
        path_template="contact-sheets/{id}/sheet.ppm",
        occurrence=1,
        when=when,
        mode=mode,
    )


def _sheet_record_rule(
    when: FaultWhen,
    mode: FaultMode,
    occurrence: int,
) -> FaultRule:
    return FaultRule(
        rule_id=f"sheet-record-occ{occurrence}-{when}-{mode}",
        path_template="contact-sheets/{id}/record.json",
        occurrence=occurrence,
        when=when,
        mode=mode,
    )


CLIP_SCENARIOS: tuple[FaultScenario, ...] = (
    FaultScenario(
        scenario_id="C1-clip-media-raise",
        workflow="clip_intake",
        summary="ordinary exception while publishing clip media",
        rule=_clip_media_rule("before_replace", "raise"),
        verdict="NOT_COMMITTED",
        client_expectation=(
            "HTTP 500 storage_failure JSON; the process keeps serving and a "
            "concurrent neighbor intake still returns 201"
        ),
        restart_expectation=(
            "GET clip -> 404; re-PUT with the same identifier -> 201 and the "
            "restarted record digest matches the upload"
        ),
        disk_expectation=(
            "no clip directory, no media, no record, no stale .tmp file for "
            "the target; neighbor and keeper objects are byte-identical"
        ),
        tags=frozenset({"raise", "cleanup", "neighbor-concurrent"}),
    ),
    FaultScenario(
        scenario_id="C2-clip-record-raise",
        workflow="clip_intake",
        summary="ordinary exception while publishing the clip record, after media is durable",
        rule=_clip_record_rule("before_replace", "raise"),
        verdict="NOT_COMMITTED",
        client_expectation=(
            "HTTP 500 storage_failure JSON; process stays alive"
        ),
        restart_expectation=(
            "GET clip -> 404 (media without a record is not a visible clip); "
            "re-PUT same identifier -> 201"
        ),
        disk_expectation=(
            "the previously published clip.avi is removed by the production "
            "rollback and the empty clip directory is removed; no record.json"
        ),
        tags=frozenset({"raise", "rollback-after-durable-media"}),
    ),
    FaultScenario(
        scenario_id="C3-clip-media-exit",
        workflow="clip_intake",
        summary="sudden process death immediately after clip media becomes durable",
        rule=_clip_media_rule("after_replace", "exit"),
        verdict="INDETERMINATE",
        client_expectation=(
            "connection-level failure with no HTTP response; the whole "
            "subprocess is dead (exit 137)"
        ),
        restart_expectation=(
            "GET clip -> 404 because no record was ever published; re-PUT "
            "same identifier -> 201, proving the stale lock file does not "
            "block the key"
        ),
        disk_expectation=(
            "complete clip.avi whose digest equals the upload, no "
            "record.json, and the lock file left behind by the dead process"
        ),
        undecidable_scope=(
            "the bytes of the attempted intake are fully present but no "
            "record exists; the specification provides no startup sweep or "
            "reconciliation, so an observer cannot determine from the data "
            "root alone whether the orphan media should be committed or "
            "discarded. The suite must not reap or commit it."
        ),
        tags=frozenset({"exit", "orphan-media", "stale-lock"}),
    ),
    FaultScenario(
        scenario_id="C4-clip-record-exit",
        workflow="clip_intake",
        summary="sudden process death immediately after the clip record is published",
        rule=_clip_record_rule("after_replace", "exit"),
        verdict="COMMITTED",
        client_expectation=(
            "connection-level failure even though the commit completed; the "
            "client error alone cannot be read as failure"
        ),
        restart_expectation=(
            "GET clip -> 200 with state registered and the original digest; "
            "re-PUT same identifier -> 409 conflict"
        ),
        disk_expectation=(
            "record.json and clip.avi both present; record digest matches "
            "the stored media and the original upload"
        ),
        tags=frozenset({"exit", "commit-despite-client-error"}),
    ),
)


SHEET_SCENARIOS: tuple[FaultScenario, ...] = (
    FaultScenario(
        scenario_id="S1-sheet-artifact-raise",
        workflow="contact_sheet",
        summary="ordinary exception while publishing the PPM artifact",
        rule=_sheet_artifact_rule("before_replace", "raise"),
        verdict="NOT_COMMITTED",
        client_expectation=(
            "HTTP 500 internal_failure JSON; process stays alive and a "
            "concurrent neighbor sheet still reaches ready (201)"
        ),
        restart_expectation=(
            "GET sheet -> state failed with a stable error code; artifact "
            "fetch -> 409 invalid state; creating a new sheet id succeeds"
        ),
        disk_expectation=(
            "record.json in failed state, no sheet.ppm, no stale .tmp; the "
            "keeper inspection and keeper sheet are unchanged"
        ),
        tags=frozenset({"raise", "failure-finalization", "neighbor-concurrent"}),
    ),
    FaultScenario(
        scenario_id="S2-sheet-ready-record-raise",
        workflow="contact_sheet",
        summary=(
            "ordinary exception while publishing the ready record, after the "
            "verified artifact is durable"
        ),
        rule=_sheet_record_rule("before_replace", "raise", occurrence=3),
        verdict="NOT_COMMITTED",
        client_expectation=(
            "HTTP 500 internal_failure JSON; process stays alive"
        ),
        restart_expectation=(
            "GET sheet -> state failed; artifact fetch -> 409; a new sheet "
            "id on the same inspection succeeds"
        ),
        disk_expectation=(
            "the production rollback deletes the already published sheet.ppm "
            "and the failed record remains; no stale .tmp"
        ),
        tags=frozenset({"raise", "rollback-after-durable-artifact"}),
    ),
    FaultScenario(
        scenario_id="S3-sheet-artifact-exit",
        workflow="contact_sheet",
        summary="sudden process death immediately after the PPM artifact becomes durable",
        rule=_sheet_artifact_rule("after_replace", "exit"),
        verdict="INDETERMINATE",
        client_expectation=(
            "connection-level failure; subprocess dead (exit 137)"
        ),
        restart_expectation=(
            "GET sheet -> 200 but state is still compositing (non-terminal); "
            "artifact fetch -> 409; reusing the same sheet id -> 409 because "
            "the record exists, while a new sheet id completes normally"
        ),
        disk_expectation=(
            "compositing record.json plus a complete valid sheet.ppm whose "
            "digest the record cannot reference yet (artifact_digest is null)"
        ),
        undecidable_scope=(
            "a verified artifact exists beside a non-terminal compositing "
            "record; the specification defines no startup recovery and no "
            "automatic failure finalization after a process death, so the "
            "suite must not mark the sheet failed or ready itself. Whether "
            "the orphan PPM is usable cannot be decided through promised "
            "mechanisms."
        ),
        tags=frozenset({"exit", "stuck-nonterminal", "orphan-artifact"}),
    ),
    FaultScenario(
        scenario_id="S4-sheet-ready-record-exit",
        workflow="contact_sheet",
        summary="sudden process death immediately after the ready record is published",
        rule=_sheet_record_rule("after_replace", "exit", occurrence=3),
        verdict="COMMITTED",
        client_expectation=(
            "connection-level failure even though the ready commit completed"
        ),
        restart_expectation=(
            "GET sheet -> 200 state ready with the tile map and digest; "
            "artifact fetch -> 200 bytes whose digest equals the record"
        ),
        disk_expectation=(
            "ready record.json and sheet.ppm both present with matching "
            "digest and the planned dimensions"
        ),
        tags=frozenset({"exit", "commit-despite-client-error"}),
    ),
)

ALL_SCENARIOS: tuple[FaultScenario, ...] = CLIP_SCENARIOS + SHEET_SCENARIOS
