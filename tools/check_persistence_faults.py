"""Persistence fault verification: ordinary exceptions vs. sudden process exit.

Run:

    PYTHONPATH=src:. python3 tools/check_persistence_faults.py

For every scenario in :mod:`tools.fault_injection.scenarios` this driver:

1. starts the real ``python -m frame_proxy serve`` program in a separate
   process with an isolated data root and a test-only fault rule;
2. seeds legal keeper objects and then drives one clip intake or contact
   sheet generation, alongside an adjacent concurrent request for a
   different object;
3. records exactly what the client observed (HTTP response versus
   connection-level failure versus dead process);
4. restarts the service against the same data root and reconciles the
   target object through the normal read APIs;
5. independently inspects the disk and cross-checks that media, records,
   and artifacts still tell the same story as one operation;
6. proves cleanup and failure finalization never touched the adjacent
   in-flight request or the pre-existing keeper objects.

The driver adds no recovery capability.  Scenarios the current specification
gives no mechanism to resolve are reported as INDETERMINATE with the exact
undecidable question documented, and the data is left as-is on disk.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from frame_proxy.media.avi_writer import build_sample_avi
from frame_proxy.media.ppm import parse_ppm_header
from tools.fault_injection.disk import (
    DiskSnapshot,
    clip_paths,
    digest_bytes,
    find_stale_temporaries,
    inspection_path,
    sheet_paths,
)
from tools.fault_injection.process import ClientError, SubprocessService
from tools.fault_injection.scenarios import (
    ALL_SCENARIOS,
    FaultScenario,
    Verdict,
)


# --------------------------------------------------------------------------- #
# Small assertion/reporting helpers
# --------------------------------------------------------------------------- #


class CheckFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CheckFailure(message)


def require_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise CheckFailure(f"{label}: expected {expected!r}, got {actual!r}")


@dataclass(slots=True)
class Observation:
    kind: str  # "http" | "connection_failure" | "process_exit"
    status: int | None = None
    error_code: str | None = None
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "status": self.status,
            "error_code": self.error_code,
            "detail": self.detail,
        }


@dataclass(slots=True)
class ScenarioReport:
    scenario_id: str
    workflow: str
    verdict: str
    fault: dict[str, Any]
    client: Observation
    restart: dict[str, Any]
    disk: dict[str, Any]
    controls: dict[str, Any]
    undecidable_scope: str
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "workflow": self.workflow,
            "verdict": self.verdict,
            "fault": self.fault,
            "client": self.client.as_dict(),
            "restart": self.restart,
            "disk": self.disk,
            "controls": self.controls,
            "undecidable_scope": self.undecidable_scope,
            "failures": list(self.failures),
        }


# --------------------------------------------------------------------------- #
# Fixture and request builders
# --------------------------------------------------------------------------- #


def _upload_clip(
    service: SubprocessService,
    clip_id: str,
    payload: bytes,
) -> Observation:
    try:
        response = service.request(
            "PUT",
            f"/v1/clips/{clip_id}",
            body=payload,
            content_type="application/x-avi",
        )
    except ClientError as exc:
        return Observation(kind="connection_failure", detail=str(exc))
    error_code = None
    if response.status >= 400:
        try:
            error_code = response.json()["error"]["code"]
        except (ValueError, KeyError, RuntimeError):
            pass
    return Observation(
        kind="http",
        status=response.status,
        error_code=error_code,
        detail=response.body.decode("utf-8", errors="replace")[:400],
    )


def _create_sheet(
    service: SubprocessService,
    inspection_id: str,
    sheet_id: str,
) -> Observation:
    body = json.dumps(
        {
            "sheet_id": sheet_id,
            "columns": 3,
            "tile_width": 96,
            "gutter": 4,
            "selection": "stored",
            "background": "#20252c",
        }
    ).encode("utf-8")
    try:
        response = service.request(
            "POST",
            f"/v1/inspections/{inspection_id}/contact-sheets",
            body=body,
        )
    except ClientError as exc:
        return Observation(kind="connection_failure", detail=str(exc))
    error_code = None
    if response.status >= 400:
        try:
            error_code = response.json()["error"]["code"]
        except (ValueError, KeyError, RuntimeError):
            pass
    return Observation(
        kind="http",
        status=response.status,
        error_code=error_code,
        detail=response.body.decode("utf-8", errors="replace")[:400],
    )


def _get_json(service: SubprocessService, path: str) -> tuple[int | None, dict[str, Any] | None]:
    try:
        response = service.request("GET", path)
    except ClientError:
        return None, None
    try:
        return response.status, response.json()
    except RuntimeError:
        return response.status, None


def _seed_inspection(
    service: SubprocessService,
    clip_id: str,
    inspection_id: str,
    payload: bytes,
) -> None:
    upload = _upload_clip(service, clip_id, payload)
    require_equal(upload.status, 201, f"seed clip {clip_id}")
    response = service.request_json(
        "POST",
        f"/v1/clips/{clip_id}/inspections",
        {
            "inspection_id": inspection_id,
            "sensitivity": 0.55,
            "sample_stride": 1,
            "min_scene_frames": 2,
        },
    )
    require_equal(response.status, 201, f"seed inspection {inspection_id}")


# --------------------------------------------------------------------------- #
# Scenario runner
# --------------------------------------------------------------------------- #


class ScenarioRunner:
    def __init__(self, workspace: Path, scenario: FaultScenario) -> None:
        self._workspace = workspace
        self._scenario = scenario
        self._root = workspace / "data"
        self._fault_config = workspace / "fault.json"
        self._event_file = workspace / "fault-events.jsonl"
        self._payload = build_sample_avi()
        self._payload_digest = digest_bytes(self._payload)
        self.failures: list[str] = []

        # Deterministic per-scenario identifiers.  The service caps
        # identifiers at 48 characters and only permits lowercase, so keep
        # the shared prefix short and lowercase.
        prefix = "".join(part[0] for part in scenario.scenario_id.split("-")).lower()
        self.keeper_clip = f"{prefix}-kclip"
        self.keeper_inspection = f"{prefix}-kinsp"
        self.keeper_sheet = f"{prefix}-ksheet"
        self.target_clip = f"{prefix}-tclip"
        self.target_inspection = f"{prefix}-tinsp"
        self.target_sheet = f"{prefix}-tsheet"
        self.neighbor_clip = f"{prefix}-nclip"
        self.neighbor_sheet = f"{prefix}-nsheet"

    def run(self) -> ScenarioReport:
        try:
            report = self._run_inner()
        except CheckFailure as exc:
            report = self._failure_report(str(exc))
        return report

    # -- setup ------------------------------------------------------------- #

    def _write_fault_config(self) -> None:
        if self._scenario.workflow == "clip_intake":
            target_id = self.target_clip
        else:
            target_id = self.target_sheet
        rendered_rule = self._scenario.rule.render(target_id)
        config = {
            "rules": [rendered_rule],
            "event_file": str(self._event_file),
        }
        self._fault_config.write_text(
            json.dumps(config, indent=2),
            encoding="utf-8",
        )

    def _seed_keepers(self, service: SubprocessService) -> None:
        """Legal objects that every fault and cleanup path must leave alone."""
        _seed_inspection(
            service,
            self.keeper_clip,
            self.keeper_inspection,
            self._payload,
        )
        sheet = _create_sheet(service, self.keeper_inspection, self.keeper_sheet)
        require_equal(sheet.status, 201, "seed keeper contact sheet")

    # -- core orchestration ------------------------------------------------ #

    def _run_inner(self) -> ScenarioReport:
        self._root.mkdir(parents=True, exist_ok=True)
        self._write_fault_config()

        # One manager owns the data root across the armed generation and the
        # clean restart, so generation counters and logs stay continuous.
        service = SubprocessService(
            data_root=self._root,
            fault_config=self._fault_config,
        )
        service.start()
        try:
            self._seed_keepers(service)
            baseline = DiskSnapshot.take(self._root)

            if self._scenario.workflow == "clip_intake":
                client_observation, neighbor_observation = (
                    self._exercise_clip_intake(service)
                )
            else:
                client_observation, neighbor_observation = (
                    self._exercise_contact_sheet(service)
                )

            # The injection event proves the fault point was really reached.
            self._require_fault_event()

            if self._scenario.rule.mode == "exit":
                process_exit = service.wait_for_exit()
                self._check(
                    process_exit == -9,
                    "subprocess must die from SIGKILL",
                )
            else:
                self._check(
                    service.is_alive(),
                    "ordinary exception must not terminate the service",
                )
        finally:
            if service.is_alive():
                service.stop()

        post_fault_snapshot = DiskSnapshot.take(self._root)

        # Restart with no fault rule against the same data root.
        service.start(fault_config=None)
        try:
            restart_evidence = self._reconcile_through_api(service)
            control_evidence = self._verify_controls(
                service,
                baseline,
                post_fault_snapshot,
            )
        finally:
            if service.is_alive():
                service.stop()

        disk_evidence = self._reconcile_through_disk(
            baseline,
            post_fault_snapshot,
        )
        self._assert_verdict(client_observation, restart_evidence, disk_evidence)

        return ScenarioReport(
            scenario_id=self._scenario.scenario_id,
            workflow=self._scenario.workflow,
            verdict=self._scenario.verdict,
            fault=self._rendered_rule(),
            client=client_observation,
            restart=restart_evidence,
            disk=disk_evidence,
            controls={
                "neighbor_request": neighbor_observation.as_dict(),
                **control_evidence,
            },
            undecidable_scope=self._scenario.undecidable_scope,
            failures=self.failures,
        )

    def _rendered_rule(self) -> dict[str, Any]:
        target_id = (
            self.target_clip
            if self._scenario.workflow == "clip_intake"
            else self.target_sheet
        )
        return self._scenario.rule.render(target_id)

    # -- workloads --------------------------------------------------------- #

    def _exercise_clip_intake(
        self,
        service: SubprocessService,
    ) -> tuple[Observation, Observation]:
        # An adjacent intake for a different clip runs concurrently so that
        # per-key locking and cleanup are proven not to cross object bounds.
        barrier = threading.Barrier(2)
        results: dict[str, Observation] = {}

        def neighbor() -> None:
            barrier.wait()
            results["neighbor"] = _upload_clip(
                service,
                self.neighbor_clip,
                self._payload,
            )

        thread = threading.Thread(target=neighbor, daemon=True)
        thread.start()
        barrier.wait()
        target = _upload_clip(service, self.target_clip, self._payload)
        thread.join(timeout=20.0)
        require(not thread.is_alive(), "neighbor intake thread hung")
        return target, results["neighbor"]

    def _exercise_contact_sheet(
        self,
        service: SubprocessService,
    ) -> tuple[Observation, Observation]:
        # The target sheet needs its own indexed inspection (no fault rule
        # matches these writes because ids differ from the keeper's).
        _seed_inspection(
            service,
            self.target_clip,
            self.target_inspection,
            self._payload,
        )
        barrier = threading.Barrier(2)
        results: dict[str, Observation] = {}

        def neighbor() -> None:
            barrier.wait()
            # The neighbor uses the keeper inspection and a distinct sheet id.
            results["neighbor"] = _create_sheet(
                service,
                self.keeper_inspection,
                self.neighbor_sheet,
            )

        thread = threading.Thread(target=neighbor, daemon=True)
        thread.start()
        barrier.wait()
        target = _create_sheet(
            service,
            self.target_inspection,
            self.target_sheet,
        )
        thread.join(timeout=20.0)
        require(not thread.is_alive(), "neighbor sheet thread hung")
        return target, results["neighbor"]

    # -- evidence ---------------------------------------------------------- #

    def _require_fault_event(self) -> None:
        require(
            self._event_file.exists(),
            "fault injection event file was never written",
        )
        events = [
            json.loads(line)
            for line in self._event_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        matching = [
            event
            for event in events
            if event.get("rule") == self._scenario.rule.rule_id
        ]
        require(
            len(matching) == 1,
            f"expected exactly one fault event, got {len(matching)}: {events}",
        )
        event = matching[0]
        require_equal(
            event.get("mode"),
            self._scenario.rule.mode,
            "fault event mode",
        )
        require_equal(
            event.get("when"),
            self._scenario.rule.when,
            "fault event timing",
        )

    def _reconcile_through_api(self, service: SubprocessService) -> dict[str, Any]:
        if self._scenario.workflow == "clip_intake":
            return self._reconcile_clip_api(service)
        return self._reconcile_sheet_api(service)

    def _reconcile_clip_api(self, service: SubprocessService) -> dict[str, Any]:
        status, body = _get_json(service, f"/v1/clips/{self.target_clip}")
        evidence: dict[str, Any] = {"get_clip_status": status}
        if status == 200 and body is not None:
            clip = body["clip"]
            evidence["record_state"] = clip["state"]
            evidence["record_digest"] = clip["digest"]
            evidence["digest_matches_upload"] = (
                clip["digest"] == self._payload_digest
            )
        # Re-using the same identifier distinguishes committed (409) from
        # absent (201) objects through the production conflict rule.
        re_put = _upload_clip(service, self.target_clip, self._payload)
        evidence["reuse_identifier_status"] = re_put.status
        if re_put.status == 200 or re_put.status == 201:
            # A second successful commit means the first attempt had not
            # committed; read the fresh record and remember its digest.
            status2, body2 = _get_json(service, f"/v1/clips/{self.target_clip}")
            evidence["post_reuse_get_status"] = status2
            if body2 is not None:
                evidence["post_reuse_digest_matches_upload"] = (
                    body2["clip"]["digest"] == self._payload_digest
                )
        return evidence

    def _reconcile_sheet_api(self, service: SubprocessService) -> dict[str, Any]:
        status, body = _get_json(
            service,
            f"/v1/contact-sheets/{self.target_sheet}",
        )
        evidence: dict[str, Any] = {
            "get_sheet_status": status,
        }
        if status == 200 and body is not None:
            sheet = body["contact_sheet"]
            evidence["record_state"] = sheet["state"]
            evidence["artifact_digest"] = sheet.get("artifact_digest")
            evidence["error_code"] = sheet.get("error_code")
            evidence["tile_count"] = len(sheet["tile_map"])

        artifact_status: int | None
        try:
            artifact = service.request(
                "GET",
                f"/v1/contact-sheets/{self.target_sheet}/artifact",
            )
            artifact_status = artifact.status
            if artifact.status == 200:
                evidence["artifact_fetch_digest"] = digest_bytes(artifact.body)
                header = parse_ppm_header(artifact.body)
                evidence["artifact_dimensions"] = [header.width, header.height]
        except ClientError as exc:
            artifact_status = None
            evidence["artifact_fetch_error"] = str(exc)
        evidence["artifact_fetch_status"] = artifact_status

        # Same identifier reuse: conflict once any record exists.
        reuse = _create_sheet(
            service,
            self.target_inspection,
            self.target_sheet,
        )
        evidence["reuse_identifier_status"] = reuse.status

        # A fresh sheet id must always work, proving the failed/stuck target
        # did not poison the inspection or the storage layer.
        fresh = _create_sheet(
            service,
            self.target_inspection,
            f"{self.target_sheet}-fresh",
        )
        evidence["fresh_identifier_status"] = fresh.status
        return evidence

    def _reconcile_through_disk(
        self,
        baseline: DiskSnapshot,
        post_fault: DiskSnapshot,
    ) -> dict[str, Any]:
        if self._scenario.workflow == "clip_intake":
            target_paths = clip_paths(self.target_clip)
            media_path = target_paths["media"]
            record_path = target_paths["record"]
        else:
            target_paths = sheet_paths(self.target_sheet)
            media_path = target_paths["artifact"]
            record_path = target_paths["record"]

        evidence: dict[str, Any] = {
            "target_media_present": post_fault.has(media_path),
            "target_record_present": post_fault.has(record_path),
            "stale_temporaries": sorted(find_stale_temporaries(post_fault)),
            "new_paths": sorted(post_fault.new_files(baseline)),
            "removed_paths": sorted(post_fault.removed_files(baseline)),
        }
        media_digest = post_fault.digest_of(media_path)
        if media_digest is not None:
            if self._scenario.workflow == "clip_intake":
                evidence["media_digest_matches_upload"] = (
                    media_digest == self._payload_digest
                )
            else:
                evidence["media_digest"] = media_digest
        record = post_fault.json_of(record_path)
        if record is not None:
            key = "clip" if "clip" in record else "contact_sheet"
            payload = record.get(key, record)
            evidence["record_state_on_disk"] = payload.get("state")
            evidence["record_digest_on_disk"] = payload.get(
                "digest"
            ) or payload.get("artifact_digest")
        return evidence

    def _verify_controls(
        self,
        service: SubprocessService,
        baseline: DiskSnapshot,
        post_fault: DiskSnapshot,
    ) -> dict[str, Any]:
        evidence: dict[str, Any] = {}

        # Keeper clip stays readable and byte-identical on disk.
        keeper_clip_status, keeper_clip_body = _get_json(
            service,
            f"/v1/clips/{self.keeper_clip}",
        )
        evidence["keeper_clip_status"] = keeper_clip_status
        keeper_media = clip_paths(self.keeper_clip)["media"]
        keeper_record = clip_paths(self.keeper_clip)["record"]
        evidence["keeper_media_unchanged"] = (
            baseline.digest_of(keeper_media) == post_fault.digest_of(keeper_media)
        )
        evidence["keeper_record_unchanged"] = (
            baseline.digest_of(keeper_record)
            == post_fault.digest_of(keeper_record)
        )

        # Keeper inspection stays indexed.
        inspection_status, inspection_body = _get_json(
            service,
            f"/v1/inspections/{self.keeper_inspection}",
        )
        evidence["keeper_inspection_status"] = inspection_status
        if inspection_body is not None:
            evidence["keeper_inspection_state"] = inspection_body["inspection"][
                "state"
            ]
        inspection_disk = inspection_path(self.keeper_inspection)
        evidence["keeper_inspection_unchanged"] = (
            baseline.digest_of(inspection_disk)
            == post_fault.digest_of(inspection_disk)
        )

        # Keeper sheet stays ready and its artifact bytes are untouched.
        keeper_sheet_status, keeper_sheet_body = _get_json(
            service,
            f"/v1/contact-sheets/{self.keeper_sheet}",
        )
        evidence["keeper_sheet_status"] = keeper_sheet_status
        if keeper_sheet_body is not None:
            evidence["keeper_sheet_state"] = keeper_sheet_body["contact_sheet"][
                "state"
            ]
        keeper_sheet_paths = sheet_paths(self.keeper_sheet)
        evidence["keeper_sheet_record_unchanged"] = (
            baseline.digest_of(keeper_sheet_paths["record"])
            == post_fault.digest_of(keeper_sheet_paths["record"])
        )
        evidence["keeper_sheet_artifact_unchanged"] = (
            baseline.digest_of(keeper_sheet_paths["artifact"])
            == post_fault.digest_of(keeper_sheet_paths["artifact"])
        )

        # Adjacent concurrent request: its outcome must be reconciled, never
        # assumed.  Under raise it must fully succeed; under exit both
        # possible outcomes are reconciled against the disk.
        evidence["neighbor"] = self._verify_neighbor(
            service,
            baseline,
            post_fault,
        )
        return evidence

    def _verify_neighbor(
        self,
        service: SubprocessService,
        baseline: DiskSnapshot,
        post_fault: DiskSnapshot,
    ) -> dict[str, Any]:
        if self._scenario.workflow == "clip_intake":
            paths = clip_paths(self.neighbor_clip)
            read_status, _ = _get_json(
                service,
                f"/v1/clips/{self.neighbor_clip}",
            )
            media_present = post_fault.has(paths["media"])
            record_present = post_fault.has(paths["record"])
            evidence = {
                "restart_read_status": read_status,
                "media_present_on_disk": media_present,
                "record_present_on_disk": record_present,
            }
            if self._scenario.rule.mode == "raise":
                self._check(
                    media_present and record_present,
                    "neighbor object must commit while the target fails",
                )
                self._check(
                    read_status == 200,
                    "neighbor object must read normally after the target failure",
                )
                return evidence
            # Clip publication order is media -> record.  SIGKILL therefore
            # permits exactly: nothing, orphan media only, or full commit.
            self._reconcile_clip_neighbor_exit(read_status, media_present, record_present, evidence)
        else:
            paths = sheet_paths(self.neighbor_sheet)
            read_status, body = _get_json(
                service,
                f"/v1/contact-sheets/{self.neighbor_sheet}",
            )
            media_present = post_fault.has(paths["artifact"])
            record_present = post_fault.has(paths["record"])
            evidence = {
                "restart_read_status": read_status,
                "media_present_on_disk": media_present,
                "record_present_on_disk": record_present,
            }
            if body is not None:
                evidence["restart_read_state"] = body["contact_sheet"]["state"]
            if self._scenario.rule.mode == "raise":
                self._check(
                    media_present and record_present,
                    "neighbor object must commit while the target fails",
                )
                self._check(
                    read_status == 200,
                    "neighbor object must read normally after the target failure",
                )
                return evidence
            # Sheet publication order is record (planned/compositing) ->
            # artifact -> record (ready).  SIGKILL permits: nothing, record
            # only (stuck non-terminal), artifact+record (still stuck because
            # the ready record never published), or full ready commit.
            self._reconcile_sheet_neighbor_exit(
                read_status,
                media_present,
                record_present,
                evidence,
            )
        return evidence

    def _reconcile_clip_neighbor_exit(
        self,
        read_status: int | None,
        media_present: bool,
        record_present: bool,
        evidence: dict[str, Any],
    ) -> None:
        if not media_present and not record_present:
            evidence["neighbor_outcome"] = "nothing_published"
            self._check(
                read_status == 404,
                "neighbor with no files must read as missing",
            )
        elif media_present and not record_present:
            evidence["neighbor_outcome"] = "indeterminate_orphan_media"
            self._check(
                read_status == 404,
                "neighbor orphan media without record must read as missing",
            )
        elif media_present and record_present:
            evidence["neighbor_outcome"] = "committed"
            self._check(
                read_status == 200,
                "fully published neighbor must read normally",
            )
        else:
            self._check(
                False,
                f"impossible clip neighbor state: record without media "
                f"(read={read_status})",
            )

    def _reconcile_sheet_neighbor_exit(
        self,
        read_status: int | None,
        media_present: bool,
        record_present: bool,
        evidence: dict[str, Any],
    ) -> None:
        state = evidence.get("restart_read_state")
        if not media_present and not record_present:
            evidence["neighbor_outcome"] = "nothing_published"
            self._check(
                read_status == 404,
                "neighbor with no files must read as missing",
            )
        elif record_present and not media_present:
            evidence["neighbor_outcome"] = "indeterminate_record_only"
            self._check(
                read_status == 200 and state in {"planned", "compositing"},
                "record-only neighbor must be a readable non-terminal job",
            )
        elif media_present and record_present:
            if state == "ready":
                evidence["neighbor_outcome"] = "committed"
                self._check(
                    read_status == 200,
                    "ready neighbor must read normally",
                )
            else:
                evidence["neighbor_outcome"] = "indeterminate_orphan_artifact"
                self._check(
                    read_status == 200 and state in {"planned", "compositing"},
                    "neighbor with artifact but no ready record must stay "
                    "non-terminal; the service must not invent readiness",
                )
        else:
            self._check(
                False,
                f"impossible sheet neighbor state: artifact without record "
                f"(read={read_status})",
            )

    # -- verdict assertions ------------------------------------------------ #

    def _assert_verdict(
        self,
        client: Observation,
        restart: dict[str, Any],
        disk: dict[str, Any],
    ) -> None:
        mode = self._scenario.rule.mode
        if mode == "raise":
            self._check(
                client.kind == "http" and client.status == 500,
                "raise fault must surface as HTTP 500, not a dead connection",
            )
            # An ordinary exception runs every finally/rollback path, so no
            # interrupted write may survive anywhere in the data root.
            self._check(
                disk["stale_temporaries"] == [],
                f"raise fault must clean every .tmp file: "
                f"{disk['stale_temporaries']}",
            )
        else:
            self._check(
                client.kind == "connection_failure",
                "SIGKILL must surface as a connection-level failure",
            )
            # Under SIGKILL no finally runs.  Any surviving .tmp belongs to an
            # interrupted write (the target or a concurrent neighbor) and is
            # recorded as process-death evidence rather than treated as a
            # cleanup failure; the spec promises no startup reaping.
            disk["stale_temporaries_after_sigkill"] = disk.pop(
                "stale_temporaries"
            )
            disk["stale_temporaries"] = []

        if self._scenario.workflow == "clip_intake":
            self._assert_clip_verdict(restart, disk)
        else:
            self._assert_sheet_verdict(restart, disk)

    def _assert_clip_verdict(
        self,
        restart: dict[str, Any],
        disk: dict[str, Any],
    ) -> None:
        verdict: Verdict = self._scenario.verdict
        if verdict == "NOT_COMMITTED":
            self._check(
                not disk["target_media_present"],
                "rollback must remove the target clip media",
            )
            self._check(
                not disk["target_record_present"],
                "no target clip record may be visible",
            )
            self._check(
                restart["get_clip_status"] == 404,
                "restarted service must report the clip missing",
            )
            self._check(
                restart["reuse_identifier_status"] in (200, 201),
                "identifier must be reusable after a non-committed attempt",
            )
            self._check(
                restart.get("post_reuse_digest_matches_upload") is True,
                "reused identifier must commit bytes matching the upload",
            )
        elif verdict == "COMMITTED":
            self._check(
                disk["target_media_present"] and disk["target_record_present"],
                "committed clip requires both media and record on disk",
            )
            self._check(
                disk.get("media_digest_matches_upload") is True,
                "committed media digest must match the upload",
            )
            self._check(
                disk.get("record_digest_on_disk") == self._payload_digest,
                "on-disk record digest must match the upload",
            )
            self._check(
                restart["get_clip_status"] == 200,
                "restarted service must read the committed clip",
            )
            self._check(
                restart.get("digest_matches_upload") is True,
                "restarted record must still describe the original upload",
            )
            self._check(
                restart["reuse_identifier_status"] == 409,
                "committed identifier must conflict on reuse",
            )
        else:  # INDETERMINATE
            self._check(
                disk["target_media_present"] and not disk["target_record_present"],
                "indeterminate clip state requires orphan media without record",
            )
            self._check(
                disk.get("media_digest_matches_upload") is True,
                "orphan media must still be the complete uploaded bytes",
            )
            self._check(
                restart["get_clip_status"] == 404,
                "service must not treat orphan media as a registered clip",
            )
            self._check(
                restart["reuse_identifier_status"] in (200, 201),
                "stale lock must not prevent reuse of the identifier",
            )
            self._check(
                bool(self._scenario.undecidable_scope),
                "indeterminate scenario must document its undecidable scope",
            )

    def _assert_sheet_verdict(
        self,
        restart: dict[str, Any],
        disk: dict[str, Any],
    ) -> None:
        verdict: Verdict = self._scenario.verdict
        if verdict == "NOT_COMMITTED":
            self._check(
                not disk["target_media_present"],
                "failure cleanup must remove the target PPM artifact",
            )
            self._check(
                disk["target_record_present"],
                "failed sheet must retain its terminal record",
            )
            self._check(
                disk.get("record_state_on_disk") == "failed",
                "on-disk sheet record must be in failed state",
            )
            self._check(
                restart["get_sheet_status"] == 200,
                "failed sheet record remains readable",
            )
            self._check(
                restart["record_state"] == "failed",
                "restarted read must show failed state",
            )
            self._check(
                restart["artifact_fetch_status"] == 409,
                "artifact must not be served from a failed sheet",
            )
            self._check(
                restart["reuse_identifier_status"] == 409,
                "failed sheet identifier remains reserved",
            )
            self._check(
                restart["fresh_identifier_status"] == 201,
                "a new sheet must still compose after failure",
            )
        elif verdict == "COMMITTED":
            self._check(
                disk["target_media_present"] and disk["target_record_present"],
                "ready sheet requires both artifact and record",
            )
            self._check(
                disk.get("record_state_on_disk") == "ready",
                "on-disk record must be ready",
            )
            self._check(
                restart["get_sheet_status"] == 200
                and restart["record_state"] == "ready",
                "restarted service must read the ready sheet",
            )
            self._check(
                restart["artifact_fetch_status"] == 200,
                "artifact must be fetchable after restart",
            )
            self._check(
                restart["artifact_fetch_digest"]
                == restart["artifact_digest"],
                "served artifact digest must match the ready record",
            )
            self._check(
                disk.get("record_digest_on_disk")
                == restart["artifact_fetch_digest"],
                "disk, record, and served bytes must be one operation",
            )
        else:  # INDETERMINATE
            self._check(
                disk["target_media_present"] and disk["target_record_present"],
                "indeterminate sheet state keeps artifact plus non-terminal record",
            )
            self._check(
                disk.get("record_state_on_disk") == "compositing",
                "record must remain stuck in compositing, not auto-finalized",
            )
            self._check(
                disk.get("record_digest_on_disk") is None,
                "stuck record must not reference the orphan artifact digest",
            )
            self._check(
                restart["record_state"] == "compositing",
                "restart must not invent a terminal state",
            )
            self._check(
                restart["artifact_fetch_status"] == 409,
                "orphan artifact must not be served while non-terminal",
            )
            self._check(
                restart["reuse_identifier_status"] == 409,
                "stuck sheet identifier remains reserved",
            )
            self._check(
                restart["fresh_identifier_status"] == 201,
                "other sheets must still compose despite the stuck one",
            )
            self._check(
                bool(self._scenario.undecidable_scope),
                "indeterminate scenario must document its undecidable scope",
            )

    # -- helpers ----------------------------------------------------------- #

    def _check(self, condition: bool, message: str) -> None:
        if not condition:
            self.failures.append(message)

    def _failure_report(self, message: str) -> ScenarioReport:
        return ScenarioReport(
            scenario_id=self._scenario.scenario_id,
            workflow=self._scenario.workflow,
            verdict=self._scenario.verdict,
            fault=self._scenario.rule.as_json(),
            client=Observation(kind="harness-error", detail=message),
            restart={},
            disk={},
            controls={},
            undecidable_scope=self._scenario.undecidable_scope,
            failures=[message],
        )


# --------------------------------------------------------------------------- #
# Restart baseline (no fault)
# --------------------------------------------------------------------------- #


def verify_clean_restart_baseline(workspace: Path) -> dict[str, Any]:
    """A normal restart, with no fault injected, must preserve every object.

    This isolates restart durability from failure handling: it proves the
    reconciliation machinery and the service itself read a healthy data root
    identically across two process generations.
    """
    data_root = workspace / "data"
    payload = build_sample_avi()
    failures: list[str] = []
    service = SubprocessService(data_root=data_root)
    service.start()
    try:
        _seed_inspection(service, "base-clip", "base-insp", payload)
        sheet = _create_sheet(service, "base-insp", "base-sheet")
        require_equal(sheet.status, 201, "baseline sheet creation")
        before = DiskSnapshot.take(data_root)

        # Fetch the artifact through the first generation for a digest.
        artifact = service.request(
            "GET",
            "/v1/contact-sheets/base-sheet/artifact",
        )
        require_equal(artifact.status, 200, "baseline artifact fetch")
        first_artifact_digest = digest_bytes(artifact.body)
    finally:
        if service.is_alive():
            service.stop()

    # Restart with no fault configuration at all.
    service.start(fault_config=None)
    try:
        after = DiskSnapshot.take(data_root)
        clip_status, clip_body = _get_json(service, "/v1/clips/base-clip")
        insp_status, insp_body = _get_json(service, "/v1/inspections/base-insp")
        sheet_status, sheet_body = _get_json(service, "/v1/contact-sheets/base-sheet")
        artifact2 = service.request(
            "GET",
            "/v1/contact-sheets/base-sheet/artifact",
        )
        health = service.request("GET", "/health")
    finally:
        if service.is_alive():
            service.stop()

    # Service generation logs are operational output, not persisted domain
    # data, and a restart legitimately creates a new one.
    def data_paths(snapshot: DiskSnapshot) -> set[str]:
        return {
            path
            for path in snapshot.relative_paths()
            if not Path(path).name.startswith("service-generation-")
        }

    before_paths = data_paths(before)
    after_paths = data_paths(after)

    def check(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    check(health.status == 200, "restarted service must be healthy")
    check(clip_status == 200, "clip must read after clean restart")
    check(insp_status == 200, "inspection must read after clean restart")
    check(sheet_status == 200, "sheet must read after clean restart")
    if clip_body is not None:
        check(
            clip_body["clip"]["digest"] == digest_bytes(payload),
            "clip digest must survive a clean restart",
        )
    if insp_body is not None:
        check(
            insp_body["inspection"]["state"] == "indexed",
            "inspection must remain indexed after restart",
        )
    if sheet_body is not None:
        check(
            sheet_body["contact_sheet"]["state"] == "ready",
            "sheet must remain ready after restart",
        )
    check(
        artifact2.status == 200
        and digest_bytes(artifact2.body) == first_artifact_digest,
        "artifact bytes must be identical across restart",
    )
    check(
        after_paths == before_paths,
        "clean restart must not add or remove any persisted data file",
    )
    changed_domain_files = {
        path
        for path in after.changed_files(before)
        if not Path(path).name.startswith("service-generation-")
    }
    check(
        changed_domain_files == set(),
        "clean restart must not modify any persisted data file",
    )
    return {
        "scenario_id": "R0-clean-restart-baseline",
        "verdict": "COMMITTED",
        "failures": failures,
        "files_preserved": len(after.relative_paths()),
        "artifact_digest": first_artifact_digest,
    }


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main() -> int:
    import os

    workspace_parent = Path(tempfile.mkdtemp(prefix="frame-proxy-fault-"))
    keep_workspace = os.environ.get("FRAME_PROXY_KEEP_WORKSPACE") == "1"
    # The report is written outside the per-scenario data roots so it can be
    # inspected after the temporary work area is removed.
    report_path = Path.cwd() / "persistence-fault-report.json"
    scenario_reports: list[ScenarioReport] = []
    raw_reports: list[dict[str, Any]] = []
    try:
        # No-fault control: a healthy data root must survive a plain restart.
        baseline_workspace = workspace_parent / "R0-clean-restart-baseline"
        baseline_workspace.mkdir(parents=True, exist_ok=True)
        baseline_report = verify_clean_restart_baseline(baseline_workspace)
        raw_reports.append(baseline_report)
        baseline_state = "PASS" if not baseline_report["failures"] else "FAIL"
        print(f"{baseline_state} {baseline_report['scenario_id']} [COMMITTED] no fault")
        for failure in baseline_report["failures"]:
            print(f"    - {failure}")

        for scenario in ALL_SCENARIOS:
            workspace = workspace_parent / scenario.scenario_id
            workspace.mkdir(parents=True, exist_ok=True)
            report = ScenarioRunner(workspace, scenario).run()
            scenario_reports.append(report)
            raw_reports.append(report.as_dict())
            status = "PASS" if not report.failures else "FAIL"
            print(
                f"{status} {report.scenario_id} "
                f"[{report.verdict}] fault={report.fault['mode']}/"
                f"{report.fault['when']}"
            )
            for failure in report.failures:
                print(f"    - {failure}")

        all_failures = [
            item for item in raw_reports if item.get("failures")
        ]
        report_path.write_text(
            json.dumps(
                {
                    "summary": {
                        "total": len(raw_reports),
                        "passed": len(raw_reports) - len(all_failures),
                        "failed": len(all_failures),
                        "indeterminate_documented": [
                            item.scenario_id
                            for item in scenario_reports
                            if item.verdict == "INDETERMINATE"
                        ],
                    },
                    "scenarios": raw_reports,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\nfull reconciliation report: {report_path}")
        return 0 if not all_failures else 1
    finally:
        if keep_workspace:
            print(f"workspace retained at: {workspace_parent}")
        else:
            shutil.rmtree(workspace_parent, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
