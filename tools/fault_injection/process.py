"""Real subprocess lifecycle for the persistence fault verification.

Unlike the in-process smoke harness, this manager launches the production
service exactly the way an operator does -- ``python -m frame_proxy serve``
-- so that a simulated sudden exit kills an entire independent interpreter,
including every worker thread and the listening socket.

Each manager instance owns one independent data root and one fault
configuration.  Restarting the process reuses the same data root, which is
what makes post-restart reconciliation possible.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
INJECTION_DIR = Path(__file__).resolve().parent

# Sentinel distinguishing "argument omitted" from an explicit disarm (None).
_UNSET = object()


class ClientError(Exception):
    """The HTTP call never produced a response (connection-level failure)."""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> dict[str, Any]:
        value = json.loads(self.body.decode("utf-8"))
        if not isinstance(value, dict):
            raise RuntimeError("response JSON was not an object")
        return value


class SubprocessService:
    def __init__(
        self,
        *,
        data_root: Path,
        fault_config: Path | None = None,
        startup_timeout: float = 15.0,
        request_timeout: float = 10.0,
    ) -> None:
        self._data_root = data_root
        self._fault_config = fault_config
        self._startup_timeout = startup_timeout
        self._request_timeout = request_timeout
        self._process: subprocess.Popen[bytes] | None = None
        self._port: int | None = None
        self._generation = 0
        self._generation_log: Path | None = None

    @property
    def port(self) -> int:
        if self._port is None:
            raise RuntimeError("service has not reported a port")
        return self._port

    @property
    def pid(self) -> int:
        if self._process is None:
            raise RuntimeError("service is not running")
        return int(self._process.pid)

    @property
    def log_tail(self) -> str:
        chunks: list[str] = []
        for generation in range(1, self._generation + 1):
            path = self._data_root / f"service-generation-{generation}.log"
            try:
                chunks.append(
                    path.read_text(encoding="utf-8", errors="replace")
                )
            except OSError:
                continue
        return "".join(chunks)

    def start(self, *, fault_config: Path | None | object = _UNSET) -> None:
        if self._process is not None:
            raise RuntimeError("service is already running; call stop() first")
        # Explicit None disarms injection on a restart; omitting the argument
        # keeps whatever configuration the manager was constructed with.
        if fault_config is not _UNSET:
            self._fault_config = None if fault_config is None else Path(
                fault_config
            )
        self._data_root.mkdir(parents=True, exist_ok=True)
        self._generation += 1
        # Each process generation gets its own log, so a restart can never
        # parse a previous generation's listening marker.
        self._generation_log = (
            self._data_root / f"service-generation-{self._generation}.log"
        )
        log_handle = open(self._generation_log, "ab")  # noqa: SIM115
        environment = dict(os.environ)
        python_path = [str(SRC_DIR), str(REPO_ROOT), str(INJECTION_DIR)]
        existing = environment.get("PYTHONPATH")
        if existing:
            python_path.append(existing)
        environment["PYTHONPATH"] = os.pathsep.join(python_path)
        environment["FRAME_PROXY_DATA_DIR"] = str(self._data_root)
        # Always make the intent explicit: a restarted process must never
        # inherit a stale fault rule from the parent environment.
        environment.pop("FRAME_PROXY_FAULT_CONFIG", None)
        if self._fault_config is not None:
            environment["FRAME_PROXY_FAULT_CONFIG"] = str(self._fault_config)
        self._process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "frame_proxy",
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                "0",
            ],
            cwd=str(REPO_ROOT),
            env=environment,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        # The child keeps its own duplicate of the descriptor; the parent's
        # copy must be closed so we do not leak one file handle per restart.
        log_handle.close()
        self._port = self._wait_for_listener()
        self._wait_for_health()

    def _wait_for_listener(self) -> int:
        deadline = time.monotonic() + self._startup_timeout
        marker = "listening on http://127.0.0.1:"
        while time.monotonic() < deadline:
            if self._process is not None and self._process.poll() is not None:
                raise RuntimeError(
                    f"service exited during startup (code {self._process.returncode}):\n"
                    f"{self.log_tail}"
                )
            if self._generation_log is None:
                time.sleep(0.02)
                continue
            try:
                text = self._generation_log.read_text(
                    encoding="utf-8",
                    errors="replace",
                )
            except OSError:
                text = ""
            if marker in text:
                tail = text.split(marker, 1)[1]
                digits = ""
                for char in tail:
                    if char.isdigit():
                        digits += char
                    elif digits:
                        break
                if digits:
                    return int(digits)
            time.sleep(0.02)
        raise RuntimeError(f"service did not bind within {self._startup_timeout}")

    def _wait_for_health(self) -> None:
        deadline = time.monotonic() + self._startup_timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                response = self.request("GET", "/health")
                if response.status == 200:
                    return
            except ClientError as exc:  # socket may not accept yet
                last_error = exc
            time.sleep(0.05)
        raise RuntimeError(f"service never became healthy: {last_error}")

    def stop(self, *, assert_exit_code: int | None = None) -> int:
        if self._process is None:
            raise RuntimeError("service is not running")
        process = self._process
        try:
            process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5.0)
        code = int(process.returncode)
        if assert_exit_code is not None and code != assert_exit_code:
            raise RuntimeError(
                f"expected exit code {assert_exit_code}, got {code}\n{self.log_tail}"
            )
        self._process = None
        return code

    def is_alive(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def wait_for_exit(self, timeout: float = 15.0) -> int:
        if self._process is None:
            raise RuntimeError("service is not running")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = self._process.poll()
            if code is not None:
                finished = self._process
                self._process = None
                return int(finished.returncode)
            time.sleep(0.05)
        raise RuntimeError("service did not exit within the timeout")

    def request(
        self,
        method: str,
        path: str,
        body: bytes = b"",
        content_type: str = "application/json",
    ) -> HttpResponse:
        connection = http.client.HTTPConnection(
            "127.0.0.1",
            self.port,
            timeout=self._request_timeout,
        )
        try:
            connection.request(
                method,
                path,
                body=body,
                headers={"Content-Type": content_type, "Connection": "close"},
            )
            response = connection.getresponse()
            data = response.read()
            return HttpResponse(
                status=response.status,
                headers=dict(response.getheaders()),
                body=data,
            )
        except (
            http.client.RemoteDisconnected,
            http.client.HTTPException,
            ConnectionError,
            socket.timeout,
            OSError,
        ) as exc:
            raise ClientError(f"{type(exc).__name__}: {exc}") from exc
        finally:
            connection.close()

    def request_json(
        self,
        method: str,
        path: str,
        value: dict[str, Any],
    ) -> HttpResponse:
        return self.request(
            method,
            path,
            json.dumps(value, separators=(",", ":")).encode("utf-8"),
        )
