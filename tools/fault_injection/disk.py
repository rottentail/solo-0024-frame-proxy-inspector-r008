"""Independent on-disk inspection of a Frame Proxy data root.

Every function here reads plain files directly.  It never opens an HTTP
connection or constructs a production repository, so it answers a different
question than the API does: what bytes and JSON documents actually survived
on the volume, regardless of what the service currently considers visible.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class DiskFile:
    relative_path: str
    size: int
    digest: str

    @classmethod
    def collect(cls, root: Path, path: Path) -> "DiskFile":
        payload = path.read_bytes()
        return cls(
            relative_path=str(path.relative_to(root)),
            size=len(payload),
            digest=hashlib.sha256(payload).hexdigest(),
        )


@dataclass(slots=True)
class DiskSnapshot:
    root: Path
    files: dict[str, DiskFile] = field(default_factory=dict)

    @classmethod
    def take(cls, root: Path) -> "DiskSnapshot":
        snapshot = cls(root=root)
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            entry = DiskFile.collect(root, path)
            snapshot.files[entry.relative_path] = entry
        return snapshot

    def relative_paths(self) -> set[str]:
        return set(self.files)

    def digest_of(self, relative_path: str) -> str | None:
        entry = self.files.get(relative_path)
        return None if entry is None else entry.digest

    def size_of(self, relative_path: str) -> int | None:
        entry = self.files.get(relative_path)
        return None if entry is None else entry.size

    def has(self, relative_path: str) -> bool:
        return relative_path in self.files

    def paths_with_suffix(self, suffix: str) -> set[str]:
        return {path for path in self.files if path.endswith(suffix)}

    def paths_under(self, prefix: str) -> set[str]:
        return {path for path in self.files if path.startswith(prefix)}

    def json_of(self, relative_path: str) -> dict[str, Any] | None:
        entry = self.files.get(relative_path)
        if entry is None:
            return None
        return json.loads((self.root / relative_path).read_text(encoding="utf-8"))

    def bytes_of(self, relative_path: str) -> bytes | None:
        if relative_path not in self.files:
            return None
        return (self.root / relative_path).read_bytes()

    def new_files(self, baseline: "DiskSnapshot") -> set[str]:
        return self.relative_paths() - baseline.relative_paths()

    def removed_files(self, baseline: "DiskSnapshot") -> set[str]:
        return baseline.relative_paths() - self.relative_paths()

    def changed_files(self, baseline: "DiskSnapshot") -> set[str]:
        changed: set[str] = set()
        for relative_path, entry in self.files.items():
            previous = baseline.files.get(relative_path)
            if previous is not None and previous.digest != entry.digest:
                changed.add(relative_path)
        return changed


def clip_paths(clip_id: str) -> dict[str, str]:
    base = f"clips/{clip_id}"
    return {
        "dir": base,
        "media": f"{base}/clip.avi",
        "record": f"{base}/record.json",
    }


def inspection_path(inspection_id: str) -> str:
    return f"inspections/{inspection_id}.json"


def sheet_paths(sheet_id: str) -> dict[str, str]:
    base = f"contact-sheets/{sheet_id}"
    return {
        "dir": base,
        "record": f"{base}/record.json",
        "artifact": f"{base}/sheet.ppm",
    }


def find_stale_temporaries(snapshot: DiskSnapshot) -> set[str]:
    """Files still carrying the NamedTemporaryFile naming of atomic writes."""
    return {
        path
        for path in snapshot.relative_paths()
        if Path(path).name.startswith(".") and path.endswith(".tmp")
    }


def digest_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
