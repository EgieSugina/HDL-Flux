from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path


class StateManager:
    def __init__(self, path: Path, error_max_len: int = 200):
        self.path = path
        self._error_max_len = error_max_len
        self._lock = threading.Lock()
        self._data: dict = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text("utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        return {"urls": {}}

    def _save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, ensure_ascii=False), "utf-8")
        tmp.replace(self.path)

    def mark_start(self, url: str, name: str):
        with self._lock:
            self._data["urls"][url] = {
                "status": "downloading",
                "name": name,
                "started": datetime.now().isoformat(),
            }
            self._save()

    def mark_done(self, url: str, output: str):
        with self._lock:
            e = self._data["urls"].setdefault(url, {})
            e.update(
                status="completed",
                output=output,
                completed=datetime.now().isoformat(),
            )
            self._save()

    def mark_failed(self, url: str, error: str = ""):
        with self._lock:
            e = self._data["urls"].setdefault(url, {})
            e.update(status="failed", error=str(error)[: self._error_max_len])
            self._save()

    def is_done(self, url: str) -> bool:
        return self._data["urls"].get(url, {}).get("status") == "completed"

    def pending(self, urls: list[str]) -> list[str]:
        return [u for u in urls if not self.is_done(u)]

    def stats(self) -> tuple[int, int, int]:
        c = f = p = 0
        for v in self._data["urls"].values():
            s = v.get("status")
            if s == "completed":
                c += 1
            elif s == "failed":
                f += 1
            elif s == "downloading":
                p += 1
        return c, f, p

    def clear(self):
        with self._lock:
            self._data = {"urls": {}}
            self._save()

