from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path


class SyncLogger:
    """Appends structured sync events to a file (next to index.json).

    Each run is separated by a header line.  Stack traces from unexpected
    errors are written here rather than to stderr; callers should print a
    brief "see sync.log" message to the user instead.
    """

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._t0 = time.monotonic()
        # Use a unique logger name to allow multiple instances in tests.
        name = f"trainings_sync._sync.{id(self)}"
        self._log = logging.getLogger(name)
        self._log.setLevel(logging.DEBUG)
        self._log.propagate = False
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        self._log.addHandler(handler)

    @property
    def path(self) -> Path:
        return self._path

    def info(self, msg: str) -> None:
        self._log.info(msg)

    def debug(self, msg: str) -> None:
        self._log.debug(msg)

    def warning(self, msg: str) -> None:
        self._log.warning(msg)

    def error(self, msg: str, *, exc_info: bool = False) -> None:
        self._log.error(msg, exc_info=exc_info)

    def run_start(self, start: date, end: date, *, force: bool) -> None:
        self._t0 = time.monotonic()
        self._log.info("=" * 60)
        self._log.info("Sync run: %s -> %s  force=%s", start, end, force)

    def run_end(self) -> None:
        elapsed = time.monotonic() - self._t0
        self._log.info("Sync run finished in %.1fs", elapsed)

    def close(self) -> None:
        for h in list(self._log.handlers):
            h.close()
            self._log.removeHandler(h)
