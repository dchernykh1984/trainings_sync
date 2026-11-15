"""Fakes shared by the tests of the connectors built on HttpApiConnector."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import requests

from app.tracking.tracker import ProgressRenderer, Task

# The shortest bytes the connectors take for a FIT file: ".FIT" at offset 8.
FIT = b"\x0e\x10\x00\x00\x00\x00\x00\x00.FIT" + b"\x00" * 8


class FakeRenderer(ProgressRenderer):
    def __init__(self) -> None:
        self.failed: list[Task] = []
        self.warnings: list[str] = []

    def on_task_added(self, task: Task) -> None:
        pass

    def on_progress(self, task: Task) -> None:
        pass

    def on_task_done(self, task: Task) -> None:
        pass

    def on_task_failed(self, task: Task) -> None:
        self.failed.append(task)

    def on_task_warning(self, task: Task, message: str) -> None:
        self.warnings.append(message)

    def on_total_updated(self, task: Task) -> None:
        pass


def resp(
    status: int = 200,
    *,
    body: object = None,
    content: bytes = b"",
    headers: dict[str, str] | None = None,
) -> requests.Response:
    """A response as requests returns it: JSON `body`, else raw `content`."""
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode() if body is not None else content
    r.headers.update(headers or {})
    r.url = "https://api.test/x"
    return r


def calls(http: MagicMock) -> list[tuple[str, str]]:
    """(method, url) of every call made through the faked Session.request."""
    return [(call.args[0], call.args[1]) for call in http.call_args_list]


def logged(log: MagicMock, level: str) -> str:
    return " ".join(str(call.args[0]) for call in getattr(log, level).call_args_list)
