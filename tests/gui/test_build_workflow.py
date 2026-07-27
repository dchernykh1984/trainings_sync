"""The build workflow and the app must agree on what the app is called.

Nothing else ties the two together: the workflow stamps the name into the
macOS bundle, the app sets the same name on the Qt application, and a rename
on one side alone is exactly how the dock ended up showing "trainings-sync".
"""

from __future__ import annotations

import re
from pathlib import Path

from app.gui.app import APP_DISPLAY_NAME

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "build.yml"


def _workflow_env(name: str) -> str:
    text = WORKFLOW.read_text(encoding="utf-8")
    match = re.search(rf"^  {re.escape(name)}: *(\S.*?) *$", text, re.MULTILINE)
    assert match is not None, f"{name} is not set in {WORKFLOW.name}"
    return match.group(1)


def _project_name() -> str:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^name *= *"(.+?)"', text, re.MULTILINE)
    assert match is not None, "pyproject.toml has no project name"
    return match.group(1)


def test_workflow_bundle_name_matches_the_app() -> None:
    # macOS shows the .app folder name in the dock and ignores the plist's
    # CFBundleName/CFBundleDisplayName, so the bundle name *is* the dock name.
    # It has to stay space-free, hence the display name with spaces removed.
    assert _workflow_env("APP_BUNDLE_NAME") == APP_DISPLAY_NAME.replace(" ", "")


def test_workflow_bundle_name_has_no_spaces() -> None:
    # The bundle is a file people copy between folders; spaces in it make
    # every shell command that touches it need quoting.
    assert " " not in _workflow_env("APP_BUNDLE_NAME")


def test_workflow_bundle_id_is_reverse_dns_for_this_project() -> None:
    # Copied between six sibling repos, so pin it to this project's own name -
    # a bundle id left pointing at a sibling would silently make macOS treat
    # the two apps as one.
    bundle_id = _workflow_env("APP_BUNDLE_ID")
    assert re.fullmatch(r"[a-z0-9]+(\.[a-z0-9-]+)+", bundle_id), bundle_id
    assert bundle_id.rsplit(".", 1)[-1] == _project_name()
