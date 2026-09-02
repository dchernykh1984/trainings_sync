#!/usr/bin/env python3
"""PostToolUse guard: keep non-ASCII characters out of the files pre-commit checks.

The repository's pre-commit config rejects any byte above 0x7F in the listed file types,
so an accidental Cyrillic letter or a typographic dash otherwise only surfaces at commit
time, after the work is done. This catches it at the edit that introduced it: it reads
the hook payload on stdin and exits 2 with an explanation when the file just written or
edited contains non-ASCII characters, which sends the message back to the agent as a
blocking error.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Mirrors the types listed for the no-non-ascii hook in .pre-commit-config.yaml.
CHECKED_SUFFIXES = {".py", ".yml", ".yaml", ".md", ".toml", ".sh", ".json"}
# Generated from commit messages, which may legitimately be non-ASCII.
EXEMPT_NAMES = {"CHANGELOG.md", "uv.lock"}
MAX_REPORTED_LINES = 5


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except ValueError:
        return 0  # JSONDecodeError subclasses ValueError; nothing to fail an edit over

    raw_path = (payload.get("tool_input") or {}).get("file_path")
    if not raw_path:
        return 0

    path = Path(raw_path)
    if path.suffix not in CHECKED_SUFFIXES or path.name in EXEMPT_NAMES:
        return 0
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return 0
    except UnicodeDecodeError:
        return 0

    offenders = []
    for number, line in enumerate(text.splitlines(), start=1):
        bad = sorted({ch for ch in line if ord(ch) > 0x7F})
        if bad:
            offenders.append((number, "".join(bad)))

    if not offenders:
        return 0

    shown = offenders[:MAX_REPORTED_LINES]
    detail = "; ".join(f"line {number}: {chars}" for number, chars in shown)
    if len(offenders) > len(shown):
        detail += f"; and {len(offenders) - len(shown)} more line(s)"
    print(
        f"{path.name} contains non-ASCII characters ({detail}). "
        "The no-non-ascii pre-commit hook will reject this file -- replace them "
        "(for example an em dash with --) before moving on.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
