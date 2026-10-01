"""Exercise Codex patch payloads, exemptions and repository boundaries."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("post_edit.py")
SPEC = importlib.util.spec_from_file_location("post_edit", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Cannot load hook")
HOOK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HOOK)


class PostEditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.checked = self.root / "source.md"
        self.checked.write_text("plain ASCII\n", encoding="utf-8")
        self.checks = {"files": r"\.md$", "exclude": r"^translations/"}

    def payload(self, command: str, **extra: str) -> dict:
        return {"cwd": str(self.root), "tool_input": {"command": command}, **extra}

    def test_multi_file_patch_reports_every_offending_file(self) -> None:
        other = self.root / "other.md"
        other.write_text(chr(0x416), encoding="utf-8")
        self.checked.write_bytes(b"\xff")
        paths = HOOK.edited_paths(
            self.payload("*** Update File: source.md\n*** Add File: other.md\n"),
            self.root,
        )
        self.assertEqual(
            HOOK.violations(paths, self.root, self.checks),
            ["source.md", "other.md"],
        )

    def test_crlf_patch_checks_every_edited_and_moved_file(self) -> None:
        other = self.root / "other.md"
        moved = self.root / "moved.md"
        for path in [self.checked, other, moved]:
            path.write_bytes(b"\xff")
        paths = HOOK.edited_paths(
            self.payload(
                "*** Begin Patch\r\n"
                "*** Update File: source.md\r\n"
                "*** Add File: other.md\r\n"
                "*** Update File: old.md\r\n"
                "*** Move to: moved.md\r\n"
                "*** End Patch\r\n"
            ),
            self.root,
        )
        self.assertEqual(
            HOOK.violations(paths, self.root, self.checks),
            ["source.md", "other.md", "moved.md"],
        )

    def test_move_checks_the_destination(self) -> None:
        self.checked.write_text(chr(0x416), encoding="utf-8")
        paths = HOOK.edited_paths(
            self.payload("*** Update File: missing.md\n*** Move to: source.md\n"),
            self.root,
        )
        self.assertEqual(HOOK.violations(paths, self.root, self.checks), ["source.md"])

    def test_delete_and_missing_files_are_ignored(self) -> None:
        paths = HOOK.edited_paths(
            self.payload("*** Delete File: source.md\n*** Add File: missing.md\n"),
            self.root,
        )
        self.assertEqual(paths, [])

    def test_subdirectory_payload_resolves_relative_files(self) -> None:
        subdir = self.root / "nested"
        subdir.mkdir()
        paths = HOOK.edited_paths(
            self.payload("*** Update File: ../source.md\n", cwd=str(subdir)),
            self.root,
        )
        self.assertEqual(paths, [self.checked])

    def test_paths_outside_the_repository_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as outside:
            external = Path(outside) / "external.md"
            external.write_text(chr(0x416), encoding="utf-8")
            paths = HOOK.edited_paths(
                self.payload(f"*** Add File: {external}\n"), self.root
            )
            self.assertEqual(paths, [])

    def test_symlink_to_outside_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as outside:
            external = Path(outside) / "external.md"
            external.write_text(chr(0x416), encoding="utf-8")
            link = self.root / "link.md"
            try:
                link.symlink_to(external)
            except OSError:
                self.skipTest("Symlinks are unavailable")
            self.assertEqual(
                HOOK.edited_paths(
                    self.payload("*** Update File: link.md\n"), self.root
                ),
                [],
            )

    def test_exempt_translations_are_allowed(self) -> None:
        translated = self.root / "translations/ru.md"
        translated.parent.mkdir()
        translated.write_text(chr(0x416), encoding="utf-8")
        self.assertEqual(HOOK.violations([translated], self.root, self.checks), [])

    def test_ascii_and_unchecked_types_are_allowed(self) -> None:
        binary = self.root / "image.png"
        binary.write_bytes(b"\xff")
        self.assertEqual(
            HOOK.violations([self.checked, binary], self.root, self.checks), []
        )

    def test_legacy_file_path_is_supported_and_deduplicated(self) -> None:
        payload = self.payload("*** Update File: source.md\n")
        payload["tool_input"]["file_path"] = str(self.checked)
        self.assertEqual(HOOK.edited_paths(payload, self.root), [self.checked])

    def test_non_object_tool_input_is_ignored(self) -> None:
        self.assertEqual(HOOK.edited_paths({"tool_input": "text"}, self.root), [])

    def test_cli_reports_bytes_from_a_different_working_directory(self) -> None:
        folder = self.root / ".codex/hooks"
        folder.mkdir(parents=True)
        script = folder / "post_edit.py"
        script.write_bytes(SCRIPT.read_bytes())
        (folder / "checks.json").write_text(json.dumps(self.checks), encoding="utf-8")
        self.checked.write_bytes(b"\xff")
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(script)],
            input=json.dumps(self.payload("*** Update File: source.md\n")),
            text=True,
            capture_output=True,
            cwd=folder,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("source.md", result.stderr)

    def test_project_exemptions_only_apply_at_the_root(self) -> None:
        checks = json.loads(SCRIPT.with_name("checks.json").read_text(encoding="utf-8"))
        generated = self.root / "CHANGELOG.md"
        nested = self.root / "nested/CHANGELOG.md"
        nested.parent.mkdir()
        generated.write_bytes(b"\xff")
        nested.write_bytes(b"\xff")
        self.assertEqual(
            HOOK.violations([generated, nested], self.root, checks),
            ["nested/CHANGELOG.md"],
        )

    def test_cli_ignores_invalid_json(self) -> None:
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(SCRIPT)],
            input="not JSON",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
