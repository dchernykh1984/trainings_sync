---
name: qt-window-tests
description: How to test the PySide6 GUI in this repo. Use when adding or debugging window/handler tests.
---

# Testing the PySide6 window

- `app/main.py` and the GUI modules are excluded from coverage (see `[tool.coverage.run]`
  in `pyproject.toml`), so the coverage number says nothing about the window. Handler
  behaviour is protected only by explicit Qt-level tests - a change to a handler needs a
  test, or it is unguarded.
- Qt runs headless in tests: `QT_QPA_PLATFORM=offscreen` (a conftest usually sets it), and
  reuse `QApplication.instance() or QApplication(sys.argv)`.
- Construct the window, drive the real handler methods directly, and assert on the
  resulting state or on a monkeypatched collaborator. Avoid real network, real subprocess
  or browser calls, and filesystem writes outside a tmp path - patch them.
- When a test fakes a Qt object whose methods mirror Qt's camelCase API (`isRunning`, ...),
  add `# noqa: N802` on those defs to satisfy ruff pep8-naming.
