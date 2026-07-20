"""Configure headless Qt for GUI tests."""

import os

# Must be set before QApplication is created.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
