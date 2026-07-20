"""Behavioural tests for the GUI widgets using pytest-qt."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from PySide6.QtCore import Qt

from app.gui.app import (
    ConnectorDialog,
    CredentialDialog,
    CredentialsTab,
    LogDialog,
    MainWindow,
    SyncGroupDialog,
    TaskRow,
    _parse_date_or_default,
)
from app.gui.config_store import (
    ConfigStore,
    ConnectorEntry,
    CredentialEntry,
    GroupSourceEntry,
    SyncGroupEntry,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def store(tmp_path: Path) -> ConfigStore:
    return ConfigStore(config_dir=tmp_path / "cfg")


# ---------------------------------------------------------------------------
# _parse_date_or_default
# ---------------------------------------------------------------------------


def test_parse_date_or_default_empty() -> None:
    assert _parse_date_or_default("", date(2000, 1, 1)) == date(2000, 1, 1)


def test_parse_date_or_default_value() -> None:
    assert _parse_date_or_default("2025-06-15", date(2000, 1, 1)) == date(2025, 6, 15)


# ---------------------------------------------------------------------------
# CredentialDialog
# ---------------------------------------------------------------------------


def test_credential_dialog_empty(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.service == ""
    assert entry.login == ""


def test_credential_dialog_prefilled(qtbot) -> None:
    existing = CredentialEntry(
        "Garmin Connect", "https://connect.garmin.com", "user", "pass"
    )
    dlg = CredentialDialog(entry=existing)
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.service == "Garmin Connect"
    assert entry.login == "user"
    assert entry.password == "pass"


def test_credential_dialog_password_echo_hidden(qtbot) -> None:
    from PySide6.QtWidgets import QLineEdit

    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    # The last QLineEdit in the dialog is the password field.
    fields = dlg.findChildren(QLineEdit)
    pw = fields[-1]
    assert pw.echoMode() == QLineEdit.EchoMode.Password


# ---------------------------------------------------------------------------
# ConnectorDialog
# ---------------------------------------------------------------------------


def test_connector_dialog_garmin_default(qtbot) -> None:
    dlg = ConnectorDialog()
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.type == "garmin"
    assert entry.id == ""


def test_connector_dialog_prefilled_strava(qtbot) -> None:
    existing = ConnectorEntry(
        id="strava",
        type="strava",
        credential_service="Strava",
        credential_url="https://www.strava.com/api/v3",
        client_id=12345,
    )
    dlg = ConnectorDialog(entry=existing)
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.type == "strava"
    assert entry.client_id == 12345


def test_connector_dialog_local_folder(qtbot) -> None:
    existing = ConnectorEntry(id="local", type="local_folder", folder="/data")
    dlg = ConnectorDialog(entry=existing)
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.type == "local_folder"
    assert entry.folder == "/data"


def test_connector_dialog_type_change_hides_cred_box(qtbot) -> None:
    dlg = ConnectorDialog()
    qtbot.addWidget(dlg)
    dlg._type.setCurrentText("local_folder")
    # isHidden() reflects the explicit hidden flag regardless of parent visibility.
    assert dlg._cred_box.isHidden()
    assert not dlg._folder_box.isHidden()


def test_connector_dialog_type_change_shows_strava_client_id(qtbot) -> None:
    dlg = ConnectorDialog()
    qtbot.addWidget(dlg)
    dlg._type.setCurrentText("strava")
    assert not dlg._client_id_spin.isHidden()


# ---------------------------------------------------------------------------
# SyncGroupDialog
# ---------------------------------------------------------------------------


def test_sync_group_dialog_add_source_and_destination(qtbot) -> None:
    dlg = SyncGroupDialog(connector_ids=["garmin", "strava"])
    qtbot.addWidget(dlg)
    dlg._id.setText("test-group")

    dlg._src_add_combo.setCurrentText("garmin")
    dlg._src_priority.setValue(2)
    qtbot.mouseClick(dlg._src_add_btn, Qt.MouseButton.LeftButton)

    dlg._dst_add_combo.setCurrentText("strava")
    qtbot.mouseClick(dlg._dst_add_btn, Qt.MouseButton.LeftButton)

    entry = dlg.result_entry()
    assert entry.id == "test-group"
    assert len(entry.sources) == 1
    assert entry.sources[0].id == "garmin"
    assert entry.sources[0].priority == 2
    assert entry.destinations == ["strava"]


def test_sync_group_dialog_remove_source(qtbot) -> None:
    existing = SyncGroupEntry(
        id="g",
        sources=[GroupSourceEntry("s1", 1), GroupSourceEntry("s2", 2)],
        destinations=[],
    )
    dlg = SyncGroupDialog(connector_ids=["s1", "s2"], entry=existing)
    qtbot.addWidget(dlg)
    dlg._sources_widget.setCurrentRow(0)
    qtbot.mouseClick(dlg._src_del_btn, Qt.MouseButton.LeftButton)

    entry = dlg.result_entry()
    assert len(entry.sources) == 1
    assert entry.sources[0].id == "s2"


def test_sync_group_dialog_prefilled(qtbot) -> None:
    existing = SyncGroupEntry(
        id="grp",
        sources=[GroupSourceEntry("garmin", 1)],
        destinations=["local"],
    )
    dlg = SyncGroupDialog(connector_ids=["garmin", "local"], entry=existing)
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.id == "grp"
    assert entry.destinations == ["local"]


# ---------------------------------------------------------------------------
# CredentialsTab
# ---------------------------------------------------------------------------


def test_credentials_tab_shows_stored_entries(qtbot, store: ConfigStore) -> None:
    store.save_credentials(
        [CredentialEntry("Garmin Connect", "https://connect.garmin.com", "u", "p")]
    )
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    assert tab._table.rowCount() == 1
    assert tab._table.item(0, 0).text() == "Garmin Connect"


def test_credentials_tab_delete_no_selection_does_nothing(
    qtbot, store: ConfigStore
) -> None:
    store.save_credentials([CredentialEntry("S", "U", "L", "P")])
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    tab._table.clearSelection()
    tab._delete()
    assert tab._table.rowCount() == 1


def test_credentials_tab_edit_no_selection_does_nothing(
    qtbot, store: ConfigStore
) -> None:
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    tab._table.clearSelection()
    # Should not raise
    tab._edit()


def test_credentials_tab_masks_password(qtbot, store: ConfigStore) -> None:
    store.save_credentials([CredentialEntry("S", "U", "L", "supersecret")])
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    displayed = tab._table.item(0, 3).text()
    assert "supersecret" not in displayed


# ---------------------------------------------------------------------------
# TaskRow
# ---------------------------------------------------------------------------


def test_task_row_progress_with_total(qtbot) -> None:
    row = TaskRow("Download", total=10)
    qtbot.addWidget(row)
    row.update_progress(5)
    assert row._bar.value() == 5
    assert "5/10" in row._count.text()


def test_task_row_progress_no_total(qtbot) -> None:
    row = TaskRow("Login", total=None)
    qtbot.addWidget(row)
    row.update_progress(3)
    assert row._count.text() == "3"


def test_task_row_mark_done_no_warnings(qtbot) -> None:
    row = TaskRow("Task", total=5)
    qtbot.addWidget(row)
    row.mark_done([])
    assert "[OK]" in row._status.text()
    assert row._bar.value() == row._bar.maximum()


def test_task_row_mark_done_with_warnings(qtbot) -> None:
    row = TaskRow("Task", total=5)
    qtbot.addWidget(row)
    row.mark_done(["warn1"])
    assert "[!]" in row._status.text()
    assert "1 warning" in row._label.text()


def test_task_row_mark_failed(qtbot) -> None:
    row = TaskRow("Task", total=5)
    qtbot.addWidget(row)
    row.mark_failed("connection refused")
    assert "[X]" in row._status.text()
    assert "connection refused" in row._label.text()


def test_task_row_update_total(qtbot) -> None:
    row = TaskRow("Task", total=None)
    qtbot.addWidget(row)
    row.update_total(20)
    assert row._total == 20
    assert row._bar.maximum() == 20


# ---------------------------------------------------------------------------
# LogDialog
# ---------------------------------------------------------------------------


def test_log_dialog_missing_file(qtbot, tmp_path: Path) -> None:
    from PySide6.QtWidgets import QTextEdit

    dlg = LogDialog(log_path=tmp_path / "missing.log")
    qtbot.addWidget(dlg)
    texts = dlg.findChildren(QTextEdit)
    assert any("not found" in w.toPlainText() for w in texts)


def test_log_dialog_existing_file(qtbot, tmp_path: Path) -> None:
    log = tmp_path / "sync.log"
    log.write_text("line1\nline2\n", encoding="utf-8")
    dlg = LogDialog(log_path=log)
    qtbot.addWidget(dlg)
    from PySide6.QtWidgets import QTextEdit

    texts = dlg.findChildren(QTextEdit)
    assert any("line1" in w.toPlainText() for w in texts)


# ---------------------------------------------------------------------------
# MainWindow
# ---------------------------------------------------------------------------


def test_main_window_has_three_tabs(qtbot, store: ConfigStore) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    tabs = window.centralWidget()
    assert tabs.count() == 3


def test_main_window_tab_labels(qtbot, store: ConfigStore) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    tabs = window.centralWidget()
    labels = [tabs.tabText(i) for i in range(tabs.count())]
    assert "Credentials" in labels
    assert "Configuration" in labels
    assert "Sync" in labels
