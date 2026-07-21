"""Behavioural tests for the GUI widgets using pytest-qt."""

from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pytest
from PySide6.QtCore import Qt

from app.gui.app import (
    ConfigTab,
    ConnectorDialog,
    CredentialDialog,
    CredentialsTab,
    LogDialog,
    MainWindow,
    SyncGroupDialog,
    SyncWorker,
    TaskRow,
    _parse_date_or_default,
    make_app_icon,
)
from app.gui.config_store import (
    ConfigStore,
    ConnectorEntry,
    CredentialEntry,
    GroupSourceEntry,
    GuiConfig,
    SyncGroupEntry,
)
from app.tracking.gui_renderer import GuiRenderer

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
    assert dlg._password.echoMode() == QLineEdit.EchoMode.Password


def test_credential_dialog_ok_disabled_until_service_filled(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    assert not dlg._ok_btn.isEnabled()
    dlg._service.setText("Garmin Connect")
    assert dlg._ok_btn.isEnabled()


def test_credential_dialog_defaults_to_manual_source(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    assert dlg._manual_radio.isChecked()
    # isHidden() reflects the explicit hidden flag regardless of parent showing.
    assert not dlg._password.isHidden()
    assert dlg._keepass_row.isHidden()


def test_credential_dialog_manual_result(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    dlg._service.setText("Garmin Connect")
    dlg._url.setText("https://connect.garmin.com")
    dlg._login.setText("me@x")
    dlg._password.setText("secret")
    entry = dlg.result_entry()
    assert entry.source == "manual"
    assert entry.password == "secret"
    assert entry.keepass_path == ""


def test_credential_dialog_keepass_switches_fields(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    dlg._keepass_radio.setChecked(True)
    assert not dlg._keepass_row.isHidden()
    assert dlg._password.isHidden()


def test_credential_dialog_keepass_requires_path(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    dlg._service.setText("Garmin Connect")
    assert dlg._ok_btn.isEnabled()  # manual: service is enough
    dlg._keepass_radio.setChecked(True)
    assert not dlg._ok_btn.isEnabled()  # keepass without a file path
    dlg._keepass_path.setText("/x/db.kdbx")
    assert dlg._ok_btn.isEnabled()


def test_credential_dialog_keepass_result(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    dlg._service.setText("Garmin Connect")
    dlg._url.setText("https://connect.garmin.com")
    dlg._login.setText("me@x")
    dlg._keepass_radio.setChecked(True)
    dlg._keepass_path.setText("/home/me/db.kdbx")
    entry = dlg.result_entry()
    assert entry.source == "keepass"
    assert entry.keepass_path == "/home/me/db.kdbx"
    assert entry.password == ""


def test_credential_dialog_prefilled_keepass(qtbot) -> None:
    existing = CredentialEntry(
        "Garmin Connect",
        "https://connect.garmin.com",
        "me@x",
        source="keepass",
        keepass_path="/x/db.kdbx",
    )
    dlg = CredentialDialog(entry=existing)
    qtbot.addWidget(dlg)
    assert dlg._keepass_radio.isChecked()
    assert dlg._keepass_path.text() == "/x/db.kdbx"


# ---------------------------------------------------------------------------
# ConnectorDialog
# ---------------------------------------------------------------------------


def test_connector_dialog_garmin_default(qtbot) -> None:
    dlg = ConnectorDialog()
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.type == "garmin"
    assert entry.id == ""


def test_connector_dialog_type_dropdown_matches_supported_types(qtbot) -> None:
    from app.gui.config_store import CONNECTOR_TYPES

    dlg = ConnectorDialog()
    qtbot.addWidget(dlg)
    items = [dlg._type.itemText(i) for i in range(dlg._type.count())]
    assert items == list(CONNECTOR_TYPES)


def test_connector_dialog_prefilled_strava(qtbot) -> None:
    existing = ConnectorEntry(
        id="strava",
        type="strava",
        credential_service="Strava",
        credential_url="https://www.strava.com/api/v3",
        client_id=12345,
    )
    creds = [CredentialEntry("Strava", "https://www.strava.com/api/v3", "cs", "rt")]
    dlg = ConnectorDialog(entry=existing, credentials=creds)
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.type == "strava"
    assert entry.client_id == 12345
    assert entry.credential_service == "Strava"


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


def test_connector_dialog_ok_needs_name_and_credential(qtbot) -> None:
    creds = [
        CredentialEntry("Garmin Connect", "https://connect.garmin.com", "me@x", "p")
    ]
    dlg = ConnectorDialog(credentials=creds)
    qtbot.addWidget(dlg)
    # A credential is auto-selected (first item), so only the name is missing.
    assert not dlg._ok_btn.isEnabled()
    dlg._id.setText("garmin")
    assert dlg._ok_btn.isEnabled()


def test_connector_dialog_ok_blocked_without_any_credential(qtbot) -> None:
    # garmin/strava require a credential; with none configured OK stays disabled.
    dlg = ConnectorDialog(credentials=[])
    qtbot.addWidget(dlg)
    dlg._id.setText("garmin")
    assert not dlg._ok_btn.isEnabled()


def test_connector_dialog_local_folder_needs_no_credential(qtbot) -> None:
    dlg = ConnectorDialog(credentials=[])
    qtbot.addWidget(dlg)
    dlg._type.setCurrentText("local_folder")
    dlg._id.setText("local")
    assert dlg._ok_btn.isEnabled()


def test_connector_dialog_credential_combo_lists_configured_accounts(qtbot) -> None:
    creds = [
        CredentialEntry("Garmin Connect", "https://connect.garmin.com", "me@x", "p"),
        CredentialEntry("Strava", "https://www.strava.com/api/v3", "cs", "rt"),
    ]
    dlg = ConnectorDialog(credentials=creds)
    qtbot.addWidget(dlg)
    labels = [dlg._cred_combo.itemText(i) for i in range(dlg._cred_combo.count())]
    assert labels == ["Garmin Connect (me@x)", "Strava (cs)"]
    # The combo is a strict picker, not free-text.
    assert not dlg._cred_combo.isEditable()


def test_connector_dialog_selecting_credential_fills_service_url_login(qtbot) -> None:
    creds = [
        CredentialEntry("Garmin Connect", "https://connect.garmin.com", "g@x", "p"),
        CredentialEntry("Strava", "https://www.strava.com/api/v3", "cs", "rt"),
    ]
    dlg = ConnectorDialog(credentials=creds)
    qtbot.addWidget(dlg)
    dlg._cred_combo.setCurrentIndex(1)  # Strava
    entry = dlg.result_entry()
    assert entry.credential_service == "Strava"
    assert entry.credential_url == "https://www.strava.com/api/v3"
    assert entry.credential_login == "cs"


def test_connector_dialog_preselects_existing_credential(qtbot) -> None:
    creds = [
        CredentialEntry("Garmin Connect", "https://connect.garmin.com", "a@x", "p"),
        CredentialEntry("Garmin Connect", "https://connect.garmin.com", "b@x", "p"),
    ]
    existing = ConnectorEntry(
        id="g",
        type="garmin",
        credential_service="Garmin Connect",
        credential_url="https://connect.garmin.com",
        credential_login="b@x",
    )
    dlg = ConnectorDialog(entry=existing, credentials=creds)
    qtbot.addWidget(dlg)
    assert dlg.result_entry().credential_login == "b@x"


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


def test_sync_group_dialog_ok_disabled_until_id_filled(qtbot) -> None:
    dlg = SyncGroupDialog(connector_ids=["garmin"])
    qtbot.addWidget(dlg)
    assert not dlg._ok_btn.isEnabled()
    dlg._id.setText("my-group")
    assert dlg._ok_btn.isEnabled()


def test_sync_group_dialog_ignores_duplicate_source(qtbot) -> None:
    dlg = SyncGroupDialog(connector_ids=["garmin", "strava"])
    qtbot.addWidget(dlg)
    dlg._src_add_combo.setCurrentText("garmin")
    qtbot.mouseClick(dlg._src_add_btn, Qt.MouseButton.LeftButton)
    qtbot.mouseClick(dlg._src_add_btn, Qt.MouseButton.LeftButton)

    entry = dlg.result_entry()
    assert [s.id for s in entry.sources] == ["garmin"]


def test_sync_group_dialog_ignores_duplicate_destination(qtbot) -> None:
    dlg = SyncGroupDialog(connector_ids=["garmin", "strava"])
    qtbot.addWidget(dlg)
    dlg._dst_add_combo.setCurrentText("strava")
    qtbot.mouseClick(dlg._dst_add_btn, Qt.MouseButton.LeftButton)
    qtbot.mouseClick(dlg._dst_add_btn, Qt.MouseButton.LeftButton)

    entry = dlg.result_entry()
    assert entry.destinations == ["strava"]


def test_sync_group_dialog_source_id_with_colon(qtbot) -> None:
    # Connector ids may contain a colon; the source must round-trip intact.
    existing = SyncGroupEntry(
        id="grp",
        sources=[GroupSourceEntry("garmin:eu", 3)],
        destinations=[],
    )
    dlg = SyncGroupDialog(connector_ids=["garmin:eu"], entry=existing)
    qtbot.addWidget(dlg)
    entry = dlg.result_entry()
    assert entry.sources[0].id == "garmin:eu"
    assert entry.sources[0].priority == 3


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


def test_credentials_tab_delete_credential_used_by_connector_is_blocked(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    store.save_credentials(
        [CredentialEntry("Garmin Connect", "https://connect.garmin.com", "me@x", "p")]
    )
    store.save_gui_config(
        GuiConfig(
            connectors=[
                ConnectorEntry(
                    id="garmin",
                    type="garmin",
                    credential_service="Garmin Connect",
                    credential_url="https://connect.garmin.com",
                    credential_login="me@x",
                )
            ]
        )
    )
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    warned: dict[str, str] = {}
    monkeypatch.setattr(
        QMessageBox, "warning", lambda _s, title, text, *a, **k: warned.update(t=text)
    )
    tab._table.selectRow(0)
    tab._delete()

    # The credential is kept and the warning names the offending connector.
    assert tab._table.rowCount() == 1
    assert "'garmin'" in warned["t"]


def test_credentials_tab_delete_unused_credential_succeeds(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    store.save_credentials([CredentialEntry("Lonely", "u", "l", "p")])
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    monkeypatch.setattr(
        QMessageBox, "question", lambda *a, **k: QMessageBox.StandardButton.Yes
    )
    tab._table.selectRow(0)
    tab._delete()

    assert tab._table.rowCount() == 0


def test_credentials_tab_masks_password(qtbot, store: ConfigStore) -> None:
    store.save_credentials([CredentialEntry("S", "U", "L", "supersecret")])
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    # Column 4 is the secret / KeePass file column.
    displayed = tab._table.item(0, 4).text()
    assert "supersecret" not in displayed


def test_credentials_tab_shows_source_column(qtbot, store: ConfigStore) -> None:
    store.save_credentials(
        [
            CredentialEntry("Manual", "u", "l", "p"),
            CredentialEntry(
                "Kp", "u", "l", source="keepass", keepass_path="/x/db.kdbx"
            ),
        ]
    )
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    assert tab._table.item(0, 3).text() == "manual"
    assert tab._table.item(1, 3).text() == "keepass"
    # A KeePass credential shows its file path (no secret to mask).
    assert tab._table.item(1, 4).text() == "/x/db.kdbx"


def test_credentials_tab_load_from_file(
    qtbot, monkeypatch, store: ConfigStore, tmp_path: Path
) -> None:
    import json as _json

    from PySide6.QtWidgets import QFileDialog

    src = tmp_path / "creds.json"
    src.write_text(
        _json.dumps(
            [{"service": "Strava", "url": "u", "login": "cs", "password": "rt"}]
        ),
        encoding="utf-8",
    )
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *a, **k: (str(src), ""))
    tab._load_from_file()

    assert tab._table.rowCount() == 1
    assert tab._table.item(0, 0).text() == "Strava"
    # The imported credentials are persisted to the fixed store location.
    assert store.load_credentials()[0].service == "Strava"


def test_credentials_tab_load_from_file_cancelled_is_noop(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QFileDialog

    store.save_credentials([CredentialEntry("Keep", "u", "l", "p")])
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *a, **k: ("", ""))
    tab._load_from_file()

    assert tab._table.rowCount() == 1
    assert tab._table.item(0, 0).text() == "Keep"


# ---------------------------------------------------------------------------
# ConfigTab - load from file
# ---------------------------------------------------------------------------

# Format mirrors config/config.strava-and-garmin.json but with fake values.
_SAMPLE_CONFIG = {
    "cache_dir": ".cache",
    "start": "2026-06-10",
    "connectors": [
        {
            "id": "garmin",
            "type": "garmin",
            "credential_service": "Garmin Connect",
            "credential_url": "https://connect.garmin.com",
            "credential_login": "rider@example.com",
        },
        {
            "id": "strava",
            "type": "strava",
            "client_id": 12345,
            "credential_service": "Strava",
            "credential_url": "https://www.strava.com/api/v3",
        },
        {"id": "local", "type": "local_folder", "folder": "/tmp/trainings"},
    ],
    "sync_groups": [
        {
            "id": "strava-to-garmin",
            "sources": [{"id": "strava", "priority": 1}],
            "destinations": ["garmin"],
        },
        {
            "id": "garmin-and-strava-to-local",
            "sources": [
                {"id": "garmin", "priority": 1},
                {"id": "strava", "priority": 2},
            ],
            "destinations": ["local"],
        },
    ],
}


def test_config_tab_load_from_file(
    qtbot, monkeypatch, store: ConfigStore, tmp_path: Path
) -> None:
    import json as _json

    from PySide6.QtWidgets import QFileDialog

    src = tmp_path / "config.strava-and-garmin.json"
    src.write_text(_json.dumps(_SAMPLE_CONFIG), encoding="utf-8")

    tab = ConfigTab(store)
    qtbot.addWidget(tab)
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *a, **k: (str(src), ""))
    tab._load_from_file()

    assert tab._conn_list.count() == 3
    assert tab._grp_list.count() == 2
    assert tab._use_start.isChecked()
    assert tab._start_date.date().toString("yyyy-MM-dd") == "2026-06-10"
    # The imported config is persisted to the fixed store location.
    reloaded = store.load_gui_config()
    assert [c.id for c in reloaded.connectors] == ["garmin", "strava", "local"]


def test_config_tab_load_from_file_cancelled_is_noop(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QFileDialog

    tab = ConfigTab(store)
    qtbot.addWidget(tab)
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *a, **k: ("", ""))
    tab._load_from_file()

    assert tab._conn_list.count() == 0


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


def test_task_row_status_label_fits_ok_tag(qtbot) -> None:
    row = TaskRow("Task", total=5)
    qtbot.addWidget(row)
    row.mark_done([])
    # The fixed status width must accommodate the full "[OK]" tag so the
    # closing bracket is not clipped.
    needed = row._status.fontMetrics().horizontalAdvance("[OK]")
    assert row._status.minimumWidth() >= needed


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


def test_log_dialog_uses_fixed_pitch_font(qtbot, tmp_path: Path) -> None:
    from PySide6.QtGui import QFontDatabase
    from PySide6.QtWidgets import QTextEdit

    dlg = LogDialog(log_path=tmp_path / "sync.log")
    qtbot.addWidget(dlg)
    edit = dlg.findChildren(QTextEdit)[0]
    mono = QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont)
    assert edit.font().family() == mono.family()


# ---------------------------------------------------------------------------
# ConfigTab
# ---------------------------------------------------------------------------


def _config_with_group(store: ConfigStore) -> None:
    store.save_gui_config(
        GuiConfig(
            connectors=[
                ConnectorEntry(id="garmin", type="garmin"),
                ConnectorEntry(id="local", type="local_folder", folder="/data"),
            ],
            sync_groups=[
                SyncGroupEntry(
                    id="g",
                    sources=[GroupSourceEntry("garmin", 1)],
                    destinations=["local"],
                )
            ],
        )
    )


def test_config_tab_delete_connector_used_in_group_is_blocked(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    _config_with_group(store)
    tab = ConfigTab(store)
    qtbot.addWidget(tab)
    warned: dict[str, str] = {}
    monkeypatch.setattr(
        QMessageBox, "warning", lambda _s, title, text, *a, **k: warned.update(t=text)
    )
    tab._conn_list.setCurrentRow(0)  # garmin - used as a source in group "g"
    tab._delete_connector()

    # The connector is kept and the warning names the offending group.
    assert [c.id for c in store.load_gui_config().connectors] == ["garmin", "local"]
    assert "'g'" in warned["t"]


def test_config_tab_delete_unused_connector_succeeds(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    _config_with_group(store)
    # Remove the group so "local" becomes unused.
    cfg = store.load_gui_config()
    cfg.sync_groups = []
    store.save_gui_config(cfg)

    tab = ConfigTab(store)
    qtbot.addWidget(tab)
    monkeypatch.setattr(
        QMessageBox, "question", lambda *a, **k: QMessageBox.StandardButton.Yes
    )
    tab._conn_list.setCurrentRow(1)  # local
    tab._delete_connector()

    assert [c.id for c in store.load_gui_config().connectors] == ["garmin"]


# ---------------------------------------------------------------------------
# SyncWorker
# ---------------------------------------------------------------------------


def test_sync_worker_logs_and_closes_logger_on_setup_failure(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    # An error raised while building the pipeline must still be written to
    # sync.log and the logger must be closed (no dangling file handler).
    import app.core.connector_factory as connector_factory

    async def _boom(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("kaboom during build")

    monkeypatch.setattr(connector_factory, "build_connectors", _boom)

    gui_config = GuiConfig(
        connectors=[ConnectorEntry(id="g", type="garmin")],
        sync_groups=[],
    )
    worker = SyncWorker(store, gui_config, GuiRenderer())

    with pytest.raises(RuntimeError, match="kaboom during build"):
        asyncio.run(worker._async_sync())

    log_text = (store.cache_dir / "sync.log").read_text(encoding="utf-8")
    assert "kaboom during build" in log_text
    assert "Sync run finished" in log_text  # run_end() ran in the finally block


def test_sync_worker_keepass_rejects_strava_connectors(
    qtbot, store: ConfigStore
) -> None:
    # A Strava connector backed by a KeePass credential is refused, since a
    # rotated refresh token cannot be written back to a .kdbx.
    store.save_credentials(
        [
            CredentialEntry(
                "Strava",
                "https://www.strava.com/api/v3",
                "cs",
                source="keepass",
                keepass_path="/x/db.kdbx",
            )
        ]
    )
    gui_config = GuiConfig(
        connectors=[
            ConnectorEntry(
                id="strava",
                type="strava",
                credential_service="Strava",
                credential_url="https://www.strava.com/api/v3",
                credential_login="cs",
                client_id=1,
            )
        ],
        sync_groups=[
            SyncGroupEntry(
                id="g",
                sources=[GroupSourceEntry("strava", 1)],
                destinations=[],
            )
        ],
    )
    worker = SyncWorker(
        store, gui_config, GuiRenderer(), keepass_passwords={"/x/db.kdbx": "pw"}
    )

    with pytest.raises(ValueError, match="does not support KeePass"):
        asyncio.run(worker._async_sync())


# ---------------------------------------------------------------------------
# SyncTab - KeePass master-password prompt
# ---------------------------------------------------------------------------


def _make_sync_tab(store: ConfigStore):
    from app.gui.app import SyncTab

    return SyncTab(store, ConfigTab(store))


def _keepass_garmin_config(store: ConfigStore, kdbx: str) -> None:
    store.save_credentials(
        [
            CredentialEntry(
                "Garmin Connect",
                "https://connect.garmin.com",
                "me@x",
                source="keepass",
                keepass_path=kdbx,
            )
        ]
    )
    store.save_gui_config(
        GuiConfig(
            connectors=[
                ConnectorEntry(
                    id="garmin",
                    type="garmin",
                    credential_service="Garmin Connect",
                    credential_url="https://connect.garmin.com",
                    credential_login="me@x",
                )
            ],
        )
    )


def test_sync_tab_prompts_keepass_password(qtbot, monkeypatch, store) -> None:
    from PySide6.QtWidgets import QInputDialog

    _keepass_garmin_config(store, "/x.kdbx")
    tab = _make_sync_tab(store)
    qtbot.addWidget(tab)
    monkeypatch.setattr(QInputDialog, "getText", lambda *a, **k: ("secret-pw", True))
    monkeypatch.setattr(SyncWorker, "start", lambda self: None)

    tab._run_sync()
    assert tab._worker is not None
    assert tab._worker._keepass_passwords == {"/x.kdbx": "secret-pw"}


def test_sync_tab_prompts_once_per_distinct_kdbx(qtbot, monkeypatch, store) -> None:
    from PySide6.QtWidgets import QInputDialog

    # Two connectors sharing one .kdbx must prompt only once.
    store.save_credentials(
        [
            CredentialEntry(
                "Garmin Connect",
                "https://connect.garmin.com",
                "me@x",
                source="keepass",
                keepass_path="/shared.kdbx",
            ),
            CredentialEntry(
                "Other",
                "https://other.example",
                "u",
                source="keepass",
                keepass_path="/shared.kdbx",
            ),
        ]
    )
    store.save_gui_config(
        GuiConfig(
            connectors=[
                ConnectorEntry(
                    id="garmin",
                    type="garmin",
                    credential_service="Garmin Connect",
                    credential_url="https://connect.garmin.com",
                    credential_login="me@x",
                ),
                ConnectorEntry(
                    id="other",
                    type="garmin",
                    credential_service="Other",
                    credential_url="https://other.example",
                    credential_login="u",
                ),
            ],
        )
    )
    tab = _make_sync_tab(store)
    qtbot.addWidget(tab)
    prompts: list[str] = []

    def _get_text(_parent, _title, label, *a, **k):
        prompts.append(label)
        return ("pw", True)

    monkeypatch.setattr(QInputDialog, "getText", _get_text)
    monkeypatch.setattr(SyncWorker, "start", lambda self: None)

    tab._run_sync()
    assert len(prompts) == 1
    assert tab._worker._keepass_passwords == {"/shared.kdbx": "pw"}


def test_sync_tab_keepass_cancel_aborts_run(qtbot, monkeypatch, store) -> None:
    from PySide6.QtWidgets import QInputDialog

    _keepass_garmin_config(store, "/x.kdbx")
    tab = _make_sync_tab(store)
    qtbot.addWidget(tab)
    started: list[bool] = []
    monkeypatch.setattr(QInputDialog, "getText", lambda *a, **k: ("", False))
    monkeypatch.setattr(SyncWorker, "start", lambda self: started.append(True))

    tab._run_sync()
    assert tab._worker is None
    assert started == []


def test_sync_tab_manual_credentials_do_not_prompt(qtbot, monkeypatch, store) -> None:
    from PySide6.QtWidgets import QInputDialog

    store.save_credentials([CredentialEntry("Garmin Connect", "u", "me@x", "pw")])
    tab = _make_sync_tab(store)
    qtbot.addWidget(tab)

    def _boom(*_a: object, **_k: object) -> object:
        raise AssertionError("must not prompt when no KeePass credentials are used")

    monkeypatch.setattr(QInputDialog, "getText", _boom)
    monkeypatch.setattr(SyncWorker, "start", lambda self: None)

    tab._run_sync()
    assert tab._worker is not None
    assert tab._worker._keepass_passwords == {}


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


def test_main_window_tab_order_sync_first(qtbot, store: ConfigStore) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    tabs = window.centralWidget()
    labels = [tabs.tabText(i) for i in range(tabs.count())]
    assert labels == ["Sync", "Configuration", "Credentials"]
    assert tabs.currentIndex() == 0  # Sync is the default active tab


def test_main_window_starts_wide_enough_for_sync_rows(
    qtbot, store: ConfigStore
) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    # Wide default so the Sync tab's long task rows fit without a horizontal
    # scrollbar on startup.
    assert window.width() >= 1200


def test_main_window_has_window_icon(qtbot, store: ConfigStore) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    assert not window.windowIcon().isNull()


# ---------------------------------------------------------------------------
# Application icon
# ---------------------------------------------------------------------------


def test_make_app_icon_renders(qtbot) -> None:
    icon = make_app_icon(64)
    assert not icon.isNull()
    assert not icon.pixmap(64, 64).isNull()
