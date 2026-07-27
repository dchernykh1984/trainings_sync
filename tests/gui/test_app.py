"""Behavioural tests for the GUI widgets using pytest-qt."""

from __future__ import annotations

import asyncio
import time
from datetime import date
from pathlib import Path

import pytest
from PySide6.QtCore import Qt

from app.gui.app import (
    _COUNT_MIN_WIDTH,
    APP_DESKTOP_NAME,
    APP_DISPLAY_NAME,
    ConfigTab,
    ConnectorDialog,
    CounterColumn,
    CredentialDialog,
    CredentialsTab,
    LogDialog,
    MainWindow,
    SyncGroupDialog,
    SyncWorker,
    TaskRow,
    _parse_date_or_default,
    configure_app_identity,
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


class _RejectingDialog:
    """Records the entry it was opened with, then cancels (no changes)."""

    def __init__(self, opened: dict, entry: object) -> None:
        opened["entry"] = entry

    def exec(self) -> int:
        from PySide6.QtWidgets import QDialog

        return QDialog.DialogCode.Rejected

    def result_entry(self) -> object:  # pragma: no cover - never called on reject
        raise AssertionError("result_entry must not be read after cancel")


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


def test_credential_dialog_service_field_labeled_account_name(qtbot) -> None:
    from PySide6.QtWidgets import QLabel

    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    labels = [w.text() for w in dlg.findChildren(QLabel)]
    assert "Account name:" in labels
    assert "Service:" not in labels


def test_credential_dialog_url_dropdown_presets_and_free_text(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    urls = [dlg._url.itemText(i) for i in range(dlg._url.count())]
    assert urls == [
        "https://connect.garmin.com",
        "https://www.strava.com/api/v3",
    ]
    assert dlg._url.isEditable()
    # A custom URL can still be typed.
    dlg._service.setText("Custom")
    dlg._url.setCurrentText("https://example.test/api")
    assert dlg.result_entry().url == "https://example.test/api"


def test_credential_dialog_manual_result(qtbot) -> None:
    dlg = CredentialDialog()
    qtbot.addWidget(dlg)
    dlg._service.setText("Garmin Connect")
    dlg._url.setCurrentText("https://connect.garmin.com")
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
    dlg._url.setCurrentText("https://connect.garmin.com")
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


def test_a_new_connector_gets_its_own_uid(qtbot) -> None:
    # The uid is what lets the cache follow a connector across renames, so a
    # fresh connector must not be left without one.
    first, second = ConnectorDialog(), ConnectorDialog()
    qtbot.addWidget(first)
    qtbot.addWidget(second)
    assert first.result_entry().uid
    assert first.result_entry().uid != second.result_entry().uid


def test_editing_a_connector_keeps_its_uid(qtbot) -> None:
    existing = ConnectorEntry(id="Garmin", uid="abc123", type="garmin")
    dlg = ConnectorDialog(entry=existing)
    qtbot.addWidget(dlg)
    dlg._id.setText("Garmin Denis")
    result = dlg.result_entry()
    assert (result.id, result.uid) == ("Garmin Denis", "abc123")


def test_editing_a_pre_uid_connector_adopts_its_name_as_the_uid(qtbot) -> None:
    # The name is what the cache is already keyed by, so it is the only safe
    # identity to adopt; a random one would look like a brand new connector.
    dlg = ConnectorDialog(entry=ConnectorEntry(id="Garmin", type="garmin"))
    qtbot.addWidget(dlg)
    assert dlg.result_entry().uid == "Garmin"


def test_a_pre_uid_connector_renamed_in_one_sitting_keeps_its_cache(qtbot) -> None:
    # Renaming straight after upgrading is the case most likely to lose data:
    # the uid has to be the old name, or the rename goes unnoticed.
    dlg = ConnectorDialog(entry=ConnectorEntry(id="Garmin", type="garmin"))
    qtbot.addWidget(dlg)
    dlg._id.setText("Garmin Denis")
    result = dlg.result_entry()
    assert (result.id, result.uid) == ("Garmin Denis", "Garmin")


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


def test_connector_dialog_folder_browse_fills_path(qtbot, monkeypatch) -> None:
    from PySide6.QtWidgets import QFileDialog

    dlg = ConnectorDialog()
    qtbot.addWidget(dlg)
    dlg._type.setCurrentText("local_folder")
    monkeypatch.setattr(
        QFileDialog, "getExistingDirectory", lambda *a, **k: "/chosen/folder"
    )
    dlg._browse_folder()
    assert dlg._folder.text() == "/chosen/folder"


def test_connector_dialog_folder_browse_cancel_keeps_path(qtbot, monkeypatch) -> None:
    from PySide6.QtWidgets import QFileDialog

    dlg = ConnectorDialog()
    qtbot.addWidget(dlg)
    dlg._folder.setText("/keep")
    monkeypatch.setattr(QFileDialog, "getExistingDirectory", lambda *a, **k: "")
    dlg._browse_folder()
    assert dlg._folder.text() == "/keep"


def test_connector_dialog_local_folder_carries_no_credential(qtbot) -> None:
    # Even with credentials configured (and auto-selected in the combo), a
    # local_folder connector must not reference any credential.
    creds = [
        CredentialEntry("Garmin Connect", "https://connect.garmin.com", "me@x", "p")
    ]
    dlg = ConnectorDialog(credentials=creds)
    qtbot.addWidget(dlg)
    dlg._type.setCurrentText("local_folder")
    dlg._folder.setText("/data")
    entry = dlg.result_entry()
    assert entry.credential_service == ""
    assert entry.credential_url == ""
    assert entry.credential_login == ""


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


def test_sync_group_dialog_ok_needs_name_and_source(qtbot) -> None:
    dlg = SyncGroupDialog(connector_ids=["garmin"])
    qtbot.addWidget(dlg)
    assert not dlg._ok_btn.isEnabled()
    dlg._id.setText("my-group")
    assert not dlg._ok_btn.isEnabled()  # still no source
    dlg._src_add_combo.setCurrentText("garmin")
    qtbot.mouseClick(dlg._src_add_btn, Qt.MouseButton.LeftButton)
    assert dlg._ok_btn.isEnabled()


def test_sync_group_dialog_ok_disabled_after_removing_last_source(qtbot) -> None:
    existing = SyncGroupEntry(
        id="g", sources=[GroupSourceEntry("garmin", 1)], destinations=[]
    )
    dlg = SyncGroupDialog(connector_ids=["garmin"], entry=existing)
    qtbot.addWidget(dlg)
    assert dlg._ok_btn.isEnabled()
    dlg._sources_widget.setCurrentRow(0)
    qtbot.mouseClick(dlg._src_del_btn, Qt.MouseButton.LeftButton)
    assert not dlg._ok_btn.isEnabled()


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


def test_sync_group_dialog_source_cannot_also_be_destination(qtbot) -> None:
    dlg = SyncGroupDialog(connector_ids=["garmin", "strava"])
    qtbot.addWidget(dlg)
    dlg._src_add_combo.setCurrentText("garmin")
    qtbot.mouseClick(dlg._src_add_btn, Qt.MouseButton.LeftButton)
    # Adding the same connector as a destination is refused.
    dlg._dst_add_combo.setCurrentText("garmin")
    qtbot.mouseClick(dlg._dst_add_btn, Qt.MouseButton.LeftButton)

    entry = dlg.result_entry()
    assert [s.id for s in entry.sources] == ["garmin"]
    assert entry.destinations == []


def test_sync_group_dialog_destination_cannot_also_be_source(qtbot) -> None:
    dlg = SyncGroupDialog(connector_ids=["garmin", "strava"])
    qtbot.addWidget(dlg)
    dlg._dst_add_combo.setCurrentText("garmin")
    qtbot.mouseClick(dlg._dst_add_btn, Qt.MouseButton.LeftButton)
    dlg._src_add_combo.setCurrentText("garmin")
    qtbot.mouseClick(dlg._src_add_btn, Qt.MouseButton.LeftButton)

    entry = dlg.result_entry()
    assert entry.destinations == ["garmin"]
    assert entry.sources == []


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


def test_credentials_tab_double_click_opens_edit(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    import app.gui.app as gui_app

    store.save_credentials([CredentialEntry("Acc", "u", "l", "p")])
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)
    opened: dict = {}
    monkeypatch.setattr(
        gui_app,
        "CredentialDialog",
        lambda **kw: _RejectingDialog(opened, kw.get("entry")),
    )
    tab._table.selectRow(0)
    tab._table.itemDoubleClicked.emit(tab._table.item(0, 0))
    assert opened["entry"].service == "Acc"


class _FakeCredentialDialog:
    def __init__(self, entry: CredentialEntry) -> None:
        self._entry = entry

    def exec(self) -> int:
        from PySide6.QtWidgets import QDialog

        return QDialog.DialogCode.Accepted

    def result_entry(self) -> CredentialEntry:
        return self._entry


def _store_with_connector_using_garmin(store: ConfigStore) -> None:
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


def test_credentials_tab_edit_identity_of_used_credential_is_blocked(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    import app.gui.app as gui_app

    _store_with_connector_using_garmin(store)
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)

    renamed = CredentialEntry(
        "Garmin Connect", "https://connect.garmin.com", "new@x", "p"
    )
    monkeypatch.setattr(
        gui_app, "CredentialDialog", lambda **kw: _FakeCredentialDialog(renamed)
    )
    warned: list[bool] = []
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: warned.append(True))
    tab._table.selectRow(0)
    tab._edit()

    assert warned == [True]
    assert store.load_credentials()[0].login == "me@x"  # unchanged


def test_credentials_tab_edit_password_of_used_credential_is_allowed(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    import app.gui.app as gui_app

    _store_with_connector_using_garmin(store)
    tab = CredentialsTab(store)
    qtbot.addWidget(tab)

    # Same identity, new password - must be allowed even while referenced.
    updated = CredentialEntry(
        "Garmin Connect", "https://connect.garmin.com", "me@x", "new-pw"
    )
    monkeypatch.setattr(
        gui_app, "CredentialDialog", lambda **kw: _FakeCredentialDialog(updated)
    )
    tab._table.selectRow(0)
    tab._edit()

    assert store.load_credentials()[0].password == "new-pw"


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


def test_task_row_counter_fits_a_large_total(qtbot) -> None:
    row = TaskRow("Wellness download", total=193372)
    qtbot.addWidget(row)
    row.update_progress(193336)
    # The counter is right-aligned and fixed-width, so text that does not fit
    # loses its leading characters: "193336/193372" used to show as
    # "36/193372".
    needed = row._count.fontMetrics().horizontalAdvance(row._count.text())
    assert row._count.minimumWidth() >= needed


def test_task_row_counter_fits_a_total_that_arrives_late(qtbot) -> None:
    # Tasks that discover their size while running report the total after the
    # row already exists, which is how the clipped counter was first seen.
    row = TaskRow("Wellness download", total=None)
    qtbot.addWidget(row)
    row.update_total(193372)
    row.update_progress(193336)
    needed = row._count.fontMetrics().horizontalAdvance(row._count.text())
    assert row._count.minimumWidth() >= needed


def test_task_row_counter_keeps_its_width_for_small_totals(qtbot) -> None:
    row = TaskRow("Task", total=5)
    qtbot.addWidget(row)
    row.update_progress(3)
    assert row._count.minimumWidth() == _COUNT_MIN_WIDTH


def test_rows_sharing_a_column_keep_one_counter_width(qtbot) -> None:
    # Equal counter widths are what keeps the progress bars in one column:
    # sizing each row to its own text would shove the wide rows' bars left.
    column = CounterColumn()
    small = TaskRow("Login", total=1, column=column)
    big = TaskRow("Wellness download", total=193372, column=column)
    qtbot.addWidget(small)
    qtbot.addWidget(big)
    small.update_progress(1)
    big.update_progress(191081)
    assert small._count.minimumWidth() == big._count.minimumWidth()
    needed = big._count.fontMetrics().horizontalAdvance(big._count.text())
    assert big._count.minimumWidth() >= needed


def test_a_row_added_later_adopts_the_column_width(qtbot) -> None:
    # Rows appear as tasks start, so one joining after the column has already
    # widened must not fall back to the narrow default.
    column = CounterColumn()
    big = TaskRow("Wellness download", total=193372, column=column)
    qtbot.addWidget(big)
    late = TaskRow("Login", total=1, column=column)
    qtbot.addWidget(late)
    assert late._count.minimumWidth() == big._count.minimumWidth()
    assert late._count.minimumWidth() > _COUNT_MIN_WIDTH


def test_a_late_total_widens_every_row_in_the_column(qtbot) -> None:
    # Wellness download reports its total only once it is running.
    column = CounterColumn()
    small = TaskRow("Login", total=1, column=column)
    late = TaskRow("Wellness download", total=None, column=column)
    qtbot.addWidget(small)
    qtbot.addWidget(late)
    before = small._count.minimumWidth()
    late.update_total(193372)
    assert small._count.minimumWidth() > before
    assert small._count.minimumWidth() == late._count.minimumWidth()


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


class _FakeConnectorDialog:
    def __init__(self, entry: ConnectorEntry) -> None:
        self._entry = entry

    def exec(self) -> int:
        from PySide6.QtWidgets import QDialog

        return QDialog.DialogCode.Accepted

    def result_entry(self) -> ConnectorEntry:
        return self._entry


def test_config_tab_add_connector_rejects_duplicate_name(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    import app.gui.app as gui_app

    store.save_gui_config(
        GuiConfig(connectors=[ConnectorEntry(id="garmin", type="local_folder")])
    )
    tab = ConfigTab(store)
    qtbot.addWidget(tab)

    dupe = ConnectorEntry(id="garmin", type="local_folder", folder="/x")
    monkeypatch.setattr(
        gui_app, "ConnectorDialog", lambda **kw: _FakeConnectorDialog(dupe)
    )
    warned: list[bool] = []
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: warned.append(True))
    tab._add_connector()

    assert warned == [True]
    assert [c.id for c in store.load_gui_config().connectors] == ["garmin"]


def test_config_tab_edit_connector_keeping_own_name_is_allowed(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    import app.gui.app as gui_app

    store.save_gui_config(
        GuiConfig(
            connectors=[ConnectorEntry(id="garmin", type="local_folder", folder="/a")]
        )
    )
    tab = ConfigTab(store)
    qtbot.addWidget(tab)

    updated = ConnectorEntry(id="garmin", type="local_folder", folder="/b")
    monkeypatch.setattr(
        gui_app, "ConnectorDialog", lambda **kw: _FakeConnectorDialog(updated)
    )
    tab._conn_list.setCurrentRow(0)
    tab._edit_connector()

    saved = store.load_gui_config().connectors
    assert len(saved) == 1
    assert saved[0].folder == "/b"


def test_renaming_a_connector_repoints_the_sync_groups(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    # Groups reference connectors by name, so leaving them on the old one
    # makes the next sync die resolving it.
    import app.gui.app as gui_app

    store.save_gui_config(
        GuiConfig(
            connectors=[
                ConnectorEntry(id="Garmin", uid="u1", type="local_folder", folder="/a"),
                ConnectorEntry(id="Local", uid="u2", type="local_folder", folder="/b"),
            ],
            sync_groups=[
                SyncGroupEntry(
                    id="g1",
                    sources=[GroupSourceEntry(id="Garmin", priority=1)],
                    destinations=["Local", "Garmin"],
                )
            ],
        )
    )
    tab = ConfigTab(store)
    qtbot.addWidget(tab)

    renamed = ConnectorEntry(
        id="Garmin Denis", uid="u1", type="local_folder", folder="/a"
    )
    monkeypatch.setattr(
        gui_app, "ConnectorDialog", lambda **kw: _FakeConnectorDialog(renamed)
    )
    tab._conn_list.setCurrentRow(0)
    tab._edit_connector()

    group = store.load_gui_config().sync_groups[0]
    assert [s.id for s in group.sources] == ["Garmin Denis"]
    assert group.destinations == ["Local", "Garmin Denis"]
    # The list on screen must not keep showing a connector that is gone.
    shown = [tab._grp_list.item(i).text() for i in range(tab._grp_list.count())]
    assert "Garmin Denis" in shown[0]
    assert "[Garmin(" not in shown[0]


def test_editing_a_connector_without_renaming_leaves_the_groups_alone(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    import app.gui.app as gui_app

    store.save_gui_config(
        GuiConfig(
            connectors=[
                ConnectorEntry(id="Garmin", uid="u1", type="local_folder", folder="/a")
            ],
            sync_groups=[
                SyncGroupEntry(
                    id="g1",
                    sources=[GroupSourceEntry(id="Garmin", priority=1)],
                    destinations=["Garmin"],
                )
            ],
        )
    )
    tab = ConfigTab(store)
    qtbot.addWidget(tab)

    monkeypatch.setattr(
        gui_app,
        "ConnectorDialog",
        lambda **kw: _FakeConnectorDialog(
            ConnectorEntry(id="Garmin", uid="u1", type="local_folder", folder="/b")
        ),
    )
    tab._conn_list.setCurrentRow(0)
    tab._edit_connector()

    group = store.load_gui_config().sync_groups[0]
    assert [s.id for s in group.sources] == ["Garmin"]
    assert group.destinations == ["Garmin"]


class _FakeGroupDialog:
    def __init__(self, entry: SyncGroupEntry) -> None:
        self._entry = entry

    def exec(self) -> int:
        from PySide6.QtWidgets import QDialog

        return QDialog.DialogCode.Accepted

    def result_entry(self) -> SyncGroupEntry:
        return self._entry


def test_config_tab_add_group_rejects_duplicate_name(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    import app.gui.app as gui_app

    store.save_gui_config(
        GuiConfig(
            connectors=[ConnectorEntry(id="local", type="local_folder", folder="/x")],
            sync_groups=[SyncGroupEntry(id="g", sources=[], destinations=[])],
        )
    )
    tab = ConfigTab(store)
    qtbot.addWidget(tab)

    dupe = SyncGroupEntry(id="g", sources=[], destinations=[])
    monkeypatch.setattr(gui_app, "SyncGroupDialog", lambda **kw: _FakeGroupDialog(dupe))
    warned: list[bool] = []
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: warned.append(True))
    tab._add_group()

    assert warned == [True]
    assert [g.id for g in store.load_gui_config().sync_groups] == ["g"]


def test_config_tab_double_click_connector_opens_edit(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    import app.gui.app as gui_app

    store.save_gui_config(
        GuiConfig(connectors=[ConnectorEntry(id="garmin", type="local_folder")])
    )
    tab = ConfigTab(store)
    qtbot.addWidget(tab)
    opened: dict = {}
    monkeypatch.setattr(
        gui_app,
        "ConnectorDialog",
        lambda **kw: _RejectingDialog(opened, kw.get("entry")),
    )
    tab._conn_list.setCurrentRow(0)
    tab._conn_list.itemDoubleClicked.emit(tab._conn_list.item(0))
    assert opened["entry"].id == "garmin"


def test_config_tab_double_click_group_opens_edit(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    import app.gui.app as gui_app

    store.save_gui_config(
        GuiConfig(
            connectors=[ConnectorEntry(id="local", type="local_folder", folder="/x")],
            sync_groups=[
                SyncGroupEntry(
                    id="g", sources=[GroupSourceEntry("local", 1)], destinations=[]
                )
            ],
        )
    )
    tab = ConfigTab(store)
    qtbot.addWidget(tab)
    opened: dict = {}
    monkeypatch.setattr(
        gui_app,
        "SyncGroupDialog",
        lambda **kw: _RejectingDialog(opened, kw.get("entry")),
    )
    tab._grp_list.setCurrentRow(0)
    tab._grp_list.itemDoubleClicked.emit(tab._grp_list.item(0))
    assert opened["entry"].id == "g"


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


def test_sync_worker_cancel_stops_a_running_sync(qtbot, store: ConfigStore) -> None:
    # Quitting mid-sync has to be able to unwind the thread: Qt aborts the
    # process if a QThread is destroyed while it is still running.
    class _SlowWorker(SyncWorker):
        async def _async_sync(self) -> int:
            await asyncio.sleep(60)
            return 0

    worker = _SlowWorker(store, GuiConfig(connectors=[], sync_groups=[]), GuiRenderer())
    worker.start()
    qtbot.waitUntil(lambda: worker._task is not None, timeout=5000)
    worker.cancel()
    assert worker.wait(5000)
    assert not worker.isRunning()


def test_cancelling_before_the_sync_arms_itself_still_stops_it(
    qtbot, store: ConfigStore
) -> None:
    # cancel() has no task to reach in the gap between start() and the
    # coroutine's first line; closing the window right after Run lands there.
    class _SlowWorker(SyncWorker):
        async def _async_sync(self) -> int:
            await asyncio.sleep(60)
            return 0

    worker = _SlowWorker(store, GuiConfig(connectors=[], sync_groups=[]), GuiRenderer())
    worker.cancel()
    worker.start()

    assert worker.wait(5000)
    assert not worker.isRunning()


def test_sync_worker_cancel_before_it_started_does_nothing(store: ConfigStore) -> None:
    worker = SyncWorker(store, GuiConfig(connectors=[], sync_groups=[]), GuiRenderer())
    worker.cancel()  # must not raise: there is no loop to reach into yet


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


def test_main_window_title_is_the_display_name(qtbot, store: ConfigStore) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    assert window.windowTitle() == APP_DISPLAY_NAME


# ---------------------------------------------------------------------------
# Application identity
# ---------------------------------------------------------------------------


def test_configure_app_identity_names_the_app_for_the_shell(qapp) -> None:
    before = (
        qapp.applicationName(),
        qapp.applicationDisplayName(),
        qapp.desktopFileName(),
    )
    try:
        configure_app_identity(qapp)
        assert qapp.applicationName() == APP_DISPLAY_NAME
        assert qapp.applicationDisplayName() == APP_DISPLAY_NAME
        assert qapp.desktopFileName() == APP_DESKTOP_NAME
    finally:
        qapp.setApplicationName(before[0])
        qapp.setApplicationDisplayName(before[1])
        qapp.setDesktopFileName(before[2])


def test_display_name_is_not_the_artifact_name() -> None:
    # The regression this guards: the shell name fell back to the packaged
    # file name, so the dock read "trainings-sync".
    assert APP_DISPLAY_NAME != APP_DESKTOP_NAME
    assert "-" not in APP_DISPLAY_NAME


def _stub_sync_tab(monkeypatch, window, *, running: bool, stops: bool = True) -> list:
    calls: list[str] = []
    monkeypatch.setattr(window._sync_tab, "sync_running", lambda: running)

    def _stop(*_args: object, **_kwargs: object) -> bool:
        calls.append("stop")
        return stops

    monkeypatch.setattr(window._sync_tab, "stop_sync", _stop)
    return calls


def test_closing_without_a_running_sync_closes_straight_away(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    calls = _stub_sync_tab(monkeypatch, window, running=False)
    assert window.close() is True
    assert calls == []


def test_closing_mid_sync_can_be_called_off(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    window = MainWindow(store)
    qtbot.addWidget(window)
    calls = _stub_sync_tab(monkeypatch, window, running=True)
    monkeypatch.setattr(
        QMessageBox,
        "question",
        staticmethod(lambda *a, **k: QMessageBox.StandardButton.No),
    )
    assert window.close() is False
    assert calls == []  # the running sync was left alone


def test_closing_mid_sync_stops_the_sync_first(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    from PySide6.QtWidgets import QMessageBox

    window = MainWindow(store)
    qtbot.addWidget(window)
    calls = _stub_sync_tab(monkeypatch, window, running=True)
    monkeypatch.setattr(
        QMessageBox,
        "question",
        staticmethod(lambda *a, **k: QMessageBox.StandardButton.Yes),
    )
    assert window.close() is True
    assert calls == ["stop"]


def test_a_stop_that_times_out_leaves_the_tab_usable(qtbot, store: ConfigStore) -> None:
    # The window stays open in this case, so the tab must not be left with a
    # dead Run button: the worker's signals are disconnected by now, and
    # nothing else ever re-enables it.
    #
    # Cancelling does not interrupt a blocking call already handed to a thread,
    # which is how every connector does its network I/O - so this is the shape
    # a real overrun takes.
    class _StubbornWorker(SyncWorker):
        async def _async_sync(self) -> int:
            await asyncio.to_thread(time.sleep, 2)
            return 0

    window = MainWindow(store)
    qtbot.addWidget(window)
    tab = window._sync_tab
    worker = _StubbornWorker(
        store, GuiConfig(connectors=[], sync_groups=[]), GuiRenderer()
    )
    worker.started_ts.connect(tab._on_started)
    worker.finished_ts.connect(tab._on_finished)
    worker.error_occurred.connect(tab._on_error)
    tab._worker = worker
    tab._run_btn.setEnabled(False)
    worker.start()
    qtbot.waitUntil(lambda: worker._task is not None, timeout=5000)

    try:
        assert tab.stop_sync(timeout_ms=1) is False
        # Run stays disabled while the abandoned worker is alive.
        assert not tab._run_btn.isEnabled()
    finally:
        # Never leave the thread running: a QThread still alive at interpreter
        # shutdown aborts the whole process.
        assert worker.wait(10000)
    qtbot.waitUntil(tab._run_btn.isEnabled, timeout=5000)


def test_run_is_refused_while_a_stopped_sync_is_still_unwinding(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    # Two syncs on one cache index means lost updates, and rebinding the
    # worker would destroy a live QThread - which aborts the process.
    from PySide6.QtWidgets import QMessageBox

    class _StubbornWorker(SyncWorker):
        async def _async_sync(self) -> int:
            await asyncio.to_thread(time.sleep, 2)
            return 0

    window = MainWindow(store)
    qtbot.addWidget(window)
    tab = window._sync_tab
    worker = _StubbornWorker(
        store, GuiConfig(connectors=[], sync_groups=[]), GuiRenderer()
    )
    tab._worker = worker
    worker.start()
    qtbot.waitUntil(lambda: worker._task is not None, timeout=5000)

    told = []
    monkeypatch.setattr(
        QMessageBox, "information", staticmethod(lambda *a, **k: told.append(a))
    )
    try:
        tab._run_sync()
        assert told, "starting a second sync should be refused"
        assert tab._worker is worker  # the live thread was not replaced
    finally:
        worker.cancel()
        assert worker.wait(10000)


def test_a_sync_ending_just_as_the_stop_times_out_still_frees_the_tab(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    # QThread.finished fires once. If it lands between the wait timing out and
    # the connect, nothing would ever re-enable Run again.
    class _SlowWorker(SyncWorker):
        async def _async_sync(self) -> int:
            await asyncio.sleep(60)
            return 0

    window = MainWindow(store)
    qtbot.addWidget(window)
    tab = window._sync_tab
    worker = _SlowWorker(store, GuiConfig(connectors=[], sync_groups=[]), GuiRenderer())
    tab._worker = worker
    tab._run_btn.setEnabled(False)
    worker.start()
    qtbot.waitUntil(lambda: worker._task is not None, timeout=5000)

    # wait() reports a timeout, but the run is over by the time we look again.
    def _wait_then_finish(_timeout: int = 0) -> bool:
        worker.cancel()
        SyncWorker.wait(worker, 5000)
        return False

    monkeypatch.setattr(worker, "wait", _wait_then_finish)

    assert tab.stop_sync(timeout_ms=1) is False
    assert tab._run_btn.isEnabled()


def test_stopping_an_idle_tab_is_a_no_op(qtbot, store: ConfigStore) -> None:
    window = MainWindow(store)
    qtbot.addWidget(window)
    assert window._sync_tab.stop_sync() is True


def test_window_stays_open_when_the_sync_refuses_to_stop(
    qtbot, monkeypatch, store: ConfigStore
) -> None:
    # Destroying a QThread that is still running aborts the process, so a sync
    # that will not stop has to keep the window alive instead of crashing.
    from PySide6.QtWidgets import QMessageBox

    window = MainWindow(store)
    qtbot.addWidget(window)
    calls = _stub_sync_tab(monkeypatch, window, running=True, stops=False)
    monkeypatch.setattr(
        QMessageBox,
        "question",
        staticmethod(lambda *a, **k: QMessageBox.StandardButton.Yes),
    )
    monkeypatch.setattr(QMessageBox, "warning", staticmethod(lambda *a, **k: None))
    assert window.close() is False
    assert calls == ["stop"]


# ---------------------------------------------------------------------------
# Application icon
# ---------------------------------------------------------------------------


def test_make_app_icon_renders(qtbot) -> None:
    icon = make_app_icon(64)
    assert not icon.isNull()
    assert not icon.pixmap(64, 64).isNull()
