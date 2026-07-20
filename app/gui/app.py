"""PySide6 GUI for trainings-sync."""

from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtCore import QDate, Qt, QThread, Signal
from PySide6.QtGui import QAction

if TYPE_CHECKING:
    from app.core.config import AppConfig
    from app.credentials.base import CredentialProvider
    from app.tracking.tracker import TaskTracker
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
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
# SyncWorker - runs the full sync pipeline in a background QThread
# ---------------------------------------------------------------------------


class SyncWorker(QThread):
    started_ts = Signal(str)  # ISO timestamp when sync starts
    finished_ts = Signal(str, int)  # ISO timestamp, download_failures count
    error_occurred = Signal(str)  # error message on unexpected failure

    def __init__(
        self,
        config_store: ConfigStore,
        gui_config: GuiConfig,
        renderer: GuiRenderer,
    ) -> None:
        super().__init__()
        self._store = config_store
        self._gui_config = gui_config
        self._renderer = renderer

    def run(self) -> None:
        ts_start = datetime.now().astimezone().isoformat(timespec="seconds")
        self.started_ts.emit(ts_start)
        try:
            failures = asyncio.run(self._async_sync())
            ts_end = datetime.now().astimezone().isoformat(timespec="seconds")
            self.finished_ts.emit(ts_end, failures)
        except Exception as exc:
            self.error_occurred.emit(str(exc))

    async def _async_sync(self) -> int:
        from app.core.cache import ActivityCache
        from app.core.config import StravaConnectorConfig
        from app.core.connector_factory import build_connectors
        from app.core.orchestrator import SyncOrchestrator
        from app.credentials.json_file import JsonFileProvider
        from app.tracking.tracker import TaskTracker

        app_config = self._store.to_app_config(self._gui_config)
        app_config.cache_dir.mkdir(parents=True, exist_ok=True)

        tracker = TaskTracker(self._renderer)
        provider = JsonFileProvider(path=self._store.credentials_path, tracker=tracker)

        strava_cred_map = {
            c.id: c.credential
            for c in app_config.connectors
            if isinstance(c, StravaConnectorConfig)
        }

        def _on_token_refresh(
            connector_id: str, new_creds: object, _label: str
        ) -> None:
            provider.update_refresh_token(
                strava_cred_map[connector_id],
                new_creds.refresh_token,  # type: ignore[attr-defined]
            )

        cache = ActivityCache(app_config.cache_dir)
        cache.load()

        connectors = await build_connectors(
            app_config, provider, tracker, on_strava_token_refresh=_on_token_refresh
        )
        login_tasks = {
            cid: asyncio.create_task(c.login()) for cid, c in connectors.items()
        }
        orchestrator = SyncOrchestrator(
            groups=app_config.sync_groups,
            connectors=connectors,
            cache=cache,
            tracker=tracker,
            login_tasks=login_tasks,
        )
        try:
            start = _parse_date_or_default(self._gui_config.start, date(2000, 1, 1))
            end = _parse_date_or_default(self._gui_config.end, date.today())
            force = self._gui_config.force

            if self._gui_config.skip_wellness:
                failures = await orchestrator.run(start, end, force=force)
            else:
                failures, _ = await asyncio.gather(
                    orchestrator.run(start, end, force=force),
                    self._run_wellness(
                        app_config, provider, tracker, connectors, login_tasks
                    ),
                )
        finally:
            for t in login_tasks.values():
                if not t.done():
                    t.cancel()
            await asyncio.gather(*login_tasks.values(), return_exceptions=True)

        return failures

    async def _run_wellness(
        self,
        app_config: AppConfig,
        provider: CredentialProvider,
        tracker: TaskTracker,
        connectors: dict,
        login_tasks: dict,
    ) -> None:
        try:
            from app.connectors.local_folder_wellness import (
                LocalFolderWellnessConnector,
            )
            from app.core.connector_factory import build_wellness_connectors
            from app.core.wellness_cache import WellnessCache
            from app.core.wellness_orchestrator import WellnessOrchestrator

            start = _parse_date_or_default(self._gui_config.start, date(2000, 1, 1))
            end = _parse_date_or_default(self._gui_config.end, date.today())

            wellness_cache = WellnessCache(app_config.cache_dir)
            wellness_connectors = await build_wellness_connectors(
                app_config, provider, tracker, connectors
            )
            for wc in wellness_connectors.values():
                if isinstance(wc, LocalFolderWellnessConnector):
                    await wc.login()
            wellness_orch = WellnessOrchestrator(
                wellness_connectors,
                wellness_cache,
                tracker,
                login_tasks=login_tasks,
            )
            await wellness_orch.run(start, end, force=self._gui_config.force)
        except Exception:  # noqa: S110
            pass  # wellness failures are non-fatal, mirroring CLI behaviour


def _parse_date_or_default(value: str, default: date) -> date:
    if value:
        return date.fromisoformat(value)
    return default


# ---------------------------------------------------------------------------
# TaskRow - one row per sync task shown in the Sync tab
# ---------------------------------------------------------------------------


class TaskRow(QWidget):
    def __init__(
        self, name: str, total: int | None, parent: QWidget | None = None
    ) -> None:
        super().__init__(parent)
        self._name = name
        self._total = total

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 2, 4, 2)

        self._status = QLabel("[~]")  # [~]
        self._status.setFixedWidth(24)
        layout.addWidget(self._status)

        self._label = QLabel(name)
        self._label.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred
        )
        layout.addWidget(self._label)

        self._bar = QProgressBar()
        self._bar.setFixedWidth(220)
        if total is not None:
            self._bar.setRange(0, total)
            self._bar.setValue(0)
        else:
            self._bar.setRange(0, 0)  # indeterminate spinner
        layout.addWidget(self._bar)

        self._count = QLabel("")
        self._count.setFixedWidth(64)
        self._count.setAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        layout.addWidget(self._count)

    def update_progress(self, progress: int) -> None:
        if self._total is not None:
            self._bar.setRange(0, self._total)
        self._bar.setValue(progress)
        self._count.setText(
            f"{progress}/{self._total}" if self._total is not None else str(progress)
        )

    def update_total(self, total: int) -> None:
        self._total = total
        self._bar.setRange(0, total)

    def mark_done(self, warnings: list[str]) -> None:
        cap = max(1, self._total or 1)
        self._bar.setRange(0, cap)
        self._bar.setValue(cap)
        if warnings:
            self._status.setText("[!]")  # [!]
            self._label.setText(f"{self._name} ({len(warnings)} warning(s))")
        else:
            self._status.setText("[OK]")  # [OK]

    def mark_failed(self, error: str) -> None:
        self._status.setText("[X]")  # [X]
        self._label.setText(f"{self._name}: {error}")


# ---------------------------------------------------------------------------
# Credentials tab
# ---------------------------------------------------------------------------


class CredentialDialog(QDialog):
    def __init__(
        self,
        entry: CredentialEntry | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Add Credential" if entry is None else "Edit Credential")
        self.setMinimumWidth(420)

        form = QFormLayout(self)

        self._service = QLineEdit(entry.service if entry else "")
        self._url = QLineEdit(entry.url if entry else "")
        self._login = QLineEdit(entry.login if entry else "")
        self._password = QLineEdit(entry.password if entry else "")
        self._password.setEchoMode(QLineEdit.EchoMode.Password)

        form.addRow("Service:", self._service)
        form.addRow("URL:", self._url)
        form.addRow("Login:", self._login)
        form.addRow("Password / Token:", self._password)

        hint = QLabel("For Strava: login = client_secret, password = refresh_token")
        hint.setWordWrap(True)
        form.addRow(hint)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        form.addRow(btns)

    def result_entry(self) -> CredentialEntry:
        return CredentialEntry(
            service=self._service.text().strip(),
            url=self._url.text().strip(),
            login=self._login.text().strip(),
            password=self._password.text(),
        )


class CredentialsTab(QWidget):
    def __init__(self, store: ConfigStore, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._store = store
        self._entries: list[CredentialEntry] = store.load_credentials()

        layout = QVBoxLayout(self)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["Service", "URL", "Login", "Password"])
        self._table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.Stretch
        )
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        layout.addWidget(self._table)

        btn_row = QHBoxLayout()
        self._add_btn = QPushButton("Add")
        self._edit_btn = QPushButton("Edit")
        self._delete_btn = QPushButton("Delete")
        for btn in (self._add_btn, self._edit_btn, self._delete_btn):
            btn_row.addWidget(btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        self._add_btn.clicked.connect(self._add)
        self._edit_btn.clicked.connect(self._edit)
        self._delete_btn.clicked.connect(self._delete)

        self._refresh_table()

    def _refresh_table(self) -> None:
        self._table.setRowCount(0)
        for entry in self._entries:
            row = self._table.rowCount()
            self._table.insertRow(row)
            self._table.setItem(row, 0, QTableWidgetItem(entry.service))
            self._table.setItem(row, 1, QTableWidgetItem(entry.url))
            self._table.setItem(row, 2, QTableWidgetItem(entry.login))
            self._table.setItem(row, 3, QTableWidgetItem("*" * 8))

    def _add(self) -> None:
        dlg = CredentialDialog(parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._entries.append(dlg.result_entry())
            self._store.save_credentials(self._entries)
            self._refresh_table()

    def _edit(self) -> None:
        row = self._table.currentRow()
        if row < 0:
            return
        dlg = CredentialDialog(entry=self._entries[row], parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._entries[row] = dlg.result_entry()
            self._store.save_credentials(self._entries)
            self._refresh_table()

    def _delete(self) -> None:
        row = self._table.currentRow()
        if row < 0:
            return
        entry = self._entries[row]
        reply = QMessageBox.question(
            self,
            "Delete credential",
            f"Delete credential for {entry.service!r}?",
        )
        if reply == QMessageBox.StandardButton.Yes:
            self._entries.pop(row)
            self._store.save_credentials(self._entries)
            self._refresh_table()


# ---------------------------------------------------------------------------
# Configuration tab
# ---------------------------------------------------------------------------


class ConnectorDialog(QDialog):
    def __init__(
        self,
        entry: ConnectorEntry | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Add Connector" if entry is None else "Edit Connector")
        self.setMinimumWidth(440)

        root = QVBoxLayout(self)
        form = QFormLayout()

        self._id = QLineEdit(entry.id if entry else "")
        form.addRow("ID:", self._id)

        self._type = QComboBox()
        self._type.addItems(["garmin", "strava", "local_folder"])
        if entry:
            idx = self._type.findText(entry.type)
            if idx >= 0:
                self._type.setCurrentIndex(idx)
        form.addRow("Type:", self._type)

        # Credential fields (garmin + strava)
        self._cred_box = QGroupBox("Credentials")
        cred_form = QFormLayout(self._cred_box)
        self._cred_service = QLineEdit(entry.credential_service if entry else "")
        self._cred_url = QLineEdit(entry.credential_url if entry else "")
        self._cred_login = QLineEdit(entry.credential_login if entry else "")
        cred_form.addRow("Service:", self._cred_service)
        cred_form.addRow("URL:", self._cred_url)
        cred_form.addRow("Login (optional):", self._cred_login)

        # Strava-only
        self._client_id_spin = QSpinBox()
        self._client_id_spin.setRange(0, 999_999_999)
        if entry:
            self._client_id_spin.setValue(entry.client_id)
        cred_form.addRow("Client ID (Strava):", self._client_id_spin)

        # Local folder
        self._folder_box = QGroupBox("Local folder")
        folder_form = QFormLayout(self._folder_box)
        self._folder = QLineEdit(entry.folder if entry else "")
        folder_form.addRow("Path:", self._folder)

        form.addRow(self._cred_box)
        form.addRow(self._folder_box)
        root.addLayout(form)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        root.addWidget(btns)

        self._type.currentTextChanged.connect(self._on_type_changed)
        self._on_type_changed(self._type.currentText())

    def _on_type_changed(self, t: str) -> None:
        self._cred_box.setVisible(t in ("garmin", "strava"))
        self._client_id_spin.setVisible(t == "strava")
        self._folder_box.setVisible(t == "local_folder")

    def result_entry(self) -> ConnectorEntry:
        t = self._type.currentText()
        return ConnectorEntry(
            id=self._id.text().strip(),
            type=t,
            credential_service=self._cred_service.text().strip(),
            credential_url=self._cred_url.text().strip(),
            credential_login=self._cred_login.text().strip(),
            client_id=self._client_id_spin.value() if t == "strava" else 0,
            folder=self._folder.text().strip() if t == "local_folder" else "",
        )


class SyncGroupDialog(QDialog):
    def __init__(
        self,
        connector_ids: list[str],
        entry: SyncGroupEntry | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Add Sync Group" if entry is None else "Edit Sync Group")
        self.setMinimumWidth(480)
        self._connector_ids = connector_ids

        root = QVBoxLayout(self)
        form = QFormLayout()

        self._id = QLineEdit(entry.id if entry else "")
        form.addRow("Group ID:", self._id)
        root.addLayout(form)

        # Sources
        src_box = QGroupBox("Sources (connector id : priority)")
        src_layout = QVBoxLayout(src_box)
        self._sources_widget = QListWidget()
        src_layout.addWidget(self._sources_widget)
        src_btn_row = QHBoxLayout()
        self._src_add_combo = QComboBox()
        self._src_add_combo.addItems(connector_ids)
        self._src_priority = QSpinBox()
        self._src_priority.setRange(1, 99)
        self._src_priority.setValue(1)
        self._src_add_btn = QPushButton("Add source")
        self._src_del_btn = QPushButton("Remove")
        self._src_add_btn.clicked.connect(self._add_source)
        self._src_del_btn.clicked.connect(self._remove_source)
        src_btn_row.addWidget(self._src_add_combo)
        src_btn_row.addWidget(QLabel("priority:"))
        src_btn_row.addWidget(self._src_priority)
        src_btn_row.addWidget(self._src_add_btn)
        src_btn_row.addWidget(self._src_del_btn)
        src_layout.addLayout(src_btn_row)

        if entry:
            for s in entry.sources:
                self._sources_widget.addItem(self._make_source_item(s.id, s.priority))

        root.addWidget(src_box)

        # Destinations
        dst_box = QGroupBox("Destinations (connector ids)")
        dst_layout = QVBoxLayout(dst_box)
        self._destinations_widget = QListWidget()
        if entry:
            for d in entry.destinations:
                self._destinations_widget.addItem(d)
        dst_layout.addWidget(self._destinations_widget)
        dst_btn_row = QHBoxLayout()
        self._dst_add_combo = QComboBox()
        self._dst_add_combo.addItems(connector_ids)
        self._dst_add_btn = QPushButton("Add destination")
        self._dst_del_btn = QPushButton("Remove")
        self._dst_add_btn.clicked.connect(self._add_destination)
        self._dst_del_btn.clicked.connect(self._remove_destination)
        dst_btn_row.addWidget(self._dst_add_combo)
        dst_btn_row.addWidget(self._dst_add_btn)
        dst_btn_row.addWidget(self._dst_del_btn)
        dst_layout.addLayout(dst_btn_row)
        root.addWidget(dst_box)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        root.addWidget(btns)

    @staticmethod
    def _make_source_item(cid: str, priority: int) -> QListWidgetItem:
        item = QListWidgetItem(f"{cid} : {priority}")
        item.setData(Qt.ItemDataRole.UserRole, (cid, priority))
        return item

    def _add_source(self) -> None:
        cid = self._src_add_combo.currentText()
        pri = self._src_priority.value()
        if cid:
            self._sources_widget.addItem(self._make_source_item(cid, pri))

    def _remove_source(self) -> None:
        row = self._sources_widget.currentRow()
        if row >= 0:
            self._sources_widget.takeItem(row)

    def _add_destination(self) -> None:
        cid = self._dst_add_combo.currentText()
        if cid:
            self._destinations_widget.addItem(cid)

    def _remove_destination(self) -> None:
        row = self._destinations_widget.currentRow()
        if row >= 0:
            self._destinations_widget.takeItem(row)

    def result_entry(self) -> SyncGroupEntry:
        sources = []
        for i in range(self._sources_widget.count()):
            cid, priority = self._sources_widget.item(i).data(Qt.ItemDataRole.UserRole)
            sources.append(GroupSourceEntry(id=cid, priority=priority))
        destinations = [
            self._destinations_widget.item(i).text()
            for i in range(self._destinations_widget.count())
        ]
        return SyncGroupEntry(
            id=self._id.text().strip(),
            sources=sources,
            destinations=destinations,
        )


class ConfigTab(QWidget):
    def __init__(self, store: ConfigStore, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._store = store
        self._config: GuiConfig = store.load_gui_config()

        root = QVBoxLayout(self)

        # Connectors
        conn_box = QGroupBox("Connectors")
        conn_layout = QVBoxLayout(conn_box)
        self._conn_list = QListWidget()
        conn_layout.addWidget(self._conn_list)
        conn_btn_row = QHBoxLayout()
        self._conn_add = QPushButton("Add")
        self._conn_edit = QPushButton("Edit")
        self._conn_del = QPushButton("Delete")
        for b in (self._conn_add, self._conn_edit, self._conn_del):
            conn_btn_row.addWidget(b)
        conn_btn_row.addStretch()
        conn_layout.addLayout(conn_btn_row)
        root.addWidget(conn_box)

        self._conn_add.clicked.connect(self._add_connector)
        self._conn_edit.clicked.connect(self._edit_connector)
        self._conn_del.clicked.connect(self._delete_connector)

        # Sync groups
        grp_box = QGroupBox("Sync Groups")
        grp_layout = QVBoxLayout(grp_box)
        self._grp_list = QListWidget()
        grp_layout.addWidget(self._grp_list)
        grp_btn_row = QHBoxLayout()
        self._grp_add = QPushButton("Add")
        self._grp_edit = QPushButton("Edit")
        self._grp_del = QPushButton("Delete")
        for b in (self._grp_add, self._grp_edit, self._grp_del):
            grp_btn_row.addWidget(b)
        grp_btn_row.addStretch()
        grp_layout.addLayout(grp_btn_row)
        root.addWidget(grp_box)

        self._grp_add.clicked.connect(self._add_group)
        self._grp_edit.clicked.connect(self._edit_group)
        self._grp_del.clicked.connect(self._delete_group)

        # Options
        opt_box = QGroupBox("Options")
        opt_layout = QFormLayout(opt_box)

        date_row_start = QHBoxLayout()
        self._use_start = QCheckBox("Use custom start date")
        self._start_date = QDateEdit()
        self._start_date.setCalendarPopup(True)
        self._start_date.setDisplayFormat("yyyy-MM-dd")
        if self._config.start:
            d = date.fromisoformat(self._config.start)
            self._start_date.setDate(QDate(d.year, d.month, d.day))
            self._use_start.setChecked(True)
        else:
            self._start_date.setDate(QDate.currentDate())
        self._start_date.setEnabled(self._use_start.isChecked())
        self._use_start.toggled.connect(self._start_date.setEnabled)
        date_row_start.addWidget(self._use_start)
        date_row_start.addWidget(self._start_date)
        opt_layout.addRow("Start:", date_row_start)

        date_row_end = QHBoxLayout()
        self._use_end = QCheckBox("Use custom end date")
        self._end_date = QDateEdit()
        self._end_date.setCalendarPopup(True)
        self._end_date.setDisplayFormat("yyyy-MM-dd")
        if self._config.end:
            d2 = date.fromisoformat(self._config.end)
            self._end_date.setDate(QDate(d2.year, d2.month, d2.day))
            self._use_end.setChecked(True)
        else:
            self._end_date.setDate(QDate.currentDate())
        self._end_date.setEnabled(self._use_end.isChecked())
        self._use_end.toggled.connect(self._end_date.setEnabled)
        date_row_end.addWidget(self._use_end)
        date_row_end.addWidget(self._end_date)
        opt_layout.addRow("End:", date_row_end)

        self._force_cb = QCheckBox("Force re-download (ignore cache)")
        self._force_cb.setChecked(self._config.force)
        opt_layout.addRow(self._force_cb)

        self._skip_wellness_cb = QCheckBox("Skip wellness sync")
        self._skip_wellness_cb.setChecked(self._config.skip_wellness)
        opt_layout.addRow(self._skip_wellness_cb)

        save_btn = QPushButton("Save configuration")
        save_btn.clicked.connect(self._save)
        opt_layout.addRow(save_btn)

        root.addWidget(opt_box)
        root.addStretch()

        self._refresh_connector_list()
        self._refresh_group_list()

    # ------------------------------------------------------------------
    # Connectors
    # ------------------------------------------------------------------

    def _refresh_connector_list(self) -> None:
        self._conn_list.clear()
        for c in self._config.connectors:
            self._conn_list.addItem(f"[{c.type}] {c.id}")

    def _add_connector(self) -> None:
        dlg = ConnectorDialog(parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._config.connectors.append(dlg.result_entry())
            self._store.save_gui_config(self._config)
            self._refresh_connector_list()

    def _edit_connector(self) -> None:
        row = self._conn_list.currentRow()
        if row < 0:
            return
        dlg = ConnectorDialog(entry=self._config.connectors[row], parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._config.connectors[row] = dlg.result_entry()
            self._store.save_gui_config(self._config)
            self._refresh_connector_list()

    def _delete_connector(self) -> None:
        row = self._conn_list.currentRow()
        if row < 0:
            return
        name = self._config.connectors[row].id
        reply = QMessageBox.question(
            self, "Delete connector", f"Delete connector {name!r}?"
        )
        if reply == QMessageBox.StandardButton.Yes:
            self._config.connectors.pop(row)
            self._store.save_gui_config(self._config)
            self._refresh_connector_list()

    # ------------------------------------------------------------------
    # Sync groups
    # ------------------------------------------------------------------

    def _connector_ids(self) -> list[str]:
        return [c.id for c in self._config.connectors]

    def _refresh_group_list(self) -> None:
        self._grp_list.clear()
        for g in self._config.sync_groups:
            src_str = ", ".join(f"{s.id}(p{s.priority})" for s in g.sources)
            dst_str = ", ".join(g.destinations)
            self._grp_list.addItem(f"{g.id}  [{src_str}] -> [{dst_str}]")

    def _add_group(self) -> None:
        dlg = SyncGroupDialog(connector_ids=self._connector_ids(), parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._config.sync_groups.append(dlg.result_entry())
            self._store.save_gui_config(self._config)
            self._refresh_group_list()

    def _edit_group(self) -> None:
        row = self._grp_list.currentRow()
        if row < 0:
            return
        dlg = SyncGroupDialog(
            connector_ids=self._connector_ids(),
            entry=self._config.sync_groups[row],
            parent=self,
        )
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._config.sync_groups[row] = dlg.result_entry()
            self._store.save_gui_config(self._config)
            self._refresh_group_list()

    def _delete_group(self) -> None:
        row = self._grp_list.currentRow()
        if row < 0:
            return
        name = self._config.sync_groups[row].id
        reply = QMessageBox.question(
            self, "Delete group", f"Delete sync group {name!r}?"
        )
        if reply == QMessageBox.StandardButton.Yes:
            self._config.sync_groups.pop(row)
            self._store.save_gui_config(self._config)
            self._refresh_group_list()

    # ------------------------------------------------------------------
    # Save options
    # ------------------------------------------------------------------

    def _save(self) -> None:
        self._config.force = self._force_cb.isChecked()
        self._config.skip_wellness = self._skip_wellness_cb.isChecked()
        self._config.start = (
            self._start_date.date().toString("yyyy-MM-dd")
            if self._use_start.isChecked()
            else ""
        )
        self._config.end = (
            self._end_date.date().toString("yyyy-MM-dd")
            if self._use_end.isChecked()
            else ""
        )
        self._store.save_gui_config(self._config)

    def current_config(self) -> GuiConfig:
        """Return live config with current option widget values."""
        self._save()
        return self._config


# ---------------------------------------------------------------------------
# Sync tab
# ---------------------------------------------------------------------------


class LogDialog(QDialog):
    def __init__(self, log_path: Path, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Full sync log")
        self.resize(800, 600)

        layout = QVBoxLayout(self)
        text = QTextEdit()
        text.setReadOnly(True)
        text.setFontFamily("monospace")
        if log_path.exists():
            text.setPlainText(log_path.read_text(encoding="utf-8", errors="replace"))
        else:
            text.setPlainText("(log file not found)")
        layout.addWidget(text)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)


class SyncTab(QWidget):
    def __init__(
        self,
        store: ConfigStore,
        config_tab: ConfigTab,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._store = store
        self._config_tab = config_tab
        self._worker: SyncWorker | None = None
        self._task_rows: dict[str, TaskRow] = {}

        root = QVBoxLayout(self)

        # Toolbar
        toolbar = QHBoxLayout()
        self._run_btn = QPushButton(">  Run Sync")
        self._run_btn.setFixedHeight(36)
        self._log_btn = QPushButton("\U0001f4cb  Show full log")
        toolbar.addWidget(self._run_btn)
        toolbar.addWidget(self._log_btn)
        toolbar.addStretch()
        root.addLayout(toolbar)

        self._status = QLabel("Ready")
        root.addWidget(self._status)

        # Scrollable task list
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._task_container = QWidget()
        self._task_layout = QVBoxLayout(self._task_container)
        self._task_layout.setSpacing(2)
        self._task_layout.addStretch()
        scroll.setWidget(self._task_container)
        root.addWidget(scroll)

        self._run_btn.clicked.connect(self._run_sync)
        self._log_btn.clicked.connect(self._show_log)

    def _run_sync(self) -> None:
        gui_config = self._config_tab.current_config()

        # Clear previous task rows
        while self._task_layout.count() > 1:  # keep the trailing stretch
            item = self._task_layout.takeAt(0)
            if item is not None:
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
        self._task_rows.clear()

        renderer = GuiRenderer()
        sigs = renderer.signals
        sigs.task_added.connect(self._on_task_added)
        sigs.progress_updated.connect(self._on_progress)
        sigs.task_done.connect(self._on_task_done)
        sigs.task_failed.connect(self._on_task_failed)
        sigs.total_updated.connect(self._on_total_updated)

        self._worker = SyncWorker(self._store, gui_config, renderer)
        self._worker.started_ts.connect(self._on_started)
        self._worker.finished_ts.connect(self._on_finished)
        self._worker.error_occurred.connect(self._on_error)

        self._run_btn.setEnabled(False)
        self._worker.start()

    def _on_started(self, ts: str) -> None:
        self._status.setText(f"Sync started: {ts}")

    def _on_finished(self, ts: str, failures: int) -> None:
        self._run_btn.setEnabled(True)
        msg = f"Sync finished: {ts}"
        if failures:
            noun = "activity" if failures == 1 else "activities"
            msg += f"  [!] {failures} {noun} failed to download"
        self._status.setText(msg)

    def _on_error(self, error: str) -> None:
        self._run_btn.setEnabled(True)
        self._status.setText(f"[X] Error: {error}")
        QMessageBox.critical(self, "Sync error", error)

    def _on_task_added(self, name: str, total: object) -> None:
        row = TaskRow(
            name, total if isinstance(total, int) else None, self._task_container
        )
        self._task_rows[name] = row
        # Insert before the trailing stretch
        self._task_layout.insertWidget(self._task_layout.count() - 1, row)

    def _on_progress(self, name: str, progress: int) -> None:
        if row := self._task_rows.get(name):
            row.update_progress(progress)

    def _on_task_done(self, name: str, warnings: list) -> None:
        if row := self._task_rows.get(name):
            row.mark_done(warnings)

    def _on_task_failed(self, name: str, error: str) -> None:
        if row := self._task_rows.get(name):
            row.mark_failed(error)

    def _on_total_updated(self, name: str, total: int) -> None:
        if row := self._task_rows.get(name):
            row.update_total(total)

    def _show_log(self) -> None:
        log_path = self._store.cache_dir / "sync.log"
        dlg = LogDialog(log_path, parent=self)
        dlg.exec()


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------


class MainWindow(QMainWindow):
    def __init__(self, store: ConfigStore) -> None:
        super().__init__()
        self.setWindowTitle("Trainings Sync")
        self.resize(800, 600)

        tabs = QTabWidget()

        self._creds_tab = CredentialsTab(store)
        self._config_tab = ConfigTab(store)
        self._sync_tab = SyncTab(store, self._config_tab)

        tabs.addTab(self._creds_tab, "Credentials")
        tabs.addTab(self._config_tab, "Configuration")
        tabs.addTab(self._sync_tab, "Sync")

        self.setCentralWidget(tabs)

        quit_action = QAction("Quit", self)
        quit_action.setShortcut("Ctrl+Q")
        quit_action.triggered.connect(self.close)
        file_menu = self.menuBar().addMenu("File")
        file_menu.addAction(quit_action)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    app = QApplication(sys.argv)
    store = ConfigStore()
    window = MainWindow(store)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
