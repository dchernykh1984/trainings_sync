from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from app.connectors.base import ActivityMeta


@dataclass(frozen=True)
class CacheEntry:
    external_id: str
    source_id: str  # stable id from config, e.g. "garmin-main"
    format: str  # "fit", "gpx", "tcx"
    start_time: datetime  # UTC
    elapsed_s: int | None  # wall-clock duration; None if unknown
    name: str = ""
    sport_type: str = ""
    filename: str = ""  # relative path within cache_dir; set by ActivityCache.put
    needs_refresh: bool = False
    uploaded_to: tuple[str, ...] = field(default_factory=tuple)  # destination ids

    def __post_init__(self) -> None:
        offset = self.start_time.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValueError("start_time must be UTC (offset 0)")
        if self.format not in _VALID_FORMATS:
            raise ValueError(
                f"invalid format {self.format!r}; "
                f"expected one of {sorted(_VALID_FORMATS)}"
            )
        if self.elapsed_s is not None and self.elapsed_s < 0:
            raise ValueError("elapsed_s must be >= 0")
        object.__setattr__(self, "uploaded_to", tuple(self.uploaded_to))


_VALID_FORMATS: frozenset[str] = frozenset({"fit", "gpx", "tcx"})


def _safe_name(value: str) -> str:
    return re.sub(r"[^\w\-]", "_", value)


_READABLE_ID_LEN: int = 80
_ID_HASH_LEN: int = 12


def _id_hash(source_id: str, external_id: str) -> str:
    key = f"{source_id}\x00{external_id}".encode()
    return hashlib.sha256(key).hexdigest()[:_ID_HASH_LEN]


def _entry_to_dict(e: CacheEntry) -> dict:
    return {
        "external_id": e.external_id,
        "source_id": e.source_id,
        "format": e.format,
        "start_time": e.start_time.isoformat(),
        "elapsed_s": e.elapsed_s,
        "name": e.name,
        "sport_type": e.sport_type,
        "filename": e.filename,
        "needs_refresh": e.needs_refresh,
        "uploaded_to": list(e.uploaded_to),
    }


def _parse_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _entry_from_dict(d: dict) -> CacheEntry:
    return CacheEntry(
        external_id=d["external_id"],
        source_id=d["source_id"],
        format=d["format"],
        start_time=_parse_utc(d["start_time"]),
        elapsed_s=d["elapsed_s"],
        name=d.get("name", ""),
        sport_type=d.get("sport_type", ""),
        filename=d["filename"],
        needs_refresh=d.get("needs_refresh", False),
        uploaded_to=tuple(d.get("uploaded_to", [])),
    )


def _intervals_overlap(
    a_start: datetime,
    a_elapsed: int | None,
    b_start: datetime,
    b_elapsed: int | None,
    min_overlap_s: int,
    fallback_s: int,
) -> bool:
    a_end = a_start + timedelta(
        seconds=a_elapsed if a_elapsed is not None else fallback_s
    )
    b_end = b_start + timedelta(
        seconds=b_elapsed if b_elapsed is not None else fallback_s
    )
    overlap_start = max(a_start, b_start)
    overlap_end = min(a_end, b_end)
    if overlap_end <= overlap_start:
        return False
    return (overlap_end - overlap_start).total_seconds() >= min_overlap_s


class ActivityCache:
    def __init__(self, cache_dir: Path) -> None:
        self._dir = cache_dir
        self._entries: list[CacheEntry] = []

    @property
    def _index_path(self) -> Path:
        return self._dir / "index.json"

    def _safe_path(self, filename: str) -> Path:
        p = Path(filename)
        if p.is_absolute():
            raise ValueError(f"filename must be relative: {filename!r}")
        resolved = (self._dir / p).resolve()
        if not resolved.is_relative_to(self._dir.resolve()):
            raise ValueError(f"filename escapes cache_dir: {filename!r}")
        return resolved

    def load(self) -> None:
        if not self._index_path.exists():
            self._entries = []
            return
        data = json.loads(self._index_path.read_text(encoding="utf-8"))
        seen: set[tuple[str, str]] = set()
        entries = []
        for raw in data.get("entries", []):
            entry = _entry_from_dict(raw)
            if not entry.filename:
                raise ValueError("index contains entry with empty filename")
            self._safe_path(entry.filename)
            key = (entry.source_id, entry.external_id)
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)
        self._entries = entries

    def save(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {"entries": [_entry_to_dict(e) for e in self._entries]},
            indent=2,
            ensure_ascii=False,
        )
        tmp = self._index_path.with_suffix(".tmp")
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(self._index_path)

    def put(self, entry: CacheEntry, content: bytes) -> CacheEntry:
        safe_source = _safe_name(entry.source_id)[:_READABLE_ID_LEN]
        if not safe_source:
            raise ValueError("source_id must not be empty after sanitization")
        ts = entry.start_time.strftime("%Y%m%dT%H%M%S")
        safe_id = _safe_name(entry.external_id)[:_READABLE_ID_LEN]
        uid = _id_hash(entry.source_id, entry.external_id)
        filename = f"{safe_source}/{ts}_{safe_id}_{uid}.{entry.format}"
        entry = dataclasses.replace(entry, filename=filename)

        file_path = self._dir / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(file_path) + ".tmp")
        tmp.write_bytes(content)
        tmp.replace(file_path)

        old_entries = [
            e
            for e in self._entries
            if e.external_id == entry.external_id and e.source_id == entry.source_id
        ]
        self._entries = [e for e in self._entries if e not in old_entries]
        for old in old_entries:
            if old.filename and old.filename != filename:
                self._safe_path(old.filename).unlink(missing_ok=True)
        self._entries.append(entry)
        self.save()
        return entry

    def has(self, external_id: str, source_id: str) -> bool:
        entry = self.get_entry(external_id, source_id)
        if entry is None:
            return False
        return self._safe_path(entry.filename).is_file() and not entry.needs_refresh

    def get_entry(self, external_id: str, source_id: str) -> CacheEntry | None:
        for e in self._entries:
            if e.external_id == external_id and e.source_id == source_id:
                return e
        return None

    def all_entries(self) -> list[CacheEntry]:
        return list(self._entries)

    def read_content(self, entry: CacheEntry) -> bytes:
        return self._safe_path(entry.filename).read_bytes()

    def mark_refresh(
        self,
        source_id: str,
        start: date | None = None,
        end: date | None = None,
    ) -> None:
        for i, e in enumerate(self._entries):
            if e.source_id != source_id:
                continue
            e_date = e.start_time.date()
            if (start is None or e_date >= start) and (end is None or e_date <= end):
                self._entries[i] = dataclasses.replace(e, needs_refresh=True)
        self.save()

    def find_overlapping(
        self,
        meta: ActivityMeta,
        min_overlap_s: int = 60,
        fallback_tolerance_s: int = 3600,
    ) -> list[CacheEntry]:
        return [
            e
            for e in self._entries
            if self._safe_path(e.filename).is_file()
            and _intervals_overlap(
                meta.start_time,
                meta.elapsed_s,
                e.start_time,
                e.elapsed_s,
                min_overlap_s,
                fallback_tolerance_s,
            )
        ]

    def mark_uploaded(self, entry: CacheEntry, destination_id: str) -> CacheEntry:
        for i, e in enumerate(self._entries):
            if e.external_id == entry.external_id and e.source_id == entry.source_id:
                if destination_id in e.uploaded_to:
                    return e
                updated = dataclasses.replace(
                    e, uploaded_to=(*e.uploaded_to, destination_id)
                )
                self._entries[i] = updated
                self.save()
                return updated
        return entry
