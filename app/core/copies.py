"""Activities a source holds twice: one workout uploaded twice, or recorded by
two devices at once."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from app.connectors.base import (
    ActivityMeta,
    ActivityUnavailableError,
    ServiceConnector,
    TransientDownloadError,
)
from app.core.cache import ActivityCache, CacheEntry

# A device's own file first: a GPX is most often a conversion of one.
_FORMAT_RANK: dict[str, int] = {"fit": 0, "tcx": 1, "gpx": 2}
# A service files an activity under its local day, a day off the UTC one.
_DAY = timedelta(days=1)
# A service may read the start of a file a little differently.
_SLACK = timedelta(minutes=5)
# The root of a GPX trainings-sync builds (see app/connectors/strava.py).
_OWN_GPX = re.compile(rb'<gpx\b[^>]*\bcreator="trainings-sync"')


@dataclass(frozen=True)
class FoundCopy:
    """An activity to delete: a service holds it because trainings-sync
    uploaded a copy of a workout there, and holds the kept copy's too."""

    connector_id: str  # the connector's uid
    activity_id: str  # the activity there
    copy: CacheEntry  # the entry it was uploaded from
    kept: CacheEntry  # the entry whose activity stays
    kept_id: str  # that activity, to look at before deleting


def _key(entry: CacheEntry) -> tuple[str, str]:
    return (entry.external_id, entry.source_id)


def _rank(entry: CacheEntry) -> tuple[int, int, str]:
    # Numeric ids grow with time, and comparing their lengths first compares
    # them as numbers: the first upload of a workout ranks first.
    return (_FORMAT_RANK[entry.format], len(entry.external_id), entry.external_id)


def find_copies(
    entries: Iterable[CacheEntry], min_overlap_s: int
) -> dict[tuple[str, str], CacheEntry]:
    """Each entry that repeats a kept one of its own source, keyed by
    (external_id, source_id) and mapped to the kept one.

    Two entries of one source are copies when they overlap by at least
    min_overlap_s and by at least half of the longer one. A run inside a long
    ride overlaps it far less, so both stay; so do entries of unknown length.
    The kept copy is the FIT, then the TCX, then the GPX, then the first upload.
    """
    timed = sorted(
        (e for e in entries if e.elapsed_s is not None),
        key=lambda e: (e.source_id, e.start_time),
    )
    near: dict[tuple[str, str], list[CacheEntry]] = {}
    for i, a in enumerate(timed):
        a_end = a.start_time + timedelta(seconds=a.elapsed_s or 0)
        for b in timed[i + 1 :]:
            if b.source_id != a.source_id or b.start_time >= a_end:
                break
            b_end = b.start_time + timedelta(seconds=b.elapsed_s or 0)
            overlap = (min(a_end, b_end) - b.start_time).total_seconds()
            if overlap >= min_overlap_s and 2 * overlap >= max(
                a.elapsed_s or 0, b.elapsed_s or 0
            ):
                near.setdefault(_key(a), []).append(b)
                near.setdefault(_key(b), []).append(a)
    # Best first, so an entry is dropped only for a copy that is itself kept:
    # with A~B and B~C but not A~C, C stays once B goes.
    better: dict[tuple[str, str], CacheEntry] = {}
    for entry in sorted((e for e in timed if _key(e) in near), key=_rank):
        kept = [
            n
            for n in near[_key(entry)]
            if _rank(n) < _rank(entry) and _key(n) not in better
        ]
        if kept:
            better[_key(entry)] = min(kept, key=_rank)
    return better


def _end(entry: CacheEntry) -> datetime:
    return entry.start_time + timedelta(seconds=entry.elapsed_s or 0)


async def _uploads_of(
    connector: ServiceConnector,
    metas: list[ActivityMeta],
    cache: ActivityCache,
    copy: CacheEntry,
    kept: CacheEntry,
) -> tuple[list[str], list[str]]:
    """The ids of the listed activities whose file is the copy's, and of
    those whose file is the kept one's."""
    lo = min(copy.start_time, kept.start_time) - _SLACK
    hi = max(_end(copy), _end(kept))
    files = (cache.read_content(copy), cache.read_content(kept))
    found: tuple[list[str], list[str]] = ([], [])
    for meta in metas:
        if not meta.downloadable or not lo <= meta.start_time <= hi:
            continue
        try:
            content = (await connector.download_activity(meta)).content
        except (ActivityUnavailableError, TransientDownloadError):
            continue  # unread, so it is taken for neither
        for ids, file in zip(found, files, strict=True):
            if content == file:
                ids.append(meta.external_id)
    return found


async def _check(
    uid: str,
    connector: ServiceConnector,
    metas: list[ActivityMeta],
    cache: ActivityCache,
    copy: CacheEntry,
    kept: CacheEntry,
) -> list[FoundCopy]:
    """The activities the copy's file made there, if the kept copy's is
    there as well: else the service holds the workout once, and that stays."""
    if uid not in kept.uploaded_to:
        return []
    copy_ids, kept_ids = await _uploads_of(connector, metas, cache, copy, kept)
    if not kept_ids:
        return []
    # Two copies of one file are one activity there: it is the kept one.
    return [
        FoundCopy(uid, i, copy, kept, kept_ids[0])
        for i in copy_ids
        if i not in kept_ids
    ]


def _span(pairs: list[tuple[CacheEntry, CacheEntry]]) -> tuple[date, date]:
    first = min(min(c.start_time, k.start_time) for c, k in pairs).date() - _DAY
    last = max(max(_end(c), _end(k)) for c, k in pairs).date() + _DAY
    return first, last


async def _uploaded_copies(
    uid: str,
    connector: ServiceConnector,
    cache: ActivityCache,
    pairs: list[tuple[CacheEntry, CacheEntry]],
) -> list[FoundCopy]:
    """What a destination holds twice: found by downloading its activities."""
    theirs = [(copy, kept) for copy, kept in pairs if uid in copy.uploaded_to]
    if not theirs:
        return []
    metas = await connector.list_activities(*_span(theirs))
    found: list[FoundCopy] = []
    for copy, kept in theirs:
        found += await _check(uid, connector, metas, cache, copy, kept)
    return found


def _origin(
    healthy: list[CacheEntry], cache: ActivityCache, uid: str, entry: CacheEntry
) -> CacheEntry | None:
    """The entry of another source that trainings-sync uploaded to `uid` and
    that became `entry` there, byte for byte; None when it came from elsewhere.

    Only a file trainings-sync built itself counts. A device's own file can sit
    in two services byte for byte with no upload by trainings-sync, and
    uploaded_to also marks an activity that was only matched to one there.
    """
    content = cache.read_content(entry)
    if _OWN_GPX.search(content, 0, 1024) is None:
        return None
    return next(
        (
            e
            for e in healthy
            if e.source_id != uid
            and uid in e.uploaded_to
            and abs(e.start_time - entry.start_time) <= _SLACK
            and cache.read_content(e) == content
        ),
        None,
    )


async def _own_copies(
    uid: str,
    connector: ServiceConnector,
    healthy: list[CacheEntry],
    cache: ActivityCache,
    pairs: list[tuple[CacheEntry, CacheEntry]],
) -> list[FoundCopy]:
    """What a source holds twice because trainings-sync uploaded a workout it
    had already: its activities are cached, so nothing is downloaded."""
    theirs = []
    for copy, kept in pairs:
        origin = _origin(healthy, cache, uid, copy) if copy.source_id == uid else None
        if origin is not None:  # a copy of the user's own stays untouched
            theirs.append((copy, kept, origin))
    if not theirs:
        return []
    metas = await connector.list_activities(*_span([(c, k) for c, k, _ in theirs]))
    listed = {m.external_id for m in metas}
    return [
        FoundCopy(uid, copy.external_id, origin, kept, kept.external_id)
        for copy, kept, origin in theirs
        if copy.external_id in listed and kept.external_id in listed
    ]


async def plan_removals(
    cache: ActivityCache,
    connectors: Mapping[str, ServiceConnector],
    min_overlap_s: int = 60,
) -> list[FoundCopy]:
    """The activities `connectors` (keyed by uid) hold twice because
    trainings-sync uploaded a second copy of a workout there.

    Every one is matched byte for byte to a file trainings-sync uploaded, and
    the kept copy must be there too, so nothing is deleted on a guess.
    """
    healthy = cache.healthy_entries()
    entries = {(e.external_id, e.source_id): e for e in healthy}
    pairs = sorted(
        (
            (entries[key], kept)
            for key, kept in find_copies(healthy, min_overlap_s).items()
        ),
        key=lambda pair: pair[0].start_time,
    )
    sources = {e.source_id for e in healthy}
    found: list[FoundCopy] = []
    for uid, connector in connectors.items():
        if uid in sources:
            found += await _own_copies(uid, connector, healthy, cache, pairs)
        else:
            found += await _uploaded_copies(uid, connector, cache, pairs)
    return found


async def remove_copies(
    found: Iterable[FoundCopy], connectors: Mapping[str, ServiceConnector]
) -> list[tuple[FoundCopy, Exception]]:
    """Delete every activity `found` names, one by one; the ones that failed."""
    failed: list[tuple[FoundCopy, Exception]] = []
    for item in found:
        try:
            await connectors[item.connector_id].delete_activity(item.activity_id)
        except Exception as exc:  # one refused delete must not stop the rest
            failed.append((item, exc))
    return failed
