"""Activities a source holds twice: one workout uploaded twice, or recorded by
two devices at once."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta

from app.core.cache import CacheEntry

# A device's own file first: a GPX is most often a conversion of one.
_FORMAT_RANK: dict[str, int] = {"fit": 0, "tcx": 1, "gpx": 2}


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
