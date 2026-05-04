from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.connectors.base import ActivityMeta
from app.core.cache import ActivityCache, CacheEntry
from app.core.planner import SourceSpec, SyncPlanner

_UTC = timezone.utc
_T0 = datetime(2026, 1, 1, 8, 0, tzinfo=_UTC)


def _dt(offset_s: int = 0) -> datetime:
    return _T0 + timedelta(seconds=offset_s)


def _meta(
    external_id: str = "act-1",
    start_offset_s: int = 0,
    elapsed_s: int | None = 3600,
) -> ActivityMeta:
    return ActivityMeta(
        external_id=external_id,
        name="Run",
        sport_type="Run",
        start_time=_dt(start_offset_s),
        elapsed_s=elapsed_s,
    )


def _entry(
    external_id: str = "act-1",
    source_id: str = "garmin",
    start_offset_s: int = 0,
    elapsed_s: int | None = 3600,
    needs_refresh: bool = False,
) -> CacheEntry:
    return CacheEntry(
        external_id=external_id,
        source_id=source_id,
        format="fit",
        start_time=_dt(start_offset_s),
        elapsed_s=elapsed_s,
        needs_refresh=needs_refresh,
    )


def _spec(source_id: str = "garmin", priority: int = 1) -> SourceSpec:
    return SourceSpec(source_id=source_id, priority=priority)


@pytest.fixture
def cache(tmp_path: Path) -> ActivityCache:
    c = ActivityCache(tmp_path / "cache")
    c.load()
    return c


class TestSyncPlannerInit:
    def test_raises_on_negative_min_overlap_s(self) -> None:
        with pytest.raises(ValueError, match="min_overlap_s"):
            SyncPlanner(min_overlap_s=-1)

    def test_raises_on_negative_fallback_s(self) -> None:
        with pytest.raises(ValueError, match="fallback_s"):
            SyncPlanner(fallback_s=-1)

    def test_zero_min_overlap_s_is_valid(self) -> None:
        SyncPlanner(min_overlap_s=0)

    def test_zero_fallback_s_is_valid(self) -> None:
        SyncPlanner(fallback_s=0)


class TestPlanEmpty:
    def test_empty_sources_returns_empty_plan(self, cache: ActivityCache) -> None:
        plan = SyncPlanner().plan([], cache)
        assert plan.to_download == ()

    def test_empty_metas_returns_empty_plan(self, cache: ActivityCache) -> None:
        plan = SyncPlanner().plan([(_spec(), [])], cache)
        assert plan.to_download == ()


class TestPlanCacheHit:
    def test_skips_exact_cache_hit_without_force(self, cache: ActivityCache) -> None:
        cache.put(_entry(), b"content")
        plan = SyncPlanner().plan([(_spec(), [_meta()])], cache)
        assert plan.to_download == ()

    def test_downloads_when_not_in_cache(self, cache: ActivityCache) -> None:
        plan = SyncPlanner().plan([(_spec(), [_meta()])], cache)
        assert len(plan.to_download) == 1

    def test_download_item_has_correct_source_and_meta(
        self, cache: ActivityCache
    ) -> None:
        meta = _meta()
        plan = SyncPlanner().plan([(_spec("garmin"), [meta])], cache)
        assert plan.to_download[0].source_id == "garmin"
        assert plan.to_download[0].meta == meta


class TestPlanNeedsRefresh:
    def test_downloads_exact_needs_refresh_entry_without_force(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(needs_refresh=True), b"content")
        plan = SyncPlanner().plan([(_spec(), [_meta()])], cache)
        assert len(plan.to_download) == 1

    def test_downloads_when_overlapping_entry_needs_refresh(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="strava", needs_refresh=True), b"content")
        plan = SyncPlanner().plan([(_spec("garmin"), [_meta()])], cache)
        assert len(plan.to_download) == 1

    def test_skips_lower_priority_source_when_higher_priority_cached(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        plan = SyncPlanner().plan(
            [
                (_spec("garmin", priority=1), []),
                (_spec("strava", priority=2), [_meta()]),
            ],
            cache,
        )
        assert plan.to_download == ()

    def test_downloads_higher_priority_source_despite_lower_priority_cache(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="strava"), b"content")
        plan = SyncPlanner().plan(
            [
                (_spec("garmin", priority=1), [_meta()]),
                (_spec("strava", priority=2), []),
            ],
            cache,
        )
        assert len(plan.to_download) == 1
        assert plan.to_download[0].source_id == "garmin"

    def test_needs_refresh_not_blocked_by_healthy_overlap_from_other_source(
        self, cache: ActivityCache
    ) -> None:
        # garmin/act-1 needs_refresh=True; strava/act-1 is healthy with same interval
        # -> garmin must still be re-downloaded despite strava's healthy presence
        cache.put(_entry(source_id="garmin", needs_refresh=True), b"content")
        cache.put(_entry(source_id="strava"), b"content")
        plan = SyncPlanner().plan([(_spec("garmin"), [_meta()])], cache)
        assert len(plan.to_download) == 1
        assert plan.to_download[0].source_id == "garmin"

    def test_needs_refresh_does_not_override_already_planned_higher_priority(
        self, cache: ActivityCache
    ) -> None:
        # lo/lo-act has needs_refresh=True, but hi already planned overlapping activity
        # -> inter-source priority still applies; lo must NOT be planned
        cache.put(
            _entry(external_id="lo-act", source_id="lo", needs_refresh=True), b"content"
        )
        meta_hi = _meta(external_id="hi-act")
        meta_lo = _meta(external_id="lo-act")
        plan = SyncPlanner().plan(
            [
                (_spec("hi", priority=1), [meta_hi]),
                (_spec("lo", priority=2), [meta_lo]),
            ],
            cache,
        )
        ids = {item.source_id for item in plan.to_download}
        assert "hi" in ids
        assert "lo" not in ids


class TestPlanForce:
    def test_downloads_cached_exact_hit_when_force(self, cache: ActivityCache) -> None:
        cache.put(_entry(), b"content")
        plan = SyncPlanner().plan([(_spec(), [_meta()])], cache, force=True)
        assert len(plan.to_download) == 1

    def test_downloads_when_healthy_overlap_and_force(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="strava"), b"content")
        plan = SyncPlanner().plan([(_spec("garmin"), [_meta()])], cache, force=True)
        assert len(plan.to_download) == 1

    def test_downloads_needs_refresh_overlap_and_force(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="strava", needs_refresh=True), b"content")
        plan = SyncPlanner().plan([(_spec("garmin"), [_meta()])], cache, force=True)
        assert len(plan.to_download) == 1

    def test_force_with_overlapping_sources_higher_priority_wins(
        self, cache: ActivityCache
    ) -> None:
        # Healthy cache exists; force=True; hi and lo overlap -> only hi planned
        cache.put(_entry(source_id="strava"), b"content")
        meta_hi = _meta(external_id="hi-act")
        meta_lo = _meta(external_id="lo-act")
        plan = SyncPlanner().plan(
            [
                (_spec("hi", priority=1), [meta_hi]),
                (_spec("lo", priority=2), [meta_lo]),
            ],
            cache,
            force=True,
        )
        ids = {item.source_id for item in plan.to_download}
        assert "hi" in ids
        assert "lo" not in ids


class TestPlanSourcePriority:
    def test_higher_priority_source_is_planned(self, cache: ActivityCache) -> None:
        meta_hi = _meta(external_id="hi-act")
        meta_lo = _meta(external_id="lo-act")
        plan = SyncPlanner().plan(
            [
                (_spec("hi", priority=1), [meta_hi]),
                (_spec("lo", priority=2), [meta_lo]),
            ],
            cache,
        )
        assert any(item.source_id == "hi" for item in plan.to_download)

    def test_lower_priority_skipped_when_overlaps_higher(
        self, cache: ActivityCache
    ) -> None:
        meta_hi = _meta(external_id="hi-act")
        meta_lo = _meta(external_id="lo-act")
        plan = SyncPlanner().plan(
            [
                (_spec("hi", priority=1), [meta_hi]),
                (_spec("lo", priority=2), [meta_lo]),
            ],
            cache,
        )
        assert not any(item.source_id == "lo" for item in plan.to_download)

    def test_priority_order_independent_of_input_order(
        self, cache: ActivityCache
    ) -> None:
        meta_hi = _meta(external_id="hi-act")
        meta_lo = _meta(external_id="lo-act")
        # lo listed first in input, but has lower priority
        plan = SyncPlanner().plan(
            [
                (_spec("lo", priority=2), [meta_lo]),
                (_spec("hi", priority=1), [meta_hi]),
            ],
            cache,
        )
        ids = {item.source_id for item in plan.to_download}
        assert ids == {"hi"}

    def test_equal_priority_first_in_list_wins(self, cache: ActivityCache) -> None:
        meta_a = _meta(external_id="a-act")
        meta_b = _meta(external_id="b-act")
        plan = SyncPlanner().plan(
            [
                (_spec("source-a", priority=1), [meta_a]),
                (_spec("source-b", priority=1), [meta_b]),
            ],
            cache,
        )
        ids = {item.source_id for item in plan.to_download}
        assert "source-a" in ids
        assert "source-b" not in ids


class TestPlanOverlapGeometry:
    def test_partial_overlap_higher_priority_wins(self, cache: ActivityCache) -> None:
        # hi: 8:00-9:00, lo: 8:30-9:30 -> 30 min overlap
        meta_hi = _meta(external_id="hi-act", start_offset_s=0, elapsed_s=3600)
        meta_lo = _meta(external_id="lo-act", start_offset_s=1800, elapsed_s=3600)
        plan = SyncPlanner().plan(
            [
                (_spec("hi", priority=1), [meta_hi]),
                (_spec("lo", priority=2), [meta_lo]),
            ],
            cache,
        )
        assert {item.source_id for item in plan.to_download} == {"hi"}

    def test_containment_higher_priority_wins(self, cache: ActivityCache) -> None:
        # hi: 8:00-10:00, lo: 8:30-9:30 (fully contained in hi)
        meta_hi = _meta(external_id="hi-act", start_offset_s=0, elapsed_s=7200)
        meta_lo = _meta(external_id="lo-act", start_offset_s=1800, elapsed_s=3600)
        plan = SyncPlanner().plan(
            [
                (_spec("hi", priority=1), [meta_hi]),
                (_spec("lo", priority=2), [meta_lo]),
            ],
            cache,
        )
        assert {item.source_id for item in plan.to_download} == {"hi"}

    def test_garmin_split_vs_strava_combined_garmin_wins(
        self, cache: ActivityCache
    ) -> None:
        # Garmin (priority=1): act-A 8:00-9:00, act-B 9:00-10:00
        # Strava (priority=2): act-C 8:00-10:00 (combined)
        meta_a = _meta(external_id="garmin-a", start_offset_s=0, elapsed_s=3600)
        meta_b = _meta(external_id="garmin-b", start_offset_s=3600, elapsed_s=3600)
        meta_c = _meta(external_id="strava-c", start_offset_s=0, elapsed_s=7200)
        plan = SyncPlanner().plan(
            [
                (_spec("garmin", priority=1), [meta_a, meta_b]),
                (_spec("strava", priority=2), [meta_c]),
            ],
            cache,
        )
        items = {(i.source_id, i.meta.external_id) for i in plan.to_download}
        assert ("garmin", "garmin-a") in items
        assert ("garmin", "garmin-b") in items
        assert ("strava", "strava-c") not in items

    def test_strava_combined_wins_when_higher_priority(
        self, cache: ActivityCache
    ) -> None:
        # Strava (priority=1): act-C 8:00-10:00
        # Garmin (priority=2): act-A 8:00-9:00, act-B 9:00-10:00
        meta_a = _meta(external_id="garmin-a", start_offset_s=0, elapsed_s=3600)
        meta_b = _meta(external_id="garmin-b", start_offset_s=3600, elapsed_s=3600)
        meta_c = _meta(external_id="strava-c", start_offset_s=0, elapsed_s=7200)
        plan = SyncPlanner().plan(
            [
                (_spec("strava", priority=1), [meta_c]),
                (_spec("garmin", priority=2), [meta_a, meta_b]),
            ],
            cache,
        )
        items = {(i.source_id, i.meta.external_id) for i in plan.to_download}
        assert ("strava", "strava-c") in items
        assert ("garmin", "garmin-a") not in items
        assert ("garmin", "garmin-b") not in items

    def test_non_overlapping_activities_both_downloaded(
        self, cache: ActivityCache
    ) -> None:
        # hi: 8:00-9:00, lo: 10:00-11:00 -- gap of 1 hour, no overlap
        meta_hi = _meta(external_id="hi-act", start_offset_s=0, elapsed_s=3600)
        meta_lo = _meta(external_id="lo-act", start_offset_s=7200, elapsed_s=3600)
        plan = SyncPlanner().plan(
            [
                (_spec("hi", priority=1), [meta_hi]),
                (_spec("lo", priority=2), [meta_lo]),
            ],
            cache,
        )
        assert len(plan.to_download) == 2


class TestPlanIntraSource:
    def test_intra_source_overlap_both_downloaded(self, cache: ActivityCache) -> None:
        # Two overlapping activities from the same source (e.g., Garmin multi-sport)
        meta_a = _meta(external_id="act-a", start_offset_s=0, elapsed_s=3600)
        meta_b = _meta(external_id="act-b", start_offset_s=1800, elapsed_s=3600)
        plan = SyncPlanner().plan([(_spec("garmin"), [meta_a, meta_b])], cache)
        assert len(plan.to_download) == 2

    def test_cached_same_source_does_not_block_new_overlapping_activity(
        self, cache: ActivityCache
    ) -> None:
        # garmin-a already cached (healthy); garmin-b is a new overlapping activity
        # -> garmin-b must still be downloaded (intra-source split is valid)
        cache.put(_entry(external_id="garmin-a"), b"content")
        meta_b = _meta(external_id="garmin-b")
        plan = SyncPlanner().plan([(_spec("garmin"), [meta_b])], cache)
        assert len(plan.to_download) == 1
        assert plan.to_download[0].meta.external_id == "garmin-b"


class TestPlanOverlapBoundary:
    def test_touching_intervals_not_overlapping(self, cache: ActivityCache) -> None:
        # strava: 8:00-9:00, garmin starts at exactly 9:00 -> 0s overlap < min
        cache.put(_entry(source_id="strava", elapsed_s=3600), b"content")
        meta = _meta(start_offset_s=3600)
        plan = SyncPlanner().plan([(_spec("garmin"), [meta])], cache)
        assert len(plan.to_download) == 1

    def test_overlap_below_min_threshold_not_blocked(
        self, cache: ActivityCache
    ) -> None:
        # strava: 8:00-9:00, garmin: 8:59-9:59 -> 60s overlap; min_overlap_s=61
        cache.put(_entry(source_id="strava", elapsed_s=3600), b"content")
        meta = _meta(start_offset_s=3540, elapsed_s=3600)
        plan = SyncPlanner(min_overlap_s=61).plan([(_spec("garmin"), [meta])], cache)
        assert len(plan.to_download) == 1

    def test_overlap_at_min_threshold_blocks_lower_priority_source(
        self, cache: ActivityCache
    ) -> None:
        # garmin: 8:00-9:00 in cache; strava: 8:59-9:59 -> 60s overlap = min_overlap_s
        cache.put(_entry(source_id="garmin", elapsed_s=3600), b"content")
        meta = _meta(start_offset_s=3540, elapsed_s=3600)
        plan = SyncPlanner(min_overlap_s=60).plan(
            [(_spec("garmin", priority=1), []), (_spec("strava", priority=2), [meta])],
            cache,
        )
        assert plan.to_download == ()

    def test_fallback_duration_used_when_elapsed_none(
        self, cache: ActivityCache
    ) -> None:
        # garmin p=1: 8:00, elapsed=None -> fallback 3600s -> ends 9:00; in cache
        # strava p=2: 8:30, elapsed=None -> fallback 3600s -> 30min overlap -> skip
        cache.put(_entry(source_id="garmin", elapsed_s=None), b"content")
        meta = _meta(start_offset_s=1800, elapsed_s=None)
        plan = SyncPlanner(fallback_s=3600, min_overlap_s=60).plan(
            [(_spec("garmin", priority=1), []), (_spec("strava", priority=2), [meta])],
            cache,
        )
        assert plan.to_download == ()
