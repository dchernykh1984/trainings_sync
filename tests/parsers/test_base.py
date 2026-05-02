from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from app.parsers.base import (
    ActivityData,
    ActivityParseError,
    ActivityParser,
    GearEvent,
    TrackPoint,
)

_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
_PT = TrackPoint(timestamp=_DT, lat=43.0, lon=77.0, heart_rate=140)


class _FakeParser(ActivityParser):
    def __init__(self, result: ActivityData | None = None) -> None:
        self._result = result or ActivityData(start_time=_DT)

    def parse(self, content: bytes) -> ActivityData:
        if content == b"bad":
            raise ActivityParseError("invalid content")
        return self._result


class TestActivityParseError:
    def test_is_exception(self) -> None:
        assert issubclass(ActivityParseError, Exception)

    def test_carries_message(self) -> None:
        err = ActivityParseError("bad FIT header")
        assert str(err) == "bad FIT header"


class TestTrackPoint:
    def test_required_timestamp(self) -> None:
        pt = TrackPoint(timestamp=_DT)
        assert pt.timestamp == _DT

    def test_optional_fields_default_to_none(self) -> None:
        pt = TrackPoint(timestamp=_DT)
        assert pt.lat is None
        assert pt.lon is None
        assert pt.altitude is None
        assert pt.distance is None
        assert pt.speed is None
        assert pt.heart_rate is None
        assert pt.cadence is None
        assert pt.power is None
        assert pt.temperature is None

    def test_is_frozen(self) -> None:
        pt = TrackPoint(timestamp=_DT)
        with pytest.raises(FrozenInstanceError):
            pt.heart_rate = 140  # type: ignore[misc]


class TestGearEvent:
    def test_required_timestamp(self) -> None:
        ev = GearEvent(timestamp=_DT)
        assert ev.timestamp == _DT

    def test_optional_fields_default_to_none(self) -> None:
        ev = GearEvent(timestamp=_DT)
        assert ev.front_gear is None
        assert ev.front_gear_num is None
        assert ev.rear_gear is None
        assert ev.rear_gear_num is None

    def test_is_frozen(self) -> None:
        ev = GearEvent(timestamp=_DT)
        with pytest.raises(FrozenInstanceError):
            ev.rear_gear = 20  # type: ignore[misc]


class TestActivityData:
    def test_required_start_time(self) -> None:
        data = ActivityData(start_time=_DT)
        assert data.start_time == _DT

    def test_optional_fields_default_to_none(self) -> None:
        data = ActivityData(start_time=_DT)
        assert data.name is None
        assert data.sport_type is None

    def test_track_defaults_to_empty_list(self) -> None:
        data = ActivityData(start_time=_DT)
        assert data.track == []

    def test_gear_events_defaults_to_empty_list(self) -> None:
        data = ActivityData(start_time=_DT)
        assert data.gear_events == []

    def test_stores_track_points(self) -> None:
        data = ActivityData(start_time=_DT, track=[_PT])
        assert len(data.track) == 1
        assert data.track[0].heart_rate == 140

    def test_elapsed_s_none_with_empty_track(self) -> None:
        assert ActivityData(start_time=_DT).elapsed_s is None

    def test_elapsed_s_none_with_single_point(self) -> None:
        assert ActivityData(start_time=_DT, track=[_PT]).elapsed_s is None

    def test_elapsed_s_computed_from_track_endpoints(self) -> None:
        t1 = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
        data = ActivityData(
            start_time=t1,
            track=[TrackPoint(timestamp=t1), TrackPoint(timestamp=t2)],
        )
        assert data.elapsed_s == 3600


class TestActivityParser:
    def test_cannot_instantiate_abc_directly(self) -> None:
        with pytest.raises(TypeError):
            ActivityParser()  # type: ignore[abstract]

    def test_parse_returns_activity_data(self) -> None:
        parser = _FakeParser(ActivityData(start_time=_DT, name="Run"))
        result = parser.parse(b"content")
        assert result.name == "Run"
        assert result.start_time == _DT

    def test_parse_raises_activity_parse_error(self) -> None:
        parser = _FakeParser()
        with pytest.raises(ActivityParseError):
            parser.parse(b"bad")
