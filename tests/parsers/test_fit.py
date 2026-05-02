from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.parsers.base import ActivityParseError
from app.parsers.fit import FitParser

_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
_FIXTURES = Path(__file__).parent / "fixtures"


def _make_session(
    start_time: datetime | None = _DT,
    sport: str | None = "running",
) -> MagicMock:
    session = MagicMock()
    session.get_value.side_effect = lambda field: {
        "start_time": start_time,
        "sport": sport,
    }.get(field)
    return session


def _make_record(
    timestamp: datetime | None = _DT,
    lat: int | None = None,
    lon: int | None = None,
    heart_rate: int | None = None,
    cadence: int | None = None,
    power: int | None = None,
    temperature: int | None = None,
) -> MagicMock:
    record = MagicMock()
    record.get_value.side_effect = lambda field: {
        "timestamp": timestamp,
        "position_lat": lat,
        "position_long": lon,
        "enhanced_altitude": None,
        "distance": None,
        "enhanced_speed": None,
        "heart_rate": heart_rate,
        "cadence": cadence,
        "power": power,
        "temperature": temperature,
    }.get(field)
    return record


def _make_event(
    event: str = "rear_gear_change",
    timestamp: datetime | None = _DT,
    front_gear: int | None = 50,
    front_gear_num: int | None = 2,
    rear_gear: int | None = 20,
    rear_gear_num: int | None = 5,
) -> MagicMock:
    ev = MagicMock()
    ev.get_value.side_effect = lambda field: {
        "event": event,
        "timestamp": timestamp,
        "front_gear": front_gear,
        "front_gear_num": front_gear_num,
        "rear_gear": rear_gear,
        "rear_gear_num": rear_gear_num,
    }.get(field)
    return ev


def _patch_fitfile(
    sessions: list,
    records: list | None = None,
    events: list | None = None,
) -> MagicMock:
    mock_fit = MagicMock()

    def get_messages(msg_type: str):
        if msg_type == "session":
            return iter(sessions)
        if msg_type == "record":
            return iter(records or [])
        if msg_type == "event":
            return iter(events or [])
        return iter([])

    mock_fit.get_messages.side_effect = get_messages
    return mock_fit


@pytest.fixture
def parser() -> FitParser:
    return FitParser()


@pytest.fixture
def garmin_fit() -> bytes:
    return (_FIXTURES / "garmin_cycling.fit").read_bytes()


@pytest.fixture
def strava_fit() -> bytes:
    return (_FIXTURES / "strava_cycling.fit").read_bytes()


class TestFitParserUnit:
    def test_returns_activity_data(self, parser: FitParser) -> None:
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()]),
        ):
            result = parser.parse(b"fake-fit")

        assert result.start_time == _DT
        assert result.sport_type == "running"
        assert result.name is None

    def test_sport_type_none_when_absent(self, parser: FitParser) -> None:
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session(sport=None)]),
        ):
            result = parser.parse(b"fake-fit")

        assert result.sport_type is None

    def test_track_points_built_from_records(self, parser: FitParser) -> None:
        records = [
            _make_record(heart_rate=140, cadence=80, power=200, temperature=22),
            _make_record(heart_rate=145, cadence=85, power=210, temperature=23),
        ]
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()], records),
        ):
            result = parser.parse(b"fake-fit")

        assert len(result.track) == 2
        assert result.track[0].heart_rate == 140
        assert result.track[0].cadence == 80
        assert result.track[0].power == 200
        assert result.track[0].temperature == 22
        assert result.track[1].heart_rate == 145

    def test_track_point_lat_lon_converted_from_semicircles(
        self, parser: FitParser
    ) -> None:
        records = [_make_record(lat=515636257, lon=919164075)]
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()], records),
        ):
            result = parser.parse(b"fake-fit")

        assert result.track[0].lat == pytest.approx(43.220132, abs=1e-4)
        assert result.track[0].lon == pytest.approx(77.043443, abs=1e-4)

    def test_track_empty_when_no_records(self, parser: FitParser) -> None:
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()]),
        ):
            result = parser.parse(b"fake-fit")

        assert result.track == []

    def test_record_without_timestamp_skipped(self, parser: FitParser) -> None:
        records = [_make_record(timestamp=None), _make_record(heart_rate=140)]
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()], records),
        ):
            result = parser.parse(b"fake-fit")

        assert len(result.track) == 1
        assert result.track[0].heart_rate == 140

    def test_gear_event_without_timestamp_skipped(self, parser: FitParser) -> None:
        events = [
            _make_event(timestamp=None),
            _make_event(rear_gear=20),
        ]
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()], events=events),
        ):
            result = parser.parse(b"fake-fit")

        assert len(result.gear_events) == 1

    def test_gear_events_extracted(self, parser: FitParser) -> None:
        events = [
            _make_event(rear_gear=20, rear_gear_num=5),
            _make_event(event="front_gear_change", front_gear=34, front_gear_num=1),
        ]
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()], events=events),
        ):
            result = parser.parse(b"fake-fit")

        assert len(result.gear_events) == 2
        assert result.gear_events[0].rear_gear == 20
        assert result.gear_events[0].rear_gear_num == 5
        assert result.gear_events[1].front_gear == 34

    def test_non_gear_events_ignored(self, parser: FitParser) -> None:
        events = [
            _make_event(event="timer"),
            _make_event(event="rear_gear_change", rear_gear=20, rear_gear_num=5),
        ]
        with patch(
            "app.parsers.fit.fitparse.FitFile",
            return_value=_patch_fitfile([_make_session()], events=events),
        ):
            result = parser.parse(b"fake-fit")

        assert len(result.gear_events) == 1

    def test_raises_when_no_session(self, parser: FitParser) -> None:
        with (
            patch(
                "app.parsers.fit.fitparse.FitFile",
                return_value=_patch_fitfile([]),
            ),
            pytest.raises(ActivityParseError, match="no session"),
        ):
            parser.parse(b"fake-fit")

    def test_raises_when_no_start_time(self, parser: FitParser) -> None:
        with (
            patch(
                "app.parsers.fit.fitparse.FitFile",
                return_value=_patch_fitfile([_make_session(start_time=None)]),
            ),
            pytest.raises(ActivityParseError, match="start_time"),
        ):
            parser.parse(b"fake-fit")

    def test_raises_on_invalid_content(self, parser: FitParser) -> None:
        with pytest.raises(ActivityParseError):
            parser.parse(b"not a fit file")

    def test_wraps_fitparse_exception(self, parser: FitParser) -> None:
        import fitparse

        with (
            patch(
                "app.parsers.fit.fitparse.FitFile",
                side_effect=fitparse.FitParseError("corrupt"),
            ),
            pytest.raises(ActivityParseError, match="corrupt"),
        ):
            parser.parse(b"fake-fit")


class TestFitParserIntegration:
    def test_parses_garmin_fit(self, parser: FitParser, garmin_fit: bytes) -> None:
        result = parser.parse(garmin_fit)

        assert result.start_time == datetime(2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc)
        assert result.sport_type == "cycling"
        assert result.name is None

    def test_garmin_track_points(self, parser: FitParser, garmin_fit: bytes) -> None:
        result = parser.parse(garmin_fit)

        assert len(result.track) == 300
        pt = result.track[0]
        assert pt.timestamp == datetime(2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc)
        assert pt.lat == pytest.approx(43.220132, abs=1e-4)
        assert pt.lon == pytest.approx(77.043443, abs=1e-4)
        assert pt.altitude == pytest.approx(1360.8, abs=0.1)
        assert pt.distance == pytest.approx(21967.56, abs=0.1)
        assert pt.speed == pytest.approx(2.762, abs=0.001)
        assert pt.heart_rate == 137
        assert pt.cadence == 51
        assert pt.power == 131
        assert pt.temperature == 22

    def test_garmin_gear_events(self, parser: FitParser, garmin_fit: bytes) -> None:
        result = parser.parse(garmin_fit)

        assert len(result.gear_events) == 3
        ev = result.gear_events[0]
        assert ev.timestamp == datetime(2026, 5, 1, 8, 30, 20, tzinfo=timezone.utc)
        assert ev.front_gear == 50
        assert ev.rear_gear == 22

    def test_parses_strava_fit(self, parser: FitParser, strava_fit: bytes) -> None:
        result = parser.parse(strava_fit)

        assert result.start_time == datetime(2026, 5, 1, 8, 33, 55, tzinfo=timezone.utc)
        assert result.sport_type == "cycling"

    def test_strava_track_points(self, parser: FitParser, strava_fit: bytes) -> None:
        result = parser.parse(strava_fit)

        assert len(result.track) == 300
        pt = result.track[0]
        assert pt.timestamp == datetime(2026, 5, 1, 8, 33, 55, tzinfo=timezone.utc)
        assert pt.altitude == pytest.approx(1393.2, abs=0.1)
        assert pt.distance == pytest.approx(22947.02, abs=0.1)
        assert pt.speed == pytest.approx(3.238, abs=0.001)
        assert pt.heart_rate == 149
        assert pt.cadence == 52
        assert pt.power == 165
        assert pt.temperature == 21

    def test_strava_gear_events(self, parser: FitParser, strava_fit: bytes) -> None:
        result = parser.parse(strava_fit)

        assert len(result.gear_events) == 1
        ev = result.gear_events[0]
        assert ev.timestamp == datetime(2026, 5, 1, 8, 35, 37, tzinfo=timezone.utc)
        assert ev.front_gear == 50
        assert ev.rear_gear == 25

    def test_start_time_is_utc_aware(
        self, parser: FitParser, garmin_fit: bytes
    ) -> None:
        result = parser.parse(garmin_fit)

        assert result.start_time.tzinfo is not None

    def test_track_timestamps_are_utc_aware(
        self, parser: FitParser, garmin_fit: bytes
    ) -> None:
        result = parser.parse(garmin_fit)

        assert all(pt.timestamp.tzinfo is not None for pt in result.track)
