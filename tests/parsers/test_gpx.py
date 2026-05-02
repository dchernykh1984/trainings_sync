from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.parsers.base import ActivityParseError
from app.parsers.gpx import GpxParser

_FIXTURES = Path(__file__).parent / "fixtures"

_GPX_FULL = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1' xmlns='http://www.topografix.com/GPX/1/1'
     xmlns:gpxtpx='http://www.garmin.com/xmlschemas/TrackPointExtension/v1'>
  <trk>
    <name>Morning Run</name>
    <type>running</type>
    <trkseg>
      <trkpt lat='43.0' lon='76.0'>
        <ele>900.0</ele>
        <time>2026-01-01T08:00:00Z</time>
        <extensions>
          <power>200</power>
          <gpxtpx:TrackPointExtension>
            <gpxtpx:hr>145</gpxtpx:hr>
            <gpxtpx:cad>85</gpxtpx:cad>
            <gpxtpx:atemp>22</gpxtpx:atemp>
          </gpxtpx:TrackPointExtension>
        </extensions>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"""

_GPX_NO_EXTENSIONS = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1'>
  <trk>
    <name>Morning Run</name>
    <type>running</type>
    <trkseg>
      <trkpt lat='43.0' lon='76.0'>
        <ele>900.0</ele>
        <time>2026-01-01T08:00:00Z</time>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"""

_GPX_NO_NAME = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1'>
  <trk>
    <trkseg>
      <trkpt lat='43.0' lon='76.0'>
        <time>2026-01-01T08:00:00Z</time>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"""

_GPX_NO_TIME = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1'>
  <trk>
    <trkseg>
      <trkpt lat='43.0' lon='76.0'/>
    </trkseg>
  </trk>
</gpx>"""

_GPX_NO_TRACKS = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1'/>"""

_GPX_EMPTY_TRACK = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1'>
  <trk>
    <trkseg/>
  </trk>
</gpx>"""

_GPX_NAIVE_TIME = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1'>
  <trk>
    <trkseg>
      <trkpt lat='43.0' lon='76.0'>
        <time>2026-01-01T08:00:00</time>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"""

_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def parser() -> GpxParser:
    return GpxParser()


@pytest.fixture
def garmin_gpx() -> bytes:
    return (_FIXTURES / "garmin_cycling.gpx").read_bytes()


@pytest.fixture
def strava_gpx() -> bytes:
    return (_FIXTURES / "strava_cycling.gpx").read_bytes()


class TestGpxParserUnit:
    def test_returns_activity_data(self, parser: GpxParser) -> None:
        result = parser.parse(_GPX_FULL.encode())

        assert result.start_time == _DT
        assert result.name == "Morning Run"
        assert result.sport_type == "running"

    def test_track_point_fields(self, parser: GpxParser) -> None:
        result = parser.parse(_GPX_FULL.encode())

        assert len(result.track) == 1
        pt = result.track[0]
        assert pt.timestamp == _DT
        assert pt.lat == pytest.approx(43.0)
        assert pt.lon == pytest.approx(76.0)
        assert pt.altitude == pytest.approx(900.0)
        assert pt.heart_rate == 145
        assert pt.cadence == 85
        assert pt.power == 200
        assert pt.temperature == 22

    def test_track_point_metrics_none_when_no_extensions(
        self, parser: GpxParser
    ) -> None:
        result = parser.parse(_GPX_NO_EXTENSIONS.encode())

        pt = result.track[0]
        assert pt.heart_rate is None
        assert pt.cadence is None
        assert pt.power is None
        assert pt.temperature is None

    def test_name_none_when_absent(self, parser: GpxParser) -> None:
        result = parser.parse(_GPX_NO_NAME.encode())

        assert result.name is None

    def test_sport_type_none_when_no_type_element(self, parser: GpxParser) -> None:
        result = parser.parse(_GPX_NO_NAME.encode())

        assert result.sport_type is None

    def test_raises_when_no_tracks(self, parser: GpxParser) -> None:
        with pytest.raises(ActivityParseError, match="no tracks"):
            parser.parse(_GPX_NO_TRACKS.encode())

    def test_raises_when_no_points(self, parser: GpxParser) -> None:
        with pytest.raises(ActivityParseError, match="no points"):
            parser.parse(_GPX_EMPTY_TRACK.encode())

    def test_raises_when_no_start_time(self, parser: GpxParser) -> None:
        with pytest.raises(ActivityParseError, match="start time"):
            parser.parse(_GPX_NO_TIME.encode())

    def test_raises_on_invalid_content(self, parser: GpxParser) -> None:
        with pytest.raises(ActivityParseError):
            parser.parse(b"not valid xml")

    def test_start_time_is_utc_aware(self, parser: GpxParser) -> None:
        result = parser.parse(_GPX_FULL.encode())

        assert result.start_time.tzinfo is not None

    def test_naive_time_gets_utc_timezone(self, parser: GpxParser) -> None:
        result = parser.parse(_GPX_NAIVE_TIME.encode())

        assert result.start_time.tzinfo == timezone.utc

    def test_gear_events_always_empty(self, parser: GpxParser) -> None:
        result = parser.parse(_GPX_FULL.encode())

        assert result.gear_events == []

    def test_bad_numeric_extension_yields_none(self, parser: GpxParser) -> None:
        gpx = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1' xmlns='http://www.topografix.com/GPX/1/1'
     xmlns:gpxtpx='http://www.garmin.com/xmlschemas/TrackPointExtension/v1'>
  <trk>
    <trkseg>
      <trkpt lat='43.0' lon='76.0'>
        <time>2026-01-01T08:00:00Z</time>
        <extensions>
          <gpxtpx:TrackPointExtension>
            <gpxtpx:hr>bad</gpxtpx:hr>
            <gpxtpx:cad>bad</gpxtpx:cad>
          </gpxtpx:TrackPointExtension>
        </extensions>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"""
        result = parser.parse(gpx.encode())

        pt = result.track[0]
        assert pt.heart_rate is None
        assert pt.cadence is None

    def test_trackpoint_without_time_skipped(self, parser: GpxParser) -> None:
        gpx = """\
<?xml version='1.0' encoding='UTF-8'?>
<gpx version='1.1' xmlns='http://www.topografix.com/GPX/1/1'>
  <trk>
    <trkseg>
      <trkpt lat='43.0' lon='76.0'>
        <time>2026-01-01T08:00:00Z</time>
      </trkpt>
      <trkpt lat='43.1' lon='76.1'/>
    </trkseg>
  </trk>
</gpx>"""
        result = parser.parse(gpx.encode())

        assert len(result.track) == 1


class TestGpxParserIntegration:
    def test_parses_garmin_gpx(self, parser: GpxParser, garmin_gpx: bytes) -> None:
        result = parser.parse(garmin_gpx)

        assert result.start_time == datetime(2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc)
        assert result.name == "Lunch Gravel Ride"
        assert result.sport_type == "gravel_biking"

    def test_garmin_track_points(self, parser: GpxParser, garmin_gpx: bytes) -> None:
        result = parser.parse(garmin_gpx)

        assert len(result.track) == 300
        pt = result.track[0]
        assert pt.timestamp == datetime(2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc)
        assert pt.lat == pytest.approx(43.220132, abs=1e-4)
        assert pt.lon == pytest.approx(77.043443, abs=1e-4)
        assert pt.altitude == pytest.approx(1360.8, abs=0.1)
        assert pt.heart_rate == 137
        assert pt.cadence == 51
        assert pt.power == 131

    def test_parses_strava_gpx(self, parser: GpxParser, strava_gpx: bytes) -> None:
        result = parser.parse(strava_gpx)

        assert result.start_time == datetime(2026, 5, 1, 8, 33, 55, tzinfo=timezone.utc)
        assert result.name == "Gravel Cycling"
        assert result.sport_type == "gravel_cycling"

    def test_strava_track_points(self, parser: GpxParser, strava_gpx: bytes) -> None:
        result = parser.parse(strava_gpx)

        assert len(result.track) == 300
        pt = result.track[0]
        assert pt.timestamp == datetime(2026, 5, 1, 8, 33, 55, tzinfo=timezone.utc)
        assert pt.lat == pytest.approx(43.2233, abs=1e-3)
        assert pt.heart_rate == 149
        assert pt.cadence == 52
        assert pt.power is None

    def test_start_time_is_utc_aware(
        self, parser: GpxParser, garmin_gpx: bytes
    ) -> None:
        result = parser.parse(garmin_gpx)

        assert result.start_time.tzinfo is not None

    def test_track_timestamps_are_utc_aware(
        self, parser: GpxParser, garmin_gpx: bytes
    ) -> None:
        result = parser.parse(garmin_gpx)

        assert all(pt.timestamp.tzinfo is not None for pt in result.track)
