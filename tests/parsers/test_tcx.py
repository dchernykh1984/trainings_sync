from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.parsers.base import ActivityParseError
from app.parsers.tcx import TcxParser

_FIXTURES = Path(__file__).parent / "fixtures"

_TCX_FULL = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase
    xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
    xmlns:ns3="http://www.garmin.com/xmlschemas/ActivityExtension/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>2026-01-01T08:00:00.000Z</Id>
      <Lap StartTime="2026-01-01T08:00:00.000Z">
        <Track>
          <Trackpoint>
            <Time>2026-01-01T08:00:00.000Z</Time>
            <Position>
              <LatitudeDegrees>43.0</LatitudeDegrees>
              <LongitudeDegrees>77.0</LongitudeDegrees>
            </Position>
            <AltitudeMeters>900.0</AltitudeMeters>
            <DistanceMeters>100.0</DistanceMeters>
            <HeartRateBpm><Value>145</Value></HeartRateBpm>
            <Cadence>85</Cadence>
            <Extensions>
              <ns3:TPX>
                <ns3:Speed>4.5</ns3:Speed>
                <ns3:Watts>200</ns3:Watts>
              </ns3:TPX>
            </Extensions>
          </Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""

_TCX_NO_METRICS = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>2026-01-01T08:00:00.000Z</Id>
      <Lap StartTime="2026-01-01T08:00:00.000Z">
        <Track>
          <Trackpoint><Time>2026-01-01T08:00:00.000Z</Time></Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""

_TCX_NO_SPORT = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity>
      <Id>2026-01-01T08:00:00.000Z</Id>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""

_TCX_NO_ACTIVITY = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities/>
</TrainingCenterDatabase>"""

_TCX_NO_ID = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""

_TCX_BAD_ID = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>not-a-datetime</Id>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""

_TCX_NAIVE_TIME = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>2026-01-01T08:00:00</Id>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""

_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def parser() -> TcxParser:
    return TcxParser()


@pytest.fixture
def strava_tcx() -> bytes:
    return (_FIXTURES / "strava_cycling.tcx").read_bytes()


class TestTcxParserUnit:
    def test_returns_activity_data(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_FULL.encode())

        assert result.start_time == _DT
        assert result.sport_type == "Biking"
        assert result.name is None

    def test_track_point_fields(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_FULL.encode())

        assert len(result.track) == 1
        pt = result.track[0]
        assert pt.timestamp == _DT
        assert pt.lat == pytest.approx(43.0)
        assert pt.lon == pytest.approx(77.0)
        assert pt.altitude == pytest.approx(900.0)
        assert pt.distance == pytest.approx(100.0)
        assert pt.speed == pytest.approx(4.5)
        assert pt.heart_rate == 145
        assert pt.cadence == 85
        assert pt.power == 200

    def test_track_point_metrics_none_when_absent(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_NO_METRICS.encode())

        pt = result.track[0]
        assert pt.lat is None
        assert pt.heart_rate is None
        assert pt.cadence is None
        assert pt.power is None

    def test_name_always_none(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_FULL.encode())

        assert result.name is None

    def test_sport_type_none_when_absent(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_NO_SPORT.encode())

        assert result.sport_type is None

    def test_raises_when_no_activity(self, parser: TcxParser) -> None:
        with pytest.raises(ActivityParseError, match="no Activity"):
            parser.parse(_TCX_NO_ACTIVITY.encode())

    def test_raises_when_no_id(self, parser: TcxParser) -> None:
        with pytest.raises(ActivityParseError, match="no Id"):
            parser.parse(_TCX_NO_ID.encode())

    def test_raises_when_id_not_datetime(self, parser: TcxParser) -> None:
        with pytest.raises(ActivityParseError, match="not a valid datetime"):
            parser.parse(_TCX_BAD_ID.encode())

    def test_raises_on_invalid_xml(self, parser: TcxParser) -> None:
        with pytest.raises(ActivityParseError):
            parser.parse(b"not valid xml")

    def test_start_time_is_utc_aware(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_FULL.encode())

        assert result.start_time.tzinfo is not None

    def test_naive_time_gets_utc_timezone(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_NAIVE_TIME.encode())

        assert result.start_time.tzinfo == timezone.utc

    def test_gear_events_always_empty(self, parser: TcxParser) -> None:
        result = parser.parse(_TCX_FULL.encode())

        assert result.gear_events == []

    def test_trackpoint_without_time_skipped(self, parser: TcxParser) -> None:
        tcx = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>2026-01-01T08:00:00.000Z</Id>
      <Lap>
        <Track>
          <Trackpoint/>
          <Trackpoint><Time>2026-01-01T08:00:01.000Z</Time></Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""
        result = parser.parse(tcx.encode())

        assert len(result.track) == 1

    def test_bad_numeric_field_yields_none(self, parser: TcxParser) -> None:
        tcx = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>2026-01-01T08:00:00.000Z</Id>
      <Lap>
        <Track>
          <Trackpoint>
            <Time>2026-01-01T08:00:00.000Z</Time>
            <HeartRateBpm><Value>bad</Value></HeartRateBpm>
            <Cadence>bad</Cadence>
          </Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""
        result = parser.parse(tcx.encode())

        pt = result.track[0]
        assert pt.heart_rate is None
        assert pt.cadence is None

    def test_trackpoint_with_invalid_time_skipped(self, parser: TcxParser) -> None:
        tcx = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>2026-01-01T08:00:00.000Z</Id>
      <Lap>
        <Track>
          <Trackpoint><Time>not-a-time</Time></Trackpoint>
          <Trackpoint><Time>2026-01-01T08:00:01.000Z</Time></Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"""
        result = parser.parse(tcx.encode())

        assert len(result.track) == 1


class TestTcxParserIntegration:
    def test_parses_strava_tcx(self, parser: TcxParser, strava_tcx: bytes) -> None:
        result = parser.parse(strava_tcx)

        assert result.start_time == datetime(2026, 5, 1, 8, 33, 55, tzinfo=timezone.utc)
        assert result.sport_type == "Biking"
        assert result.name is None

    def test_strava_track_points(self, parser: TcxParser, strava_tcx: bytes) -> None:
        result = parser.parse(strava_tcx)

        assert len(result.track) == 300
        pt = result.track[0]
        assert pt.timestamp == datetime(2026, 5, 1, 8, 33, 55, tzinfo=timezone.utc)
        assert pt.lat == pytest.approx(43.2233, abs=1e-3)
        assert pt.altitude == pytest.approx(1393.2, abs=0.1)
        assert pt.distance == pytest.approx(22947.02, abs=0.1)
        assert pt.speed == pytest.approx(3.238, abs=0.001)
        assert pt.heart_rate == 149
        assert pt.cadence == 52
        assert pt.power == 165

    def test_start_time_is_utc_aware(
        self, parser: TcxParser, strava_tcx: bytes
    ) -> None:
        result = parser.parse(strava_tcx)

        assert result.start_time.tzinfo is not None

    def test_track_timestamps_are_utc_aware(
        self, parser: TcxParser, strava_tcx: bytes
    ) -> None:
        result = parser.parse(strava_tcx)

        assert all(pt.timestamp.tzinfo is not None for pt in result.track)
