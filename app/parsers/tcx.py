from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from app.parsers.base import (
    ActivityData,
    ActivityParseError,
    ActivityParser,
    TrackPoint,
)

_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
_NS3 = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
_TAG = f"{{{_NS}}}"
_TAG3 = f"{{{_NS3}}}"


def _parse_time(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _to_float(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(s: str | None) -> int | None:
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _to_int_via_float(s: str | None) -> int | None:
    f = _to_float(s)
    return int(f) if f is not None else None


class TcxParser(ActivityParser):
    def parse(self, content: bytes) -> ActivityData:
        try:
            root = ET.fromstring(content)  # noqa: S314
        except ET.ParseError as exc:
            raise ActivityParseError(f"Failed to parse TCX file: {exc}") from exc

        activity = root.find(f".//{_TAG}Activity")
        if activity is None:
            raise ActivityParseError("TCX file contains no Activity element")

        id_text = activity.findtext(f"{_TAG}Id")
        if not id_text:
            raise ActivityParseError("TCX Activity has no Id (start time)")

        try:
            start_time = _parse_time(id_text)
        except ValueError as exc:
            raise ActivityParseError(
                f"TCX Activity Id is not a valid datetime: {exc}"
            ) from exc

        sport = activity.attrib.get("Sport") or None

        track = []
        for tp in activity.findall(f".//{_TAG}Trackpoint"):
            time_str = tp.findtext(f"{_TAG}Time")
            if not time_str:
                continue

            try:
                ts = _parse_time(time_str)
            except ValueError:
                continue

            pos = tp.find(f"{_TAG}Position")
            lat_str = (
                pos.findtext(f"{_TAG}LatitudeDegrees") if pos is not None else None
            )
            lon_str = (
                pos.findtext(f"{_TAG}LongitudeDegrees") if pos is not None else None
            )

            alt_str = tp.findtext(f"{_TAG}AltitudeMeters")
            dist_str = tp.findtext(f"{_TAG}DistanceMeters")
            speed_str = tp.findtext(f".//{_TAG3}Speed")
            hr_str = tp.findtext(f".//{_TAG}HeartRateBpm/{_TAG}Value")
            cad_str = tp.findtext(f"{_TAG}Cadence")
            watts_str = tp.findtext(f".//{_TAG3}Watts")

            track.append(
                TrackPoint(
                    timestamp=ts,
                    lat=_to_float(lat_str),
                    lon=_to_float(lon_str),
                    altitude=_to_float(alt_str),
                    distance=_to_float(dist_str),
                    speed=_to_float(speed_str),
                    heart_rate=_to_int(hr_str),
                    cadence=_to_int(cad_str),
                    power=_to_int_via_float(watts_str),
                )
            )

        return ActivityData(
            start_time=start_time,
            sport_type=sport,
            track=track,
        )
