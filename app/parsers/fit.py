from __future__ import annotations

import io
from datetime import datetime, timezone

import fitparse  # type: ignore[import-untyped]

from app.parsers.base import (
    ActivityData,
    ActivityParseError,
    ActivityParser,
    GearEvent,
    TrackPoint,
)

_SEMICIRCLE_TO_DEG = 180.0 / 2**31
_GEAR_EVENTS = frozenset({"rear_gear_change", "front_gear_change"})


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


class FitParser(ActivityParser):
    def parse(self, content: bytes) -> ActivityData:
        try:
            fit = fitparse.FitFile(io.BytesIO(content))
            sessions = list(fit.get_messages("session"))
            records = list(fit.get_messages("record"))
            events = list(fit.get_messages("event"))
        except Exception as exc:
            raise ActivityParseError(f"Failed to parse FIT file: {exc}") from exc

        if not sessions:
            raise ActivityParseError("FIT file contains no session message")

        session = sessions[0]
        start_time = session.get_value("start_time")
        if start_time is None:
            raise ActivityParseError("FIT session has no start_time")
        start_time = _utc(start_time)

        sport = session.get_value("sport")

        track = []
        for rec in records:
            ts = rec.get_value("timestamp")
            if ts is None:
                continue
            ts = _utc(ts)

            lat = rec.get_value("position_lat")
            lon = rec.get_value("position_long")

            track.append(
                TrackPoint(
                    timestamp=ts,
                    lat=lat * _SEMICIRCLE_TO_DEG if lat is not None else None,
                    lon=lon * _SEMICIRCLE_TO_DEG if lon is not None else None,
                    altitude=rec.get_value("enhanced_altitude"),
                    distance=rec.get_value("distance"),
                    speed=rec.get_value("enhanced_speed"),
                    heart_rate=rec.get_value("heart_rate"),
                    cadence=rec.get_value("cadence"),
                    power=rec.get_value("power"),
                    temperature=rec.get_value("temperature"),
                )
            )

        gear_events = []
        for ev in events:
            if ev.get_value("event") not in _GEAR_EVENTS:
                continue
            ts = ev.get_value("timestamp")
            if ts is None:
                continue
            gear_events.append(
                GearEvent(
                    timestamp=_utc(ts),
                    front_gear=ev.get_value("front_gear"),
                    front_gear_num=ev.get_value("front_gear_num"),
                    rear_gear=ev.get_value("rear_gear"),
                    rear_gear_num=ev.get_value("rear_gear_num"),
                )
            )

        return ActivityData(
            start_time=start_time,
            sport_type=str(sport) if sport is not None else None,
            track=track,
            gear_events=gear_events,
        )
