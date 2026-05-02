from __future__ import annotations

from datetime import timezone

import gpxpy  # type: ignore[import-untyped]

from app.parsers.base import (
    ActivityData,
    ActivityParseError,
    ActivityParser,
    TrackPoint,
)


def _local_tag(element: object) -> str:
    tag = element.tag  # type: ignore[attr-defined]
    return tag.split("}")[1] if "}" in tag else tag


def _safe_int(text: str) -> int | None:
    try:
        return int(text)
    except ValueError:
        return None


def _safe_float(text: str) -> float | None:
    try:
        return float(text)
    except ValueError:
        return None


def _parse_extensions(
    point: object,
) -> tuple[int | None, int | None, int | None, float | None]:
    hr = cad = pwr = temp = None
    for ext in point.extensions:  # type: ignore[attr-defined]
        local = _local_tag(ext)
        if local == "power" and ext.text:
            pwr = _safe_int(ext.text)
        elif local == "TrackPointExtension":
            for child in ext:
                clocal = _local_tag(child)
                if clocal == "hr" and child.text:
                    hr = _safe_int(child.text)
                elif clocal == "cad" and child.text:
                    cad = _safe_int(child.text)
                elif clocal == "atemp" and child.text:
                    temp = _safe_float(child.text)
    return hr, cad, pwr, temp


class GpxParser(ActivityParser):
    def parse(self, content: bytes) -> ActivityData:
        try:
            gpx = gpxpy.parse(content.decode())
        except Exception as exc:
            raise ActivityParseError(f"Failed to parse GPX file: {exc}") from exc

        if not gpx.tracks:
            raise ActivityParseError("GPX file contains no tracks")

        trk = gpx.tracks[0]
        points = [p for seg in trk.segments for p in seg.points]

        if not points:
            raise ActivityParseError("GPX track contains no points")

        start_time = points[0].time
        if start_time is None:
            raise ActivityParseError("GPX track has no start time")
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        track = []
        for p in points:
            ts = p.time
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            hr, cad, pwr, temp = _parse_extensions(p)
            track.append(
                TrackPoint(
                    timestamp=ts,
                    lat=p.latitude,
                    lon=p.longitude,
                    altitude=p.elevation,
                    heart_rate=hr,
                    cadence=cad,
                    power=pwr,
                    temperature=temp,
                )
            )

        return ActivityData(
            start_time=start_time,
            name=trk.name or None,
            sport_type=trk.type or None,
            track=track,
        )
