from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


class ActivityParseError(Exception):
    pass


@dataclass(frozen=True)
class TrackPoint:
    timestamp: datetime
    lat: float | None = None
    lon: float | None = None
    altitude: float | None = None
    distance: float | None = None
    speed: float | None = None
    heart_rate: int | None = None
    cadence: int | None = None
    power: int | None = None
    temperature: float | None = None


@dataclass(frozen=True)
class GearEvent:
    timestamp: datetime
    front_gear: int | None = None
    front_gear_num: int | None = None
    rear_gear: int | None = None
    rear_gear_num: int | None = None


@dataclass
class ActivityData:
    start_time: datetime
    name: str | None = None
    sport_type: str | None = None
    track: list[TrackPoint] = field(default_factory=list)
    gear_events: list[GearEvent] = field(default_factory=list)


class ActivityParser(ABC):
    @abstractmethod
    def parse(self, content: bytes) -> ActivityData: ...
