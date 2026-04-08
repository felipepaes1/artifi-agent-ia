from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Any


@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class WorkingHours:
    start_time: time
    end_time: time
    weekdays: tuple[int, ...] = (0, 1, 2, 3, 4)


@dataclass(frozen=True)
class CalendarEvent:
    event_id: str
    summary: str
    start: datetime
    end: datetime
    timezone: str
    status: str
    calendar_id: str
    html_link: str | None = None
    description: str | None = None
    location: str | None = None
    attendees: tuple[str, ...] = ()
    transparency: str | None = None

    def blocks_time(self) -> bool:
        return self.status != "cancelled" and self.transparency != "transparent"


@dataclass(frozen=True)
class SuggestedSlot:
    start: datetime
    end: datetime
    timezone: str


@dataclass(frozen=True)
class OAuthTokenRecord:
    provider: str
    account_id: str
    access_token: str
    refresh_token: str | None
    expiry: datetime | None
    scope: tuple[str, ...] = ()
    token_type: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_expired(self, *, skew_seconds: int = 60, now: datetime | None = None) -> bool:
        if self.expiry is None:
            return False
        reference = now or datetime.now(self.expiry.tzinfo)
        return self.expiry <= reference
