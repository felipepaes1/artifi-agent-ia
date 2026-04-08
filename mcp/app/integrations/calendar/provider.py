from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Sequence

from .models import CalendarEvent


class CalendarProvider(ABC):
    @abstractmethod
    async def list_events(
        self,
        *,
        account_id: str,
        calendar_id: str,
        time_min: datetime,
        time_max: datetime,
        timezone: str,
        max_results: int,
    ) -> list[CalendarEvent]:
        raise NotImplementedError

    @abstractmethod
    async def get_event(
        self,
        *,
        account_id: str,
        calendar_id: str,
        event_id: str,
        timezone: str,
    ) -> CalendarEvent:
        raise NotImplementedError

    @abstractmethod
    async def create_event(
        self,
        *,
        account_id: str,
        calendar_id: str,
        title: str,
        start: datetime,
        end: datetime,
        timezone: str,
        description: str | None,
        location: str | None,
        attendees: Sequence[str],
    ) -> CalendarEvent:
        raise NotImplementedError

    @abstractmethod
    async def update_event(
        self,
        *,
        account_id: str,
        calendar_id: str,
        event_id: str,
        timezone: str,
        title: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        description: str | None = None,
        location: str | None = None,
        attendees: Sequence[str] | None = None,
    ) -> CalendarEvent:
        raise NotImplementedError

    @abstractmethod
    async def cancel_event(
        self,
        *,
        account_id: str,
        calendar_id: str,
        event_id: str,
    ) -> None:
        raise NotImplementedError
