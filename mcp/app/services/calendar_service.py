from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Sequence

from ..errors import CalendarConflictError, CalendarValidationError
from ..integrations.calendar.models import CalendarEvent, SuggestedSlot, WorkingHours
from ..integrations.calendar.provider import CalendarProvider
from ..timeutils import (
    clamp_range,
    combine_date_time,
    ensure_aware_datetime,
    iter_dates,
    normalize_datetime_range,
    overlaps,
)


@dataclass(frozen=True)
class AvailabilityResult:
    available: bool
    conflicts: tuple[CalendarEvent, ...]
    start: datetime
    end: datetime
    timezone: str


class CalendarService:
    def __init__(
        self,
        *,
        provider: CalendarProvider,
        default_account_id: str,
        default_calendar_id: str,
        default_timezone: str,
        slot_increment_minutes: int,
        max_suggested_slots: int,
    ) -> None:
        self.provider = provider
        self.default_account_id = default_account_id
        self.default_calendar_id = default_calendar_id
        self.default_timezone = default_timezone
        self.slot_increment_minutes = slot_increment_minutes
        self.max_suggested_slots = max_suggested_slots

    async def check_availability(
        self,
        *,
        start_datetime: datetime,
        end_datetime: datetime,
        timezone: str | None,
        calendar_id: str | None,
    ) -> AvailabilityResult:
        start, end, timezone_name = normalize_datetime_range(
            start_datetime,
            end_datetime,
            timezone,
            self.default_timezone,
        )
        conflicts = await self._load_conflicts(
            calendar_id=calendar_id,
            start=start,
            end=end,
            timezone=timezone_name,
        )
        return AvailabilityResult(
            available=not conflicts,
            conflicts=tuple(conflicts),
            start=start,
            end=end,
            timezone=timezone_name,
        )

    async def list_events(
        self,
        *,
        time_min: datetime,
        time_max: datetime,
        timezone: str | None,
        calendar_id: str | None,
        max_results: int,
    ) -> list[CalendarEvent]:
        start, end, timezone_name = normalize_datetime_range(
            time_min,
            time_max,
            timezone,
            self.default_timezone,
        )
        return await self.provider.list_events(
            account_id=self.default_account_id,
            calendar_id=calendar_id or self.default_calendar_id,
            time_min=start,
            time_max=end,
            timezone=timezone_name,
            max_results=max_results,
        )

    async def suggest_slots(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
        slot_duration_minutes: int,
        timezone: str | None,
        calendar_id: str | None,
        buffer_before_minutes: int,
        buffer_after_minutes: int,
        working_hours: WorkingHours | None,
    ) -> list[SuggestedSlot]:
        if slot_duration_minutes <= 0:
            raise CalendarValidationError("slot_duration_minutes must be greater than zero")
        start, end, timezone_name = normalize_datetime_range(
            window_start,
            window_end,
            timezone,
            self.default_timezone,
        )
        events = await self.provider.list_events(
            account_id=self.default_account_id,
            calendar_id=calendar_id or self.default_calendar_id,
            time_min=start,
            time_max=end,
            timezone=timezone_name,
            max_results=500,
        )
        busy_windows = []
        for event in events:
            if not event.blocks_time():
                continue
            candidate = clamp_range(
                event.start - timedelta(minutes=buffer_before_minutes),
                event.end + timedelta(minutes=buffer_after_minutes),
                start,
                end,
            )
            if candidate is not None:
                busy_windows.append(candidate)

        suggested: list[SuggestedSlot] = []
        slot_duration = timedelta(minutes=slot_duration_minutes)
        increment = timedelta(minutes=self.slot_increment_minutes)
        hours = working_hours

        for day_value in iter_dates(start, end):
            if hours is not None and day_value.weekday() not in hours.weekdays:
                continue
            day_start = (
                combine_date_time(day_value, hours.start_time, timezone_name)
                if hours
                else combine_date_time(day_value, time.min, timezone_name)
            )
            day_end = (
                combine_date_time(day_value, hours.end_time, timezone_name)
                if hours
                else combine_date_time(day_value + timedelta(days=1), time.min, timezone_name)
            )
            day_window = clamp_range(day_start, day_end, start, end)
            if day_window is None:
                continue
            cursor = day_window[0]
            while cursor + slot_duration <= day_window[1]:
                slot_end = cursor + slot_duration
                if not any(overlaps(cursor, slot_end, busy_start, busy_end) for busy_start, busy_end in busy_windows):
                    suggested.append(SuggestedSlot(start=cursor, end=slot_end, timezone=timezone_name))
                    if len(suggested) >= self.max_suggested_slots:
                        return suggested
                cursor += increment
        return suggested

    async def create_event(
        self,
        *,
        title: str,
        start_datetime: datetime,
        end_datetime: datetime,
        timezone: str | None,
        description: str | None,
        location: str | None,
        attendees: Sequence[str],
        calendar_id: str | None,
        allow_conflicts: bool,
    ) -> CalendarEvent:
        start, end, timezone_name = normalize_datetime_range(
            start_datetime,
            end_datetime,
            timezone,
            self.default_timezone,
        )
        resolved_calendar_id = calendar_id or self.default_calendar_id
        if not allow_conflicts:
            conflicts = await self._load_conflicts(
                calendar_id=resolved_calendar_id,
                start=start,
                end=end,
                timezone=timezone_name,
            )
            if conflicts:
                raise CalendarConflictError(
                    "Cannot create event because the time range conflicts with existing events.",
                    details={"conflicts": [event.event_id for event in conflicts]},
                )
        return await self.provider.create_event(
            account_id=self.default_account_id,
            calendar_id=resolved_calendar_id,
            title=title,
            start=start,
            end=end,
            timezone=timezone_name,
            description=description,
            location=location,
            attendees=attendees,
        )

    async def update_event(
        self,
        *,
        event_id: str,
        timezone: str | None,
        calendar_id: str | None,
        title: str | None,
        start_datetime: datetime | None,
        end_datetime: datetime | None,
        description: str | None,
        location: str | None,
        attendees: Sequence[str] | None,
        allow_conflicts: bool,
    ) -> CalendarEvent:
        resolved_calendar_id = calendar_id or self.default_calendar_id
        timezone_name = timezone or self.default_timezone
        current_event = await self.provider.get_event(
            account_id=self.default_account_id,
            calendar_id=resolved_calendar_id,
            event_id=event_id,
            timezone=timezone_name,
        )

        new_start = ensure_aware_datetime(start_datetime, timezone_name) if start_datetime else current_event.start
        new_end = ensure_aware_datetime(end_datetime, timezone_name) if end_datetime else current_event.end
        new_start, new_end, timezone_name = normalize_datetime_range(
            new_start,
            new_end,
            timezone_name,
            self.default_timezone,
        )

        if not any(
            value is not None
            for value in (title, start_datetime, end_datetime, description, location, attendees)
        ):
            raise CalendarValidationError("At least one mutable field must be provided to update_event")

        if not allow_conflicts and (new_start != current_event.start or new_end != current_event.end):
            conflicts = await self._load_conflicts(
                calendar_id=resolved_calendar_id,
                start=new_start,
                end=new_end,
                timezone=timezone_name,
                exclude_event_id=event_id,
            )
            if conflicts:
                raise CalendarConflictError(
                    "Cannot update event because the new time range conflicts with existing events.",
                    details={"conflicts": [event.event_id for event in conflicts]},
                )

        return await self.provider.update_event(
            account_id=self.default_account_id,
            calendar_id=resolved_calendar_id,
            event_id=event_id,
            timezone=timezone_name,
            title=title,
            start=new_start if start_datetime or end_datetime else None,
            end=new_end if start_datetime or end_datetime else None,
            description=description,
            location=location,
            attendees=attendees,
        )

    async def cancel_event(
        self,
        *,
        event_id: str,
        calendar_id: str | None,
    ) -> str:
        resolved_calendar_id = calendar_id or self.default_calendar_id
        await self.provider.cancel_event(
            account_id=self.default_account_id,
            calendar_id=resolved_calendar_id,
            event_id=event_id,
        )
        return event_id

    async def _load_conflicts(
        self,
        *,
        calendar_id: str | None,
        start: datetime,
        end: datetime,
        timezone: str,
        exclude_event_id: str | None = None,
    ) -> list[CalendarEvent]:
        events = await self.provider.list_events(
            account_id=self.default_account_id,
            calendar_id=calendar_id or self.default_calendar_id,
            time_min=start,
            time_max=end,
            timezone=timezone,
            max_results=250,
        )
        conflicts = []
        for event in events:
            if exclude_event_id and event.event_id == exclude_event_id:
                continue
            if event.blocks_time() and overlaps(start, end, event.start, event.end):
                conflicts.append(event)
        return conflicts
