from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, TypeVar

from fastmcp import FastMCP
from pydantic import ValidationError

from ..container import get_calendar_service
from ..errors import CalendarIntegrationError, CalendarValidationError
from ..integrations.calendar.models import CalendarEvent, SuggestedSlot, WorkingHours
from ..observability import log_event
from ..schemas import (
    CancelEventRequest,
    CancelEventResponse,
    CheckAvailabilityRequest,
    CheckAvailabilityResponse,
    ConflictSummary,
    CreateEventRequest,
    CreateEventResponse,
    EventResponse,
    ListEventsRequest,
    ListEventsResponse,
    NormalizedRange,
    SuggestedSlotResponse,
    SuggestSlotsRequest,
    SuggestSlotsResponse,
    UpdateEventRequest,
    UpdateEventResponse,
)
from ..timeutils import parse_hhmm


logger = logging.getLogger("mcp.calendar.tools")
T = TypeVar("T")


def _event_response(event: CalendarEvent) -> EventResponse:
    return EventResponse(
        event_id=event.event_id,
        summary=event.summary,
        start_datetime=event.start.isoformat(),
        end_datetime=event.end.isoformat(),
        timezone=event.timezone,
        status=event.status,
        calendar_id=event.calendar_id,
        html_link=event.html_link,
        description=event.description,
        location=event.location,
        attendees=list(event.attendees),
    )


def _slot_response(slot: SuggestedSlot) -> SuggestedSlotResponse:
    return SuggestedSlotResponse(
        start_datetime=slot.start.isoformat(),
        end_datetime=slot.end.isoformat(),
        timezone=slot.timezone,
    )


def _working_hours(schema: Any) -> WorkingHours | None:
    if schema is None:
        return None
    return WorkingHours(
        start_time=parse_hhmm(schema.start_time, "working_hours.start_time"),
        end_time=parse_hhmm(schema.end_time, "working_hours.end_time"),
        weekdays=tuple(schema.weekdays),
    )


async def _run_tool(name: str, operation: Callable[[], Awaitable[T]]) -> T | dict[str, Any]:
    try:
        return await operation()
    except ValidationError as exc:
        error = CalendarValidationError("Invalid calendar tool input", details={"errors": exc.errors()})
        log_event(logger, logging.WARNING, "calendar_tool_validation_error", tool=name, error=error.to_dict())
        return {"error": error.to_dict()}
    except CalendarIntegrationError as exc:
        log_event(logger, logging.WARNING, "calendar_tool_error", tool=name, error=exc.to_dict())
        return {"error": exc.to_dict()}
    except Exception as exc:
        error = {
            "code": "internal_error",
            "message": str(exc) or "Unexpected internal error",
            "retryable": False,
        }
        log_event(logger, logging.ERROR, "calendar_tool_unhandled_error", tool=name, error=error)
        return {"error": error}


def register_calendar_tools(mcp: FastMCP) -> None:
    service = get_calendar_service()

    @mcp.tool()
    async def check_availability(
        start_datetime: str,
        end_datetime: str,
        timezone: str | None = None,
        calendar_id: str | None = None,
    ) -> dict[str, Any]:
        """Check whether a time range is free and return summarized conflicts."""

        async def operation() -> dict[str, Any]:
            payload = CheckAvailabilityRequest(
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                timezone=timezone,
                calendar_id=calendar_id,
            )
            result = await service.check_availability(
                start_datetime=payload.start_datetime,
                end_datetime=payload.end_datetime,
                timezone=payload.timezone,
                calendar_id=payload.calendar_id,
            )
            response = CheckAvailabilityResponse(
                available=result.available,
                conflicts=[
                    ConflictSummary(
                        event_id=event.event_id,
                        summary=event.summary,
                        start_datetime=event.start.isoformat(),
                        end_datetime=event.end.isoformat(),
                        timezone=event.timezone,
                        status=event.status,
                        html_link=event.html_link,
                    )
                    for event in result.conflicts
                ],
                normalized_range=NormalizedRange(
                    start_datetime=result.start.isoformat(),
                    end_datetime=result.end.isoformat(),
                    timezone=result.timezone,
                ),
            )
            return response.model_dump(mode="json")

        return await _run_tool("check_availability", operation)

    @mcp.tool()
    async def list_events(
        time_min: str,
        time_max: str,
        timezone: str | None = None,
        calendar_id: str | None = None,
        max_results: int = 20,
    ) -> dict[str, Any]:
        """List future or bounded events in a normalized timezone."""

        async def operation() -> dict[str, Any]:
            payload = ListEventsRequest(
                time_min=time_min,
                time_max=time_max,
                timezone=timezone,
                calendar_id=calendar_id,
                max_results=max_results,
            )
            events = await service.list_events(
                time_min=payload.time_min,
                time_max=payload.time_max,
                timezone=payload.timezone,
                calendar_id=payload.calendar_id,
                max_results=payload.max_results,
            )
            response = ListEventsResponse(events=[_event_response(event) for event in events])
            return response.model_dump(mode="json")

        return await _run_tool("list_events", operation)

    @mcp.tool()
    async def suggest_slots(
        window_start: str,
        window_end: str,
        slot_duration_minutes: int,
        timezone: str | None = None,
        calendar_id: str | None = None,
        buffer_before_minutes: int = 0,
        buffer_after_minutes: int = 0,
        working_hours: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Suggest free slots within a window while respecting buffers and optional working hours."""

        async def operation() -> dict[str, Any]:
            payload = SuggestSlotsRequest(
                window_start=window_start,
                window_end=window_end,
                slot_duration_minutes=slot_duration_minutes,
                timezone=timezone,
                calendar_id=calendar_id,
                buffer_before_minutes=buffer_before_minutes,
                buffer_after_minutes=buffer_after_minutes,
                working_hours=working_hours,
            )
            slots = await service.suggest_slots(
                window_start=payload.window_start,
                window_end=payload.window_end,
                slot_duration_minutes=payload.slot_duration_minutes,
                timezone=payload.timezone,
                calendar_id=payload.calendar_id,
                buffer_before_minutes=payload.buffer_before_minutes,
                buffer_after_minutes=payload.buffer_after_minutes,
                working_hours=_working_hours(payload.working_hours),
            )
            response = SuggestSlotsResponse(suggested_slots=[_slot_response(slot) for slot in slots])
            return response.model_dump(mode="json")

        return await _run_tool("suggest_slots", operation)

    @mcp.tool()
    async def create_event(
        title: str,
        start_datetime: str,
        end_datetime: str,
        timezone: str | None = None,
        description: str | None = None,
        location: str | None = None,
        attendees: list[str] | None = None,
        calendar_id: str | None = None,
        allow_conflicts: bool = False,
    ) -> dict[str, Any]:
        """Create a calendar event after validating the interval and checking conflicts by default."""

        async def operation() -> dict[str, Any]:
            payload = CreateEventRequest(
                title=title,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                timezone=timezone,
                description=description,
                location=location,
                attendees=attendees or [],
                calendar_id=calendar_id,
                allow_conflicts=allow_conflicts,
            )
            created_event = await service.create_event(
                title=payload.title,
                start_datetime=payload.start_datetime,
                end_datetime=payload.end_datetime,
                timezone=payload.timezone,
                description=payload.description,
                location=payload.location,
                attendees=payload.attendees,
                calendar_id=payload.calendar_id,
                allow_conflicts=payload.allow_conflicts,
            )
            response = CreateEventResponse(
                event_id=created_event.event_id,
                html_link=created_event.html_link,
                status=created_event.status,
                summary=created_event.summary,
            )
            return response.model_dump(mode="json")

        return await _run_tool("create_event", operation)

    @mcp.tool()
    async def update_event(
        event_id: str,
        title: str | None = None,
        start_datetime: str | None = None,
        end_datetime: str | None = None,
        timezone: str | None = None,
        description: str | None = None,
        location: str | None = None,
        attendees: list[str] | None = None,
        calendar_id: str | None = None,
        allow_conflicts: bool = False,
    ) -> dict[str, Any]:
        """Update mutable fields of an event and re-check conflicts when the time changes."""

        async def operation() -> dict[str, Any]:
            payload = UpdateEventRequest(
                event_id=event_id,
                title=title,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                timezone=timezone,
                description=description,
                location=location,
                attendees=attendees,
                calendar_id=calendar_id,
                allow_conflicts=allow_conflicts,
            )
            updated_event = await service.update_event(
                event_id=payload.event_id,
                title=payload.title,
                start_datetime=payload.start_datetime,
                end_datetime=payload.end_datetime,
                timezone=payload.timezone,
                description=payload.description,
                location=payload.location,
                attendees=payload.attendees,
                calendar_id=payload.calendar_id,
                allow_conflicts=payload.allow_conflicts,
            )
            response = UpdateEventResponse(updated_event=_event_response(updated_event))
            return response.model_dump(mode="json")

        return await _run_tool("update_event", operation)

    @mcp.tool()
    async def cancel_event(event_id: str, calendar_id: str | None = None) -> dict[str, Any]:
        """Cancel an event by ID."""

        async def operation() -> dict[str, Any]:
            payload = CancelEventRequest(event_id=event_id, calendar_id=calendar_id)
            canceled_event_id = await service.cancel_event(
                event_id=payload.event_id,
                calendar_id=payload.calendar_id,
            )
            response = CancelEventResponse(success=True, canceled_event_id=canceled_event_id)
            return response.model_dump(mode="json")

        return await _run_tool("cancel_event", operation)
