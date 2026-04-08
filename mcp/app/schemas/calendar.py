from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

from ..timeutils import parse_hhmm, resolve_timezone


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


class WorkingHoursInput(BaseModel):
    start_time: str = Field(..., examples=["09:00"])
    end_time: str = Field(..., examples=["18:00"])
    weekdays: list[int] = Field(default_factory=lambda: [0, 1, 2, 3, 4])

    @field_validator("weekdays")
    @classmethod
    def validate_weekdays(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError("weekdays must not be empty")
        for weekday in value:
            if weekday < 0 or weekday > 6:
                raise ValueError("weekdays must contain values between 0 and 6")
        return value

    @model_validator(mode="after")
    def validate_range(self) -> "WorkingHoursInput":
        start_time = parse_hhmm(self.start_time, "start_time")
        end_time = parse_hhmm(self.end_time, "end_time")
        if end_time <= start_time:
            raise ValueError("working_hours.end_time must be after start_time")
        return self


class CalendarRequestBase(BaseModel):
    timezone: str | None = None
    calendar_id: str | None = None

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        resolve_timezone(cleaned, cleaned)
        return cleaned

    @field_validator("calendar_id")
    @classmethod
    def normalize_calendar_id(cls, value: str | None) -> str | None:
        return _clean_text(value)


class CheckAvailabilityRequest(CalendarRequestBase):
    start_datetime: datetime
    end_datetime: datetime


class ListEventsRequest(CalendarRequestBase):
    time_min: datetime
    time_max: datetime
    max_results: int = Field(default=20, ge=1, le=250)


class SuggestSlotsRequest(CalendarRequestBase):
    window_start: datetime
    window_end: datetime
    slot_duration_minutes: int = Field(..., ge=1, le=1440)
    buffer_before_minutes: int = Field(default=0, ge=0, le=240)
    buffer_after_minutes: int = Field(default=0, ge=0, le=240)
    working_hours: WorkingHoursInput | None = None


class CreateEventRequest(CalendarRequestBase):
    title: str = Field(..., min_length=1, max_length=200)
    start_datetime: datetime
    end_datetime: datetime
    description: str | None = Field(default=None, max_length=4000)
    location: str | None = Field(default=None, max_length=512)
    attendees: list[str] = Field(default_factory=list)
    allow_conflicts: bool = False

    @field_validator("title", "description", "location")
    @classmethod
    def normalize_text_fields(cls, value: str | None) -> str | None:
        return _clean_text(value)

    @field_validator("attendees")
    @classmethod
    def normalize_attendees(cls, value: list[str]) -> list[str]:
        unique_values: list[str] = []
        for attendee in value:
            cleaned = attendee.strip()
            if cleaned and cleaned not in unique_values:
                unique_values.append(cleaned)
        return unique_values


class UpdateEventRequest(CalendarRequestBase):
    event_id: str = Field(..., min_length=1)
    title: str | None = Field(default=None, max_length=200)
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    description: str | None = Field(default=None, max_length=4000)
    location: str | None = Field(default=None, max_length=512)
    attendees: list[str] | None = None
    allow_conflicts: bool = False

    @field_validator("event_id", "title", "description", "location")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        return _clean_text(value)

    @field_validator("attendees")
    @classmethod
    def normalize_optional_attendees(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        unique_values: list[str] = []
        for attendee in value:
            cleaned = attendee.strip()
            if cleaned and cleaned not in unique_values:
                unique_values.append(cleaned)
        return unique_values

    @model_validator(mode="after")
    def ensure_mutation_exists(self) -> "UpdateEventRequest":
        if not any(
            value is not None
            for value in (
                self.title,
                self.start_datetime,
                self.end_datetime,
                self.description,
                self.location,
                self.attendees,
            )
        ):
            raise ValueError("At least one mutable field must be provided")
        return self


class CancelEventRequest(CalendarRequestBase):
    event_id: str = Field(..., min_length=1)

    @field_validator("event_id")
    @classmethod
    def normalize_event_id(cls, value: str) -> str:
        return value.strip()


class NormalizedRange(BaseModel):
    start_datetime: str
    end_datetime: str
    timezone: str


class ConflictSummary(BaseModel):
    event_id: str
    summary: str
    start_datetime: str
    end_datetime: str
    timezone: str
    status: str
    html_link: str | None = None


class EventResponse(BaseModel):
    event_id: str
    summary: str
    start_datetime: str
    end_datetime: str
    timezone: str
    status: str
    calendar_id: str
    html_link: str | None = None
    description: str | None = None
    location: str | None = None
    attendees: list[str] = Field(default_factory=list)


class SuggestedSlotResponse(BaseModel):
    start_datetime: str
    end_datetime: str
    timezone: str


class CheckAvailabilityResponse(BaseModel):
    available: bool
    conflicts: list[ConflictSummary]
    normalized_range: NormalizedRange


class ListEventsResponse(BaseModel):
    events: list[EventResponse]


class SuggestSlotsResponse(BaseModel):
    suggested_slots: list[SuggestedSlotResponse]


class CreateEventResponse(BaseModel):
    event_id: str
    html_link: str | None = None
    status: str
    summary: str


class UpdateEventResponse(BaseModel):
    updated_event: EventResponse


class CancelEventResponse(BaseModel):
    success: bool
    canceled_event_id: str
