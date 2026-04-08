from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import CalendarValidationError


def resolve_timezone(timezone_name: str | None, default_timezone: str) -> tuple[str, ZoneInfo]:
    name = (timezone_name or default_timezone or "").strip()
    if not name:
        raise CalendarValidationError("timezone is required")
    try:
        return name, ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise CalendarValidationError(
            f"Invalid timezone: {name}",
            details={"timezone": name},
        ) from exc


def ensure_aware_datetime(value: datetime, timezone_name: str) -> datetime:
    _, tzinfo = resolve_timezone(timezone_name, timezone_name)
    if value.tzinfo is None:
        return value.replace(tzinfo=tzinfo)
    return value.astimezone(tzinfo)


def normalize_datetime_range(
    start: datetime,
    end: datetime,
    timezone_name: str | None,
    default_timezone: str,
) -> tuple[datetime, datetime, str]:
    tz_name, _ = resolve_timezone(timezone_name, default_timezone)
    normalized_start = ensure_aware_datetime(start, tz_name)
    normalized_end = ensure_aware_datetime(end, tz_name)
    if normalized_end <= normalized_start:
        raise CalendarValidationError("end_datetime must be after start_datetime")
    return normalized_start, normalized_end, tz_name


def clamp_range(
    start: datetime,
    end: datetime,
    window_start: datetime,
    window_end: datetime,
) -> tuple[datetime, datetime] | None:
    clamped_start = max(start, window_start)
    clamped_end = min(end, window_end)
    if clamped_end <= clamped_start:
        return None
    return clamped_start, clamped_end


def overlaps(
    left_start: datetime,
    left_end: datetime,
    right_start: datetime,
    right_end: datetime,
) -> bool:
    return left_start < right_end and right_start < left_end


def parse_hhmm(value: str, field_name: str) -> time:
    try:
        parsed = datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise CalendarValidationError(
            f"{field_name} must be in HH:MM format",
            details={field_name: value},
        ) from exc
    return parsed.time()


def iter_dates(start: datetime, end: datetime) -> list[date]:
    dates: list[date] = []
    cursor = start.date()
    limit = end.date()
    while cursor <= limit:
        dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def combine_date_time(value: date, clock: time, timezone_name: str) -> datetime:
    _, tzinfo = resolve_timezone(timezone_name, timezone_name)
    return datetime.combine(value, clock, tzinfo=tzinfo)
