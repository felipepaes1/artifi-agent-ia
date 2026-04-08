from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time
from typing import Any, Sequence
from urllib.parse import quote

import httpx

from ...errors import (
    CalendarAuthError,
    CalendarNotFoundError,
    CalendarProviderError,
    CalendarTransientError,
)
from ...observability import log_event
from ...timeutils import ensure_aware_datetime, resolve_timezone
from .auth import GoogleOAuthClient
from .models import CalendarEvent, OAuthTokenRecord
from .provider import CalendarProvider
from .token_store import TokenStore


class GoogleCalendarProvider(CalendarProvider):
    provider_name = "google_calendar"

    def __init__(
        self,
        *,
        token_store: TokenStore,
        oauth_client: GoogleOAuthClient,
        api_base_url: str,
        timeout: float,
        logger: logging.Logger | None = None,
    ) -> None:
        self.token_store = token_store
        self.oauth_client = oauth_client
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = timeout
        self.logger = logger or logging.getLogger("mcp.calendar.google")

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
        payload = await self._request_json(
            account_id=account_id,
            method="GET",
            path=f"calendars/{quote(calendar_id, safe='')}/events",
            params={
                "timeMin": time_min.isoformat(),
                "timeMax": time_max.isoformat(),
                "timeZone": timezone,
                "singleEvents": "true",
                "showDeleted": "false",
                "orderBy": "startTime",
                "maxResults": str(max_results),
            },
            safe_to_retry=True,
        )
        return [self._map_event(item, calendar_id=calendar_id, default_timezone=timezone) for item in payload.get("items", [])]

    async def get_event(
        self,
        *,
        account_id: str,
        calendar_id: str,
        event_id: str,
        timezone: str,
    ) -> CalendarEvent:
        payload = await self._request_json(
            account_id=account_id,
            method="GET",
            path=f"calendars/{quote(calendar_id, safe='')}/events/{quote(event_id, safe='')}",
            params={"timeZone": timezone},
            safe_to_retry=True,
        )
        return self._map_event(payload, calendar_id=calendar_id, default_timezone=timezone)

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
        payload = {
            "summary": title,
            "start": {"dateTime": start.isoformat(), "timeZone": timezone},
            "end": {"dateTime": end.isoformat(), "timeZone": timezone},
        }
        if description is not None:
            payload["description"] = description
        if location is not None:
            payload["location"] = location
        if attendees:
            payload["attendees"] = [{"email": attendee} for attendee in attendees]
        response = await self._request_json(
            account_id=account_id,
            method="POST",
            path=f"calendars/{quote(calendar_id, safe='')}/events",
            params={"sendUpdates": "none"},
            json_payload=payload,
        )
        return self._map_event(response, calendar_id=calendar_id, default_timezone=timezone)

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
        payload: dict[str, Any] = {}
        if title is not None:
            payload["summary"] = title
        if start is not None:
            payload["start"] = {"dateTime": start.isoformat(), "timeZone": timezone}
        if end is not None:
            payload["end"] = {"dateTime": end.isoformat(), "timeZone": timezone}
        if description is not None:
            payload["description"] = description
        if location is not None:
            payload["location"] = location
        if attendees is not None:
            payload["attendees"] = [{"email": attendee} for attendee in attendees]
        response = await self._request_json(
            account_id=account_id,
            method="PATCH",
            path=f"calendars/{quote(calendar_id, safe='')}/events/{quote(event_id, safe='')}",
            params={"sendUpdates": "none"},
            json_payload=payload,
        )
        return self._map_event(response, calendar_id=calendar_id, default_timezone=timezone)

    async def cancel_event(
        self,
        *,
        account_id: str,
        calendar_id: str,
        event_id: str,
    ) -> None:
        await self._request_json(
            account_id=account_id,
            method="DELETE",
            path=f"calendars/{quote(calendar_id, safe='')}/events/{quote(event_id, safe='')}",
        )

    async def store_authorization_code(
        self,
        *,
        code: str,
        account_id: str,
        redirect_uri: str | None = None,
    ) -> OAuthTokenRecord:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            record = await self.oauth_client.exchange_code(
                code=code,
                account_id=account_id,
                redirect_uri=redirect_uri,
                client=client,
            )
        self.token_store.put(record)
        return record

    async def _request_json(
        self,
        *,
        account_id: str,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        json_payload: dict[str, Any] | None = None,
        safe_to_retry: bool = False,
        retry_on_auth: bool = True,
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            token_record = await self._ensure_token(account_id=account_id, client=client)
            url = f"{self.api_base_url}/{path.lstrip('/')}"
            headers = {"Authorization": f"Bearer {token_record.access_token}"}
            max_attempts = 2 if safe_to_retry else 1
            attempt = 0
            while True:
                try:
                    response = await client.request(
                        method=method,
                        url=url,
                        params=params,
                        json=json_payload,
                        headers=headers,
                    )
                except httpx.TimeoutException as exc:
                    if safe_to_retry and attempt < max_attempts - 1:
                        attempt += 1
                        await asyncio.sleep(0.25)
                        continue
                    raise CalendarTransientError("Google Calendar request timed out") from exc
                except httpx.HTTPError as exc:
                    if safe_to_retry and attempt < max_attempts - 1:
                        attempt += 1
                        await asyncio.sleep(0.25)
                        continue
                    raise CalendarTransientError("Google Calendar request failed") from exc

                if response.status_code == 401 and retry_on_auth:
                    token_record = await self._refresh_token(account_id=account_id, client=client)
                    headers["Authorization"] = f"Bearer {token_record.access_token}"
                    retry_on_auth = False
                    continue

                if response.status_code == 404:
                    raise CalendarNotFoundError("Calendar resource not found")

                if response.status_code in {429, 500, 502, 503, 504}:
                    if safe_to_retry and attempt < max_attempts - 1:
                        attempt += 1
                        await asyncio.sleep(0.25)
                        continue
                    raise CalendarTransientError("Google Calendar is temporarily unavailable")

                if response.status_code >= 400:
                    error_payload = response.json() if response.content else {}
                    message = (
                        error_payload.get("error", {}).get("message")
                        or response.text
                        or "Google Calendar request failed"
                    )
                    log_event(
                        self.logger,
                        logging.ERROR,
                        "google_calendar_request_error",
                        account_id=account_id,
                        method=method,
                        path=path,
                        status_code=response.status_code,
                        error=error_payload,
                    )
                    if response.status_code in {401, 403}:
                        raise CalendarAuthError(message)
                    raise CalendarProviderError(message)

                if response.status_code == 204 or not response.content:
                    return {}
                return response.json()
        raise CalendarProviderError("Google Calendar request failed after retries")

    async def _ensure_token(self, *, account_id: str, client: httpx.AsyncClient) -> OAuthTokenRecord:
        token_record = self.token_store.get(self.provider_name, account_id)
        if token_record is None:
            raise CalendarAuthError(
                "Google Calendar is not authorized for this account. Complete OAuth setup before using the tools."
            )
        if token_record.is_expired():
            return await self._refresh_token(account_id=account_id, client=client)
        return token_record

    async def _refresh_token(self, *, account_id: str, client: httpx.AsyncClient) -> OAuthTokenRecord:
        current_record = self.token_store.get(self.provider_name, account_id)
        if current_record is None or not current_record.refresh_token:
            raise CalendarAuthError("Missing refresh token for Google Calendar account")
        refreshed_record = await self.oauth_client.refresh(
            refresh_token=current_record.refresh_token,
            account_id=account_id,
            client=client,
        )
        self.token_store.put(refreshed_record)
        log_event(self.logger, logging.INFO, "google_calendar_token_refreshed", account_id=account_id)
        return refreshed_record

    def _map_event(self, payload: dict[str, Any], *, calendar_id: str, default_timezone: str) -> CalendarEvent:
        start, timezone_name = self._parse_google_datetime(payload.get("start", {}), default_timezone)
        end, _ = self._parse_google_datetime(payload.get("end", {}), timezone_name)
        attendees = tuple(
            attendee.get("email", "").strip()
            for attendee in payload.get("attendees", [])
            if attendee.get("email")
        )
        return CalendarEvent(
            event_id=payload.get("id", ""),
            summary=payload.get("summary", ""),
            start=start,
            end=end,
            timezone=timezone_name,
            status=payload.get("status", "confirmed"),
            calendar_id=calendar_id,
            html_link=payload.get("htmlLink"),
            description=payload.get("description"),
            location=payload.get("location"),
            attendees=attendees,
            transparency=payload.get("transparency"),
        )

    def _parse_google_datetime(
        self,
        payload: dict[str, Any],
        default_timezone: str,
    ) -> tuple[datetime, str]:
        timezone_name = payload.get("timeZone") or default_timezone
        resolve_timezone(timezone_name, default_timezone)
        if payload.get("dateTime"):
            parsed = datetime.fromisoformat(payload["dateTime"].replace("Z", "+00:00"))
            return ensure_aware_datetime(parsed, timezone_name), timezone_name
        if payload.get("date"):
            parsed_date = date.fromisoformat(payload["date"])
            _, tzinfo = resolve_timezone(timezone_name, default_timezone)
            return datetime.combine(parsed_date, time.min, tzinfo=tzinfo), timezone_name
        raise CalendarProviderError("Google Calendar returned an event without start/end datetime")
