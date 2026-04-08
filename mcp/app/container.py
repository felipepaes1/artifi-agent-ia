from __future__ import annotations

import logging
from functools import lru_cache

from .errors import CalendarConfigurationError
from .integrations.calendar import FileTokenStore, GoogleCalendarProvider
from .integrations.calendar.auth import GoogleOAuthClient
from .services import CalendarService
from .settings import (
    CALENDAR_PROVIDER,
    CALENDAR_DEFAULT_ACCOUNT_ID,
    CALENDAR_DEFAULT_ID,
    CALENDAR_DEFAULT_TIMEZONE,
    CALENDAR_MAX_SUGGESTED_SLOTS,
    CALENDAR_SLOT_INCREMENT_MINUTES,
    GOOGLE_CALENDAR_API_BASE_URL,
    GOOGLE_CALENDAR_SCOPES,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_OAUTH_ACCESS_TYPE,
    GOOGLE_OAUTH_AUTH_URI,
    GOOGLE_OAUTH_INCLUDE_GRANTED_SCOPES,
    GOOGLE_OAUTH_PROMPT,
    GOOGLE_OAUTH_REDIRECT_URI,
    GOOGLE_OAUTH_TOKEN_URI,
    GOOGLE_TOKEN_STORE_PATH,
    HTTP_TIMEOUT,
)


@lru_cache(maxsize=1)
def get_calendar_service() -> CalendarService:
    logger = logging.getLogger("mcp.calendar")
    if CALENDAR_PROVIDER != "google_calendar":
        raise CalendarConfigurationError(
            f"Unsupported CALENDAR_PROVIDER={CALENDAR_PROVIDER!r}. Only 'google_calendar' is implemented."
        )
    token_store = FileTokenStore(GOOGLE_TOKEN_STORE_PATH)
    oauth_client = GoogleOAuthClient(
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        auth_uri=GOOGLE_OAUTH_AUTH_URI,
        token_uri=GOOGLE_OAUTH_TOKEN_URI,
        scopes=GOOGLE_CALENDAR_SCOPES,
        redirect_uri=GOOGLE_OAUTH_REDIRECT_URI,
        access_type=GOOGLE_OAUTH_ACCESS_TYPE,
        prompt=GOOGLE_OAUTH_PROMPT,
        include_granted_scopes=GOOGLE_OAUTH_INCLUDE_GRANTED_SCOPES,
        logger=logger,
    )
    provider = GoogleCalendarProvider(
        token_store=token_store,
        oauth_client=oauth_client,
        api_base_url=GOOGLE_CALENDAR_API_BASE_URL,
        timeout=HTTP_TIMEOUT,
        logger=logger,
    )
    return CalendarService(
        provider=provider,
        default_account_id=CALENDAR_DEFAULT_ACCOUNT_ID,
        default_calendar_id=CALENDAR_DEFAULT_ID,
        default_timezone=CALENDAR_DEFAULT_TIMEZONE,
        slot_increment_minutes=CALENDAR_SLOT_INCREMENT_MINUTES,
        max_suggested_slots=CALENDAR_MAX_SUGGESTED_SLOTS,
    )
