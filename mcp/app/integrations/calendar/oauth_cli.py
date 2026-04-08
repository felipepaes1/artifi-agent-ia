from __future__ import annotations

import argparse
import asyncio
import logging
import secrets

from ...observability import configure_logging
from ...settings import (
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
from .auth import GoogleOAuthClient
from .google_calendar import GoogleCalendarProvider
from .token_store import FileTokenStore


logger = logging.getLogger("mcp.calendar.oauth_cli")


def build_provider() -> GoogleCalendarProvider:
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
    return GoogleCalendarProvider(
        token_store=token_store,
        oauth_client=oauth_client,
        api_base_url=GOOGLE_CALENDAR_API_BASE_URL,
        timeout=HTTP_TIMEOUT,
        logger=logger,
    )


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="Google Calendar OAuth bootstrap helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    auth_url_parser = subparsers.add_parser("auth-url", help="Print the Google OAuth authorization URL")
    auth_url_parser.add_argument("--state", default="", help="Opaque state value. Defaults to a random token.")
    auth_url_parser.add_argument("--login-hint", default="", help="Optional Google account email hint.")

    exchange_parser = subparsers.add_parser("exchange-code", help="Exchange an auth code and persist tokens")
    exchange_parser.add_argument("--code", required=True, help="Authorization code received from Google")
    exchange_parser.add_argument(
        "--account-id",
        default="default",
        help="Logical account identifier used by the token store",
    )
    exchange_parser.add_argument(
        "--redirect-uri",
        default="",
        help="Override redirect URI if it differs from GOOGLE_OAUTH_REDIRECT_URI",
    )

    args = parser.parse_args()
    provider = build_provider()
    oauth_client = provider.oauth_client

    if args.command == "auth-url":
        state = args.state or secrets.token_urlsafe(24)
        url = oauth_client.build_authorization_url(
            state=state,
            login_hint=args.login_hint or None,
        )
        print(url)
        print(f"state={state}")
        return

    if args.command == "exchange-code":
        record = await provider.store_authorization_code(
            code=args.code,
            account_id=args.account_id,
            redirect_uri=args.redirect_uri or None,
        )
        print("OAuth exchange completed.")
        print(f"account_id={record.account_id}")
        print(f"scope={' '.join(record.scope)}")
        print(f"expiry={record.expiry.isoformat() if record.expiry else 'none'}")


def main() -> None:
    configure_logging()
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
