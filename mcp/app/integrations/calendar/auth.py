from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx

from ...errors import CalendarAuthError, CalendarConfigurationError, CalendarProviderError
from ...observability import log_event
from .models import OAuthTokenRecord


class GoogleOAuthClient:
    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        auth_uri: str,
        token_uri: str,
        scopes: tuple[str, ...],
        redirect_uri: str,
        access_type: str,
        prompt: str,
        include_granted_scopes: bool,
        logger,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_uri = auth_uri
        self.token_uri = token_uri
        self.scopes = scopes
        self.redirect_uri = redirect_uri
        self.access_type = access_type
        self.prompt = prompt
        self.include_granted_scopes = include_granted_scopes
        self.logger = logger

    def validate_configuration(self) -> None:
        if not self.client_id or not self.client_secret:
            raise CalendarConfigurationError(
                "Google Calendar OAuth is not configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET."
            )

    def build_authorization_url(
        self,
        *,
        state: str,
        login_hint: str | None = None,
        redirect_uri: str | None = None,
        scopes: tuple[str, ...] | None = None,
    ) -> str:
        self.validate_configuration()
        query = {
            "client_id": self.client_id,
            "redirect_uri": redirect_uri or self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(scopes or self.scopes),
            "access_type": self.access_type,
            "prompt": self.prompt,
            "state": state,
        }
        if self.include_granted_scopes:
            query["include_granted_scopes"] = "true"
        if login_hint:
            query["login_hint"] = login_hint
        return f"{self.auth_uri}?{urlencode(query)}"

    async def exchange_code(
        self,
        *,
        code: str,
        account_id: str,
        redirect_uri: str | None = None,
        client: httpx.AsyncClient,
    ) -> OAuthTokenRecord:
        self.validate_configuration()
        payload = {
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": redirect_uri or self.redirect_uri,
            "grant_type": "authorization_code",
        }
        return await self._token_request(payload=payload, account_id=account_id, client=client)

    async def refresh(
        self,
        *,
        refresh_token: str,
        account_id: str,
        client: httpx.AsyncClient,
    ) -> OAuthTokenRecord:
        self.validate_configuration()
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        return await self._token_request(
            payload=payload,
            account_id=account_id,
            client=client,
            inherited_refresh_token=refresh_token,
        )

    async def _token_request(
        self,
        *,
        payload: dict[str, str],
        account_id: str,
        client: httpx.AsyncClient,
        inherited_refresh_token: str | None = None,
    ) -> OAuthTokenRecord:
        try:
            response = await client.post(
                self.token_uri,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        except httpx.TimeoutException as exc:
            raise CalendarProviderError("Google OAuth request timed out", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise CalendarProviderError("Google OAuth request failed", retryable=True) from exc

        data = response.json() if response.content else {}
        if response.status_code >= 400:
            error_description = data.get("error_description") or data.get("error") or response.text
            log_event(
                self.logger,
                40,
                "google_oauth_error",
                account_id=account_id,
                status_code=response.status_code,
                error=data,
            )
            raise CalendarAuthError(f"Google OAuth failed: {error_description}")

        expires_in = int(data.get("expires_in", 0) or 0)
        expiry = None
        if expires_in:
            expiry = datetime.now(timezone.utc) + timedelta(seconds=max(expires_in - 30, 0))
        scope_value = data.get("scope") or " ".join(self.scopes)
        refresh_token = data.get("refresh_token") or inherited_refresh_token
        if not data.get("access_token"):
            raise CalendarAuthError("Google OAuth response did not include an access_token")
        return OAuthTokenRecord(
            provider="google_calendar",
            account_id=account_id,
            access_token=data["access_token"],
            refresh_token=refresh_token,
            expiry=expiry,
            scope=tuple(scope_value.split()),
            token_type=data.get("token_type"),
            metadata={},
        )
