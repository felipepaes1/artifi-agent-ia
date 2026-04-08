from __future__ import annotations

from typing import Any


class CalendarIntegrationError(Exception):
    code = "calendar_error"
    retryable = False

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        retryable: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}
        if retryable is not None:
            self.retryable = retryable

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.details:
            payload["details"] = self.details
        return payload


class CalendarConfigurationError(CalendarIntegrationError):
    code = "calendar_configuration_error"


class CalendarValidationError(CalendarIntegrationError):
    code = "calendar_validation_error"


class CalendarAuthError(CalendarIntegrationError):
    code = "calendar_auth_error"


class CalendarConflictError(CalendarIntegrationError):
    code = "calendar_conflict_error"


class CalendarNotFoundError(CalendarIntegrationError):
    code = "calendar_not_found"


class CalendarProviderError(CalendarIntegrationError):
    code = "calendar_provider_error"


class CalendarTransientError(CalendarIntegrationError):
    code = "calendar_transient_error"
    retryable = True
