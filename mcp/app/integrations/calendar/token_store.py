from __future__ import annotations

import json
import os
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

from ...errors import CalendarConfigurationError
from .models import OAuthTokenRecord


class TokenStore(ABC):
    @abstractmethod
    def get(self, provider: str, account_id: str) -> OAuthTokenRecord | None:
        raise NotImplementedError

    @abstractmethod
    def put(self, record: OAuthTokenRecord) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, provider: str, account_id: str) -> None:
        raise NotImplementedError


class FileTokenStore(TokenStore):
    def __init__(self, path: str) -> None:
        if not path.strip():
            raise CalendarConfigurationError("GOOGLE_TOKEN_STORE_PATH is required")
        self.path = Path(path)
        self._lock = threading.Lock()

    def get(self, provider: str, account_id: str) -> OAuthTokenRecord | None:
        payload = self._read_all()
        record = payload.get(provider, {}).get(account_id)
        if not record:
            return None
        expiry_raw = record.get("expiry")
        expiry = datetime.fromisoformat(expiry_raw) if expiry_raw else None
        return OAuthTokenRecord(
            provider=provider,
            account_id=account_id,
            access_token=record["access_token"],
            refresh_token=record.get("refresh_token"),
            expiry=expiry,
            scope=tuple(record.get("scope", [])),
            token_type=record.get("token_type"),
            metadata=record.get("metadata", {}),
        )

    def put(self, record: OAuthTokenRecord) -> None:
        with self._lock:
            payload = self._read_all()
            provider_bucket = payload.setdefault(record.provider, {})
            provider_bucket[record.account_id] = {
                "access_token": record.access_token,
                "refresh_token": record.refresh_token,
                "expiry": record.expiry.isoformat() if record.expiry else None,
                "scope": list(record.scope),
                "token_type": record.token_type,
                "metadata": record.metadata,
            }
            self._write_all(payload)

    def delete(self, provider: str, account_id: str) -> None:
        with self._lock:
            payload = self._read_all()
            provider_bucket = payload.get(provider, {})
            provider_bucket.pop(account_id, None)
            if not provider_bucket and provider in payload:
                payload.pop(provider, None)
            self._write_all(payload)

    def _read_all(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_all(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
        os.replace(temp_path, self.path)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass
