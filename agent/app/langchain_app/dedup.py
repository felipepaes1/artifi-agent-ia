import threading
import time
from typing import Dict


class TTLSet:
    def __init__(self) -> None:
        self._values: Dict[str, float] = {}
        self._lock = threading.Lock()

    def seen(self, key: str, ttl_seconds: int) -> bool:
        if not key:
            return False
        now = time.time()
        with self._lock:
            self._purge(now, ttl_seconds)
            if key in self._values:
                return True
            self._values[key] = now
            return False

    def _purge(self, now: float, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            return
        cutoff = now - ttl_seconds
        expired = [k for k, v in self._values.items() if v < cutoff]
        for key in expired:
            self._values.pop(key, None)
