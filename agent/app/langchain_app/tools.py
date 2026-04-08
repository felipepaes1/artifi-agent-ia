import json
from typing import Any, Dict, List

import httpx

from .config import Settings
from .profiles import Profile


def _safe_json(response: httpx.Response) -> Dict[str, Any]:
    try:
        data = response.json()
        if isinstance(data, dict):
            return data
        return {"data": data}
    except Exception:
        return {"text": response.text}


def _normalize_tools(tools: List[str]) -> List[str]:
    return [t.strip().lower() for t in tools if t.strip()]


def build_tools(settings: Settings, profile: Profile) -> List[Any]:
    try:
        from langchain_core.tools import tool
    except Exception:  # pragma: no cover
        try:
            from langchain.tools import tool  # type: ignore
        except Exception:
            raise RuntimeError("LangChain tools are not available. Install langchain-core.")

    enabled = _normalize_tools(profile.tools)
    tools: List[Any] = []

    calendar_enabled = bool(settings.calendar_api_url)
    open_api_enabled = bool(settings.open_api_base_url)

    def _wants(name: str) -> bool:
        if not enabled:
            return True
        return name in enabled

    if calendar_enabled and (_wants("calendar") or _wants("calendar_get_availability") or _wants("calendar_create_booking")):

        @tool("calendar_get_availability", return_direct=False)
        async def calendar_get_availability(date: str = "", timezone: str = "") -> Dict[str, Any]:
            """Get available slots from the calendar system."""
            headers = {"Content-Type": "application/json"}
            if settings.calendar_api_key:
                headers["X-Api-Key"] = settings.calendar_api_key
            payload = {"date": date, "timezone": timezone}
            url = f"{settings.calendar_api_url.rstrip('/')}/availability"
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code >= 400:
                    raise RuntimeError(f"calendar_get_availability failed: {resp.status_code}")
                return _safe_json(resp)

        @tool("calendar_create_booking", return_direct=False)
        async def calendar_create_booking(
            name: str,
            email: str,
            phone: str,
            start_time: str,
            notes: str = "",
        ) -> Dict[str, Any]:
            """Create a booking in the calendar system."""
            headers = {"Content-Type": "application/json"}
            if settings.calendar_api_key:
                headers["X-Api-Key"] = settings.calendar_api_key
            payload = {
                "name": name,
                "email": email,
                "phone": phone,
                "start_time": start_time,
                "notes": notes,
            }
            url = f"{settings.calendar_api_url.rstrip('/')}/bookings"
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code >= 400:
                    raise RuntimeError(f"calendar_create_booking failed: {resp.status_code}")
                return _safe_json(resp)

        tools.extend([calendar_get_availability, calendar_create_booking])

    if open_api_enabled and (_wants("open_api") or _wants("open_api_request")):

        @tool("open_api_request", return_direct=False)
        async def open_api_request(
            method: str,
            path: str,
            query: str = "",
            body: str = "",
        ) -> Dict[str, Any]:
            """Call an open API endpoint. Use method GET/POST/PUT/DELETE and path without base URL."""
            headers = {"Content-Type": "application/json"}
            if settings.open_api_key:
                headers["Authorization"] = f"Bearer {settings.open_api_key}"
            url = f"{settings.open_api_base_url.rstrip('/')}/{path.lstrip('/')}"
            json_body = None
            if body:
                try:
                    json_body = json.loads(body)
                except json.JSONDecodeError:
                    json_body = {"raw": body}
            params = None
            if query:
                params = {"q": query}
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.request(method.upper(), url, params=params, json=json_body, headers=headers)
                if resp.status_code >= 400:
                    raise RuntimeError(f"open_api_request failed: {resp.status_code}")
                return _safe_json(resp)

        tools.append(open_api_request)

    return tools
