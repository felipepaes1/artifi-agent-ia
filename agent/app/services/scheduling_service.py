import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import os
from typing import Any, Dict, Optional

try:
    from agents import function_tool
except Exception:
    function_tool = None

try:
    from fastmcp import Client as FastMCPClient
except Exception:
    FastMCPClient = None

from ..booking_flow import BookingFlow, build_prebooking_message, build_proof_received_message
from ..core.profiles import PROFILE_DEFAULT_ID, PROFILE_FLOWS
from ..core.state import get_schedule_option_details, get_schedule_options, store_schedule_options
from ..utils.text import normalize_text


CURRENT_CHAT_ID = None
CURRENT_PROFILE_ID = None
resolve_profile_for_chat = None
is_ariane_profile = None

DEFAULT_SCHEDULING_MCP_URL = "http://mcp:8001/mcp/"
DEFAULT_SCHEDULING_TIMEZONE = "America/Sao_Paulo"
MCP_SCHEDULING_PROVIDER = "mcp_google_calendar"
WEEKDAY_ALIASES = {
    "segunda": 0,
    "segunda-feira": 0,
    "terca": 1,
    "terca-feira": 1,
    "terça": 1,
    "terça-feira": 1,
    "quarta": 2,
    "quarta-feira": 2,
    "quinta": 3,
    "quinta-feira": 3,
    "sexta": 4,
    "sexta-feira": 4,
    "sabado": 5,
    "sábado": 5,
    "domingo": 6,
}


def configure_runtime(*, chat_context, profile_context, profile_resolver, ariane_matcher) -> None:
    global CURRENT_CHAT_ID, CURRENT_PROFILE_ID, resolve_profile_for_chat, is_ariane_profile
    CURRENT_CHAT_ID = chat_context
    CURRENT_PROFILE_ID = profile_context
    resolve_profile_for_chat = profile_resolver
    is_ariane_profile = ariane_matcher


def resolve_flow_profile_id(
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> str:
    if force_ariane:
        return "ariane"
    if profile_id:
        return profile_id
    if chat_id and resolve_profile_for_chat is not None:
        resolved = resolve_profile_for_chat(chat_id)
        if resolved:
            return resolved
    if is_ariane_profile is not None and is_ariane_profile(profile_id, chat_id):
        return "ariane"
    return PROFILE_DEFAULT_ID or ""


def get_booking_flow(
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> Optional[BookingFlow]:
    flow_profile_id = resolve_flow_profile_id(profile_id, chat_id, force_ariane=force_ariane)
    if not flow_profile_id:
        return None
    return PROFILE_FLOWS.get(flow_profile_id)


def uses_mcp_scheduling(
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> bool:
    flow = get_booking_flow(profile_id, chat_id, force_ariane=force_ariane)
    if flow is None:
        return False
    return flow.schedule_provider == MCP_SCHEDULING_PROVIDER


def extract_day_time(text: str) -> Optional[str]:
    if not text:
        return None
    lowered = normalize_text(text)
    day_tokens = {
        "segunda": "Segunda",
        "terca": "Terca",
        "quarta": "Quarta",
        "quinta": "Quinta",
        "sexta": "Sexta",
        "sabado": "Sabado",
        "domingo": "Domingo",
    }
    day = None
    for token, label in day_tokens.items():
        if token in lowered:
            day = label
            break
    if not day:
        return None
    time_match = None
    for candidate in lowered.split():
        cleaned = candidate.strip(".,;!?")
        if cleaned.endswith("h") and cleaned[:-1].isdigit():
            time_match = f"{cleaned[:-1]}:00"
            break
        if cleaned.count(":") == 1 and cleaned.replace(":", "").isdigit():
            hours, minutes = cleaned.split(":")
            if len(minutes) == 1:
                minutes = f"{minutes}0"
            time_match = f"{hours}:{minutes}"
            break
        if "h" in cleaned and cleaned.replace("h", "").isdigit():
            hours, minutes = cleaned.split("h", 1)
            if not minutes:
                minutes = "00"
            elif len(minutes) == 1:
                minutes = f"{minutes}0"
            time_match = f"{hours}:{minutes}"
            break
    if not time_match:
        for token in lowered.split():
            cleaned = token.strip(".,;!?")
            if cleaned.isdigit():
                time_match = f"{cleaned}:00"
                break
    if not time_match:
        return None
    return f"{day} {time_match}"


def try_match_schedule_option(chat_id: str, text: str) -> Optional[str]:
    options = get_schedule_options(chat_id)
    if not options:
        return None
    normalized = normalize_text(text)
    for option in options:
        if normalize_text(option) in normalized:
            return option
    extracted = extract_day_time(text)
    if extracted:
        for option in options:
            if normalize_text(option) == normalize_text(extracted):
                return option
    for weekday in ("segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"):
        if weekday not in normalized:
            continue
        matches = [option for option in options if weekday in normalize_text(option)]
        if len(matches) == 1:
            return matches[0]
    return None


def build_schedule_confirmation(
    option: str,
    user_text: str,
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> str:
    flow = get_booking_flow(profile_id, chat_id, force_ariane=force_ariane)
    if flow is not None:
        return build_prebooking_message(flow, option)

    response = f"Perfeito! Vou reservar para {option}."
    lowered = normalize_text(user_text)
    if any(token in lowered for token in ("demora", "quanto tempo", "tempo leva", "demor", "duracao")):
        response += " Sobre o tempo, isso pode variar conforme o caso e explicamos melhor na avaliação."
    return response


def build_signal_received_confirmation(
    option: Optional[str],
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> str:
    flow = get_booking_flow(profile_id, chat_id, force_ariane=force_ariane)
    if flow is not None and flow.requires_deposit:
        return build_proof_received_message(flow, option or "")
    if option:
        return f"Perfeito! Horario confirmado na agenda ({option})."
    return "Perfeito! Horario confirmado na agenda."


def is_schedule_check_message(text: str) -> bool:
    if not text:
        return False
    lowered = normalize_text(text)
    return any(
        phrase in lowered
        for phrase in (
            "vou verificar os horários",
            "vou ver os horários",
            "vou consultar a agenda",
            "consultar a agenda",
            "um momento, por favor",
            "so um instante",
        )
    )


def parse_schedule_preference(text: str) -> Optional[str]:
    if not text:
        return None
    lowered = normalize_text(text)
    if "manha" in lowered:
        return "morning"
    if "tarde" in lowered:
        return "afternoon"
    if "noite" in lowered:
        return "evening"
    return None


def weekday_pt_br(value: date) -> str:
    names = [
        "Segunda",
        "Terca",
        "Quarta",
        "Quinta",
        "Sexta",
        "Sabado",
        "Domingo",
    ]
    return names[value.weekday()]


def parse_weekday_preference(text: str) -> Optional[int]:
    lowered = normalize_text(text or "")
    for token, weekday in WEEKDAY_ALIASES.items():
        if token in lowered:
            return weekday
    return None


def resolve_scheduling_timezone(raw_timezone: str) -> str:
    cleaned = str(raw_timezone or "").strip() or DEFAULT_SCHEDULING_TIMEZONE
    try:
        ZoneInfo(cleaned)
    except Exception:
        return DEFAULT_SCHEDULING_TIMEZONE
    return cleaned


def combine_iso(day_value: date, hhmm: str, timezone_name: str) -> str:
    hours_text, minutes_text = hhmm.split(":", 1)
    combined = datetime(
        year=day_value.year,
        month=day_value.month,
        day=day_value.day,
        hour=int(hours_text),
        minute=int(minutes_text),
        tzinfo=ZoneInfo(timezone_name),
    )
    return combined.isoformat()


def build_candidate_days(preferred_weekday: Optional[int], limit: int = 5) -> list[date]:
    start = date.today() + timedelta(days=1)
    if preferred_weekday is None:
        return next_business_days(start, limit)

    days: list[date] = []
    cursor = start
    max_days_to_check = 28
    while len(days) < limit and max_days_to_check > 0:
        if cursor.weekday() == preferred_weekday:
            days.append(cursor)
        cursor += timedelta(days=1)
        max_days_to_check -= 1
    return days


def format_slot_option(start_datetime: str, timezone_name: str) -> str:
    slot_dt = datetime.fromisoformat(start_datetime)
    localized = slot_dt.astimezone(ZoneInfo(timezone_name))
    return f"{weekday_pt_br(localized.date())} {localized.strftime('%H:%M')}"


def build_slot_periods(preference: Optional[str]) -> list[tuple[str, str]]:
    if preference == "morning":
        return [("09:00", "12:00")]
    if preference == "afternoon":
        return [("13:30", "18:30")]
    if preference == "evening":
        return [("18:00", "18:30")]
    return [("09:00", "12:00"), ("13:30", "18:30")]


def coerce_mcp_payload(result: Any) -> Dict[str, Any]:
    if isinstance(result, dict):
        return result
    data = getattr(result, "data", None)
    if isinstance(data, dict):
        return data
    structured_content = getattr(result, "structured_content", None)
    if isinstance(structured_content, dict):
        return structured_content
    return {}


async def call_mcp_tool(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    if FastMCPClient is None:
        return {"error": {"message": "fastmcp_client_not_available"}}
    base_url = os.getenv("AGENT_SCHEDULING_MCP_URL", DEFAULT_SCHEDULING_MCP_URL).strip() or DEFAULT_SCHEDULING_MCP_URL
    try:
        async with FastMCPClient(base_url) as client:
            result = await client.call_tool(tool_name, arguments)
    except Exception as exc:
        return {"error": {"message": str(exc) or "mcp_tool_call_failed"}}
    return coerce_mcp_payload(result)


async def fetch_mcp_schedule_options(
    *,
    profile_id: Optional[str],
    chat_id: str,
    timezone: str,
    preference_text: str,
    force_ariane: bool = False,
) -> Dict[str, Any]:
    flow = get_booking_flow(profile_id, chat_id, force_ariane=force_ariane)
    if flow is None:
        return {"status": "error", "message": "booking_flow_not_found"}

    preference = parse_schedule_preference(preference_text)
    preferred_weekday = parse_weekday_preference(preference_text)
    timezone_name = resolve_scheduling_timezone(timezone)
    candidate_days = build_candidate_days(preferred_weekday)
    periods = build_slot_periods(preference)
    options: list[str] = []
    details: Dict[str, Dict[str, Any]] = {}

    for current_day in candidate_days:
        for start_hhmm, end_hhmm in periods:
            payload = await call_mcp_tool(
                "suggest_slots",
                {
                    "window_start": combine_iso(current_day, start_hhmm, timezone_name),
                    "window_end": combine_iso(current_day, end_hhmm, timezone_name),
                    "slot_duration_minutes": flow.slot_duration_minutes,
                    "timezone": timezone_name,
                    "calendar_id": flow.calendar_id or None,
                    "working_hours": {
                        "start_time": start_hhmm,
                        "end_time": end_hhmm,
                        "weekdays": [current_day.weekday()],
                    },
                },
            )
            if payload.get("error"):
                return {
                    "status": "error",
                    "message": "mcp_suggest_slots_failed",
                    "details": payload.get("error"),
                }
            for slot in payload.get("suggested_slots") or []:
                start_datetime = str(slot.get("start_datetime") or "").strip()
                end_datetime = str(slot.get("end_datetime") or "").strip()
                if not start_datetime or not end_datetime:
                    continue
                option = format_slot_option(start_datetime, timezone_name)
                if option in details:
                    continue
                details[option] = {
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                    "timezone": timezone_name,
                    "calendar_id": flow.calendar_id or "",
                }
                options.append(option)
                if len(options) >= 3:
                    return {
                        "status": "ok",
                        "task": "get_horarios",
                        "options": options,
                        "timezone": timezone_name,
                        "details": details,
                    }

    return {
        "status": "ok",
        "task": "get_horarios",
        "options": options,
        "timezone": timezone_name,
        "details": details,
    }


def render_event_title(flow: BookingFlow, slot: str) -> str:
    template = (flow.event_title_template or "Agendamento via WhatsApp").strip()
    try:
        rendered = template.format(slot=slot)
    except Exception:
        rendered = template
    return rendered or "Agendamento via WhatsApp"


async def confirm_mcp_schedule_option(
    option: str,
    *,
    profile_id: Optional[str],
    chat_id: str,
    phone: str = "",
    notes: str = "",
    force_ariane: bool = False,
) -> Dict[str, Any]:
    flow = get_booking_flow(profile_id, chat_id, force_ariane=force_ariane)
    if flow is None:
        return {"status": "error", "message": "booking_flow_not_found", "option": option}

    details = get_schedule_option_details(chat_id, option)
    start_datetime = str(details.get("start_datetime") or "").strip()
    end_datetime = str(details.get("end_datetime") or "").strip()
    timezone_name = resolve_scheduling_timezone(str(details.get("timezone") or ""))
    if not start_datetime or not end_datetime:
        return {"status": "error", "message": "schedule_option_not_found", "option": option}

    description_lines = [
        "Origem: WhatsApp",
        f"Perfil: {resolve_flow_profile_id(profile_id, chat_id, force_ariane=force_ariane)}",
        f"Slot selecionado: {option}",
    ]
    if phone:
        description_lines.append(f"Telefone: {phone}")
    elif chat_id:
        description_lines.append(f"Chat ID: {chat_id}")
    if notes:
        description_lines.append(f"Notas: {notes}")

    payload = await call_mcp_tool(
        "create_event",
        {
            "title": render_event_title(flow, option),
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "timezone": timezone_name,
            "description": "\n".join(description_lines),
            "location": flow.event_location or None,
            "calendar_id": flow.calendar_id or None,
            "attendees": [],
            "allow_conflicts": False,
        },
    )
    if payload.get("error"):
        return {
            "status": "error",
            "message": "mcp_create_event_failed",
            "option": option,
            "details": payload.get("error"),
        }

    return {
        "status": "confirmed",
        "task": "make_call_meeting",
        "option": option,
        "event_id": payload.get("event_id"),
        "html_link": payload.get("html_link"),
        "summary": payload.get("summary"),
    }


def next_business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    cursor = start
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def fake_schedule_options(preference: Optional[str]) -> list[str]:
    start = date.today() + timedelta(days=1)
    days = next_business_days(start, 3)
    if preference == "morning":
        times = ["09:30", "10:30", "11:30"]
    elif preference == "afternoon":
        times = ["14:00", "15:30", "17:00"]
    elif preference == "evening":
        times = ["18:00", "18:30", "19:00"]
    else:
        times = ["10:00", "15:00", "17:30"]
    return [f"{weekday_pt_br(day)} {time}" for day, time in zip(days, times)]


def should_inject_fake_schedule(reply: str) -> bool:
    if not reply:
        return False
    lowered = normalize_text(reply)
    if any(
        phrase in lowered
        for phrase in (
            "preciso de algumas informacoes",
            "qual e o seu nome",
            "qual o seu nome",
            "nome completo",
            "motivo principal",
            "preferencia de horario",
            "assim que eu tiver",
            "assim que tiver",
            "poderei verificar os horarios",
            "posso verificar os horarios",
            "depois que eu tiver",
        )
    ):
        return False
    if any(
        token in lowered
        for token in (
            "tenho estes horarios",
            "tenho esses horarios",
            "tenho os horarios",
        )
    ):
        return False
    if reply_contains_schedule_options(reply):
        return False
    if any(
        token in lowered
        for token in (
            "horarios disponiveis",
            "consultar a agenda",
            "vou verificar os horarios",
            "vou ver os horarios",
            "um momento, por favor",
            "so um instante",
        )
    ):
        return True
    return False


def reply_contains_schedule_options(reply: str) -> bool:
    if not reply:
        return False
    lowered = normalize_text(reply)
    if any(
        day in lowered
        for day in (
            "segunda",
            "terca",
            "quarta",
            "quinta",
            "sexta",
            "sabado",
            "domingo",
        )
    ):
        if re.search(r"\b\d{1,2}(?::\d{2}|h\d{0,2})\b", lowered):
            return True
    if re.search(r"\b\d{1,2}h(\d{2})?\b", lowered):
        return True
    if re.search(r"\b\d{1,2}:\d{2}\b", lowered):
        return True
    return False


def inject_fake_schedule(chat_id: str, body: str, reply: str, has_scheduling_tool: bool) -> str:
    if has_scheduling_tool:
        return reply
    if not should_inject_fake_schedule(reply):
        return reply
    preference = parse_schedule_preference(body)
    options = fake_schedule_options(preference)
    store_schedule_options(chat_id, options)
    horarios = ", ".join(options)
    suggestion = f"Tenho estes horarios disponiveis nesta semana: {horarios}. Qual prefere?"
    return f"{reply}\n\n{suggestion}"


def looks_like_payment_confirmation(text: str) -> bool:
    if not text:
        return False
    lowered = normalize_text(text)
    return any(
        token in lowered
        for token in (
            "comprovante",
            "pix pago",
            "pix feito",
            "pix enviado",
            "paguei",
            "pagamento",
            "transferi",
            "ja enviei",
            "ja mandei",
            "acabei de enviar",
        )
    )


def build_scheduling_tool():
    if function_tool is None:
        return None

    @function_tool(timeout=20.0)
    async def tool_agente_scheduling(
        task: str,
        preference: str = "",
        day: str = "",
        timezone: str = "",
        phone: str = "",
        notes: str = "",
        option: str = "",
    ) -> Dict[str, Any]:
        normalized_task = (task or "").strip().lower()
        chat_id = CURRENT_CHAT_ID.get("") if CURRENT_CHAT_ID is not None else ""
        profile_id = (CURRENT_PROFILE_ID.get("") if CURRENT_PROFILE_ID is not None else "") or (
            resolve_profile_for_chat(chat_id) if resolve_profile_for_chat is not None else ""
        )
        if not profile_id:
            profile_id = PROFILE_DEFAULT_ID or ""
        preference_text = " ".join(part for part in (preference, day, notes) if part).strip()

        if normalized_task in ("get_horarios", "get_slots", "get_availability"):
            if uses_mcp_scheduling(profile_id, chat_id):
                payload = await fetch_mcp_schedule_options(
                    profile_id=profile_id,
                    chat_id=chat_id,
                    timezone=timezone,
                    preference_text=preference_text,
                )
                if payload.get("status") == "ok":
                    store_schedule_options(
                        chat_id,
                        payload.get("options") or [],
                        payload.get("details") or {},
                    )
                    payload.pop("details", None)
                return payload

            pref = parse_schedule_preference(preference_text or "")
            options = fake_schedule_options(pref)
            if chat_id:
                store_schedule_options(chat_id, options)
            return {
                "status": "ok",
                "task": "get_horarios",
                "options": options,
                "timezone": timezone or "",
            }
        if normalized_task in ("make_call_meeting", "create_booking", "book", "reserve"):
            chosen = option or day
            if uses_mcp_scheduling(profile_id, chat_id):
                return await confirm_mcp_schedule_option(
                    chosen,
                    profile_id=profile_id,
                    chat_id=chat_id,
                    phone=phone,
                    notes=notes,
                )
            return {
                "status": "confirmed",
                "task": "make_call_meeting",
                "option": chosen,
                "phone": phone,
                "notes": notes,
            }
        return {"status": "error", "message": f"Unknown task: {task}"}

    return tool_agente_scheduling
