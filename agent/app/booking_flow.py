import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable


@dataclass(frozen=True)
class FlowField:
    id: str
    label: str
    required: bool = True


@dataclass(frozen=True)
class BookingFlow:
    profile_id: str
    flow_id: str
    schedule_provider: str
    slot_duration_minutes: int
    calendar_id: str
    event_title_template: str
    event_location: str
    requires_deposit: bool
    pre_reserve_template: str
    collect_fields: tuple[FlowField, ...]
    prebooking_message_template: str
    deposit_amount_text: str
    deposit_policy_text: str
    pix_key: str
    pix_holder: str
    proof_received_template: str


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "y", "on", "sim", "s"):
            return True
        if lowered in ("0", "false", "no", "n", "off", "nao", "não"):
            return False
    return default


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_template_format(template: str, **kwargs: str) -> str:
    text = _as_text(template)
    if not text:
        return ""
    try:
        return text.format(**kwargs)
    except Exception:
        return text


def _default_flow(profile_id: str) -> BookingFlow:
    return BookingFlow(
        profile_id=profile_id,
        flow_id="default_v1",
        schedule_provider="fake",
        slot_duration_minutes=60,
        calendar_id="",
        event_title_template="Agendamento via WhatsApp",
        event_location="",
        requires_deposit=False,
        pre_reserve_template="Perfeito! Vou reservar para {slot}.",
        collect_fields=(),
        prebooking_message_template="",
        deposit_amount_text="",
        deposit_policy_text="",
        pix_key="",
        pix_holder="",
        proof_received_template=(
            "Perfeito! Horário confirmado na agenda ({slot}).\n"
            "Se precisar de ajuda, fico a disposição por aqui."
        ),
    )


def _resolve_path(base_dir: str, value: str) -> str:
    if not value:
        return ""
    if os.path.isabs(value):
        return value
    return os.path.abspath(os.path.join(base_dir, value))


def _build_collect_fields(raw: Any) -> tuple[FlowField, ...]:
    if not isinstance(raw, list):
        return ()
    fields: list[FlowField] = []
    for item in raw:
        if isinstance(item, str):
            label = item.strip()
            if not label:
                continue
            field_id = label.lower().replace(" ", "_")
            fields.append(FlowField(id=field_id, label=label, required=True))
            continue
        if not isinstance(item, dict):
            continue
        field_id = _as_text(item.get("id") or item.get("name") or item.get("label"))
        label = _as_text(item.get("label") or item.get("name") or field_id)
        if not field_id or not label:
            continue
        required = _coerce_bool(item.get("required"), True)
        fields.append(FlowField(id=field_id, label=label, required=required))
    return tuple(fields)


def _coerce_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def load_profile_flows(
    profiles: Iterable[Dict[str, Any]],
    base_dir: str,
) -> Dict[str, BookingFlow]:
    flows: Dict[str, BookingFlow] = {}
    for raw_profile in profiles:
        if not isinstance(raw_profile, dict):
            continue
        profile_id = _as_text(raw_profile.get("id"))
        if not profile_id:
            continue
        flows[profile_id] = _load_profile_flow(raw_profile, base_dir)
    return flows


def _load_profile_flow(raw_profile: Dict[str, Any], base_dir: str) -> BookingFlow:
    profile_id = _as_text(raw_profile.get("id"))
    fallback = _default_flow(profile_id)

    flow_path = _resolve_path(base_dir, _as_text(raw_profile.get("flow_path")))
    if not flow_path or not os.path.isfile(flow_path):
        return fallback

    try:
        with open(flow_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return fallback

    if not isinstance(payload, dict):
        return fallback

    schedule = payload.get("schedule") if isinstance(payload.get("schedule"), dict) else {}
    booking = payload.get("booking") if isinstance(payload.get("booking"), dict) else {}

    requires_deposit = _coerce_bool(booking.get("requires_deposit"), False)
    pre_reserve_template = _as_text(
        schedule.get("pre_reserve_template") or fallback.pre_reserve_template
    )
    collect_fields = _build_collect_fields(booking.get("collect_fields"))

    return BookingFlow(
        profile_id=profile_id,
        flow_id=_as_text(payload.get("flow_id") or fallback.flow_id),
        schedule_provider=_as_text(schedule.get("provider") or fallback.schedule_provider).lower() or "fake",
        slot_duration_minutes=_coerce_positive_int(
            schedule.get("slot_duration_minutes"),
            fallback.slot_duration_minutes,
        ),
        calendar_id=_as_text(schedule.get("calendar_id")),
        event_title_template=_as_text(
            schedule.get("event_title_template") or fallback.event_title_template
        ),
        event_location=_as_text(schedule.get("event_location") or fallback.event_location),
        requires_deposit=requires_deposit,
        pre_reserve_template=pre_reserve_template,
        collect_fields=collect_fields,
        prebooking_message_template=_as_text(booking.get("prebooking_message_template")),
        deposit_amount_text=_as_text(booking.get("deposit_amount_text")),
        deposit_policy_text=_as_text(booking.get("deposit_policy_text")),
        pix_key=_as_text(booking.get("pix_key")),
        pix_holder=_as_text(booking.get("pix_holder")),
        proof_received_template=_as_text(
            booking.get("proof_received_template") or fallback.proof_received_template
        ),
    )


def build_prebooking_message(flow: BookingFlow, slot: str) -> str:
    slot = _as_text(slot)
    if not slot:
        slot = "horário selecionado"

    if flow.prebooking_message_template:
        text = _safe_template_format(
            flow.prebooking_message_template,
            slot=slot,
            deposit_amount=flow.deposit_amount_text,
            pix_key=flow.pix_key,
            pix_holder=flow.pix_holder,
        )
        return text.strip()

    if not flow.requires_deposit:
        template = flow.pre_reserve_template or _default_flow(flow.profile_id).pre_reserve_template
        return _safe_template_format(template, slot=slot).strip()

    lines: list[str] = [_safe_template_format(flow.pre_reserve_template, slot=slot)]

    if flow.collect_fields:
        lines.append("Antes de finalizar, preciso destes dados:")
        for field in flow.collect_fields:
            lines.append(f"{field.label}:")

    if flow.deposit_amount_text:
        lines.append("")
        lines.append(
            f"Para eu reservar esse horário para você na agenda ({slot}), "
            f"a clínica solicita um sinal de {flow.deposit_amount_text}, "
            "abatido no valor final do atendimento."
        )

    if flow.deposit_policy_text:
        lines.append(flow.deposit_policy_text)

    if flow.pix_key:
        lines.append(f"Pix: {flow.pix_key}")
    if flow.pix_holder:
        lines.append(flow.pix_holder)

    lines.append("")
    lines.append("A partir do envio do comprovante, seu horário fica agendado.")

    return "\n".join(lines).strip()


def build_proof_received_message(flow: BookingFlow, slot: str) -> str:
    slot = _as_text(slot) or "seu horário"
    template = flow.proof_received_template or _default_flow(flow.profile_id).proof_received_template
    text = _safe_template_format(template, slot=slot)
    return text.strip()
