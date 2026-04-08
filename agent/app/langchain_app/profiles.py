import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .config import Settings


@dataclass(frozen=True)
class Profile:
    id: str
    label: str
    instructions_path: str
    greeting_name: str
    docs_dir: str
    examples_dir: str
    tools: List[str]
    model: Optional[str]
    temperature: Optional[float]
    max_tokens: Optional[int]


@dataclass(frozen=True)
class ProfilesData:
    poll_name: str
    profiles: List[Profile]
    label_to_id: Dict[str, str]
    default_id: str


_DEFAULT_PROFILES_DATA: Dict[str, Any] = {
    "pollName": "Ola. Para testar o atendimento, qual segmento voce prefere?",
    "profiles": [
        {
            "id": "default",
            "label": "Padrao",
            "instructions_path": "assistant_instructions.txt",
            "greeting_name": "Assistente",
        }
    ],
}


def _resolve_path(base_dir: str, value: str) -> str:
    if not value:
        return ""
    if os.path.isabs(value):
        return value
    return os.path.abspath(os.path.join(base_dir, value))


def _load_profiles_json(path: str) -> Dict[str, Any]:
    if not path:
        return _DEFAULT_PROFILES_DATA
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict) and data.get("profiles"):
                return data
    except FileNotFoundError:
        return _DEFAULT_PROFILES_DATA
    except Exception:
        return _DEFAULT_PROFILES_DATA
    return _DEFAULT_PROFILES_DATA


def load_profiles(settings: Settings) -> ProfilesData:
    data = _load_profiles_json(settings.profiles_path)
    poll_name = data.get("pollName") or _DEFAULT_PROFILES_DATA["pollName"]
    profiles: List[Profile] = []

    base_dir = os.path.dirname(settings.profiles_path) or settings.prompts_dir
    for raw in data.get("profiles") or []:
        if not isinstance(raw, dict):
            continue
        profile_id = (raw.get("id") or "").strip()
        label = (raw.get("label") or profile_id or "").strip()
        if not profile_id:
            continue

        instructions_path = (raw.get("instructions_path") or "").strip()
        if not instructions_path:
            instructions_path = settings.instructions_path
        else:
            instructions_path = _resolve_path(base_dir, instructions_path)

        greeting_name = (raw.get("greeting_name") or label or "Assistente").strip()
        docs_dir = _resolve_path(base_dir, (raw.get("docs_dir") or settings.docs_dir or "").strip())
        examples_dir = _resolve_path(
            base_dir,
            (raw.get("examples_dir") or settings.examples_dir or "").strip(),
        )
        tools = raw.get("tools") or []
        if isinstance(tools, str):
            tools = [tools]
        tools = [str(t).strip() for t in tools if str(t).strip()]

        model = (raw.get("model") or "").strip() or None
        temperature = raw.get("temperature")
        max_tokens = raw.get("max_tokens")
        if temperature is not None:
            try:
                temperature = float(temperature)
            except (TypeError, ValueError):
                temperature = None
        if max_tokens is not None:
            try:
                max_tokens = int(max_tokens)
            except (TypeError, ValueError):
                max_tokens = None

        profiles.append(
            Profile(
                id=profile_id,
                label=label,
                instructions_path=instructions_path,
                greeting_name=greeting_name,
                docs_dir=docs_dir,
                examples_dir=examples_dir,
                tools=tools,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        )

    if not profiles:
        fallback = _DEFAULT_PROFILES_DATA["profiles"][0]
        profiles = [
            Profile(
                id=fallback["id"],
                label=fallback["label"],
                instructions_path=_resolve_path(base_dir, fallback["instructions_path"]),
                greeting_name=fallback["greeting_name"],
                docs_dir="",
                examples_dir="",
                tools=[],
                model=None,
                temperature=None,
                max_tokens=None,
            )
        ]

    label_to_id = {
        profile.label.strip().lower(): profile.id
        for profile in profiles
        if profile.label and profile.id
    }
    default_id = profiles[0].id if profiles else ""

    return ProfilesData(
        poll_name=poll_name,
        profiles=profiles,
        label_to_id=label_to_id,
        default_id=default_id,
    )


def get_profile(profiles: ProfilesData, profile_id: Optional[str]) -> Profile:
    if not profile_id:
        return profiles.profiles[0]
    for profile in profiles.profiles:
        if profile.id == profile_id:
            return profile
    return profiles.profiles[0]


def resolve_profile_id_from_label(profiles: ProfilesData, label: str) -> Optional[str]:
    if not label:
        return None
    return profiles.label_to_id.get(label.strip().lower())
