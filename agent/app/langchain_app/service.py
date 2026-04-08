import asyncio
import hashlib
import logging
import os
import re
import subprocess
import tempfile
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import anyio
from fastapi import HTTPException
from openai import OpenAI

from .chains import build_chain_with_history, build_runnable, build_system_prompt
from .config import Settings
from .dedup import TTLSet
from .documents import load_documents_from_dir
from .examples import load_examples
from .memory import build_message_history_factory
from .profiles import ProfilesData, get_profile, resolve_profile_id_from_label
from .retrieval import retrieve_context
from .state import ProfileStateStore
from .tools import build_tools
from .waha import WahaClient


logger = logging.getLogger("langchain_app")


class WebhookService:
    def __init__(self, settings: Settings, profiles: ProfilesData) -> None:
        self.settings = settings
        self.profiles = profiles
        self.state_store = ProfileStateStore(settings.profile_state_db)
        self.waha = WahaClient(settings.waha_base_url, settings.waha_api_key, settings.waha_session)
        self._recent_events = TTLSet()
        self._recent_messages = TTLSet()
        self._poll_throttle = TTLSet()
        self._history_factory = build_message_history_factory(settings)
        self._openai_client = OpenAI(api_key=settings.openai_api_key) if settings.openai_api_key else None
        self._docs_cache: Dict[str, list] = {}

    async def handle_event(self, data: Dict[str, Any]) -> Dict[str, Any]:
        event = data.get("event")
        if self.settings.log_webhook_payloads:
            logger.info("Webhook payload (%s): %s", event, data)

        if event in ("poll.vote", "poll.vote.failed"):
            return await self._handle_poll_vote(data)
        if event not in ("message", "message.any", "message.new"):
            return {"ok": True, "ignored": "event"}

        payload = data.get("payload") or {}
        event_id = self._extract_event_id(data, payload)
        if self._recent_events.seen(event_id, self.settings.recent_event_ttl_seconds):
            return {"ok": True, "ignored": "duplicate_event"}

        msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
        if msg_type in ("poll_vote", "pollvote", "poll_vote_event"):
            return await self._handle_poll_vote(data)
        if payload.get("poll") and (payload.get("vote") or payload.get("pollVote")):
            return await self._handle_poll_vote(data)
        if payload.get("fromMe"):
            return {"ok": True, "ignored": "fromMe"}

        chat_id = payload.get("from")
        if not chat_id:
            return {"ok": False, "error": "missing chat_id"}

        message_id = self._extract_message_id(payload)
        if message_id:
            message_key = f"{chat_id}:{message_id}"
            if self._recent_messages.seen(message_key, self.settings.recent_event_ttl_seconds):
                return {"ok": True, "ignored": "duplicate_message"}
        else:
            fingerprint = self._message_fingerprint(event, payload)
            if self._recent_messages.seen(fingerprint, self.settings.recent_event_ttl_seconds):
                return {"ok": True, "ignored": "duplicate_message_fallback"}

        if (not self.settings.allow_groups) and str(chat_id).endswith("@g.us"):
            return {"ok": True, "ignored": "group"}

        if self._is_non_text_media(payload):
            reply = "Consigo acessar apenas mensagens de texto e audio. Envie em texto ou audio."
            await self._send_text_parts(chat_id, reply)
            return {"ok": True, "ignored": "non_text_media"}

        is_audio = self._is_audio_payload(payload)
        body = (payload.get("body") or "").strip()
        if is_audio:
            media_url = self._extract_media_url(payload)
            if not media_url:
                reply = "Consigo ouvir audios, mas nao consegui acessar esse. Pode reenviar?"
                await self._send_text_parts(chat_id, reply)
                return {"ok": True, "ignored": "missing_audio_url"}
            transcription = await self._transcribe_audio(media_url, payload)
            if not transcription:
                reply = "Nao consegui transcrever o audio. Pode reenviar ou mandar em texto?"
                await self._send_text_parts(chat_id, reply)
                return {"ok": True, "ignored": "transcription_failed"}
            body = transcription

        if not body:
            return {"ok": True, "ignored": "empty"}

        return await self._handle_message(chat_id, body, payload, is_audio)

    async def _handle_message(
        self, chat_id: str, body: str, payload: Dict[str, Any], is_audio: bool
    ) -> Dict[str, Any]:
        profile_id: Optional[str] = None

        if self.settings.profile_routing_enabled:
            if self._wants_profile_switch(body):
                self.state_store.clear_state(chat_id)
                poll_id = await self.waha.send_poll(
                    chat_id,
                    self.profiles.poll_name,
                    [p.label for p in self.profiles.profiles if p.label],
                )
                self.state_store.update_state(chat_id, poll_id=poll_id, pending_message=None)
                return {"ok": True, "profile_switch": True}

            state = self.state_store.get_state(chat_id)
            profile_id = state.get("profile_id")
            if not profile_id:
                if state.get("poll_id"):
                    self.state_store.update_state(chat_id, pending_message=body)
                    await self._send_text_parts(
                        chat_id,
                        "Para continuar, escolha um perfil na enquete acima, por favor.",
                    )
                    return {"ok": True, "awaiting_poll": True}

                if self._poll_throttle.seen(chat_id, self.settings.poll_throttle_seconds):
                    self.state_store.update_state(chat_id, pending_message=body)
                    await self._send_text_parts(
                        chat_id,
                        "Ja enviei a enquete acima. Escolha um perfil para continuarmos.",
                    )
                    return {"ok": True, "poll_throttled": True}

                poll_id = await self.waha.send_poll(
                    chat_id,
                    self.profiles.poll_name,
                    [p.label for p in self.profiles.profiles if p.label],
                )
                self.state_store.update_state(chat_id, poll_id=poll_id, pending_message=body)
                if poll_id:
                    return {"ok": True, "poll_sent": True}

                profile_id = self.profiles.default_id
                self.state_store.update_state(chat_id, profile_id=profile_id, poll_id=None, pending_message=None)
        else:
            profile_id = self.profiles.default_id

        if not self.settings.openai_api_key:
            raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

        profile = get_profile(self.profiles, profile_id)
        reply = await self._run_chain(chat_id, body, profile)
        reply = self._truncate(reply)

        if not reply:
            reply = "Desculpe, nao consegui responder agora."

        await self._send_text_parts(chat_id, reply)
        return {"ok": True, "answered": True, "profile": profile.id, "audio": is_audio}

    async def _handle_poll_vote(self, data: Dict[str, Any]) -> Dict[str, Any]:
        payload = data.get("payload") or {}
        poll = payload.get("poll") or data.get("poll") or {}
        vote = payload.get("vote") or payload.get("pollVote") or data.get("vote") or {}
        chat_id = vote.get("from") or payload.get("from") or payload.get("chatId")
        if not chat_id:
            return {"ok": True, "ignored": "missing_chat_id"}

        selected = (
            vote.get("selectedOptions")
            or vote.get("options")
            or vote.get("selectedOption")
            or vote.get("selectedOptionIds")
            or payload.get("selectedOptionIds")
            or payload.get("selectedOptionsIds")
            or []
        )
        normalized = self._normalize_selected_options(selected)
        if not normalized:
            await self._send_text_parts(
                str(chat_id),
                "Desculpe, nao consegui entender sua escolha. Vou reenviar a enquete.",
            )
            new_poll_id = await self.waha.send_poll(
                str(chat_id),
                self.profiles.poll_name,
                [p.label for p in self.profiles.profiles if p.label],
            )
            self.state_store.update_state(str(chat_id), poll_id=new_poll_id)
            return {"ok": True, "poll_vote_failed": True}

        profile_id = self._resolve_profile_id_from_vote(normalized)
        if not profile_id:
            await self._send_text_parts(str(chat_id), "Nao consegui identificar o perfil selecionado.")
            new_poll_id = await self.waha.send_poll(
                str(chat_id),
                self.profiles.poll_name,
                [p.label for p in self.profiles.profiles if p.label],
            )
            self.state_store.update_state(str(chat_id), poll_id=new_poll_id)
            return {"ok": True, "profile_missing": True}

        state = self.state_store.get_state(str(chat_id))
        pending_message = (state.get("pending_message") or "").strip()
        self.state_store.update_state(str(chat_id), profile_id=profile_id, poll_id=None, pending_message=None)

        contact_name = await self.waha.get_contact_name(str(chat_id))
        first_name = self._first_name(contact_name or "")
        greeting = self._build_greeting(first_name, profile_id)

        if pending_message:
            reply = await self._run_chain(str(chat_id), pending_message, get_profile(self.profiles, profile_id))
            reply = self._truncate(reply)
            if reply:
                combined = f"{greeting}\n\n{reply}"
            else:
                combined = greeting
            await self._send_text_parts(str(chat_id), combined)
            return {"ok": True, "profile_selected": profile_id, "handled_pending": True}

        await self._send_text_parts(str(chat_id), greeting)
        return {"ok": True, "profile_selected": profile_id, "handled_pending": False}

    async def _run_chain(self, chat_id: str, user_input: str, profile) -> str:
        examples = load_examples(profile.examples_dir)
        docs = self._get_docs(profile.docs_dir)
        context = retrieve_context(docs, user_input)
        include_context = bool(context)

        system_prompt = build_system_prompt(self.settings, profile, examples, include_context)
        tools = build_tools(self.settings, profile)
        runnable = build_runnable(self.settings, profile, system_prompt, tools, include_context)
        chain = build_chain_with_history(runnable, self._history_factory)

        payload = {"input": user_input, "context": context}
        config = {"configurable": {"session_id": chat_id}}
        if hasattr(chain, "ainvoke"):
            result = await chain.ainvoke(payload, config=config)
        else:
            result = await anyio.to_thread.run_sync(chain.invoke, payload, config)
        return self._extract_text(result)

    def _get_docs(self, docs_dir: str) -> list:
        if not docs_dir:
            return []
        cached = self._docs_cache.get(docs_dir)
        if cached is not None:
            return cached
        docs = load_documents_from_dir(docs_dir)
        self._docs_cache[docs_dir] = docs
        return docs

    def _truncate(self, text: str) -> str:
        if not text:
            return ""
        if self.settings.max_reply_chars <= 0:
            return text
        if len(text) <= self.settings.max_reply_chars:
            return text
        return text[: self.settings.max_reply_chars].rstrip()

    def _extract_text(self, result: Any) -> str:
        if result is None:
            return ""
        if isinstance(result, str):
            return result.strip()
        if isinstance(result, dict):
            for key in ("output", "text", "result"):
                value = result.get(key)
                if value:
                    return str(value).strip()
        content = getattr(result, "content", None)
        if content:
            return str(content).strip()
        return str(result).strip()

    async def _send_text_parts(self, chat_id: str, text: str) -> None:
        parts = self._split_messages(text)
        delay = self.settings.message_delay_ms / 1000.0 if self.settings.message_delay_ms > 0 else 0
        for idx, part in enumerate(parts):
            if idx > 0 and delay:
                await asyncio.sleep(delay)
            await self.waha.send_text(chat_id, part)

    def _split_messages(self, text: str) -> list[str]:
        if not text:
            return []
        parts = [part.strip() for part in text.split("\n\n")]
        return [part for part in parts if part]

    def _extract_event_id(self, data: Dict[str, Any], payload: Dict[str, Any]) -> str:
        return (
            str(data.get("id") or "")
            or str(data.get("eventId") or "")
            or str(payload.get("id") or "")
            or str(payload.get("messageId") or "")
            or str(payload.get("_id") or "")
        )

    def _extract_message_id(self, payload: Dict[str, Any]) -> str:
        return str(payload.get("messageId") or payload.get("id") or payload.get("_id") or "")

    def _message_fingerprint(self, event: str, payload: Dict[str, Any]) -> str:
        base = {
            "event": event,
            "from": payload.get("from"),
            "body": payload.get("body"),
            "timestamp": payload.get("timestamp") or payload.get("t"),
        }
        digest = hashlib.sha1(str(base).encode("utf-8")).hexdigest()
        return f"fp:{digest}"

    def _normalize_selected_options(self, selected: Any) -> list[str]:
        if selected is None:
            return []
        if isinstance(selected, dict):
            selected = [selected]
        if isinstance(selected, str):
            selected = [selected]
        if not isinstance(selected, list):
            return []
        options = []
        for item in selected:
            if isinstance(item, dict):
                value = (
                    item.get("name")
                    or item.get("optionName")
                    or item.get("title")
                    or item.get("value")
                )
            else:
                value = str(item)
            if value:
                options.append(str(value).strip())
        return [opt for opt in options if opt]

    def _resolve_profile_id_from_vote(self, options: list[str]) -> Optional[str]:
        for opt in options:
            profile_id = resolve_profile_id_from_label(self.profiles, opt)
            if profile_id:
                return profile_id
        return None

    def _first_name(self, name: str) -> str:
        if not name:
            return ""
        return name.strip().split()[0]

    def _build_greeting(self, first_name: str, profile_id: Optional[str]) -> str:
        profile = get_profile(self.profiles, profile_id)
        greeting_name = profile.greeting_name or profile.label or "Assistente"
        if first_name:
            return (
                f"Oii, tudo bem? Sou a assistente da {greeting_name}.\n"
                f"Como posso te ajudar hoje, {first_name}?"
            )
        return f"Oii, tudo bem? Sou a assistente da {greeting_name}.\nComo posso te ajudar hoje?"

    def _wants_profile_switch(self, text: str) -> bool:
        normalized = text.lower().strip()
        return any(word in normalized for word in ("trocar perfil", "mudar perfil", "outro perfil"))

    def _is_audio_payload(self, payload: Dict[str, Any]) -> bool:
        msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
        if msg_type in ("audio", "voice", "ptt", "voicenote", "voice_note"):
            return True
        mimetype = (payload.get("mimetype") or payload.get("mimeType") or "").lower()
        if mimetype.startswith("audio/"):
            return True
        media = payload.get("media")
        if isinstance(media, dict):
            media_type = (media.get("type") or "").lower()
            if media_type in ("audio", "voice", "ptt", "voicenote", "voice_note"):
                return True
            media_mime = (media.get("mimetype") or media.get("mimeType") or "").lower()
            if media_mime.startswith("audio/"):
                return True
        return False

    def _is_non_text_media(self, payload: Dict[str, Any]) -> bool:
        if self._is_audio_payload(payload):
            return False
        msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
        if msg_type and msg_type not in ("chat", "text", "conversation"):
            return True
        if payload.get("hasMedia") is True:
            return True
        if payload.get("media"):
            return True
        if payload.get("mimetype") or payload.get("mimeType"):
            return True
        if payload.get("mediaUrl") or payload.get("fileUrl") or payload.get("downloadUrl"):
            return True
        return False

    def _extract_media_url(self, payload: Dict[str, Any]) -> Optional[str]:
        for key in ("mediaUrl", "fileUrl", "downloadUrl", "url"):
            value = (payload.get(key) or "").strip()
            if value:
                return value
        media = payload.get("media")
        if isinstance(media, dict):
            for key in ("mediaUrl", "fileUrl", "downloadUrl", "url"):
                value = (media.get(key) or "").strip()
                if value:
                    return value
        return None

    def _extract_mimetype(self, payload: Dict[str, Any]) -> str:
        mimetype = (payload.get("mimetype") or payload.get("mimeType") or "").lower()
        if not mimetype and isinstance(payload.get("media"), dict):
            media = payload.get("media") or {}
            mimetype = (media.get("mimetype") or media.get("mimeType") or "").lower()
        return mimetype.split(";", 1)[0].strip()

    def _guess_audio_filename(self, payload: Dict[str, Any], media_url: Optional[str] = None) -> str:
        mimetype = self._extract_mimetype(payload)
        if mimetype in ("audio/ogg", "audio/opus"):
            return "audio.ogg"
        if mimetype == "audio/mpeg":
            return "audio.mp3"
        if mimetype in ("audio/mp4", "audio/m4a"):
            return "audio.m4a"
        if media_url:
            path = urlparse(media_url).path.lower()
            if path.endswith(".oga"):
                return "audio.oga"
            if path.endswith(".ogg"):
                return "audio.ogg"
            if path.endswith(".mp3"):
                return "audio.mp3"
            if path.endswith(".m4a"):
                return "audio.m4a"
            if path.endswith(".wav"):
                return "audio.wav"
        return "audio"

    def _should_convert_to_wav(self, payload: Dict[str, Any], media_url: Optional[str]) -> bool:
        mimetype = self._extract_mimetype(payload)
        if mimetype in ("audio/ogg", "audio/opus"):
            return True
        if media_url:
            path = urlparse(media_url).path.lower()
            if path.endswith(".oga") or path.endswith(".ogg"):
                return True
        return False

    def _convert_ogg_to_wav_bytes(self, audio_bytes: bytes, input_name: str) -> Optional[bytes]:
        if not audio_bytes:
            return None
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                input_path = os.path.join(tmpdir, input_name)
                output_path = os.path.join(tmpdir, "output.wav")
                with open(input_path, "wb") as handle:
                    handle.write(audio_bytes)
                subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-i",
                        input_path,
                        "-ac",
                        "1",
                        "-ar",
                        "16000",
                        output_path,
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=True,
                )
                with open(output_path, "rb") as handle:
                    return handle.read()
        except Exception as exc:
            logger.exception("Audio conversion failed: %s", exc)
            return None

    async def _transcribe_audio(self, url: str, payload: Dict[str, Any]) -> Optional[str]:
        if not self._openai_client:
            return None
        audio_bytes = await self.waha.download_media(url)
        if not audio_bytes:
            return None

        filename = self._guess_audio_filename(payload, url)
        if self._should_convert_to_wav(payload, url):
            wav_bytes = await anyio.to_thread.run_sync(
                self._convert_ogg_to_wav_bytes, audio_bytes, filename
            )
            if not wav_bytes:
                return None
            audio_bytes = wav_bytes
            filename = "audio.wav"

        def _call_openai() -> Optional[str]:
            result = self._openai_client.audio.transcriptions.create(
                model=self.settings.openai_transcribe_model,
                file=(filename, audio_bytes),
                language=self.settings.openai_transcribe_language or None,
            )
            text = (getattr(result, "text", "") or "").strip()
            return text or None

        try:
            return await anyio.to_thread.run_sync(_call_openai)
        except Exception as exc:
            logger.exception("Audio transcription failed: %s", exc)
            return None
