import logging
import os
import re
from typing import Any, Dict, Optional

import httpx
try:
    from agents import function_tool
except Exception:
    function_tool = None
from openai import OpenAI

from ..config.settings import OPENAI_API_KEY
from ..core.profiles import PROFILE_DEFAULT_ID, get_docs_dir_for_profile, get_vector_store_ids
from ..utils.text import normalize_service_text


logger = logging.getLogger("agent")
OPENAI_CLIENT = OpenAI()

CURRENT_CHAT_ID = None
CURRENT_PROFILE_ID = None
resolve_profile_for_chat = None


def configure_runtime(*, chat_context, profile_context, profile_resolver) -> None:
    global CURRENT_CHAT_ID, CURRENT_PROFILE_ID, resolve_profile_for_chat
    CURRENT_CHAT_ID = chat_context
    CURRENT_PROFILE_ID = profile_context
    resolve_profile_for_chat = profile_resolver


def as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        try:
            data = dump()
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            data = to_dict()
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    raw = getattr(value, "__dict__", None)
    if isinstance(raw, dict):
        return dict(raw)
    return {}


def extract_vector_search_items(payload: Any) -> list[Any]:
    if isinstance(payload, dict):
        data = payload.get("data")
        return data if isinstance(data, list) else []
    data = getattr(payload, "data", None)
    if isinstance(data, list):
        return data
    dumped = as_dict(payload)
    data = dumped.get("data")
    return data if isinstance(data, list) else []


def extract_vector_result_text(result: Any) -> str:
    result_dict = as_dict(result)
    content = result_dict.get("content")
    parts: list[str] = []
    if isinstance(content, list):
        for entry in content:
            if isinstance(entry, str):
                text = entry.strip()
            else:
                entry_dict = as_dict(entry)
                text = str(entry_dict.get("text") or entry_dict.get("content") or entry_dict.get("value") or "").strip()
            if text:
                parts.append(text)
    elif isinstance(content, str):
        text = content.strip()
        if text:
            parts.append(text)

    if not parts:
        for key in ("text", "snippet", "chunk"):
            value = result_dict.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
                break
    return "\n".join(parts).strip()


def vector_result_source(result: Dict[str, Any], vector_store_id: str) -> str:
    filename = str(result.get("filename") or result.get("file_name") or "").strip()
    file_id = str(result.get("file_id") or "").strip()
    if filename and file_id:
        return f"{filename} ({file_id})"
    if filename:
        return filename
    if file_id:
        return file_id
    return f"vector_store:{vector_store_id}"


def search_vector_store_sdk(vector_store_id: str, query: str, max_num_results: int) -> Any:
    search_functions: list[Any] = []
    vector_stores_api = getattr(OPENAI_CLIENT, "vector_stores", None)
    if vector_stores_api is not None and hasattr(vector_stores_api, "search"):
        search_functions.append(getattr(vector_stores_api, "search"))
    beta_api = getattr(OPENAI_CLIENT, "beta", None)
    beta_vector_stores_api = getattr(beta_api, "vector_stores", None) if beta_api is not None else None
    if beta_vector_stores_api is not None and hasattr(beta_vector_stores_api, "search"):
        search_functions.append(getattr(beta_vector_stores_api, "search"))
    if not search_functions:
        raise RuntimeError("vector_store_search_not_supported_by_sdk")

    last_error: Optional[Exception] = None
    for search_fn in search_functions:
        try:
            return search_fn(
                vector_store_id=vector_store_id,
                query=query,
                max_num_results=max_num_results,
            )
        except TypeError:
            try:
                return search_fn(
                    vector_store_id,
                    query=query,
                    max_num_results=max_num_results,
                )
            except Exception as exc:
                last_error = exc
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("vector_store_search_failed")


KNOWLEDGE_QUERY_STOPWORDS = {
    "a",
    "ao",
    "aos",
    "as",
    "biovita",
    "com",
    "como",
    "da",
    "das",
    "de",
    "do",
    "dos",
    "e",
    "em",
    "na",
    "nas",
    "no",
    "nos",
    "o",
    "os",
    "ou",
    "para",
    "por",
    "qual",
    "quais",
    "que",
    "se",
    "tem",
    "tenho",
    "voces",
    "voce",
}
KNOWLEDGE_DOMAIN_HINTS = {
    "aceita",
    "aceitam",
    "agendar",
    "agendamento",
    "atende",
    "atendem",
    "cidade",
    "cidades",
    "clinica",
    "clinicas",
    "convenio",
    "convenios",
    "endereco",
    "enderecos",
    "exame",
    "exames",
    "faz",
    "funciona",
    "funcionamento",
    "horario",
    "horarios",
    "laboratorio",
    "plano",
    "planos",
    "preparo",
    "realiza",
    "resultado",
    "resultados",
    "unidade",
    "unidades",
}
KNOWLEDGE_TOKEN_EQUIVALENTS = {
    "especialista": {"especialidade", "profissional", "medico", "doutor", "doutora"},
    "especialistas": {"especialidades", "profissionais", "medicos", "doutores", "doutoras"},
    "gestacao": {"obstetra", "obstetricia", "pre", "natal"},
    "medico": {"profissional", "doutor", "doutora"},
    "medicos": {"profissionais", "doutores", "doutoras"},
    "obstetra": {"obstetricia", "gestacao", "pre", "natal"},
    "obstetricia": {"obstetra", "gestacao", "pre", "natal"},
    "prenatal": {"pre", "natal", "obstetra", "obstetricia", "gestacao"},
    "profissional": {"medico", "doutor", "doutora"},
    "profissionais": {"medicos", "doutores", "doutoras"},
}


def search_vector_store_http(vector_store_id: str, query: str, max_num_results: int) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("openai_api_key_not_set")
    url = f"https://api.openai.com/v1/vector_stores/{vector_store_id}/search"
    payload = {
        "query": query,
        "max_num_results": max_num_results,
    }
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    timeout = httpx.Timeout(20.0, connect=10.0)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
    return data if isinstance(data, dict) else {"data": []}


def knowledge_query_tokens(text: str, *, drop_domain_hints: bool = False) -> list[str]:
    tokens: list[str] = []
    for token in normalize_service_text(text).split():
        if len(token) < 2 or token in KNOWLEDGE_QUERY_STOPWORDS:
            continue
        if drop_domain_hints and token in KNOWLEDGE_DOMAIN_HINTS:
            continue
        tokens.append(token)
    return tokens


def knowledge_canonical_token(token: str) -> str:
    normalized = normalize_service_text(token)
    if not normalized or " " in normalized:
        return normalized
    if len(normalized) <= 3:
        return normalized
    if normalized.endswith(("oes", "aes")) and len(normalized) > 4:
        return f"{normalized[:-3]}ao"
    if normalized.endswith("ais") and len(normalized) > 4:
        return f"{normalized[:-3]}al"
    if normalized.endswith("eis") and len(normalized) > 4:
        return f"{normalized[:-3]}el"
    if normalized.endswith("is") and len(normalized) > 4:
        return f"{normalized[:-2]}il"
    if normalized.endswith("s") and len(normalized) > 4:
        return normalized[:-1]
    return normalized


def knowledge_expand_token(token: str) -> set[str]:
    normalized = normalize_service_text(token)
    if not normalized:
        return set()
    expanded: set[str] = {normalized}
    canonical = knowledge_canonical_token(normalized)
    if canonical:
        expanded.add(canonical)

    pending = list(expanded)
    seen = set(pending)
    while pending:
        current = pending.pop()
        for alias in KNOWLEDGE_TOKEN_EQUIVALENTS.get(current, set()):
            alias_normalized = normalize_service_text(alias)
            if not alias_normalized:
                continue
            alias_tokens = alias_normalized.split()
            for alias_token in alias_tokens or [alias_normalized]:
                canonical_alias = knowledge_canonical_token(alias_token)
                for candidate in (alias_token, canonical_alias):
                    if candidate and candidate not in seen:
                        seen.add(candidate)
                        expanded.add(candidate)
                        pending.append(candidate)
    return expanded


def knowledge_expand_token_set(tokens: list[str]) -> set[str]:
    expanded: set[str] = set()
    for token in tokens:
        expanded.update(knowledge_expand_token(token))
    return expanded


def split_markdown_sections(content: str) -> list[str]:
    normalized = (content or "").replace("\r\n", "\n").strip()
    if not normalized:
        return []
    sections: list[str] = []
    for block in re.split(r"\n(?=#{1,6}\s)", normalized):
        block = block.strip()
        if not block:
            continue
        for chunk in re.split(r"\n-{3,}\n", block):
            piece = chunk.strip()
            if piece:
                sections.append(piece)
    return sections or [normalized]


def compact_knowledge_text(text: str, limit: int = 900) -> str:
    cleaned = re.sub(r"\n{3,}", "\n\n", (text or "").strip())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit].rstrip()}..."


def score_local_knowledge_chunk(query: str, chunk: str, source_name: str) -> float:
    normalized_query = normalize_service_text(query)
    normalized_chunk = normalize_service_text(chunk)
    if not normalized_query or not normalized_chunk:
        return 0.0

    query_tokens = knowledge_query_tokens(query)
    if not query_tokens:
        return 0.0
    specific_tokens = knowledge_query_tokens(query, drop_domain_hints=True)
    source_tokens = normalize_service_text(source_name.replace(".md", ""))
    searchable = f"{source_tokens} {normalized_chunk}".strip()
    searchable_token_set = knowledge_expand_token_set(searchable.split())
    matched_tokens = [
        token for token in query_tokens if knowledge_expand_token(token).intersection(searchable_token_set)
    ]
    matched_specific = [
        token for token in specific_tokens if knowledge_expand_token(token).intersection(searchable_token_set)
    ]
    if specific_tokens and not matched_specific:
        return 0.0

    heading = normalize_service_text((chunk.splitlines()[0] if chunk.splitlines() else ""))
    heading_token_set = knowledge_expand_token_set(heading.split())
    source_token_set = knowledge_expand_token_set(source_tokens.split())
    asks_for_unit_details = any(
        token in {"cidade", "cidades", "endereco", "enderecos", "horario", "horarios", "unidade", "unidades"}
        for token in query_tokens
    )
    asks_for_convenio = any(
        token in {"convenio", "convenios", "plano", "planos"}
        for token in query_tokens
    )
    score = 0.0
    if normalized_query in searchable:
        score += 8.0
    score += len(matched_tokens) * 1.6
    score += len(matched_specific) * 1.8
    if specific_tokens and len(matched_specific) == len(specific_tokens):
        score += 4.0
    elif len(matched_tokens) == len(query_tokens):
        score += 2.5
    if heading and matched_specific and all(
        knowledge_expand_token(token).intersection(heading_token_set) for token in matched_specific
    ):
        score += 2.2
    elif heading and any(
        knowledge_expand_token(token).intersection(heading_token_set) for token in matched_tokens
    ):
        score += 1.0
    if source_tokens and any(
        knowledge_expand_token(token).intersection(source_token_set) for token in matched_tokens
    ):
        score += 0.8
    if asks_for_unit_details and source_token_set.intersection({"unidade", "unidades"}):
        score += 1.4
    if asks_for_convenio and source_token_set.intersection({"convenio", "convenios", "plano", "planos"}):
        score += 1.4
    return round(score, 4) if score >= 2.0 else 0.0


def search_profile_local_docs_knowledge(profile_id: str, query: str, top_k: int) -> Dict[str, Any]:
    docs_dir = get_docs_dir_for_profile(profile_id)
    if not docs_dir:
        return {"status": "unavailable", "profile_id": profile_id, "results": []}
    if not os.path.isdir(docs_dir):
        return {
            "status": "error",
            "profile_id": profile_id,
            "docs_dir": docs_dir,
            "message": "docs_dir_not_found",
            "results": [],
        }

    ranked: list[Dict[str, Any]] = []
    for root, _, filenames in os.walk(docs_dir):
        for filename in sorted(filenames):
            if not filename.lower().endswith(".md"):
                continue
            path = os.path.join(root, filename)
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    content = handle.read()
            except OSError as exc:
                logger.warning("Failed to read docs file %s: %s", path, exc)
                continue
            relative_name = os.path.relpath(path, docs_dir)
            for section in split_markdown_sections(content):
                score = score_local_knowledge_chunk(query, section, relative_name)
                if score <= 0:
                    continue
                ranked.append(
                    {
                        "source": relative_name,
                        "content": compact_knowledge_text(section),
                        "score": score,
                    }
                )

    ranked.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    deduped: list[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in ranked:
        key = (str(item.get("source") or ""), str(item.get("content") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= top_k:
            break

    return {
        "status": "ok",
        "profile_id": profile_id,
        "docs_dir": docs_dir,
        "results": deduped,
    }


def search_profile_vector_knowledge(profile_id: str, query: str, top_k: int) -> Dict[str, Any]:
    vector_store_ids = get_vector_store_ids(profile_id)
    normalized_query = (query or "").strip() or "informacoes gerais da clinica"
    per_store_limit = max(top_k, 3)
    collected: list[Dict[str, Any]] = []
    errors: list[Dict[str, str]] = []
    local_docs_payload = search_profile_local_docs_knowledge(
        profile_id=profile_id,
        query=normalized_query,
        top_k=per_store_limit,
    )

    if not vector_store_ids:
        errors.append(
            {
                "vector_store_id": "",
                "error": "vector_store_not_configured",
            }
        )

    for vector_store_id in vector_store_ids:
        payload: Any = None
        try:
            payload = search_vector_store_sdk(vector_store_id, normalized_query, per_store_limit)
        except Exception as sdk_exc:
            logger.warning(
                "Vector store SDK search failed profile=%s vector_store=%s: %s",
                profile_id,
                vector_store_id,
                sdk_exc,
            )
            try:
                payload = search_vector_store_http(vector_store_id, normalized_query, per_store_limit)
            except Exception as http_exc:
                errors.append(
                    {
                        "vector_store_id": vector_store_id,
                        "error": str(http_exc),
                    }
                )
                continue

        for raw_item in extract_vector_search_items(payload):
            item = as_dict(raw_item)
            content = extract_vector_result_text(raw_item)
            if not content:
                continue
            if len(content) > 1800:
                content = content[:1800]
            score_raw = item.get("score")
            try:
                score = float(score_raw)
            except Exception:
                score = 0.0
            collected.append(
                {
                    "source": vector_result_source(item, vector_store_id),
                    "content": content,
                    "vector_store_id": vector_store_id,
                    "source_type": "vector_store",
                    "_score": score,
                    "_rank": 6.0 + max(min(score, 1.0), 0.0) * 4.0,
                }
            )

    for item in list(local_docs_payload.get("results") or []):
        score_raw = item.get("score")
        try:
            score = float(score_raw)
        except Exception:
            score = 0.0
        collected.append(
            {
                "source": f"docs:{str(item.get('source') or '').strip()}",
                "content": str(item.get("content") or "").strip(),
                "vector_store_id": "",
                "source_type": "local_docs",
                "_score": score,
                "_rank": score,
            }
        )

    collected.sort(
        key=lambda entry: (entry.get("_rank", 0.0), entry.get("_score", 0.0)),
        reverse=True,
    )
    deduped: list[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for entry in collected:
        key = (str(entry.get("source") or ""), str(entry.get("content") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
        if len(deduped) >= top_k:
            break

    results: list[Dict[str, Any]] = []
    for entry in deduped:
        item: Dict[str, Any] = {
            "source": entry["source"],
            "content": entry["content"],
            "source_type": entry.get("source_type") or "vector_store",
        }
        vector_store_id = str(entry.get("vector_store_id") or "").strip()
        if vector_store_id:
            item["vector_store_id"] = vector_store_id
        score = entry.get("_score")
        if isinstance(score, float) and score > 0:
            item["score"] = round(score, 4)
        results.append(item)

    payload: Dict[str, Any] = {
        "status": "ok",
        "profile_id": profile_id,
        "query": normalized_query,
        "vector_store_ids": vector_store_ids,
        "results": results,
        "confirmation_status": "confirmed" if results else "unconfirmed",
        "search_sources": {
            "vector_store": len([item for item in results if item.get("source_type") == "vector_store"]),
            "local_docs": len([item for item in results if item.get("source_type") == "local_docs"]),
        },
        "answering_guidance": (
            "Se a busca vier sem resultados, trate como informacao nao confirmada no momento. "
            "Nao negue a existencia de unidade, convenio, exame ou servico apenas porque a busca veio vazia."
        ),
    }
    if errors:
        payload["warnings"] = errors
    if local_docs_payload.get("status") == "error":
        payload.setdefault("warnings", []).append(
            {
                "docs_dir": str(local_docs_payload.get("docs_dir") or ""),
                "error": str(local_docs_payload.get("message") or "local_docs_search_failed"),
            }
        )
    if not results:
        payload["message"] = "no_results"
    return payload


def build_knowledge_tool():
    if function_tool is None:
        return None

    @function_tool
    def buscar_info_clinica(query: str = "", top_k: int = 3) -> Dict[str, Any]:
        chat_id = CURRENT_CHAT_ID.get("") if CURRENT_CHAT_ID is not None else ""
        profile_id = (CURRENT_PROFILE_ID.get("") if CURRENT_PROFILE_ID is not None else "") or (
            resolve_profile_for_chat(chat_id) if resolve_profile_for_chat is not None else ""
        )
        if not profile_id:
            profile_id = PROFILE_DEFAULT_ID or ""
        try:
            requested_top_k = int(top_k)
        except Exception:
            requested_top_k = 3
        requested_top_k = max(1, min(requested_top_k, 6))
        return search_profile_vector_knowledge(
            profile_id=profile_id,
            query=query or "",
            top_k=requested_top_k,
        )

    return buscar_info_clinica
