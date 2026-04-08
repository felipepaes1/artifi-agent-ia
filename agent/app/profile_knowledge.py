import os
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple


_TOKEN_RE = re.compile(r"[a-z0-9]{3,}")


@dataclass(frozen=True)
class KnowledgeDoc:
    source: str
    content: str
    tokens: Tuple[str, ...]


_CACHE: Dict[Tuple[str, int, int], Tuple[str, List[KnowledgeDoc]]] = {}


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    lowered = unicodedata.normalize("NFD", text.lower())
    return "".join(ch for ch in lowered if unicodedata.category(ch) != "Mn")


def _tokenize(text: str) -> Tuple[str, ...]:
    return tuple(_TOKEN_RE.findall(_normalize_text(text)))


def _list_doc_files(docs_dir: str) -> List[str]:
    paths: List[str] = []
    if not docs_dir or not os.path.isdir(docs_dir):
        return paths
    for dirpath, _, filenames in os.walk(docs_dir):
        for name in sorted(filenames):
            lowered = name.lower()
            if not (lowered.endswith(".md") or lowered.endswith(".txt") or lowered.endswith(".json")):
                continue
            paths.append(os.path.join(dirpath, name))
    return sorted(paths)


def _build_signature(paths: Sequence[str]) -> str:
    parts: List[str] = []
    for path in paths:
        try:
            stat = os.stat(path)
            parts.append(f"{path}|{stat.st_size}|{stat.st_mtime_ns}")
        except OSError:
            continue
    return "\n".join(parts)


def _read_docs(docs_dir: str, max_docs: int, max_chars_per_doc: int) -> List[KnowledgeDoc]:
    paths = _list_doc_files(docs_dir)
    signature = _build_signature(paths)
    cache_key = (docs_dir, max_docs, max_chars_per_doc)
    cached = _CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return cached[1]

    docs: List[KnowledgeDoc] = []
    for path in paths[:max_docs]:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                content = handle.read().strip()
        except Exception:
            continue
        if not content:
            continue
        if max_chars_per_doc > 0:
            content = content[:max_chars_per_doc]
        tokens = _tokenize(content)
        docs.append(KnowledgeDoc(source=path, content=content, tokens=tokens))

    _CACHE[cache_key] = (signature, docs)
    return docs


def _score(query_tokens: Tuple[str, ...], doc_tokens: Tuple[str, ...]) -> int:
    if not query_tokens or not doc_tokens:
        return 0
    doc_set = set(doc_tokens)
    return sum(1 for token in set(query_tokens) if token in doc_set)


def search_profile_knowledge(
    docs_dir: str,
    query: str,
    top_k: int = 3,
    max_docs: int = 80,
    max_chars_per_doc: int = 5000,
    max_chars_total: int = 2200,
) -> List[Dict[str, str]]:
    docs = _read_docs(docs_dir, max_docs=max_docs, max_chars_per_doc=max_chars_per_doc)
    if not docs:
        return []

    query_tokens = _tokenize(query or "")
    scored: List[Tuple[int, KnowledgeDoc]] = []

    if not query_tokens:
        picked = docs[: max(1, top_k)]
    else:
        for doc in docs:
            score = _score(query_tokens, doc.tokens)
            if score > 0:
                scored.append((score, doc))
        scored.sort(key=lambda item: item[0], reverse=True)
        picked = [doc for _, doc in scored[: max(1, top_k)]]

    results: List[Dict[str, str]] = []
    total = 0
    for doc in picked:
        if total >= max_chars_total:
            break
        content = doc.content
        remaining = max_chars_total - total
        if len(content) > remaining:
            content = content[:remaining]
        results.append(
            {
                "source": doc.source,
                "content": content,
            }
        )
        total += len(content)

    return results
