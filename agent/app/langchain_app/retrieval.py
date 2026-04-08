import re
from typing import List

from .documents import Document


_WORD_RE = re.compile(r"[a-zA-Z0-9]{3,}")


def _tokenize(text: str) -> List[str]:
    return [t.lower() for t in _WORD_RE.findall(text or "")]


def _score(query_tokens: List[str], doc_tokens: List[str]) -> int:
    if not query_tokens or not doc_tokens:
        return 0
    doc_set = set(doc_tokens)
    return sum(1 for token in query_tokens if token in doc_set)


def retrieve_context(
    docs: List[Document],
    query: str,
    top_k: int = 4,
    max_chars: int = 4000,
) -> str:
    if not docs:
        return ""
    query_tokens = _tokenize(query)
    scored = []
    for doc in docs:
        doc_tokens = _tokenize(doc.page_content)
        score = _score(query_tokens, doc_tokens)
        if score > 0:
            scored.append((score, doc))
    if not scored:
        return ""
    scored.sort(key=lambda item: item[0], reverse=True)
    picked = [doc for _, doc in scored[:top_k]]
    chunks: List[str] = []
    total = 0
    for doc in picked:
        text = doc.page_content.strip()
        if not text:
            continue
        source = doc.metadata.get("source") if isinstance(doc.metadata, dict) else None
        if source:
            header = f"Source: {source}"
            text = f"{header}\n{text}"
        if total + len(text) > max_chars:
            remaining = max_chars - total
            if remaining <= 0:
                break
            text = text[:remaining]
        chunks.append(text)
        total += len(text)
        if total >= max_chars:
            break
    return "\n\n".join(chunks)
