import os
from dataclasses import dataclass
from typing import List


try:
    from langchain_core.documents import Document
except Exception:  # pragma: no cover
    @dataclass
    class Document:  # type: ignore
        page_content: str
        metadata: dict


def _read_file(path: str, max_chars: int) -> str:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except Exception:
        return ""
    text = text.strip()
    if max_chars > 0:
        text = text[:max_chars]
    return text


def load_documents_from_dir(
    docs_dir: str,
    max_docs: int = 50,
    max_chars_per_doc: int = 4000,
) -> List[Document]:
    if not docs_dir or not os.path.isdir(docs_dir):
        return []

    docs: List[Document] = []
    count = 0
    for dirpath, _, filenames in os.walk(docs_dir):
        for name in sorted(filenames):
            lower = name.lower()
            if not (lower.endswith(".txt") or lower.endswith(".md")):
                continue
            path = os.path.join(dirpath, name)
            text = _read_file(path, max_chars_per_doc)
            if not text:
                continue
            docs.append(Document(page_content=text, metadata={"source": path}))
            count += 1
            if count >= max_docs:
                return docs
    return docs
