import os
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class ExamplesText:
    good: str
    bad: str


def _read_text_files(paths: List[str], max_chars: int) -> str:
    chunks: List[str] = []
    total = 0
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                text = handle.read().strip()
        except Exception:
            continue
        if not text:
            continue
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


def _collect_files(root: str) -> List[str]:
    files: List[str] = []
    for dirpath, _, filenames in os.walk(root):
        for name in sorted(filenames):
            lower = name.lower()
            if lower.endswith(".txt") or lower.endswith(".md"):
                files.append(os.path.join(dirpath, name))
    return files


def load_examples(examples_dir: str, max_chars: int = 2000) -> ExamplesText:
    if not examples_dir:
        return ExamplesText(good="", bad="")
    if not os.path.isdir(examples_dir):
        return ExamplesText(good="", bad="")

    good_dir = os.path.join(examples_dir, "good")
    bad_dir = os.path.join(examples_dir, "bad")

    if os.path.isdir(good_dir) or os.path.isdir(bad_dir):
        good_files = _collect_files(good_dir) if os.path.isdir(good_dir) else []
        bad_files = _collect_files(bad_dir) if os.path.isdir(bad_dir) else []
    else:
        all_files = _collect_files(examples_dir)
        good_files = [p for p in all_files if "good" in os.path.basename(p).lower()]
        bad_files = [p for p in all_files if "bad" in os.path.basename(p).lower()]

    good_text = _read_text_files(good_files, max_chars)
    bad_text = _read_text_files(bad_files, max_chars)
    return ExamplesText(good=good_text, bad=bad_text)
