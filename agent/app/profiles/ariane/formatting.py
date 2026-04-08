import re

from ...utils.text import normalize_text, strip_list_prefix


def looks_like_check_item(text: str) -> bool:
    cleaned = strip_list_prefix(text)
    if not cleaned:
        return False
    if cleaned.endswith("?"):
        return False
    if len(cleaned) > 180:
        return False
    lowered = normalize_text(cleaned)
    if lowered.startswith("gostaria de") or lowered.startswith("quer que eu"):
        return False
    return True


def normalize_ariane_inline_blocks(text: str) -> str:
    normalized = (text or "").strip()
    if not normalized:
        return ""
    normalized = re.sub(r"\s*✅\s*", "\n✅ ", normalized)
    normalized = re.sub(
        r"(?<!\n)(Consulta Capilar:|Terapia Capilar:|Est[eé]tica Facial:)",
        r"\n\1",
        normalized,
    )
    if normalized.count("\n✅ ") >= 2:
        normalized = re.sub(
            r"\s+(?=(?:Me conta,|Quer que eu|Gostaria de|Posso |Prefere |Restou alguma duvida))",
            "\n\n",
            normalized,
            count=1,
            flags=re.IGNORECASE,
        )
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def format_ariane_checklists(text: str) -> str:
    if not text:
        return text

    formatted_paragraphs: list[str] = []
    paragraphs = re.split(r"\n\s*\n", normalize_ariane_inline_blocks(text))

    for paragraph in paragraphs:
        raw_lines = [line.rstrip() for line in paragraph.splitlines() if line.strip()]
        if len(raw_lines) < 2:
            formatted_paragraphs.append("\n".join(strip_list_prefix(line) for line in raw_lines).strip())
            continue

        normalized_lines = [strip_list_prefix(line) for line in raw_lines]
        first_line = normalized_lines[0]
        first_lowered = normalize_text(first_line)
        has_heading = first_line.endswith(":") or any(
            marker in first_lowered
            for marker in (
                "voce pode esperar",
                "servicos",
                "beneficios",
                "o que voce recebe",
                "inclui",
                "entregaveis",
            )
        )

        start_idx = 1 if has_heading else 0
        candidate_items = normalized_lines[start_idx:]
        checklist_candidates = [line for line in candidate_items if looks_like_check_item(line)]
        colon_style_items = [line for line in candidate_items if ":" in line and not line.endswith("?")]
        existing_checks = [line for line in candidate_items if line.startswith("✅")]

        should_format = False
        if len(existing_checks) >= 2:
            should_format = True
        elif len(checklist_candidates) >= 2 and len(checklist_candidates) == len(candidate_items):
            should_format = True
        elif len(colon_style_items) >= 2 and len(colon_style_items) == len(candidate_items):
            should_format = True

        if not should_format:
            formatted_paragraphs.append("\n".join(normalized_lines).strip())
            continue

        trailing_questions: list[str] = []
        while candidate_items and candidate_items[-1].strip().endswith("?"):
            trailing_questions.insert(0, candidate_items.pop().strip())

        lines: list[str] = []
        if has_heading:
            lines.append(first_line)
        for item in candidate_items:
            item_text = strip_list_prefix(item)
            if not item_text:
                continue
            if item_text.startswith("✅"):
                item_text = item_text.lstrip("✅").strip()
            if looks_like_check_item(item_text) or ":" in item_text:
                lines.append(f"✅ {item_text}")
            else:
                lines.append(item_text)
        formatted_paragraphs.append("\n".join(lines).strip())
        if trailing_questions:
            formatted_paragraphs.append("\n".join(trailing_questions).strip())

    return "\n\n".join(part for part in formatted_paragraphs if part).strip()


def split_ariane_trailing_question_blocks(text: str) -> str:
    if not text:
        return text

    formatted_paragraphs: list[str] = []
    paragraphs = re.split(r"\n\s*\n", text.strip())

    for paragraph in paragraphs:
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        if not lines:
            continue
        if any(line.startswith("✅ ") for line in lines):
            formatted_paragraphs.append("\n".join(lines))
            continue

        paragraph_text = " ".join(lines).strip()
        if paragraph_text.count("?") == 0:
            formatted_paragraphs.append(paragraph_text)
            continue

        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", paragraph_text) if part.strip()]
        if len(sentences) < 2:
            formatted_paragraphs.append(paragraph_text)
            continue

        trailing_questions: list[str] = []
        while sentences and sentences[-1].endswith("?"):
            trailing_questions.insert(0, sentences.pop())

        explanation = " ".join(sentences).strip()
        question_block = " ".join(trailing_questions).strip()
        if explanation and question_block and len(explanation) >= 90:
            formatted_paragraphs.append(explanation)
            formatted_paragraphs.append(question_block)
        else:
            formatted_paragraphs.append(paragraph_text)

    return "\n\n".join(part for part in formatted_paragraphs if part).strip()

