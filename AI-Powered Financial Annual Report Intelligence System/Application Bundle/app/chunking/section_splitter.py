from __future__ import annotations

import re
from typing import List, Optional

from app.models.schemas import PageData, SectionRecord
from app.utils.ids import generate_uuid


SECTION_PATTERNS = {
    "chairman": [
        r"chairman.?s message",
        r"message from the chairman",
        r"letter from the chairman",
    ],
    "management": [
        r"management discussion",
        r"management discussion and analysis",
        r"md&a",
    ],
    "risk": [
        r"risk management",
        r"principal risks",
        r"risks and concerns",
    ],
    "governance": [
        r"corporate governance",
        r"governance report",
    ],
    "directors_report": [
        r"board.?s report",
        r"directors.? report",
    ],
    "financials": [
        r"financial statements",
        r"standalone financial statements",
        r"consolidated financial statements",
        r"balance sheet",
        r"statement of profit and loss",
        r"cash flow statement",
        r"notes to accounts",
        r"notes forming part",
    ],
}


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_title(text: str) -> str:
    return normalize_spaces(text).lower()


def classify_section(title: str) -> str:
    normalized = normalize_title(title)
    for section_type, patterns in SECTION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, normalized, flags=re.IGNORECASE):
                return section_type
    return "general"


def is_probable_heading(line: str) -> bool:
    line = normalize_spaces(line)
    if not line:
        return False

    if len(line) < 4 or len(line) > 120:
        return False

    lowered = line.lower()

    noisy_lines = {
        "annual report",
        "table of contents",
        "contents",
    }
    if lowered in noisy_lines:
        return False

    if line.isupper() and len(line.split()) <= 12:
        return True

    heading_keywords = [
        "message",
        "management discussion",
        "risk management",
        "corporate governance",
        "board report",
        "directors report",
        "financial statements",
        "balance sheet",
        "cash flow",
        "notes to accounts",
        "notes forming part",
    ]
    if any(keyword in lowered for keyword in heading_keywords):
        return True

    if re.match(r"^\d+(\.\d+)*\s+[A-Za-z].*$", line):
        return True

    return False


def extract_heading_from_page(text: str) -> Optional[str]:
    lines = [normalize_spaces(line) for line in text.splitlines() if normalize_spaces(line)]

    for line in lines[:15]:
        if is_probable_heading(line):
            return line

    return None


def _build_section(
    document_id: str,
    title: str,
    start_page: int,
    end_page: int,
    text_parts: List[str],
) -> Optional[SectionRecord]:
    section_text = "\n\n".join(part for part in text_parts if normalize_spaces(part)).strip()

    if not section_text:
        return None

    return SectionRecord(
        section_id=generate_uuid(),
        document_id=document_id,
        title=title,
        normalized_title=normalize_title(title),
        section_type=classify_section(title),
        start_page=start_page,
        end_page=end_page,
        text=section_text,
    )


def split_into_sections(document_id: str, pages: List[PageData]) -> List[SectionRecord]:
    if not pages:
        return []

    sections: List[SectionRecord] = []
    current_title = "Document Opening"
    current_start_page = pages[0].page_no
    current_text_parts: List[str] = []

    for idx, page in enumerate(pages):
        page_text = getattr(page, "cleaned_text", "") or getattr(page, "text", "") or ""
        detected_heading = extract_heading_from_page(page_text)

        if idx == 0 and detected_heading:
            current_title = detected_heading

        if detected_heading and current_text_parts and idx != 0:
            previous_page_no = pages[idx - 1].page_no

            section = _build_section(
                document_id=document_id,
                title=current_title,
                start_page=current_start_page,
                end_page=previous_page_no,
                text_parts=current_text_parts,
            )
            if section is not None:
                sections.append(section)

            current_title = detected_heading
            current_start_page = page.page_no
            current_text_parts = [page_text]
        else:
            if normalize_spaces(page_text):
                current_text_parts.append(page_text)

    final_section = _build_section(
        document_id=document_id,
        title=current_title,
        start_page=current_start_page,
        end_page=pages[-1].page_no,
        text_parts=current_text_parts,
    )
    if final_section is not None:
        sections.append(final_section)

    return sections