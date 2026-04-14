import re
from typing import Optional


def is_heading_candidate(line: str) -> bool:
    line = line.strip()

    if not line:
        return False

    if len(line) > 120:
        return False

    if line.endswith("."):
        return False

    words = line.split()
    if len(words) > 12:
        return False

    alpha_chars = [c for c in line if c.isalpha()]
    if not alpha_chars:
        return False

    uppercase_ratio = sum(1 for c in alpha_chars if c.isupper()) / len(alpha_chars)
    title_case_like = sum(1 for w in words if w[:1].isupper()) >= max(1, len(words) // 2)

    if uppercase_ratio > 0.8 or title_case_like:
        return True

    if re.match(r"^\d+(\.\d+)*\s+[A-Z][A-Za-z0-9 ,/&()\-]+$", line):
        return True

    return False


def detect_section_title(page_text: str) -> Optional[str]:
    if not page_text:
        return None

    lines = [line.strip() for line in page_text.split("\n") if line.strip()]
    for line in lines[:10]:
        if is_heading_candidate(line):
            return line

    return None