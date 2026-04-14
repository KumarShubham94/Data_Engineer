import os
import re
from collections import Counter
from typing import Optional


EXCLUDE_WORDS = {
    "annual report",
    "integrated report",
    "contents",
    "table of contents",
    "chairman’s message",
    "chairman's message",
    "board of directors",
    "management discussion",
    "risk management",
    "corporate governance",
    "financial statements",
    "standalone financial statements",
    "consolidated financial statements",
    "notice",
    "auditors' report",
    "independent auditor's report",
    "director's report",
    "boards' report",
    "statutory reports",
}

LEGAL_SUFFIX_PATTERN = (
    r"(Limited|Ltd\.?|Private Limited|Pvt\.?\s*Ltd\.?|Bank|Finance|Financial Services|Industries|Corporation)"
)

EMAIL_PATTERN = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_company_name(name: str) -> str:
    name = normalize_spaces(name)

    noise_patterns = [
        r"\bAnnual Report\b.*",
        r"\bIntegrated Report\b.*",
        r"\bFinancial Statements\b.*",
        r"\bFY\s?\d{2,4}[-/–]?\d{0,4}\b.*",
        r"\b20\d{2}[-/–]20\d{2}\b.*",
        r"\b20\d{2}\b.*",
    ]

    for pattern in noise_patterns:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE).strip()

    name = re.sub(r"\bLtd\.\b", "Limited", name, flags=re.IGNORECASE)
    name = re.sub(r"\bLtd\b", "Limited", name, flags=re.IGNORECASE)
    name = re.sub(r"\bPvt\.\s*Ltd\.\b", "Private Limited", name, flags=re.IGNORECASE)
    name = re.sub(r"\bPvt\s*Ltd\b", "Private Limited", name, flags=re.IGNORECASE)

    return normalize_spaces(name)


def has_legal_suffix(text: str) -> bool:
    return bool(re.search(LEGAL_SUFFIX_PATTERN, text, flags=re.IGNORECASE))


def looks_like_company_name(text: str) -> bool:
    text = normalize_spaces(text)
    lower_text = text.lower()

    if not text:
        return False
    if len(text) < 3 or len(text) > 120:
        return False
    if any(ex in lower_text for ex in EXCLUDE_WORDS):
        return False
    if re.search(r"[,:;]$", text):
        return False

    words = text.split()
    if len(words) > 12:
        return False

    if not re.search(r"[A-Za-z]", text):
        return False

    legal_pattern = (
        rf"\b([A-Z][A-Za-z&,\-().']*(?:\s+[A-Z][A-Za-z&,\-().']*){{0,8}})\s+{LEGAL_SUFFIX_PATTERN}\b"
    )
    if re.search(legal_pattern, text, flags=re.IGNORECASE):
        return True

    if 1 < len(words) <= 6:
        title_case_words = sum(1 for w in words if w[:1].isupper() or w.isupper())
        if title_case_words >= max(2, len(words) - 1):
            return True

    return False


def extract_candidates_from_line(line: str) -> list[str]:
    line = normalize_spaces(line)
    candidates = []

    if not line:
        return candidates

    legal_pattern = (
        rf"\b([A-Z][A-Za-z&,\-().']*(?:\s+[A-Z][A-Za-z&,\-().']*){{0,8}})\s+{LEGAL_SUFFIX_PATTERN}\b"
    )

    for match in re.finditer(legal_pattern, line, flags=re.IGNORECASE):
        candidate = normalize_company_name(match.group(0))
        if looks_like_company_name(candidate):
            candidates.append(candidate)

    contextual_patterns = [
        rf"(?:Registered Office|Corporate Office|For and on behalf of|The Company[,:\-]?)\s*[:\-]?\s*([A-Z][A-Za-z&,\-().']*(?:\s+[A-Z][A-Za-z&,\-().']*){{0,8}}(?:\s+{LEGAL_SUFFIX_PATTERN})?)",
        rf"([A-Z][A-Za-z&,\-().']*(?:\s+[A-Z][A-Za-z&,\-().']*){{0,8}}(?:\s+{LEGAL_SUFFIX_PATTERN})?)\s*\(\s*[\"']?the Company[\"']?\s*\)",
    ]

    for pattern in contextual_patterns:
        for match in re.finditer(pattern, line, flags=re.IGNORECASE):
            candidate = normalize_company_name(match.group(1))
            if looks_like_company_name(candidate):
                candidates.append(candidate)

    if looks_like_company_name(line):
        candidates.append(normalize_company_name(line))

    return list(dict.fromkeys(candidates))


def extract_candidates_from_filename(filename: str) -> list[str]:
    stem = os.path.splitext(os.path.basename(filename))[0]
    stem = stem.replace("_", " ").replace("-", " ")
    stem = normalize_spaces(stem)

    stem = re.sub(r"\bannual report\b", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"\bintegrated report\b", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"\bfy\s?\d{2,4}[-/–]?\d{0,4}\b", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"\b20\d{2}[-/–]20\d{2}\b", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"\b20\d{2}\b", "", stem, flags=re.IGNORECASE)
    stem = normalize_spaces(stem)

    candidates = []
    if looks_like_company_name(stem):
        candidates.append(normalize_company_name(stem))

    return list(dict.fromkeys(candidates))


def extract_candidates_from_page_text(page_text: str) -> list[str]:
    if not page_text:
        return []

    lines = [normalize_spaces(line) for line in page_text.splitlines() if line.strip()]
    candidates = []

    for line in lines[:120]:
        candidates.extend(extract_candidates_from_line(line))

    return list(dict.fromkeys(candidates))


def extract_email(texts: list[str]) -> Optional[str]:
    for text in texts:
        if not text:
            continue
        match = re.search(EMAIL_PATTERN, text)
        if match:
            return match.group(0)
    return None


def choose_best_candidate(
    filename_candidates: list[str],
    last_page_text_candidates: list[str],
    last_page_ocr_candidates: list[str],
) -> tuple[Optional[str], str, str]:
    score_counter = Counter()

    for candidate in filename_candidates:
        score_counter[candidate] += 2

    for candidate in last_page_text_candidates:
        score_counter[candidate] += 5
        if has_legal_suffix(candidate):
            score_counter[candidate] += 5

    for candidate in last_page_ocr_candidates:
        score_counter[candidate] += 4
        if has_legal_suffix(candidate):
            score_counter[candidate] += 3

    for candidate in list(score_counter.keys()):
        if candidate in last_page_text_candidates and candidate in last_page_ocr_candidates:
            score_counter[candidate] += 5

    if not score_counter:
        return None, "not_found", "low"

    best_candidate, best_score = score_counter.most_common(1)[0]

    sources = []
    if best_candidate in filename_candidates:
        sources.append("filename")
    if best_candidate in last_page_text_candidates:
        sources.append("last_page_text")
    if best_candidate in last_page_ocr_candidates:
        sources.append("last_page_ocr")

    source = "+".join(sources) if sources else "not_found"

    if best_score >= 10 and has_legal_suffix(best_candidate):
        confidence = "high"
    elif best_score >= 6:
        confidence = "medium"
    else:
        confidence = "low"

    return best_candidate, source, confidence


def extract_document_metadata(
    filename: str,
    last_page_text: str,
    last_page_ocr_text: str,
) -> dict:
    filename_candidates = extract_candidates_from_filename(filename)
    last_page_text_candidates = extract_candidates_from_page_text(last_page_text)
    last_page_ocr_candidates = extract_candidates_from_page_text(last_page_ocr_text)

    company_name, source, confidence = choose_best_candidate(
        filename_candidates=filename_candidates,
        last_page_text_candidates=last_page_text_candidates,
        last_page_ocr_candidates=last_page_ocr_candidates,
    )

    contact_email = extract_email([last_page_text, last_page_ocr_text])

    return {
        "company_name": company_name,
        "company_name_source": source,
        "company_name_confidence": confidence,
        "contact_email": contact_email,
    }