## Phase 2

from __future__ import annotations

import re
from typing import List


def normalize_spaces(text: str) -> str:
    return re.sub(r"[ \t]+", " ", text).strip()


def split_paragraphs(text: str) -> List[str]:
    paragraphs = re.split(r"\n\s*\n+", text)
    return [normalize_spaces(p) for p in paragraphs if normalize_spaces(p)]


def word_count(text: str) -> int:
    return len(text.split())


def build_overlap(text: str, overlap_words: int) -> str:
    words = text.split()
    if not words:
        return ""
    return " ".join(words[-overlap_words:])


def chunk_section_text(
    text: str,
    chunk_size_words: int = 700,
    overlap_words: int = 120,
) -> List[str]:
    paragraphs = split_paragraphs(text)
    if not paragraphs:
        return []

    chunks: List[str] = []
    current_parts: List[str] = []
    current_words = 0

    for para in paragraphs:
        para_words = word_count(para)

        if para_words > chunk_size_words:
            if current_parts:
                chunks.append("\n\n".join(current_parts).strip())
                current_parts = []
                current_words = 0

            sentence_parts = re.split(r"(?<=[.!?])\s+", para)
            temp_parts: List[str] = []
            temp_words = 0

            for sentence in sentence_parts:
                sentence = normalize_spaces(sentence)
                if not sentence:
                    continue

                sentence_words = word_count(sentence)

                if temp_words + sentence_words > chunk_size_words and temp_parts:
                    chunk_text = " ".join(temp_parts).strip()
                    chunks.append(chunk_text)

                    overlap_text = build_overlap(chunk_text, overlap_words)
                    temp_parts = [overlap_text, sentence] if overlap_text else [sentence]
                    temp_words = word_count(" ".join(temp_parts))
                else:
                    temp_parts.append(sentence)
                    temp_words += sentence_words

            if temp_parts:
                chunks.append(" ".join(temp_parts).strip())

            continue

        if current_words + para_words > chunk_size_words and current_parts:
            full_chunk = "\n\n".join(current_parts).strip()
            chunks.append(full_chunk)

            overlap_text = build_overlap(full_chunk, overlap_words)
            current_parts = [overlap_text, para] if overlap_text else [para]
            current_words = word_count("\n\n".join(current_parts))
        else:
            current_parts.append(para)
            current_words += para_words

    if current_parts:
        chunks.append("\n\n".join(current_parts).strip())

    return [chunk for chunk in chunks if chunk.strip()]