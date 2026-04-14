## Phase 2

from __future__ import annotations

from typing import List

from app.models.schemas import ChunkRecord, SectionRecord


def validate_sections(sections: List[SectionRecord]) -> None:
    for section in sections:
        if not section.title.strip():
            raise ValueError("Section title cannot be empty.")
        if section.start_page > section.end_page:
            raise ValueError(
                f"Invalid section page range: {section.start_page} > {section.end_page}"
            )
        if not section.text.strip():
            raise ValueError(f"Section '{section.title}' has empty text.")


def validate_chunks(chunks: List[ChunkRecord]) -> None:
    for chunk in chunks:
        if not chunk.text.strip():
            raise ValueError(f"Chunk {chunk.chunk_id} has empty text.")
        if chunk.word_count <= 0:
            raise ValueError(f"Chunk {chunk.chunk_id} has invalid word count.")
        if chunk.page_start > chunk.page_end:
            raise ValueError(f"Chunk {chunk.chunk_id} has invalid page range.")