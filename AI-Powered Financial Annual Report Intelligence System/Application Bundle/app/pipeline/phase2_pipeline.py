## Phase 2

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Tuple

from app.chunking.chunker import chunk_section_text
from app.chunking.metadata_builder import build_chunk_records
from app.chunking.section_splitter import split_into_sections
from app.chunking.validators import validate_chunks, validate_sections
from app.models.schemas import ChunkRecord, DocumentMetadata, PageData
from app.utils.paths import CHUNKS_DIR, PAGE_TEXT_DIR, PARSED_DIR, STATE_DIR, ensure_data_dirs


def load_document_metadata(metadata_path: str | Path) -> DocumentMetadata:
    metadata_path = Path(metadata_path)

    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with open(metadata_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "metadata" in data and isinstance(data["metadata"], dict):
        data = data["metadata"]

    return DocumentMetadata(**data)


def load_page_texts(page_text_path: str | Path) -> List[PageData]:
    page_text_path = Path(page_text_path)

    if not page_text_path.exists():
        raise FileNotFoundError(f"Page text file not found: {page_text_path}")

    with open(page_text_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "pages" in data:
        data = data["pages"]

    return [PageData(**item) for item in data]


def save_chunks(chunks: List[ChunkRecord], output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            [chunk.to_dict() for chunk in chunks],
            f,
            ensure_ascii=False,
            indent=2,
        )


def read_latest_phase1_run() -> dict:
    state_path = STATE_DIR / "latest_run.json"

    if not state_path.exists():
        raise FileNotFoundError(
            f"Missing state file: {state_path}. Run Phase 1 first."
        )

    with open(state_path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_latest_phase2_run(doc_id: str, chunks_path: Path) -> None:
    payload = {
        "doc_id": doc_id,
        "chunks_path": str(chunks_path),
    }

    with open(STATE_DIR / "latest_phase2_run.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def run_phase2_pipeline(
    metadata_path: str | Path,
    page_text_path: str | Path,
    output_chunks_path: str | Path,
    chunk_size_words: int = 700,
    overlap_words: int = 120,
) -> Tuple[DocumentMetadata, List[ChunkRecord]]:
    document_meta = load_document_metadata(metadata_path)
    pages = load_page_texts(page_text_path)

    if not pages:
        raise ValueError("No pages found in page text file.")

    sections = split_into_sections(document_meta.doc_id, pages)
    validate_sections(sections)

    all_chunks: List[ChunkRecord] = []

    for section in sections:
        chunk_texts = chunk_section_text(
            text=section.text,
            chunk_size_words=chunk_size_words,
            overlap_words=overlap_words,
        )

        if not chunk_texts:
            continue

        chunk_records = build_chunk_records(
            document_meta=document_meta,
            section=section,
            chunk_texts=chunk_texts,
        )
        all_chunks.extend(chunk_records)

    validate_chunks(all_chunks)
    save_chunks(all_chunks, output_chunks_path)
    write_latest_phase2_run(document_meta.doc_id, Path(output_chunks_path))

    return document_meta, all_chunks


def run_phase2_from_latest(
    chunk_size_words: int = 700,
    overlap_words: int = 120,
) -> Tuple[DocumentMetadata, List[ChunkRecord]]:
    ensure_data_dirs()

    latest = read_latest_phase1_run()
    doc_id = latest["doc_id"]

    metadata_path = PARSED_DIR / f"{doc_id}_metadata.json"
    page_text_path = PAGE_TEXT_DIR / f"{doc_id}_pages.json"
    output_chunks_path = CHUNKS_DIR / f"{doc_id}_chunks.json"

    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    if not page_text_path.exists():
        raise FileNotFoundError(f"Page text file not found: {page_text_path}")

    return run_phase2_pipeline(
        metadata_path=metadata_path,
        page_text_path=page_text_path,
        output_chunks_path=output_chunks_path,
        chunk_size_words=chunk_size_words,
        overlap_words=overlap_words,
    )