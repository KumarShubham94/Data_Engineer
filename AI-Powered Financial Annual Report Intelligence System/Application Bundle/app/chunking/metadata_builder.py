## Phase 2

from __future__ import annotations

from typing import List

from app.models.schemas import ChunkRecord, DocumentMetadata, SectionRecord
from app.utils.ids import generate_uuid


def build_chunk_records(
    document_meta: DocumentMetadata,
    section: SectionRecord,
    chunk_texts: List[str],
) -> List[ChunkRecord]:
    records: List[ChunkRecord] = []

    for idx, chunk_text in enumerate(chunk_texts):
        records.append(
            ChunkRecord(
                chunk_id=generate_uuid(),
                document_id=document_meta.doc_id,
                document_name=document_meta.doc_name,
                company_name=document_meta.company_name,
                financial_year=document_meta.financial_year,
                report_title=document_meta.report_title,
                section_id=section.section_id,
                section_name=section.title,
                section_type=section.section_type,
                chunk_index=idx,
                page_start=section.start_page,
                page_end=section.end_page,
                text=chunk_text,
                word_count=len(chunk_text.split()),
                char_count=len(chunk_text),
            )
        )

    return records