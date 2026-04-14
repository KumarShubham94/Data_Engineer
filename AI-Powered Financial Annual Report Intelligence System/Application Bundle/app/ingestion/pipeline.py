from __future__ import annotations

import logging
from pathlib import Path

from app.ingestion.doc_metadata_extractor import extract_document_metadata
from app.ingestion.ocr import extract_ocr_text_from_last_page
from app.ingestion.parser import parse_pdf
from app.models.schemas import DocumentMetadata, ParsedDocument
from app.storage.file_store import copy_raw_file, save_json
from app.utils.ids import generate_doc_id
from app.utils.paths import PAGE_TEXT_DIR, PARSED_DIR, ensure_data_dirs


logger = logging.getLogger("rag_pdf")


def _safe_dump(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return obj


def run_ingestion_pipeline(pdf_path: str | Path) -> ParsedDocument:
    ensure_data_dirs()

    source_path = Path(pdf_path)
    if not source_path.exists():
        raise FileNotFoundError(f"PDF file not found: {source_path}")

    doc_id = generate_doc_id()
    doc_name = source_path.name

    logger.info("Starting ingestion for %s", doc_name)

    stored_pdf_path = copy_raw_file(source_path, doc_name)

    pages = parse_pdf(stored_pdf_path, doc_id, doc_name)

    total_pages = len(pages)
    empty_pages = sum(
        1 for page in pages if not getattr(page, "cleaned_text", "").strip()
    )
    parsed_pages = total_pages - empty_pages
    extraction_status = "success" if pages else "failed"

    last_page_text = ""
    if pages:
        last_page = pages[-1]
        last_page_text = getattr(last_page, "cleaned_text", "") or getattr(last_page, "text", "") or ""

    logger.info("Running OCR on last page for %s", doc_name)
    ocr_pages_scanned = 0
    last_page_ocr_text = ""

    try:
        ocr_page_no, last_page_ocr_text = extract_ocr_text_from_last_page(stored_pdf_path)
        ocr_used = bool(last_page_ocr_text.strip())
        ocr_pages_scanned = 1 if ocr_page_no else 0
    except Exception as exc:
        logger.warning("Last-page OCR failed for %s: %s", doc_name, exc)
        ocr_used = False
        last_page_ocr_text = ""
        ocr_pages_scanned = 0

    metadata_fields = extract_document_metadata(
        filename=doc_name,
        last_page_text=last_page_text,
        last_page_ocr_text=last_page_ocr_text,
    )

    metadata = DocumentMetadata(
        doc_id=doc_id,
        doc_name=doc_name,
        source_path=str(stored_pdf_path),
        total_pages=total_pages,
        parsed_pages=parsed_pages,
        empty_pages=empty_pages,
        extraction_status=extraction_status,
        company_name=metadata_fields.get("company_name"),
        company_name_source=metadata_fields.get("company_name_source"),
        company_name_confidence=metadata_fields.get("company_name_confidence"),
        financial_year=metadata_fields.get("financial_year"),
        report_title=metadata_fields.get("report_title"),
        contact_email=metadata_fields.get("contact_email"),
        ocr_used=ocr_used,
        ocr_pages_scanned=ocr_pages_scanned,
    )

    metadata_output_path = PARSED_DIR / f"{doc_id}_metadata.json"
    pages_output_path = PAGE_TEXT_DIR / f"{doc_id}_pages.json"

    save_json(metadata.model_dump(), metadata_output_path)
    save_json([_safe_dump(page) for page in pages], pages_output_path)

    logger.info("Saved metadata to %s", metadata_output_path)
    logger.info("Saved page text to %s", pages_output_path)

    return ParsedDocument(metadata=metadata, pages=pages)