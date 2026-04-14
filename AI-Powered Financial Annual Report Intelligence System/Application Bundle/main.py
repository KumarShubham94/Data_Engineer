# from app.ingestion.pipeline import run_ingestion_pipeline


# if __name__ == "__main__":
#     pdf_path = "/home/yash/Dev_Projects/RAG PDF/project/Reports/AL_Annual-Report-FY-2024-25.pdf"  # replace with your PDF path
#     result = run_ingestion_pipeline(pdf_path)

#     print("\nDocument parsed successfully.\n")
#     print(f"Document ID: {result.metadata.doc_id}")
#     print(f"Document Name: {result.metadata.doc_name}")
#     print(f"Total Pages: {result.metadata.total_pages}")
#     print(f"Parsed Pages: {result.metadata.parsed_pages}")
#     print(f"Empty Pages: {result.metadata.empty_pages}")
#     print(f"Extraction Status: {result.metadata.extraction_status}")
#     print(f"Company Name: {result.metadata.company_name}")
#     print(f"Company Name Source: {result.metadata.company_name_source}")
#     print(f"Company Name Confidence: {result.metadata.company_name_confidence}")
#     print(f"Contact Email: {result.metadata.contact_email}")
#     print(f"OCR Used: {result.metadata.ocr_used}")
#     print(f"OCR Pages Scanned: {result.metadata.ocr_pages_scanned}")


## New phase1 main

import json

from app.ingestion.pipeline import run_ingestion_pipeline
from app.utils.paths import STATE_DIR, ensure_data_dirs


def write_latest_run(result) -> None:
    ensure_data_dirs()

    payload = {
        "doc_id": result.metadata.doc_id,
        "doc_name": result.metadata.doc_name,
        "source_path": result.metadata.source_path,
    }

    with open(STATE_DIR / "latest_run.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    pdf_path = "/home/yash/Dev_Projects/RAG PDF/project/Reports/AL_Annual-Report-FY-2024-25.pdf"

    result = run_ingestion_pipeline(pdf_path)
    write_latest_run(result)

    print("\nDocument parsed successfully.\n")
    print(f"Document ID: {result.metadata.doc_id}")
    print(f"Document Name: {result.metadata.doc_name}")
    print(f"Total Pages: {result.metadata.total_pages}")
    print(f"Parsed Pages: {result.metadata.parsed_pages}")
    print(f"Empty Pages: {result.metadata.empty_pages}")
    print(f"Extraction Status: {result.metadata.extraction_status}")
    print(f"Company Name: {result.metadata.company_name}")
    print(f"Company Name Source: {result.metadata.company_name_source}")
    print(f"Company Name Confidence: {result.metadata.company_name_confidence}")
    print(f"Contact Email: {result.metadata.contact_email}")
    print(f"OCR Used: {result.metadata.ocr_used}")
    print(f"OCR Pages Scanned: {result.metadata.ocr_pages_scanned}")