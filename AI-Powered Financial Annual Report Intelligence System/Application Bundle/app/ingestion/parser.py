import fitz

from app.ingestion.cleaner import clean_text
from app.ingestion.section_detector import detect_section_title
from app.models.schemas import PageData


def parse_pdf(pdf_path: str, doc_id: str, doc_name: str) -> list[PageData]:
    pdf = fitz.open(pdf_path)
    pages: list[PageData] = []

    try:
        for index, page in enumerate(pdf):
            raw_text = page.get_text("text", sort=True)
            cleaned_text = clean_text(raw_text)
            section_title = detect_section_title(raw_text)

            pages.append(
                PageData(
                    doc_id=doc_id,
                    doc_name=doc_name,
                    page_no=index + 1,
                    raw_text=raw_text,
                    cleaned_text=cleaned_text,
                    section_title=section_title,
                    char_count=len(cleaned_text),
                )
            )
    finally:
        pdf.close()

    return pages