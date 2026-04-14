# from datetime import datetime
# from typing import List, Optional

# from pydantic import BaseModel, Field


# class PageData(BaseModel):
#     doc_id: str
#     doc_name: str
#     page_no: int
#     raw_text: str
#     cleaned_text: str
#     section_title: Optional[str] = None
#     char_count: int


# class DocumentMetadata(BaseModel):
#     doc_id: str
#     doc_name: str
#     source_path: str
#     upload_timestamp: datetime

#     total_pages: int
#     parsed_pages: int
#     empty_pages: int

#     is_text_based_pdf: bool
#     extraction_status: str

#     company_name: Optional[str] = None
#     company_name_source: Optional[str] = None
#     company_name_confidence: Optional[str] = None

#     contact_email: Optional[str] = None

#     ocr_used: bool = False
#     ocr_pages_scanned: List[int] = Field(default_factory=list)

#     financial_year: Optional[str] = None
#     report_title: Optional[str] = None


# class ParsedDocument(BaseModel):
#     metadata: DocumentMetadata
#     pages: List[PageData]




########### Phase 2 #############
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(init=False)
class PageData:
    page_no: int
    raw_text: str
    cleaned_text: str
    text: str
    doc_id: Optional[str]
    doc_name: Optional[str]
    extraction_method: Optional[str]

    def __init__(
        self,
        page_no: Optional[int] = None,
        page_number: Optional[int] = None,
        raw_text: Optional[str] = None,
        cleaned_text: Optional[str] = None,
        text: Optional[str] = None,
        doc_id: Optional[str] = None,
        doc_name: Optional[str] = None,
        extraction_method: Optional[str] = None,
        **_: Any,
    ) -> None:
        resolved_page_no = page_no if page_no is not None else page_number
        if resolved_page_no is None:
            raise ValueError("PageData requires 'page_no' or 'page_number'.")

        resolved_raw_text = raw_text if raw_text is not None else ""
        resolved_cleaned_text = (
            cleaned_text
            if cleaned_text is not None
            else (text if text is not None else resolved_raw_text)
        )
        resolved_text = text if text is not None else resolved_cleaned_text

        self.page_no = resolved_page_no
        self.raw_text = resolved_raw_text
        self.cleaned_text = resolved_cleaned_text
        self.text = resolved_text
        self.doc_id = doc_id
        self.doc_name = doc_name
        self.extraction_method = extraction_method

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page_no": self.page_no,
            "raw_text": self.raw_text,
            "cleaned_text": self.cleaned_text,
            "text": self.text,
            "doc_id": self.doc_id,
            "doc_name": self.doc_name,
            "extraction_method": self.extraction_method,
        }

    def model_dump(self) -> Dict[str, Any]:
        return self.to_dict()


@dataclass(init=False)
class DocumentMetadata:
    doc_id: str
    doc_name: str
    total_pages: int
    parsed_pages: int
    empty_pages: int
    extraction_status: str
    company_name: Optional[str]
    company_name_source: Optional[str]
    company_name_confidence: Optional[float]
    financial_year: Optional[str]
    report_title: Optional[str]
    contact_email: Optional[str]
    ocr_used: bool
    ocr_pages_scanned: int
    source_path: Optional[str]

    def __init__(
        self,
        doc_id: str,
        doc_name: str,
        total_pages: int,
        parsed_pages: int,
        empty_pages: int,
        extraction_status: str,
        company_name: Optional[str] = None,
        company_name_source: Optional[str] = None,
        company_name_confidence: Optional[float] = None,
        financial_year: Optional[str] = None,
        report_title: Optional[str] = None,
        contact_email: Optional[str] = None,
        ocr_used: bool = False,
        ocr_pages_scanned: int = 0,
        source_path: Optional[str] = None,
        **_: Any,
    ) -> None:
        self.doc_id = doc_id
        self.doc_name = doc_name
        self.total_pages = total_pages
        self.parsed_pages = parsed_pages
        self.empty_pages = empty_pages
        self.extraction_status = extraction_status
        self.company_name = company_name
        self.company_name_source = company_name_source
        self.company_name_confidence = company_name_confidence
        self.financial_year = financial_year
        self.report_title = report_title
        self.contact_email = contact_email
        self.ocr_used = ocr_used
        self.ocr_pages_scanned = ocr_pages_scanned
        self.source_path = source_path

    def to_dict(self) -> Dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "doc_name": self.doc_name,
            "total_pages": self.total_pages,
            "parsed_pages": self.parsed_pages,
            "empty_pages": self.empty_pages,
            "extraction_status": self.extraction_status,
            "company_name": self.company_name,
            "company_name_source": self.company_name_source,
            "company_name_confidence": self.company_name_confidence,
            "financial_year": self.financial_year,
            "report_title": self.report_title,
            "contact_email": self.contact_email,
            "ocr_used": self.ocr_used,
            "ocr_pages_scanned": self.ocr_pages_scanned,
            "source_path": self.source_path,
        }

    def model_dump(self) -> Dict[str, Any]:
        return self.to_dict()


@dataclass
class ParsedDocument:
    metadata: DocumentMetadata
    pages: List[PageData]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "pages": [page.to_dict() for page in self.pages],
        }

    def model_dump(self) -> Dict[str, Any]:
        return self.to_dict()


@dataclass
class SectionRecord:
    section_id: str
    document_id: str
    title: str
    normalized_title: str
    section_type: str
    start_page: int
    end_page: int
    text: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def model_dump(self) -> Dict[str, Any]:
        return self.to_dict()


@dataclass
class ChunkRecord:
    chunk_id: str
    document_id: str
    document_name: str
    company_name: Optional[str]
    financial_year: Optional[str]
    report_title: Optional[str]
    section_id: str
    section_name: str
    section_type: str
    chunk_index: int
    page_start: int
    page_end: int
    text: str
    word_count: int
    char_count: int
    embedding_model: Optional[str] = None
    embedding: Optional[List[float]] = field(default=None, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def model_dump(self) -> Dict[str, Any]:
        return self.to_dict()