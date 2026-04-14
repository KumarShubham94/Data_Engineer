import fitz
import pytesseract
from PIL import Image


def extract_ocr_text_from_page(
    pdf_path: str,
    page_no: int,
    zoom_x: float = 2.0,
    zoom_y: float = 2.0,
) -> str:
    pdf = fitz.open(pdf_path)
    try:
        if page_no < 1 or page_no > len(pdf):
            return ""

        page = pdf[page_no - 1]
        matrix = fitz.Matrix(zoom_x, zoom_y)
        pix = page.get_pixmap(matrix=matrix)

        mode = "RGB" if pix.alpha == 0 else "RGBA"
        image = Image.frombytes(mode, [pix.width, pix.height], pix.samples)

        return pytesseract.image_to_string(image) or ""
    finally:
        pdf.close()


def extract_ocr_text_from_last_page(pdf_path: str) -> tuple[int, str]:
    pdf = fitz.open(pdf_path)
    try:
        total_pages = len(pdf)
    finally:
        pdf.close()

    ocr_text = extract_ocr_text_from_page(pdf_path=pdf_path, page_no=total_pages)
    return total_pages, ocr_text