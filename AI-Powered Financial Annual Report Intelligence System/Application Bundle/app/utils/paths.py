from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DOCUMENTS_DIR = DATA_DIR / "raw_documents"
PARSED_DIR = DATA_DIR / "parsed"
PAGE_TEXT_DIR = DATA_DIR / "page_text"
CHUNKS_DIR = DATA_DIR / "chunks"
VECTORSTORE_DIR = DATA_DIR / "vectorstore"
LOGS_DIR = DATA_DIR / "logs"
STATE_DIR = DATA_DIR / "state"


def ensure_data_dirs() -> None:
    for path in [
        DATA_DIR,
        RAW_DOCUMENTS_DIR,
        PARSED_DIR,
        PAGE_TEXT_DIR,
        CHUNKS_DIR,
        VECTORSTORE_DIR,
        LOGS_DIR,
        STATE_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)