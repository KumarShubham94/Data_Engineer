from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from app.utils.paths import (
    LOGS_DIR,
    PAGE_TEXT_DIR,
    PARSED_DIR,
    RAW_DOCUMENTS_DIR,
    ensure_data_dirs,
)


def copy_raw_file(source_path: str | Path, filename: str | None = None) -> str:
    ensure_data_dirs()

    source_path = Path(source_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    target_name = filename or source_path.name
    destination = RAW_DOCUMENTS_DIR / target_name

    shutil.copy2(source_path, destination)
    return str(destination)


def save_json(data: Any, output_path: str | Path) -> str:
    ensure_data_dirs()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return str(output_path)


def save_page_text(doc_id: str, pages: list[Any]) -> str:
    ensure_data_dirs()

    output_path = PAGE_TEXT_DIR / f"{doc_id}_pages.json"
    payload = [
        page.model_dump() if hasattr(page, "model_dump") else page.to_dict()
        for page in pages
    ]
    return save_json(payload, output_path)


def save_metadata(doc_id: str, metadata: Any) -> str:
    ensure_data_dirs()

    output_path = PARSED_DIR / f"{doc_id}_metadata.json"
    payload = metadata.model_dump() if hasattr(metadata, "model_dump") else metadata.to_dict()
    return save_json(payload, output_path)


def get_log_file_path(filename: str = "app.log") -> str:
    ensure_data_dirs()
    return str(LOGS_DIR / filename)