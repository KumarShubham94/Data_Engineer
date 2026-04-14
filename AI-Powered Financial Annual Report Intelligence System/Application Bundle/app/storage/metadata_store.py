from __future__ import annotations

from typing import Any

from app.storage.file_store import save_metadata as _save_metadata


def save_metadata(doc_id: str, metadata: Any) -> str:
    return _save_metadata(doc_id, metadata)