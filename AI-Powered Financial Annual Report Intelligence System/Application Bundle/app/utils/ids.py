# import uuid


# def generate_doc_id() -> str:
#     return uuid.uuid4().hex

### Phase 2

import uuid


def generate_doc_id() -> str:
    return uuid.uuid4().hex


def generate_uuid() -> str:
    return str(uuid.uuid4())