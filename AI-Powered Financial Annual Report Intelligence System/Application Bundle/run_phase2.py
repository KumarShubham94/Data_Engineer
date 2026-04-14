from app.pipeline.phase2_pipeline import run_phase2_from_latest


def main() -> None:
    document_meta, chunks = run_phase2_from_latest()

    print(f"Document Name  : {document_meta.doc_name}")
    print(f"Document ID    : {document_meta.doc_id}")
    print(f"Company Name   : {document_meta.company_name}")
    print(f"Financial Year : {document_meta.financial_year}")
    print(f"Total Chunks   : {len(chunks)}")

    if chunks:
        first_chunk = chunks[0]
        print("\nFirst Chunk Metadata")
        print(f"Chunk ID       : {first_chunk.chunk_id}")
        print(f"Section Name   : {first_chunk.section_name}")
        print(f"Section Type   : {first_chunk.section_type}")
        print(f"Page Range     : {first_chunk.page_start} - {first_chunk.page_end}")
        print(f"Word Count     : {first_chunk.word_count}")

        print("\nFirst Chunk Preview")
        print(first_chunk.text[:1000])


if __name__ == "__main__":
    main()