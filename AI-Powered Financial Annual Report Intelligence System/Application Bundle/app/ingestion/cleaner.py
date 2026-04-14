import re


def clean_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(line.rstrip() for line in text.split("\n"))
    text = re.sub(r"[ \t]{2,}", " ", text)

    cleaned_lines = []
    for line in text.split("\n"):
        stripped = line.strip()

        # Remove standalone page number lines like "12" or "- 12 -"
        if re.fullmatch(r"[-–—]?\s*\d+\s*[-–—]?", stripped):
            continue

        cleaned_lines.append(line)

    text = "\n".join(cleaned_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()