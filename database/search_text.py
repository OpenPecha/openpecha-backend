import unicodedata


def normalize_search_text(value: str) -> str:
    return unicodedata.normalize("NFC", value).casefold()


def build_substring_search_value(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = normalize_search_text(value.strip())
    return normalized or None
