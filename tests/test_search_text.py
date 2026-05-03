# ruff: noqa: S101
import unicodedata

from database.search_text import build_substring_search_value, normalize_search_text


def test_normalize_search_text_casefolds_latin_text() -> None:
    assert normalize_search_text("Straße DHARMA") == "strasse dharma"


def test_normalize_search_text_canonicalizes_unicode_sequences() -> None:
    decomposed = "Praj" + "n\u0303" + "a"

    assert normalize_search_text(decomposed) == unicodedata.normalize("NFC", decomposed).casefold()


def test_build_substring_search_value_strips_blank_queries() -> None:
    assert build_substring_search_value("  DHARMA  ") == "dharma"
    assert build_substring_search_value("   ") is None
    assert build_substring_search_value(None) is None
