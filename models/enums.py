from enum import StrEnum


class TextType(StrEnum):
    ROOT = "root"
    COMMENTARY = "commentary"
    TRANSLATION = "translation"
    TRANSLATION_SOURCE = "translation_source"
    NONE = "none"


class ContributorRole(StrEnum):
    TRANSLATOR = "translator"
    REVISER = "reviser"
    AUTHOR = "author"
    SCHOLAR = "scholar"


class AnnotationType(StrEnum):
    SEGMENTATION = "segmentation"
    ALIGNMENT = "alignment"
    PAGINATION = "pagination"
    VERSION = "version"
    BIBLIOGRAPHY = "bibliography"
    TABLE_OF_CONTENTS = "table_of_contents"
    DURCHEN = "durchen"
    SEARCH_SEGMENTATION = "search_segmentation"


class EditionType(StrEnum):
    DIPLOMATIC = "diplomatic"
    CRITICAL = "critical"
    COLLATED = "collated"


class LicenseType(StrEnum):
    # based on https://creativecommons.org/licenses/
    CC0 = "cc0"
    PUBLIC_DOMAIN_MARK = "public"
    CC_BY = "cc-by"
    CC_BY_SA = "cc-by-sa"
    CC_BY_ND = "cc-by-nd"
    CC_BY_NC = "cc-by-nc"
    CC_BY_NC_SA = "cc-by-nc-sa"
    CC_BY_NC_ND = "cc-by-nc-nd"
    UNDER_COPYRIGHT = "copyrighted"
    UNKNOWN = "unknown"


class NoteType(StrEnum):
    DURCHEN = "durchen"


class BibliographyType(StrEnum):
    COLOPHON = "colophon"
    INCIPIT = "incipit"
    ALT_INCIPIT = "alt_incipit"
    ALT_TITLE = "alt_title"
    PERSON = "person"
    TITLE = "title"
    AUTHOR = "author"


class AttributeType(StrEnum):
    OCR_CONFIDENCE = "ocr_confidence"
