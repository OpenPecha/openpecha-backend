# Lesson 02 — Domain Concepts: Work / Text / Edition

## Learning Objectives

- Explain the three-level hierarchy and why it exists
- Know which fields belong at each level
- Understand the rules for translations and commentaries
- Read the Pydantic models and map them to graph nodes
- Know what validation fires on creation vs. update

---

## 1. The Three-Level Hierarchy

```
Work          ← abstract intellectual entity ("the Tibetan text on emptiness")
  └── Text    ← a specific linguistic version ("the Tibetan original")
        │     ← another version ("the English translation")
        └── Edition  ← a concrete realisation ("the 2003 critical edition")
```

This mirrors the bibliographic **FRBR** model (Functional Requirements for Bibliographic Records): Work → Expression (≈ Text) → Manifestation (≈ Edition).

### Why three levels?

| Level | Question it answers |
|---|---|
| **Work** | "What is this about?" — groups all versions under one identity |
| **Text** | "In what language and form?" — carries linguistic metadata, contributions |
| **Edition** | "Which specific realisation?" — carries the base text content and all annotations |

---

## 2. Work Node

**Properties:** `id`, optional `wiki` (Wikidata ID), optional `bdrc` (Buddhist Digital Resource Center ID)

A Work is usually **created implicitly** when a standalone Text is created:

```python
# TextDatabase.create_with_transaction
work_id = generate_id()
await tx.run(TextDatabase.CREATE_STANDALONE_QUERY, work_id=work_id, ...)
```

For a **translation**, the new Text joins the existing Work:
```cypher
MATCH (target:Text {id: $target_id})-[:TEXT_OF]->(w:Work)
CREATE (e:Text {...})
MERGE (e)-[:TEXT_OF {original: false}]->(w)
MERGE (e)-[:TRANSLATION_OF]->(target)
```

For a **commentary**, a brand-new Work is created because a commentary is its own intellectual work:
```cypher
CREATE (w:Work {id: $work_id})
CREATE (e:Text {...})
MERGE (e)-[:COMMENTARY_OF]->(target)
MERGE (e)-[:TEXT_OF {original: true}]->(w)
```

---

## 3. Text Node

### 3.1 Pydantic Model (`models/text.py`)

```python
class TextBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None       # BDRC ID
    wiki: NonEmptyStr | None = None       # Wikidata ID
    date: NonEmptyStr | None = None       # Creation date
    title: LocalizedString                # Required: {lang_code: text, ...}
    alt_titles: list[LocalizedString] | None = None
    language: NonEmptyStr                 # BCP-47 code, e.g. "bo", "en", "sa"
    commentary_of: NonEmptyStr | None = None   # text_id of target
    translation_of: NonEmptyStr | None = None  # text_id of original
    category_id: NonEmptyStr              # Required
    license: LicenseType = LicenseType.PUBLIC_DOMAIN_MARK

class TextInput(TextBase):
    contributions: list[ContributionInput | AIContribution]
    tag_ids: list[NonEmptyStr] = []
```

### 3.2 Validation Rules

| Rule | Where enforced |
|---|---|
| `commentary_of` and `translation_of` are mutually exclusive | `TextBase.validate_text` (Pydantic) |
| `title` must include an entry for the text's own language | `TextBase.validate_text` (Pydantic) |
| Duplicate alt_titles are removed (deduplication vs. primary title) | `remove_duplicate_alt_titles` (Pydantic) |
| Title must be unique across all texts | `DatabaseValidator.validate_text_title_unique` |
| All referenced Person IDs must exist | `DatabaseValidator.validate_person_references` |
| Language code must exist in Language nodes | `DatabaseValidator.validate_language_code_exists` |
| Category must exist | `DatabaseValidator.validate_category_exists` |
| Translation must use a different language than its source | `_validate_translation_language` |

### 3.3 TextOutput (response shape)

```python
class TextOutput(TextBase):
    id: NonEmptyStr
    contributions: list[ContributionOutput | AIContribution]
    commentaries: list[str]   # IDs of texts that comment on this one
    translations: list[str]   # IDs of texts that translate this one
    editions: list[str]       # IDs of editions of this text
    tag_ids: list[str]
```

Notice that `TextOutput` includes **reverse relationship** data (`commentaries`, `translations`, `editions`). These are computed in the Cypher return:

```cypher
commentaries: [(e)<-[:COMMENTARY_OF]-(c_child:Text) | c_child.id],
translations: [(e)<-[:TRANSLATION_OF]-(t_child:Text) | t_child.id],
editions: [(e)<-[:EDITION_OF]-(m:Edition) | m.id]
```

### 3.4 TextPatch (partial update)

```python
class TextPatch(PatchModel):
    # All fields optional; only provided fields are updated
    bdrc / wiki / date / title / alt_titles / language / category_id / license / tag_ids
```

The update is a **merge** — existing values are kept for any field not included in the patch request. The code explicitly reads the current state, merges it, then writes back.

---

## 4. Edition Node

### 4.1 Pydantic Model (`models/edition.py`)

```python
class EditionBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    type: EditionType                    # Required: diplomatic|critical|collated
    source: NonEmptyStr | None = None    # Publisher or source name
    colophon: NonEmptyStr | None = None  # End-page text
    incipit_title: LocalizedString | None = None
    alt_incipit_titles: list[LocalizedString] | None = None
```

### 4.2 Edition-Type Validation Rules

| Rule | When |
|---|---|
| `diplomatic` edition **must** have `bdrc` | Model validator |
| `critical` edition **must not** have `bdrc` | Model validator |
| Only **one** critical edition per Text | `_validate_no_critical_exists` in DB layer |
| `alt_incipit_titles` requires `incipit_title` to be set first | Model validator |

### 4.3 EditionOutput

```python
class EditionOutput(EditionBase):
    id: NonEmptyStr
    text_id: NonEmptyStr     # Which Text this Edition belongs to
```

### 4.4 Creating an Edition (`POST /v2/texts/{text_id}/editions`)

The request body is `EditionRequestModel`:

```python
class EditionRequestModel:
    metadata: EditionInput          # Edition metadata
    content: str                    # Base text content → stored in S3
    segmentation: SegmentationInput | None  # Initial segmentation (see rules below)
    pagination: PaginationInput | None      # Initial pagination (see rules below)
```

**Important — annotation requirements by edition type (`EditionRequestModel.validate_annotation`):**

| Edition type | `segmentation` | `pagination` |
|---|---|---|
| `critical` | **required** | **forbidden** |
| `diplomatic` | **forbidden** | **required** |
| `collated` | optional | optional |

Sending a critical edition without segmentation, or a diplomatic edition without pagination, returns `422 Unprocessable Entity`. These constraints are enforced at the Pydantic model level before anything hits the database.

The endpoint:
1. Generates a new edition ID
2. Stores `content` to S3 at `texts/{text_id}/editions/{edition_id}`
3. Creates Edition node + optional Segmentation/Pagination in a **single transaction**
4. Triggers a background task to notify the search segmenter

```python
# routers/texts.py
edition_id = generate_id()
await storage.store_base_text(text_id, edition_id, data.content)
await db.edition.create(edition=data.metadata, edition_id=edition_id, ...)
background_tasks.add_task(trigger_search_segmenter, edition_id)
```

---

## 5. Relationship Summary Diagram

```
                       ┌─────────────────────────────────────────────────────┐
                       │                     WORK                            │
                       │  id, wiki?, bdrc?                                   │
                       │  (abstract intellectual work)                       │
                       └──────┬──────────────────────────┬───────────────────┘
                              │ TEXT_OF {original: true} │ HAS_CATEGORY
              ┌───────────────┘                          ▼
              ▼                                  ┌──────────────┐
       ┌──────────────────────────────────────┐  │  Category    │
       │                TEXT                  │  └──────────────┘
       │  id, wiki?, bdrc?, date?             │
       │  title (via Nomen), language         │  ┌──────────────┐
       │  license                             ├─►│  Tag         │
       │  contributions (via Contribution)    │  └──────────────┘
       └──────────┬─────────────┬─────────────┘
                  │ EDITION_OF  │ TRANSLATION_OF / COMMENTARY_OF
                  ▼             ▼
          ┌────────────┐  (another Text node)
          │  EDITION   │
          │  id        │
          │  type      │ ──── HAS_TYPE ──► EditionType
          │  bdrc?     │
          │  wiki?     │ ──── HAS_INCIPIT_TITLE ──► Nomen
          │  source?   │ ──── HAS_SOURCE ──────────► Source
          │  colophon? │
          └──────┬─────┘
                 │
       ┌─────────┼──────────┬──────────┬──────────────┬──────────┬──────────┐
       ▼         ▼          ▼          ▼              ▼          ▼          ▼
 Segmentation Pagination TableOfContents Note   Bibliographic Attribute (base text in S3)
   (graph)     (graph)     (graph)     (graph)    (graph)      (graph)
```

---

## 6. ID Generation

All IDs are generated by `identifier.py`. They are opaque strings (not guessable, not sequential). The same function is used for all entity types — no type prefixes are enforced at the identifier level.

```python
from identifier import generate_id

edition_id = generate_id()  # e.g., "01jd8r3p9k..."
```

---

## 7. Data Adapter Layer (`database/data_adapter.py`)

Neo4j returns raw Python dicts. The `DataAdapter` class converts them into Pydantic models:

```python
@staticmethod
def edition(data: dict) -> EditionOutput:
    ...
    return EditionOutput(
        id=data["id"],
        type=EditionType(data["type"]),
        text_id=data["text_id"],
        ...
    )
```

This layer is the boundary between "raw Neo4j record format" and "typed Pydantic output." Any change to the Cypher `RETURN` clause requires a corresponding update in `DataAdapter`.

---

## 8. Searching Texts (Substring Search)

The `title` search filter uses a preprocessed field `search_text` on `LocalizedText` nodes. Before storing, the text is transformed by `build_substring_search_value()` (lowercased, possibly normalized). The query uses `CONTAINS` on this field:

```cypher
MATCH (lt:LocalizedText)
WHERE lt.search_text CONTAINS title_search
MATCH (lt)<-[:HAS_LOCALIZATION]-(n:Nomen)
...
```

This supports case-insensitive arbitrary substring search across all language variants of all titles and alt-titles.

---

## 9. Application Scoping

Tags and Categories are scoped to an Application. When a client requests data with an `X-Application` header, the response filters:

```cypher
WHERE ($application IS NULL OR (t)-[:BELONGS_TO]->(:Application {id: $application}))
```

This allows multiple consumer applications to maintain their own tag/category taxonomies over the same underlying texts.

---

## Practice Questions

1. Can two Text nodes have the same title in the same language? (Check the validator)
2. What happens to the Work node when you create a translation? Does it get a new Work?
3. Why can there only be one critical edition per Text?
4. If a Text has `translation_of: "T_001"`, what graph structure is created in Neo4j?
5. What is stored in S3 vs Neo4j when you create an edition?

---

## Next Lesson

→ [03_api_reference.md](03_api_reference.md) — Complete endpoint reference with examples
