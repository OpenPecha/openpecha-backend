# Lesson 01 — Neo4j Graph Data Model

## Learning Objectives

- Read and interpret `database/neo4j_schema.yaml`
- Identify every node label and its required/optional properties
- Trace every relationship and explain its cardinality
- Distinguish enum nodes from data nodes
- Understand how multilingual strings are stored (Nomen pattern)

---

## 1. Why a Graph?

The data has **deeply interconnected** structure:

- A Text relates to other Texts (translation, commentary chains)
- An Edition belongs to a Text, which belongs to a Work
- Segments live inside Segmentations, which belong to Editions
- Spans point to Segments (or Pages, Notes, BibMeta, Attributes)
- Segments inside alignments point to segments in other editions

SQL would require dozens of join tables. Neo4j makes these traversals natural and performant.

---

## 2. Complete Node Catalogue

### 2.1 Auth & Application Nodes

```
┌─────────────────────────────────────────┐
│ Application                             │
│  id       : string (unique, required)   │
│  name     : string (required)           │
└──────────────────┬──────────────────────┘
                   │  BOUND_TO (0..1)
                   ▼
┌─────────────────────────────────────────┐
│ ApiKey                                  │
│  id            : string (unique)        │
│  name          : string                 │
│  email         : string                 │
│  api_key_hash  : string (unique)        │
│  is_active     : boolean                │
│  created_at    : datetime               │
└─────────────────────────────────────────┘
```

Every API request must present a key that hashes to a stored `api_key_hash`. The key may be bound to an application via `BOUND_TO`.

---

### 2.2 Core Literary Hierarchy

This is the backbone of the entire schema:

```
┌──────────────────────────────┐
│ Work                         │
│  id   : string (unique)      │
│  wiki : string (optional)    │
│  bdrc : string (optional)    │
└────────┬─────────────────────┘
         │  TEXT_OF {original: boolean}  (many Texts per Work)
         │  (A Work can have one "original" Text and many translations)
         ▼
┌──────────────────────────────┐
│ Text                         │
│  id   : string (unique)      │
│  wiki : string (optional)    │
│  bdrc : string (optional)    │
│  date : string (optional)    │
└────────┬─────────────────────┘
         │  EDITION_OF  (many Editions per Text)
         ▼
┌──────────────────────────────┐
│ Edition                      │
│  id       : string (unique)  │
│  colophon : string (optional)│
│  bdrc     : string (optional)│
│  wiki     : string (optional)│
└──────────────────────────────┘
```

**Example instantiation:**

```
(Work {id:"W001", bdrc:"W12345"})
  <-[:TEXT_OF {original:true}]-
  (Text {id:"T001", bdrc:"W12345"})
    <-[:TEXT_OF {original:false}]-
    (Text {id:"T002"})          ← translation
  <-[:EDITION_OF]-
  (Edition {id:"E001"})         ← diplomatic edition of T001
```

---

### 2.3 Text Relationships

A Text can be:
- A **standalone** original (no outgoing TRANSLATION_OF or COMMENTARY_OF)
- A **translation** of another Text
- A **commentary** on another Text

```
Text ──[TRANSLATION_OF]──► Text   (same Work; different language)
Text ──[COMMENTARY_OF]───► Text   (different Work)
```

Rule enforced in code: a Text cannot be both a translation and a commentary.

---

### 2.4 Edition Relationships

```
Edition ──[EDITION_OF]────────► Text
Edition ──[HAS_TYPE]──────────► EditionType   {diplomatic|critical|collated}
Edition ──[HAS_SOURCE]────────► Source        (optional publisher/source)
Edition ──[HAS_INCIPIT_TITLE]─► Nomen         (optional opening title)
```

**EditionType enum:**

| Value | Meaning |
|---|---|
| `diplomatic` | Faithful copy; requires BDRC ID |
| `critical` | Scholarly edition; no BDRC (only one allowed per Text) |
| `collated` | Collated from multiple sources |

---

### 2.5 Segmentation & Segment Nodes

This sub-graph is the most complex. An edition's text is sliced into segments.

```
Edition
  ◄──[SEGMENTATION_OF]──
  Segmentation  (labels: :Display or :Aligned or :Target)
    ◄──[SEGMENT_OF]──
    Segment
      ◄──[SPAN_OF]──
      Span {start: int, end: int}
```

**Segmentation subtypes (Neo4j labels):**

| Label | Created by | Purpose |
|---|---|---|
| `:Display` | `POST /editions/{id}/segmentations` | User-facing, shown in app |
| `:Aligned` | `POST /editions/{id}/alignments` | Source side of alignment |
| `:Target`  | `POST /editions/{id}/alignments` | Target side of alignment |

A Segment may have **multiple Span nodes** (for multi-line segments):

```
Segment ◄──[SPAN_OF]── Span {start: 0,  end: 50}   ← line 1
        ◄──[SPAN_OF]── Span {start: 50, end: 100}   ← line 2
```

Segments are **sorted by min(span.start)** when retrieved.

---

### 2.6 Alignment Relationships

When two editions are aligned, segments cross-reference each other:

```
(Segment in Aligned segmentation) ──[ALIGNED_TO]──► (Segment in Target segmentation)
```

One source segment can align to **multiple** target segments (many-to-many at the alignment level):

```
source_seg_A ──[ALIGNED_TO]──► target_seg_1
source_seg_A ──[ALIGNED_TO]──► target_seg_2
source_seg_B ──[ALIGNED_TO]──► target_seg_2
```

---

### 2.7 Pagination Nodes

```
Edition
  ◄──[PAGINATION_OF]──
  Pagination
    ◄──[VOLUME_OF]──
    Volume {index: int|null}
      ◄──[PAGE_OF]──
      Page {reference: string}
        ◄──[SPAN_OF]──
        Span {start: int, end: int}
```

- Single-volume editions: Volume has `index = null`
- Multi-volume editions: Volume indexes are 1-based, continuous, unique

---

### 2.8 Annotation Nodes (span-based)

Three annotation types attach directly to an Edition via their own relationship:

```
BibliographicMetadata ──[BIBLIOGRAPHY_OF]──► Edition
Note                  ──[NOTE_OF]──────────► Edition
Attribute             ──[ATTRIBUTE_OF]──────► Edition
```

Each points at a Span:

```
Span ──[SPAN_OF]──► BibliographicMetadata
Span ──[SPAN_OF]──► Note
Span ──[SPAN_OF]──► Attribute
```

And each has a type enum node:

```
BibliographicMetadata ──[HAS_TYPE]──► BibliographyType
Note                  ──[HAS_TYPE]──► NoteType
Attribute             ──[HAS_TYPE]──► AttributeType
```

**BibliographyType values:** `colophon`, `incipit`, `alt_incipit`, `alt_title`, `person`, `title`, `author`

**NoteType values:** `durchen` (Tibetan: textual variant notation)

**AttributeType values:** `ocr_confidence`

---

### 2.9 The Nomen Pattern (Multilingual Strings)

This is a key design pattern throughout the schema. Instead of storing a string directly in a node property, multilingual text lives in a **sub-graph**:

```
(Source node) ──[HAS_TITLE / HAS_NAME]──►
  Nomen {id}
    ──[HAS_LOCALIZATION]──► LocalizedText {text: "ལོ་ཙཱ་བ།"}
                                ──[HAS_LANGUAGE]──► Language {code: "bo"}
    ──[HAS_LOCALIZATION]──► LocalizedText {text: "Translator"}
                                ──[HAS_LANGUAGE]──► Language {code: "en"}
    
    (Nomen) ◄──[ALTERNATIVE_OF]── Nomen     ← alt names/titles
```

**Why?**
- Supports arbitrary number of languages
- Supports alternative names (alt titles, pen names, etc.)
- Language relationship carries optional `bcp47` property for dialect specificity (e.g. `bo-x-ewts`)

**Who uses Nomen:**

| Source node | Relationship |
|---|---|
| Text | `HAS_TITLE` |
| Edition | `HAS_INCIPIT_TITLE` |
| Person | `HAS_NAME` |
| Tag | `HAS_TITLE`, `HAS_DESCRIPTION` |
| Category | `HAS_TITLE`, `HAS_DESCRIPTION` |

---

### 2.10 Person & Contribution Nodes

```
Text ──[HAS_CONTRIBUTION]──►
  Contribution
    ──[BY]──► Person {id, bdrc, wiki}
                ──[HAS_NAME]──► Nomen
    ──[WITH_ROLE]──► RoleType {name: "author"|"translator"|"reviser"|"scholar"}
```

AI contributions are also supported:

```
Contribution ──[BY]──► AI {id}
```

---

### 2.11 Taxonomy Nodes

```
Work ──[HAS_CATEGORY]──► Category
                           ──[BELONGS_TO]──► Application
                           ──[HAS_TITLE]──► Nomen
                           ──[HAS_DESCRIPTION]──► Nomen (optional)
                           ──[CHILD_OF]──► Category (optional parent)

Work ──[HAS_TAG]──► Tag
                     ──[BELONGS_TO]──► Application
                     ──[HAS_TITLE]──► Nomen
                     ──[HAS_DESCRIPTION]──► Nomen (optional)

Segment ──[HAS_TAG]──► Tag
```

Tags and Categories are **application-scoped** — they belong to a specific Application node, allowing different consumer apps to have their own taxonomies.

---

## 3. Full Relationship Reference Table

| Relationship | From | To | Cardinality | Notes |
|---|---|---|---|---|
| `BOUND_TO` | ApiKey | Application | 0..1 | Optional app binding |
| `TEXT_OF` | Text | Work | 1 | props: `original: bool` |
| `TRANSLATION_OF` | Text | Text | 0..1 | Different language |
| `COMMENTARY_OF` | Text | Text | 0..1 | Different work |
| `HAS_LANGUAGE` | Text | Language | 1 | props: `bcp47` |
| `HAS_TITLE` | Text/Tag/Category/Edition | Nomen | 1 | |
| `HAS_DESCRIPTION` | Tag/Category | Nomen | 0..1 | |
| `HAS_LICENSE` | Text | LicenseType | 1 | |
| `HAS_CONTRIBUTION` | Text | Contribution | 0..* | |
| `HAS_CATEGORY` | Work | Category | 1 | |
| `HAS_TAG` | Work / Segment | Tag | 0..* | |
| `EDITION_OF` | Edition | Text | 1 | |
| `HAS_TYPE` | Edition | EditionType | 1 | |
| `HAS_SOURCE` | Edition | Source | 0..1 | |
| `HAS_INCIPIT_TITLE` | Edition | Nomen | 0..1 | |
| `SEGMENTATION_OF` | Segmentation | Edition | 1 | |
| `HAS_METADATA` | Segm./Bib./Note/Attr./Pag. | AnnotationMetadata | 0..1 | |
| `SEGMENT_OF` | Segment | Segmentation | 1 | |
| `ALIGNED_TO` | Segment | Segment | 0..* | Source→Target |
| `SPAN_OF` | Span | Segment/Page/BibMeta/Note/Attr | 1 | |
| `PAGINATION_OF` | Pagination | Edition | 1 | |
| `VOLUME_OF` | Volume | Pagination | 1 | |
| `PAGE_OF` | Page | Volume | 1 | |
| `BIBLIOGRAPHY_OF` | BibliographicMetadata | Edition | 1 | |
| `NOTE_OF` | Note | Edition | 1 | |
| `ATTRIBUTE_OF` | Attribute | Edition | 1 | |
| `BY` | Contribution | Person/AI | 1 | |
| `WITH_ROLE` | Contribution | RoleType | 1 | |
| `HAS_NAME` | Person | Nomen | 1 | |
| `BELONGS_TO` | Tag/Category | Application | 1 | |
| `CHILD_OF` | Category | Category | 0..1 | Parent category |
| `HAS_LOCALIZATION` | Nomen | LocalizedText | 1..* | One per language |
| `ALTERNATIVE_OF` | Nomen | Nomen | 0..1 | Alt name/title |
| `HAS_LANGUAGE` | LocalizedText | Language | 1 | props: `bcp47` |

---

## 4. Enum Nodes

Enum nodes are pre-seeded singleton nodes identified by their `name` property. They are **merged, never created**:

```cypher
MERGE (mt:EditionType {name: $type})
```

| Enum Node | Values |
|---|---|
| `EditionType` | `diplomatic`, `critical`, `collated` |
| `LicenseType` | `cc0`, `public`, `cc-by`, `cc-by-sa`, `cc-by-nd`, `cc-by-nc`, `cc-by-nc-sa`, `cc-by-nc-nd`, `copyrighted`, `unknown` |
| `BibliographyType` | `colophon`, `incipit`, `alt_incipit`, `alt_title`, `person`, `title`, `author` |
| `NoteType` | `durchen` |
| `AttributeType` | `ocr_confidence` |
| `RoleType` | `reviser`, `author`, `translator`, `scholar` |

---

## 5. Constraints (from `neo4j_constraints.cypher`)

All `id` and `wiki`/`bdrc` properties with `unique: true` in the schema have corresponding Neo4j uniqueness constraints. This ensures:
- No duplicate node IDs
- Upsert operations (MERGE) work correctly
- ConstraintError is caught in Python and re-raised as `DataConflictError`

---

## 6. Worked Example: Full Text Node Subgraph

Here is a concrete property graph for one Tibetan text with an English translation:

```
(Work {id:"W_001"})
  ← [:TEXT_OF {original:true}] ──
  (Text {id:"T_001"})
    → [:HAS_LANGUAGE] → (Language {code:"bo"})
    → [:HAS_TITLE] → (Nomen {id:"N_001"})
        → [:HAS_LOCALIZATION] → (LocalizedText {text:"བདེ་གཤེགས་སྙིང་པོ།"})
                → [:HAS_LANGUAGE] → (Language {code:"bo"})
        ← [:ALTERNATIVE_OF] ── (Nomen {id:"N_002"})
            → [:HAS_LOCALIZATION] → (LocalizedText {text:"De Shegs Nyingpo"})
                    → [:HAS_LANGUAGE] → (Language {code:"en-phonetic"})
    → [:HAS_CONTRIBUTION] → (Contribution)
        → [:BY] → (Person {id:"P_001"})
        → [:WITH_ROLE] → (RoleType {name:"author"})
    → [:HAS_LICENSE] → (LicenseType {name:"public"})
    → [:TEXT_OF {original:false}] ← (Work {id:"W_001"})

(Text {id:"T_002"})
    → [:TRANSLATION_OF] → (Text {id:"T_001"})
    → [:TEXT_OF {original:false}] → (Work {id:"W_001"})
    → [:HAS_LANGUAGE] → (Language {code:"en"})
```

---

## Next Lesson

→ [02_domain_concepts.md](02_domain_concepts.md) — Work / Text / Edition hierarchy in depth
