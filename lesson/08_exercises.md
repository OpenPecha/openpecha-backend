# Lesson 08 — Hands-On Lab Exercises

These exercises are ordered by difficulty. Each has a clear scenario, acceptance criteria, and hints. Work through them against a running instance of the API.

---

## Setup

```bash
export API_KEY="your-api-key"
export BASE="http://localhost:8000"
export H="X-Api-Key: $API_KEY"
```

All examples use `curl`. You can also use the interactive docs at `$BASE/docs`.

---

## Exercise 1 — Create a Complete Text + Edition

**Scenario:** You're adding the Tibetan text "ཤེས་རབ་སྙིང་པོ།" (Heart Sutra) to the database.

**Requirements:**
1. A category named "Sutra Literature" must exist
2. A person (Nagarjuna) with BDRC `P7103` must exist
3. Create the Tibetan root text
4. Create a diplomatic edition with 5 segments

**Step 1: Create category** (needs X-Application header — use an existing application ID)

```bash
curl -X POST "$BASE/v2/categories" \
  -H "$H" \
  -H "X-Application: your-app-id" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {"en": "Sutra Literature", "bo": "མདོ།"},
    "description": {"en": "Canonical sutras"}
  }'
# Save: CAT_ID=...
```

**Step 2: Create person**

```bash
curl -X POST "$BASE/v2/persons" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {"en": "Nagarjuna", "bo": "ཀླུ་སྒྲུབ།"},
    "bdrc": "P7103"
  }'
# Save: PERSON_ID=...
```

**Step 3: Create text**

```bash
curl -X POST "$BASE/v2/texts" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d "{
    \"title\": {\"bo\": \"ཤེས་རབ་སྙིང་པོ།\", \"en\": \"Heart Sutra\"},
    \"language\": \"bo\",
    \"category_id\": \"$CAT_ID\",
    \"license\": \"public\",
    \"contributions\": [{\"person_id\": \"$PERSON_ID\", \"role\": \"author\"}]
  }"
# Save: TEXT_ID=...
```

**Step 4: Create edition with pagination**

Diplomatic editions require `pagination` and must NOT include `segmentation`. The content is 50 chars; map it to one page.

```bash
curl -X POST "$BASE/v2/texts/$TEXT_ID/editions" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "type": "diplomatic",
      "bdrc": "W12345",
      "source": "BDRC scan"
    },
    "content": "ཤེས་རབ་སྙིང་པོ། འདི་སྐད་བདག་གིས་ཐོས་པ། གང་གི་ཚེ། གང་ཞིག། དེ་ལྟར།",
    "pagination": {
      "volumes": [
        {
          "pages": [
            {"reference": "1a", "lines": [{"start": 0, "end": 50}]}
          ]
        }
      ]
    }
  }'
# Save: EDITION_ID=...
```

**Step 5: Add a segmentation separately** (diplomatic editions get their segmentation via a separate POST after creation)

```bash
curl -X POST "$BASE/v2/editions/$EDITION_ID/segmentations" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "segments": [
      {"lines": [{"start": 0, "end": 13}]},
      {"lines": [{"start": 13, "end": 28}]},
      {"lines": [{"start": 28, "end": 36}]},
      {"lines": [{"start": 36, "end": 43}]},
      {"lines": [{"start": 43, "end": 50}]}
    ]
  }'
# Save: SGN_ID=...
```

**Acceptance criteria:**
- [ ] `GET /v2/texts/$TEXT_ID` returns the text with `editions: ["$EDITION_ID"]`
- [ ] `GET /v2/editions/$EDITION_ID/pagination` returns 1 pagination with 1 page
- [ ] `GET /v2/editions/$EDITION_ID/segmentations` returns 1 segmentation with 5 segments
- [ ] `GET /v2/editions/$EDITION_ID/content` returns the base text

---

## Exercise 2 — Add a Translation and Align It

**Scenario:** Add an English translation of the Heart Sutra from Exercise 1 and align it sentence-by-sentence.

**Step 1: Create the translation text**

```bash
curl -X POST "$BASE/v2/texts" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d "{
    \"title\": {\"en\": \"Heart Sutra (English)\"},
    \"language\": \"en\",
    \"translation_of\": \"$TEXT_ID\",
    \"category_id\": \"$CAT_ID\",
    \"license\": \"cc-by\",
    \"contributions\": []
  }"
# Save: EN_TEXT_ID=...
```

**Step 2: Create the English edition**

Critical editions require `segmentation` (and forbid `pagination`). The English content is 54 chars; create 5 segments to match the alignment plan.

```bash
curl -X POST "$BASE/v2/texts/$EN_TEXT_ID/editions" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {"type": "critical"},
    "content": "Thus I have heard. At one time. Something. That. Such.",
    "segmentation": {
      "segments": [
        {"lines": [{"start": 0, "end": 19}]},
        {"lines": [{"start": 19, "end": 32}]},
        {"lines": [{"start": 32, "end": 43}]},
        {"lines": [{"start": 43, "end": 49}]},
        {"lines": [{"start": 49, "end": 54}]}
      ]
    }
  }'
# Save: EN_EDITION_ID=...
```

**Step 3: Create alignment**

Note: The English content is 53 chars total. Map each clause:
- "Thus I have heard. " → [0,19)
- "At one time. " → [19,32)
- "Something. " → [32,43)
- "That. " → [43,49)
- "Such." → [49,54)

Tibetan segments were [0,13), [13,28), [28,36), [36,43), [43,50).

```bash
curl -X POST "$BASE/v2/editions/$EDITION_ID/alignments" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d "{
    \"target_edition_id\": \"$EN_EDITION_ID\",
    \"target_segments\": [
      {\"lines\": [{\"start\": 0, \"end\": 19}]},
      {\"lines\": [{\"start\": 19, \"end\": 32}]},
      {\"lines\": [{\"start\": 32, \"end\": 43}]},
      {\"lines\": [{\"start\": 43, \"end\": 49}]},
      {\"lines\": [{\"start\": 49, \"end\": 54}]}
    ],
    \"aligned_segments\": [
      {\"lines\": [{\"start\": 0, \"end\": 13}], \"target_indices\": [0]},
      {\"lines\": [{\"start\": 13, \"end\": 28}], \"target_indices\": [1]},
      {\"lines\": [{\"start\": 28, \"end\": 36}], \"target_indices\": [2]},
      {\"lines\": [{\"start\": 36, \"end\": 43}], \"target_indices\": [3]},
      {\"lines\": [{\"start\": 43, \"end\": 50}], \"target_indices\": [4]}
    ]
  }"
# Save: ALIGNMENT_ID=...
```

**Acceptance criteria:**
- [ ] `GET /v2/editions/$EDITION_ID/alignments` returns 1 alignment with 5 aligned segments
- [ ] `GET /v2/editions/$EN_EDITION_ID/alignments` also returns the same alignment (bidirectional)
- [ ] `GET /v2/editions/$EN_TEXT_ID` shows `translation_of: "$TEXT_ID"`
- [ ] `GET /v2/editions/$EDITION_ID/related` returns the English edition

---

## Exercise 3 — Content Patch and Span Verification

**Scenario:** Fix a typo in the first segment of the Tibetan edition from Exercise 1.

The current text at position [0,13) reads "ཤེས་རབ་སྙིང་པོ།". Say you need to insert a space after position 8.

**Step 1: Get current content**

```bash
curl "$BASE/v2/editions/$EDITION_ID/content" -H "$H"
```

**Step 2: Get current segmentation (record span positions)**

```bash
curl "$BASE/v2/editions/$EDITION_ID/segmentations" -H "$H"
```

**Step 3: Apply insert**

```bash
curl -X PATCH "$BASE/v2/editions/$EDITION_ID/content" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{"type": "insert", "position": 8, "text": " "}'
```

**Step 4: Verify spans moved correctly**

```bash
curl "$BASE/v2/editions/$EDITION_ID/segmentations" -H "$H"
```

**Expected:**
- Segment 1 that was `[0,13)` should now be `[0,14)` (expanded by 1)
- Segment 2 that was `[13,28)` should now be `[14,29)` (shifted by 1)
- All subsequent segments should shift by 1

**Acceptance criteria:**
- [ ] New content length = old + 1
- [ ] Segment 1 spans the new character
- [ ] No gaps between segments

---

## Exercise 4 — Add Bibliographic Metadata

**Scenario:** The last segment of the Heart Sutra edition contains a colophon. Mark it.

Assuming the colophon spans positions [43,50):

```bash
curl -X POST "$BASE/v2/editions/$EDITION_ID/bibliographic" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "span": {"start": 43, "end": 50},
    "type": "colophon"
  }'
# Save: BIB_ID=...
```

**Verify:**

```bash
curl "$BASE/v2/editions/$EDITION_ID/bibliographic" -H "$H"
```

Expected: array with one entry of type "colophon".

---

## Exercise 5 — Pagination

**Scenario:** Add pagination to the edition. Assume the text spans one folio (one page).

```bash
curl -X POST "$BASE/v2/editions/$EDITION_ID/pagination" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "volumes": [
      {
        "pages": [
          {
            "reference": "1a",
            "lines": [
              {"start": 0, "end": 25},
              {"start": 25, "end": 50}
            ]
          }
        ]
      }
    ]
  }'
```

**Now try to add a second pagination — expect a conflict error:**

```bash
curl -X POST "$BASE/v2/editions/$EDITION_ID/pagination" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "volumes": [{"pages": [{"reference": "1b", "lines": [{"start": 0, "end": 50}]}]}]
  }'
# Should return 4xx error
```

---

## Exercise 6 — Text Metadata Update

**Scenario:** Add an alternative title to the Heart Sutra text.

```bash
curl -X PATCH "$BASE/v2/texts/$TEXT_ID" \
  -H "$H" \
  -H "Content-Type: application/json" \
  -d '{
    "alt_titles": [
      {"en": "Prajnaparamita Hridaya Sutra"},
      {"sa": "प्रज्ञापारमिताहृदयसूत्र"}
    ]
  }'
```

**Verify:**

```bash
curl "$BASE/v2/texts/$TEXT_ID" -H "$H"
```

Expected: `alt_titles` contains the two entries. Primary title unchanged.

---

## Exercise 7 — Delete and Verify Cascade

**Scenario:** Delete the alignment from Exercise 2 and verify both segmentations are gone.

```bash
# Record segmentation IDs first
curl "$BASE/v2/editions/$EDITION_ID/alignments" -H "$H"
# Note the segmentation_id in aligned_segments (that's the Aligned segmentation)
# Note the segmentation_id in target_segments (that's the Target segmentation)

# Delete the alignment
curl -X DELETE "$BASE/v2/alignments/$ALIGNMENT_ID" -H "$H"
# Expect 204

# Verify both segmentations are gone
curl "$BASE/v2/segmentations/$ALIGNMENT_ID" -H "$H"
# Expect 404
```

---

## Exercise 8 — Related Segments Query

**Scenario:** Find all related segments for a specific span within the Tibetan edition.

```bash
curl "$BASE/v2/editions/$EDITION_ID/segments/related?span_start=0&span_end=13&limit=10&offset=0" \
  -H "$H"
```

After re-creating the alignment from Exercise 2, this should return the English segment covering `[0,19)` in E_EN.

**Response shape:** `PaginatedResponse[SegmentOutput]` — a flat list where each item includes `edition_id`, `text_id`, and `segmentation_id`.

**Acceptance criteria:**
- [ ] Response has `items` array and `has_more` field
- [ ] At least one item has `edition_id == $EN_EDITION_ID`
- [ ] That item's lines span `[0,19)` in the English text

---

## Exercise 9 — Edge Case: Invalid Operations

Test the validation rules:

**A. Try to create a critical edition for a text that already has one:**

```bash
# Create first critical edition
curl -X POST "$BASE/v2/texts/$EN_TEXT_ID/editions" -H "$H" -H "Content-Type: application/json" \
  -d '{"metadata": {"type": "critical"}, "content": "test"}'

# Try to create a second
curl -X POST "$BASE/v2/texts/$EN_TEXT_ID/editions" -H "$H" -H "Content-Type: application/json" \
  -d '{"metadata": {"type": "critical"}, "content": "test2"}'
# Expect: 409 Conflict - "Critical edition already present"
```

**B. Try to create a diplomatic edition without BDRC:**

```bash
curl -X POST "$BASE/v2/texts/$TEXT_ID/editions" -H "$H" -H "Content-Type: application/json" \
  -d '{"metadata": {"type": "diplomatic"}, "content": "test"}'
# Expect: 422 - "When type is 'diplomatic', bdrc must be provided"
```

**C. Try to create a translation in the same language:**

```bash
curl -X POST "$BASE/v2/texts" -H "$H" -H "Content-Type: application/json" \
  -d "{
    \"title\": {\"bo\": \"ཐར་ལམ།\"},
    \"language\": \"bo\",
    \"translation_of\": \"$TEXT_ID\",
    \"category_id\": \"$CAT_ID\",
    \"license\": \"public\",
    \"contributions\": []
  }"
# Expect: 4xx - "Translation must have a different language than the target text"
```

**D. Try DELETE on an Aligned segmentation:**

```bash
# Get the alignment ID (this is the :Aligned segmentation ID)
ALIGN_SGN=$(curl "$BASE/v2/editions/$EDITION_ID/alignments" -H "$H" | \
  python3 -c "import json,sys; data=json.load(sys.stdin); print(data[0]['id'])")

# Try to delete it via the segmentations endpoint (not the alignments endpoint)
curl -X DELETE "$BASE/v2/segmentations/$ALIGN_SGN" -H "$H"
# Expect: 4xx - "Segmentation is part of an alignment. Use alignment delete instead."
```

Note: there is no `GET /v2/segments/{id}` endpoint. To inspect a segment, use `GET /v2/segments/{id}/content` for its text, or fetch the parent segmentation via `GET /v2/segmentations/{segmentation_id}`.

---

## Exercise 10 — Full Graph Query (Cypher Exploration)

If you have access to Neo4j Browser:

**Query 1: Find all nodes created for your text**

```cypher
MATCH path = (t:Text {id: "$TEXT_ID"})-[*1..3]-(n)
RETURN path LIMIT 50
```

**Query 2: Trace the full alignment path**

```cypher
MATCH (e1:Edition {id: "$EDITION_ID"})
      <-[:SEGMENTATION_OF]-(s1:Segmentation:Aligned)
      <-[:SEGMENT_OF]-(seg1:Segment)
      -[:ALIGNED_TO]->(seg2:Segment)
      -[:SEGMENT_OF]->(s2:Segmentation:Target)
      -[:SEGMENTATION_OF]->(e2:Edition)
RETURN e1.id, seg1.id, seg2.id, e2.id
```

**Query 3: Find all spans for an edition**

```cypher
MATCH (e:Edition {id: "$EDITION_ID"})
      <-[:SEGMENTATION_OF]-(:Segmentation:Display)
      <-[:SEGMENT_OF]-(:Segment)
      <-[:SPAN_OF]-(span:Span)
RETURN span.start, span.end
ORDER BY span.start
```

---

## Final Project

Build a small Python script that:

1. Creates a Tibetan text; creates a diplomatic edition with pagination covering the full text
2. Adds a display segmentation with 3 segments (via `POST /v2/editions/{id}/segmentations`)
3. Creates an English translation text; creates a critical edition with 3 segments included at creation time
4. Creates a 1:1 alignment between the Tibetan and English editions
5. Patches the Tibetan content (insert 5 characters at position 0)
6. Fetches the segmentation again and verifies all 3 segment spans shifted by 5
7. Calls `GET /v2/editions/{tibetan_edition_id}/segments/related?span_start=0&span_end=X` and prints the `edition_id` and `lines` of each returned `SegmentOutput`

Response from step 7 is `PaginatedResponse[SegmentOutput]` — iterate `response["items"]`.

Use the `httpx` or `requests` library. All API keys should come from environment variables.
