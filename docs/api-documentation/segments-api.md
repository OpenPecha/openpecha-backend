# Segments API Documentation

This document provides comprehensive documentation for all Segments-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Segment Endpoints](#segment-endpoints)
   - [Get Segment Content](#get-segment-content)
   - [Find Related Segments](#find-related-segments)
   - [Search Segments](#search-segments)
4. [Edition-Level Segment Queries](#edition-level-segment-queries)
   - [Find Related Segments by Span](#find-related-segments-by-span)
5. [Data Models](#data-models)
6. [Error Responses](#error-responses)
7. [Best Practices](#best-practices)

---

## Overview

Segments are portions of edition content defined by character spans (one or more ranges). They are created as part of segmentation annotations and enable fine-grained text operations, alignment between editions, and targeted content retrieval.

### Key Concepts

- **Segment**: A logical unit of text defined by one or more character spans
- **Lines**: Character spans that define segment boundaries (can be non-contiguous)
- **Segmentation**: Collection of segments that divide an edition's content
- **Alignment**: Relationship between segments in different editions
- **Related Segments**: Segments that are aligned together across editions

### Segment Creation

Segments are not created directly. They are created as part of:
1. **Segmentation annotations** via `POST /v2/editions/{edition_id}/annotations`
2. **Alignment annotations** via `POST /v2/editions/{edition_id}/annotations`
3. **Edition creation** with inline annotations via `POST /v2/texts/{text_id}/editions`

### Base URL

```
Development: https://api-l25bgmwqoa-uc.a.run.app
Production: https://api-aq25662yyq-uc.a.run.app
Test: https://api-kwgjscy6gq-uc.a.run.app
Local: http://127.0.0.1:5001/pecha-backend-test-3a4d0/us-central1/api
```

---

## Authentication

All API requests require authentication using an API key.

**Header:**
```
X-API-Key: your_api_key_here
```

---

## Segment Endpoints

### Get Segment Content

Retrieve the base text content for a segment based on its character span(s).

**Endpoint:**
```
GET /v2/segments/{segment_id}/content
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `segment_id` | string | path | Yes | The ID of the segment |

**Response: 200 OK**

```json
"This is the text content of the segment."
```

**Response Type:** Plain text string (JSON-encoded)

The response contains the actual text content extracted from the edition based on the segment's span(s).

**Error Responses:**
- `404 Not Found`: Segment does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/SEG001/content" \
  -H "X-API-Key: your_api_key"
```

**Use Cases:**
- Display segment text in UI
- Export segment content for analysis
- Retrieve aligned text portions
- Build parallel text views

---

### Find Related Segments

Find all segments that are aligned to a specific segment. Returns segments from other editions that have alignment relationships with the query segment.

**Endpoint:**
```
GET /v2/segments/{segment_id}/related
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `segment_id` | string | path | Yes | The ID of the segment to find related segments for |

**Response: 200 OK**

```json
[
  {
    "id": "SEG001",
    "manifestation_id": "M12345678",
    "text_id": "E12345678",
    "lines": [
      {
        "start": 0,
        "end": 100
      }
    ]
  },
  {
    "id": "SEG002",
    "manifestation_id": "M87654321",
    "text_id": "E87654321",
    "lines": [
      {
        "start": 100,
        "end": 200
      }
    ]
  }
]
```

**Response Structure:**
- Returns array of segment objects
- Each segment includes its manifestation_id and text_id
- Segments from different editions/manifestations
- Empty array `[]` if no related segments

**Error Responses:**
- `404 Not Found`: Segment does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/SEG001/related" \
  -H "X-API-Key: your_api_key"
```

**Use Cases:**
- Find translation segments for source text segment
- Navigate between aligned editions
- Display parallel text views
- Build cross-edition references

---

### Search Segments

Search segments by forwarding request to external search API and enriching results with overlapping segmentation annotation segment IDs.

**Endpoint:**
```
GET /v2/segments/search
```

**Parameters:**

| Name | Type | Location | Required | Default | Description |
|------|------|----------|----------|---------|-------------|
| `query` | string | query | Yes | - | The search query text |
| `search_type` | string | query | No | "hybrid" | Type of search: `hybrid`, `bm25`, `semantic`, or `exact` |
| `limit` | integer | query | No | 10 | Maximum number of results (1-100) |
| `title` | string | query | No | - | Filter results by title |
| `return_text` | boolean | query | No | true | Whether to include text content in results |

**Search Types:**

| Type | Description | Best For |
|------|-------------|----------|
| `hybrid` | Combines BM25 and semantic search | General purpose, balanced results |
| `bm25` | Keyword-based ranking (BM25 algorithm) | Exact term matching, keyword search |
| `semantic` | Vector similarity search | Meaning-based search, conceptual queries |
| `exact` | Exact text matching | Precise phrase matching |

**Response: 200 OK**

```json
{
  "query": "བོད་ཀྱི་རིག་གནས།",
  "search_type": "hybrid",
  "results": [
    {
      "id": "SEG_SEARCH_001",
      "distance": 0.85,
      "entity": {
        "text": "Sample text content from the segment"
      },
      "segmentation_ids": [
        "SEG_001",
        "SEG_002",
        "SEG_003"
      ]
    },
    {
      "id": "SEG_SEARCH_002",
      "distance": 0.78,
      "entity": {
        "text": "Another matching text segment"
      },
      "segmentation_ids": [
        "SEG_004",
        "SEG_005"
      ]
    }
  ],
  "count": 2
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `query` | string | The search query that was used |
| `search_type` | string | The search type that was used |
| `results` | array | List of search results with enriched data |
| `results[].id` | string | Search segmentation segment ID |
| `results[].distance` | number | Search relevance score/distance |
| `results[].entity` | object | Additional entity data (includes `text` if return_text=true) |
| `results[].segmentation_ids` | array | Overlapping segmentation annotation segment IDs |
| `count` | integer | Number of results returned |

**Key Feature - Segmentation Mapping:**

The search endpoint enriches external search results with local segmentation IDs. For each search result:
1. The search API returns a segment with its span
2. The endpoint finds all segmentation annotation segments that overlap with that span
3. Returns both the search result and the overlapping segmentation IDs

This enables:
- Mapping search results to your annotation segments
- Finding related segments via alignment
- Integrating external search with local segmentation

**Error Responses:**
- `400 Bad Request`: Invalid query parameters
- `422 Validation Error`: Validation failed (e.g., invalid search_type, limit out of range)
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Basic search with default settings
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=བོད་ཀྱི་རིག་གནས།" \
  -H "X-API-Key: your_api_key"

# Hybrid search with custom limit
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=Heart%20Sutra&search_type=hybrid&limit=20" \
  -H "X-API-Key: your_api_key"

# BM25 keyword search
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=prajnaparamita&search_type=bm25" \
  -H "X-API-Key: your_api_key"

# Semantic search
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=emptiness%20teaching&search_type=semantic" \
  -H "X-API-Key: your_api_key"

# Exact phrase search
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=སྟོང་པ་ཉིད་&search_type=exact" \
  -H "X-API-Key: your_api_key"

# Search with title filter
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=wisdom&title=Heart%20Sutra" \
  -H "X-API-Key: your_api_key"

# Search without text content (faster)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=meditation&return_text=false" \
  -H "X-API-Key: your_api_key"
```

---

## Edition-Level Segment Queries

### Find Related Segments by Span

Find segments that overlap with a given character span in the edition, then return all related (aligned) segments from other manifestations.

**Endpoint:**
```
GET /v2/editions/{edition_id}/segments/related
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition (manifestation) |
| `span_start` | integer | query | Yes | Start character position (inclusive, >= 0) |
| `span_end` | integer | query | Yes | End character position (exclusive, >= 1) |

**How It Works:**

1. Finds all segments in the specified edition that overlap with `[span_start, span_end)`
2. For each overlapping segment, finds all aligned segments in other editions
3. Returns the complete set of related segments

**Response: 200 OK**

```json
[
  {
    "id": "SEG001",
    "manifestation_id": "M12345678",
    "text_id": "E12345678",
    "lines": [
      {
        "start": 0,
        "end": 100
      }
    ]
  },
  {
    "id": "SEG002",
    "manifestation_id": "M87654321",
    "text_id": "E87654321",
    "lines": [
      {
        "start": 50,
        "end": 150
      }
    ]
  }
]
```

**Empty Result:**

```json
[]
```

**Error Responses:**
- `400 Bad Request`: Invalid span parameters (e.g., start >= end, negative values)
- `404 Not Found`: Edition does not exist
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Find related segments for first 100 characters
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/segments/related?span_start=0&span_end=100" \
  -H "X-API-Key: your_api_key"

# Find related segments for middle portion
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/segments/related?span_start=500&span_end=750" \
  -H "X-API-Key: your_api_key"

# Find related segments for single character position
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/segments/related?span_start=100&span_end=101" \
  -H "X-API-Key: your_api_key"
```

**Use Cases:**
- User selects text range → show aligned translations
- Click on segment → navigate to corresponding segment in other edition
- Build parallel text viewers
- Cross-reference between source and translation

---

## Data Models

### SegmentOutput

Schema for segment data:

```typescript
{
  id: string;              // Unique segment identifier
  manifestation_id: string; // ID of the manifestation this segment belongs to
  text_id: string;         // ID of the expression (text) this segment belongs to
  lines: SpanModel[];      // Character spans (at least 1)
}
```

**Example:**

```json
{
  "id": "segment_001",
  "manifestation_id": "M12345678",
  "text_id": "E12345678",
  "lines": [
    {
      "start": 0,
      "end": 100
    }
  ]
}
```

**Multi-Span Segment Example:**

```json
{
  "id": "segment_002",
  "manifestation_id": "M12345678",
  "text_id": "E12345678",
  "lines": [
    {
      "start": 0,
      "end": 50
    },
    {
      "start": 100,
      "end": 150
    }
  ]
}
```

---

### SpanModel

Character range in text content:

```typescript
{
  start: number;   // Start position (inclusive, >= 0)
  end: number;     // End position (exclusive, >= 1, must be > start)
}
```

**Conventions:**
- Zero-based indexing
- Start is inclusive, end is exclusive
- Format: `[start, end)`
- Valid span: `0 <= start < end <= content_length`

---

### SearchResult

Schema for search result:

```typescript
{
  id: string;              // Search segmentation segment ID
  distance: number;        // Search relevance score (higher = more relevant)
  entity: {                // Search result metadata
    text?: string;         // Segment text (if return_text=true)
    [key: string]: any;    // Additional search metadata
  };
  segmentation_ids: string[]; // Overlapping segmentation annotation segment IDs
}
```

**Example:**

```json
{
  "id": "SEG_SEARCH_001",
  "distance": 0.92,
  "entity": {
    "text": "བོད་ཀྱི་རིག་གནས་ནི་མི་རབས་མང་པོའི་རིང་ལ་གོང་འཕེལ་བྱུང་བ་ཞིག་ཡིན།",
    "title": "Tibetan Culture",
    "author": "Scholar Name"
  },
  "segmentation_ids": [
    "SEG_ANNO_001",
    "SEG_ANNO_002"
  ]
}
```

---

### SearchResponse

Complete search response:

```typescript
{
  query: string;           // The search query used
  search_type: string;     // Search type used (hybrid, bm25, semantic, exact)
  results: SearchResult[]; // Array of search results
  count: number;           // Number of results returned
}
```

**Example:**

```json
{
  "query": "རིག་གནས།",
  "search_type": "hybrid",
  "results": [
    {
      "id": "SEG_SEARCH_001",
      "distance": 0.92,
      "entity": {
        "text": "བོད་ཀྱི་རིག་གནས།"
      },
      "segmentation_ids": ["SEG_001", "SEG_002"]
    }
  ],
  "count": 1
}
```

---

## Error Responses

### 400 Bad Request

```json
{
  "error": "There was an error with the request"
}
```

**Common Causes:**
- Invalid span parameters (start >= end, negative values)
- Invalid query parameters
- Missing required query parameter

### 404 Not Found

```json
{
  "error": "Resource was not found"
}
```

**Common Causes:**
- Segment ID does not exist
- Edition ID does not exist
- Segment was deleted

### 422 Validation Error

```json
{
  "error": "Validation error",
  "details": [
    {
      "loc": ["query", "search_type"],
      "msg": "invalid search type",
      "type": "value_error"
    }
  ]
}
```

**Common Causes:**
- Invalid search_type value
- Limit out of range (must be 1-100)
- Invalid span values
- Empty query string

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

---

## Best Practices

### 1. Retrieving Segment Content

**Do:**
- Cache segment content when displaying multiple times
- Use segment IDs from segmentation queries
- Batch multiple content retrievals if needed
- Handle empty or missing segments gracefully

**Don't:**
- Query content repeatedly for same segment
- Assume segment content is always available
- Request content for non-existent segments without error handling

---

### 2. Finding Related Segments

**Do:**
- Use this to build parallel text viewers
- Display source and target segments together
- Handle empty results (no alignments)
- Cache alignment relationships

**Don't:**
- Query related segments repeatedly for same segment
- Assume all segments have alignments
- Ignore manifestation_id when processing results

**Example - Building Parallel View:**

```bash
# Get segment content for source
SOURCE_TEXT=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/SEG_TIBETAN/content" \
  -H "X-API-Key: your_api_key")

# Get related segments
RELATED=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/SEG_TIBETAN/related" \
  -H "X-API-Key: your_api_key")

# Get content for each related segment
echo "$RELATED" | jq -r '.[].id' | while read seg_id; do
  RELATED_TEXT=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$seg_id/content" \
    -H "X-API-Key: your_api_key")
  
  echo "Segment $seg_id: $RELATED_TEXT"
done
```

---

### 3. Searching Segments

**Do:**
- Choose appropriate search_type for your use case
- Use `hybrid` for general search (balanced precision and recall)
- Use `bm25` for keyword/term-based search
- Use `semantic` for conceptual/meaning-based search
- Use `exact` for phrase matching
- Set reasonable limit (10-20 for UI, higher for batch processing)
- Use title filter to narrow results to specific texts
- Set return_text=false for faster metadata-only queries

**Don't:**
- Use unnecessarily large limits (impacts performance)
- Ignore the distance/relevance score
- Forget to URL-encode query strings with special characters
- Mix search types without understanding differences

**Search Type Selection:**

| Use Case | Recommended Type | Example Query |
|----------|------------------|---------------|
| User keyword search | `hybrid` | "emptiness teaching" |
| Specific term lookup | `bm25` | "སྟོང་པ་ཉིད" |
| Conceptual search | `semantic` | "nature of reality" |
| Exact phrase | `exact` | "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ" |
| General search | `hybrid` | "meditation practice" |

---

### 4. Working with Segmentation IDs

The search endpoint returns two types of IDs:

1. **Search Segment ID** (`results[].id`): From external search system
2. **Segmentation IDs** (`results[].segmentation_ids`): From your annotation segments

**Use the segmentation IDs to:**
- Get related segments via alignment
- Retrieve segment content
- Navigate your local segmentation structure

**Example:**

```bash
# Search for content
SEARCH_RESULT=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=wisdom&limit=1" \
  -H "X-API-Key: your_api_key")

# Extract first segmentation ID
SEG_ID=$(echo "$SEARCH_RESULT" | jq -r '.results[0].segmentation_ids[0]')

# Get related segments using the segmentation ID
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$SEG_ID/related" \
  -H "X-API-Key: your_api_key"

# Get segment content
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$SEG_ID/content" \
  -H "X-API-Key: your_api_key"
```

---

## Workflow Examples

### Scenario 1: Display Segment with Translations

```bash
#!/bin/bash

SEGMENT_ID="SEG001"

# Get segment content
echo "Original segment:"
CONTENT=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$SEGMENT_ID/content" \
  -H "X-API-Key: your_api_key")
echo "$CONTENT"

# Get related segments (translations)
echo -e "\nTranslations:"
RELATED=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$SEGMENT_ID/related" \
  -H "X-API-Key: your_api_key")

# Display each translation
echo "$RELATED" | jq -r '.[].id' | while read rel_id; do
  REL_CONTENT=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$rel_id/content" \
    -H "X-API-Key: your_api_key")
  
  MANIFESTATION=$(echo "$RELATED" | jq -r ".[] | select(.id == \"$rel_id\") | .manifestation_id")
  
  echo "  [$MANIFESTATION] $REL_CONTENT"
done
```

---

### Scenario 2: Search and Navigate to Aligned Content

```bash
# Step 1: Search for segments
SEARCH=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=སྟོང་པ་ཉིད&search_type=hybrid&limit=5" \
  -H "X-API-Key: your_api_key")

echo "Search found: $(echo "$SEARCH" | jq '.count') results"

# Step 2: Process each result
echo "$SEARCH" | jq -c '.results[]' | while read result; do
  SEARCH_ID=$(echo "$result" | jq -r '.id')
  DISTANCE=$(echo "$result" | jq -r '.distance')
  TEXT=$(echo "$result" | jq -r '.entity.text')
  SEG_IDS=$(echo "$result" | jq -r '.segmentation_ids[]')
  
  echo -e "\nResult: $SEARCH_ID (score: $DISTANCE)"
  echo "Text: $TEXT"
  echo "Segmentation IDs: $SEG_IDS"
  
  # Step 3: For first segmentation ID, find related segments
  FIRST_SEG=$(echo "$result" | jq -r '.segmentation_ids[0]')
  
  if [ "$FIRST_SEG" != "null" ]; then
    RELATED=$(curl -s -X GET \
      "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$FIRST_SEG/related" \
      -H "X-API-Key: your_api_key")
    
    RELATED_COUNT=$(echo "$RELATED" | jq 'length')
    echo "Found $RELATED_COUNT related segments"
  fi
done
```

---

### Scenario 3: Building Parallel Text Viewer

```python
import requests
from typing import List, Dict

class ParallelTextViewer:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"X-API-Key": api_key}
    
    def get_segment_content(self, segment_id: str) -> str:
        """Get content for a segment."""
        response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/content",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_related_segments(self, segment_id: str) -> List[Dict]:
        """Get all related segments."""
        response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/related",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def build_parallel_view(self, segment_id: str) -> Dict:
        """Build parallel view with source and all translations."""
        # Get source content
        source_content = self.get_segment_content(segment_id)
        
        # Get related segments
        related = self.get_related_segments(segment_id)
        
        # Build parallel structure
        parallel_view = {
            "source": {
                "segment_id": segment_id,
                "content": source_content
            },
            "translations": []
        }
        
        # Get content for each related segment
        for seg in related:
            related_content = self.get_segment_content(seg['id'])
            parallel_view["translations"].append({
                "segment_id": seg['id'],
                "manifestation_id": seg['manifestation_id'],
                "text_id": seg['text_id'],
                "content": related_content
            })
        
        return parallel_view

# Usage
viewer = ParallelTextViewer(
    api_key="your_api_key",
    base_url="https://api-l25bgmwqoa-uc.a.run.app"
)

# Get parallel view for a segment
parallel = viewer.build_parallel_view("SEG_TIBETAN_001")

print("Source:", parallel['source']['content'])
print("\nTranslations:")
for trans in parallel['translations']:
    print(f"  [{trans['manifestation_id']}] {trans['content']}")
```

---

### Scenario 4: Segment Search Integration

```javascript
// Frontend search component
class SegmentSearcher {
  constructor(apiKey, baseUrl) {
    this.apiKey = apiKey;
    this.baseUrl = baseUrl;
  }
  
  async search(query, options = {}) {
    const {
      searchType = 'hybrid',
      limit = 10,
      title = null,
      returnText = true
    } = options;
    
    const params = new URLSearchParams({
      query,
      search_type: searchType,
      limit: limit.toString(),
      return_text: returnText.toString()
    });
    
    if (title) {
      params.set('title', title);
    }
    
    const response = await fetch(
      `${this.baseUrl}/v2/segments/search?${params}`,
      {
        headers: {
          'X-API-Key': this.apiKey
        }
      }
    );
    
    if (!response.ok) {
      throw new Error(`Search failed: ${response.statusText}`);
    }
    
    return await response.json();
  }
  
  async getSegmentWithRelated(segmentId) {
    // Get segment content
    const contentResponse = await fetch(
      `${this.baseUrl}/v2/segments/${segmentId}/content`,
      {
        headers: { 'X-API-Key': this.apiKey }
      }
    );
    const content = await contentResponse.json();
    
    // Get related segments
    const relatedResponse = await fetch(
      `${this.baseUrl}/v2/segments/${segmentId}/related`,
      {
        headers: { 'X-API-Key': this.apiKey }
      }
    );
    const related = await relatedResponse.json();
    
    // Get content for related segments
    const relatedWithContent = await Promise.all(
      related.map(async (seg) => {
        const resp = await fetch(
          `${this.baseUrl}/v2/segments/${seg.id}/content`,
          {
            headers: { 'X-API-Key': this.apiKey }
          }
        );
        return {
          ...seg,
          content: await resp.json()
        };
      })
    );
    
    return {
      segment: {
        id: segmentId,
        content
      },
      related: relatedWithContent
    };
  }
}

// Usage
const searcher = new SegmentSearcher('your_api_key', 'https://api-l25bgmwqoa-uc.a.run.app');

// Search
const results = await searcher.search('བོད་ཀྱི་རིག་གནས', {
  searchType: 'hybrid',
  limit: 20
});

console.log(`Found ${results.count} results`);

// Get first result with related segments
if (results.results.length > 0) {
  const firstSegId = results.results[0].segmentation_ids[0];
  const parallel = await searcher.getSegmentWithRelated(firstSegId);
  
  console.log('Source:', parallel.segment.content);
  console.log('Translations:', parallel.related.map(r => r.content));
}
```

---

## Advanced Usage

### Search Result Processing

Process search results to extract relevant information:

```python
import requests

def process_search_results(query: str, search_type: str = "hybrid") -> list:
    """Search and process results with segmentation mapping."""
    headers = {"X-API-Key": "your_api_key"}
    
    response = requests.get(
        f"https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search",
        headers=headers,
        params={
            "query": query,
            "search_type": search_type,
            "limit": 20,
            "return_text": True
        }
    )
    response.raise_for_status()
    data = response.json()
    
    processed = []
    for result in data['results']:
        # Extract main information
        item = {
            'search_id': result['id'],
            'relevance': result['distance'],
            'text': result['entity'].get('text', ''),
            'segmentation_ids': result['segmentation_ids']
        }
        
        # Get related segments for first segmentation ID
        if result['segmentation_ids']:
            first_seg_id = result['segmentation_ids'][0]
            
            related_response = requests.get(
                f"https://api-l25bgmwqoa-uc.a.run.app/v2/segments/{first_seg_id}/related",
                headers=headers
            )
            
            if related_response.ok:
                item['related_segments'] = related_response.json()
        
        processed.append(item)
    
    return processed

# Usage
results = process_search_results("སྟོང་པ་ཉིད", "semantic")

for i, result in enumerate(results, 1):
    print(f"\n{i}. Relevance: {result['relevance']:.3f}")
    print(f"   Text: {result['text'][:100]}...")
    print(f"   Segmentation IDs: {len(result['segmentation_ids'])}")
    print(f"   Related segments: {len(result.get('related_segments', []))}")
```

---

### Finding Segments by Content Position

Use edition-level query to find segments at specific positions:

```bash
# User selects characters 500-600 in edition
EDITION_ID="I12345678"
SPAN_START=500
SPAN_END=600

# Find segments overlapping this range
SEGMENTS=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION_ID/segments/related?span_start=$SPAN_START&span_end=$SPAN_END" \
  -H "X-API-Key: your_api_key")

# Display segments
echo "Segments overlapping [$SPAN_START, $SPAN_END):"
echo "$SEGMENTS" | jq -r '.[] | "  Segment \(.id) in \(.manifestation_id)"'

# Get content for each segment
echo "$SEGMENTS" | jq -r '.[].id' | while read seg_id; do
  CONTENT=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$seg_id/content" \
    -H "X-API-Key: your_api_key")
  
  echo -e "\n$seg_id:"
  echo "$CONTENT"
done
```

---

### Batch Segment Content Retrieval

Efficiently retrieve content for multiple segments:

```python
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_segment_content_batch(segment_ids: list, api_key: str, max_workers: int = 5) -> dict:
    """Get content for multiple segments in parallel."""
    base_url = "https://api-l25bgmwqoa-uc.a.run.app"
    headers = {"X-API-Key": api_key}
    
    def fetch_content(seg_id):
        try:
            response = requests.get(
                f"{base_url}/v2/segments/{seg_id}/content",
                headers=headers
            )
            response.raise_for_status()
            return seg_id, response.json()
        except Exception as e:
            return seg_id, f"Error: {str(e)}"
    
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_content, seg_id): seg_id 
                  for seg_id in segment_ids}
        
        for future in as_completed(futures):
            seg_id, content = future.result()
            results[seg_id] = content
    
    return results

# Usage
segment_ids = ["SEG001", "SEG002", "SEG003", "SEG004", "SEG005"]
contents = get_segment_content_batch(segment_ids, "your_api_key")

for seg_id, content in contents.items():
    print(f"{seg_id}: {content[:50]}...")
```

---

## Search Examples

### Example 1: Tibetan Text Search

```bash
# Search Tibetan text
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=སྟོང་པ་ཉིད&search_type=hybrid&limit=10" \
  -H "X-API-Key: your_api_key" \
  | jq '{
    query: .query,
    count: .count,
    results: .results | map({
      id: .id,
      score: .distance,
      text: .entity.text,
      seg_count: .segmentation_ids | length
    })
  }'
```

---

### Example 2: English Translation Search

```bash
# Search English translations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=emptiness&search_type=semantic&limit=15" \
  -H "X-API-Key: your_api_key" \
  | jq '.results[] | {
    score: .distance,
    preview: .entity.text[:100],
    segments: .segmentation_ids
  }'
```

---

### Example 3: Filtered Search by Title

```bash
# Search only within Heart Sutra texts
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=wisdom&title=Heart%20Sutra&limit=5" \
  -H "X-API-Key: your_api_key" \
  | jq '.'
```

---

### Example 4: Exact Phrase Search

```bash
# Find exact phrase
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ&search_type=exact" \
  -H "X-API-Key: your_api_key" \
  | jq '.results[] | {
    text: .entity.text,
    segments: .segmentation_ids
  }'
```

---

## Integration Patterns

### Pattern 1: Search → View → Navigate

Complete user flow from search to aligned content:

```
1. User searches: GET /v2/segments/search
2. Display results with text previews
3. User clicks result → extract segmentation_ids[0]
4. Get segment content: GET /v2/segments/{id}/content
5. Get related segments: GET /v2/segments/{id}/related
6. Display parallel view with translations
```

---

### Pattern 2: Edition Navigation

Navigate edition via segments:

```
1. Load edition: GET /v2/editions/{id}/content
2. Load segmentation: GET /v2/editions/{id}/annotations?type=segmentation
3. User clicks segment → get segment_id
4. Get segment content: GET /v2/segments/{segment_id}/content
5. Get translations: GET /v2/segments/{segment_id}/related
6. Display in parallel viewer
```

---

### Pattern 3: Search-Based Navigation

Use search to find and navigate content:

```
1. User searches: GET /v2/segments/search?query=...
2. Extract segmentation_ids from results
3. For each segmentation_id:
   a. Get segment content
   b. Get related segments (translations)
   c. Display in results with translation preview
4. User selects result → navigate to full parallel view
```

---

## Troubleshooting

### Common Issues

**Issue: "Resource was not found" when querying segment**

**Solution:**
- Segment ID may be incorrect or segment was deleted
- Verify segment ID from segmentation query
- Check if segmentation annotation still exists

```bash
# Get valid segment IDs from edition
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=segmentation" \
  -H "X-API-Key: your_api_key" \
  | jq '.segmentations[0].segments[].id'
```

---

**Issue: Related segments query returns empty array**

**Solution:**
- Segment has no alignments to other editions
- Check if alignment annotations exist
- Verify alignment was created correctly

```bash
# Check if edition has alignments
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=alignment" \
  -H "X-API-Key: your_api_key"
```

---

**Issue: Search returns no results**

**Solution:**
- Query text may not exist in corpus
- Try different search_type (semantic vs bm25)
- Broaden query terms
- Remove title filter
- Check if search index is populated

```bash
# Try different search types
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=wisdom&search_type=bm25" \
  -H "X-API-Key: your_api_key"

curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=wisdom&search_type=semantic" \
  -H "X-API-Key: your_api_key"
```

---

**Issue: Span query returns segments from wrong edition**

**Solution:**
- Related segments query returns segments from ALL aligned editions
- Filter results by manifestation_id if needed
- Check alignment relationships

```bash
# Get related segments
RELATED=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/segments/related?span_start=0&span_end=100" \
  -H "X-API-Key: your_api_key")

# Filter to specific manifestation
echo "$RELATED" | jq '[.[] | select(.manifestation_id == "M_TARGET")]'
```

---

**Issue: Search segmentation_ids don't match local segments**

**Solution:**
- Search system uses separate segmentation for indexing
- Use segmentation_ids as keys to find overlapping segments
- The mapping connects search segments to your annotation segments

---

## Performance Optimization

### Caching Strategies

**Segment Content:**
- Cache segment content (rarely changes)
- TTL: Long (hours/days)
- Invalidate on content operations

**Related Segments:**
- Cache alignment relationships
- TTL: Long (stable unless alignment deleted)
- Key: segment_id

**Search Results:**
- Cache search results per query
- TTL: Medium (minutes/hours)
- Key: query + search_type + filters

**Example Cache Keys:**
```
segment:content:SEG001
segment:related:SEG001
search:hybrid:wisdom:limit10
search:semantic:emptiness:limit20:title=Heart+Sutra
```

---

### Parallel Requests

Fetch multiple segment contents in parallel:

```python
import asyncio
import aiohttp

async def fetch_segment_content(session, segment_id, api_key):
    """Async fetch segment content."""
    url = f"https://api-l25bgmwqoa-uc.a.run.app/v2/segments/{segment_id}/content"
    headers = {"X-API-Key": api_key}
    
    async with session.get(url, headers=headers) as response:
        return segment_id, await response.json()

async def fetch_multiple_segments(segment_ids, api_key):
    """Fetch multiple segment contents in parallel."""
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_segment_content(session, seg_id, api_key)
            for seg_id in segment_ids
        ]
        results = await asyncio.gather(*tasks)
        return dict(results)

# Usage
segment_ids = ["SEG001", "SEG002", "SEG003"]
contents = asyncio.run(fetch_multiple_segments(segment_ids, "your_api_key"))

for seg_id, content in contents.items():
    print(f"{seg_id}: {content}")
```

---

## Search Query Tips

### Query Formulation

**For Tibetan Text:**
- Use actual Tibetan script (not Wylie)
- Include particles for more specific results
- Try both short and long forms

```bash
# Short form
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=སྟོང་ཉིད" \
  -H "X-API-Key: your_api_key"

# Long form with particles
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=སྟོང་པ་ཉིད་ཀྱི་ལྟ་བ" \
  -H "X-API-Key: your_api_key"
```

---

**For English Translation:**
- Use semantic search for concepts
- Use bm25 for specific terms
- Use exact for phrases

```bash
# Conceptual search
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=nature%20of%20reality&search_type=semantic" \
  -H "X-API-Key: your_api_key"

# Term search
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=emptiness&search_type=bm25" \
  -H "X-API-Key: your_api_key"

# Exact phrase
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=form%20is%20emptiness&search_type=exact" \
  -H "X-API-Key: your_api_key"
```

---

### Relevance Scoring

Search results include distance/relevance scores:

- **Higher score = more relevant** (typically 0.0 to 1.0)
- Use to rank results
- Filter by minimum threshold

```python
# Filter results by minimum relevance
search_response = requests.get(
    "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search",
    headers={"X-API-Key": "your_api_key"},
    params={"query": "wisdom", "limit": 50}
).json()

# Keep only high-relevance results
MIN_RELEVANCE = 0.7
filtered_results = [
    r for r in search_response['results'] 
    if r['distance'] >= MIN_RELEVANCE
]

print(f"Found {len(filtered_results)} high-relevance results")
```

---

## Complete Examples

### Example 1: Interactive Segment Explorer

```python
import requests

class SegmentExplorer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def search(self, query: str, search_type: str = "hybrid", limit: int = 10):
        """Search segments."""
        response = requests.get(
            f"{self.base_url}/v2/segments/search",
            headers=self.headers,
            params={
                "query": query,
                "search_type": search_type,
                "limit": limit
            }
        )
        response.raise_for_status()
        return response.json()
    
    def get_content(self, segment_id: str) -> str:
        """Get segment content."""
        response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/content",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_related(self, segment_id: str) -> list:
        """Get related segments."""
        response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/related",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def explore_segment(self, segment_id: str):
        """Explore segment with all related information."""
        print(f"\n{'='*60}")
        print(f"Segment: {segment_id}")
        print(f"{'='*60}")
        
        # Get content
        content = self.get_content(segment_id)
        print(f"\nContent:\n{content}")
        
        # Get related
        related = self.get_related(segment_id)
        
        if related:
            print(f"\nRelated Segments: {len(related)}")
            for seg in related:
                rel_content = self.get_content(seg['id'])
                print(f"\n  {seg['id']} [{seg['manifestation_id']}]:")
                print(f"  {rel_content[:100]}...")
        else:
            print("\nNo related segments found")
    
    def interactive_search(self):
        """Interactive search interface."""
        while True:
            query = input("\nEnter search query (or 'quit'): ")
            if query.lower() == 'quit':
                break
            
            results = self.search(query)
            
            print(f"\nFound {results['count']} results:")
            for i, result in enumerate(results['results'], 1):
                print(f"\n{i}. Score: {result['distance']:.3f}")
                print(f"   Text: {result['entity']['text'][:100]}...")
                print(f"   Segmentation IDs: {len(result['segmentation_ids'])}")
            
            # Explore first result
            if results['results']:
                seg_ids = results['results'][0]['segmentation_ids']
                if seg_ids:
                    choice = input(f"\nExplore first result? (y/n): ")
                    if choice.lower() == 'y':
                        self.explore_segment(seg_ids[0])

# Usage
explorer = SegmentExplorer("your_api_key")
explorer.interactive_search()
```

---

### Example 2: Segment-Based Translation Memory

```python
import requests
from typing import List, Dict, Tuple

class TranslationMemory:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def find_similar_segments(self, query: str, limit: int = 10) -> List[Tuple[str, str, float]]:
        """Find similar segments with their translations."""
        # Search for similar segments
        response = requests.get(
            f"{self.base_url}/v2/segments/search",
            headers=self.headers,
            params={
                "query": query,
                "search_type": "semantic",
                "limit": limit
            }
        )
        response.raise_for_status()
        search_data = response.json()
        
        translation_pairs = []
        
        for result in search_data['results']:
            if not result['segmentation_ids']:
                continue
            
            # Get first segmentation ID
            seg_id = result['segmentation_ids'][0]
            
            # Get source content
            source_content = self.get_segment_content(seg_id)
            
            # Get related segments (translations)
            related = self.get_related_segments(seg_id)
            
            # Get translation content
            for rel_seg in related:
                trans_content = self.get_segment_content(rel_seg['id'])
                translation_pairs.append((
                    source_content,
                    trans_content,
                    result['distance']
                ))
        
        return translation_pairs
    
    def get_segment_content(self, segment_id: str) -> str:
        """Get segment content."""
        response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/content",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_related_segments(self, segment_id: str) -> List[Dict]:
        """Get related segments."""
        response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/related",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()

# Usage
tm = TranslationMemory("your_api_key")

# Find similar translations
query = "བདེ་བ་ཆེན་པོ་"
similar = tm.find_similar_segments(query, limit=5)

print(f"Translation suggestions for: {query}\n")
for source, translation, score in similar:
    print(f"Score: {score:.3f}")
    print(f"Source: {source}")
    print(f"Translation: {translation}")
    print()
```

---

## Quick Reference

### HTTP Methods Summary

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/v2/segments/{segment_id}/content` | Get segment text content |
| GET | `/v2/segments/{segment_id}/related` | Find aligned segments |
| GET | `/v2/segments/search` | Search segments with mapping |
| GET | `/v2/editions/{edition_id}/segments/related` | Find segments by span |

### Query Parameters Summary

| Endpoint | Parameters | Description |
|----------|------------|-------------|
| `/segments/{id}/content` | None | Returns segment text |
| `/segments/{id}/related` | None | Returns aligned segments |
| `/segments/search` | query, search_type, limit, title, return_text | Search parameters |
| `/editions/{id}/segments/related` | span_start, span_end | Character range |

### Status Codes

| Status | Meaning | Common Use |
|--------|---------|------------|
| 200 | OK | Successful retrieval |
| 400 | Bad Request | Invalid parameters |
| 404 | Not Found | Segment/edition doesn't exist |
| 422 | Validation Error | Invalid search parameters |
| 500 | Server Error | Internal error |

---

## Validation Rules

### Segment Content

| Rule | Validation |
|------|------------|
| segment_id | Required, must reference existing segment |

### Related Segments

| Rule | Validation |
|------|------------|
| segment_id | Required, must reference existing segment |

### Search Segments

| Rule | Validation |
|------|------------|
| query | Required, non-empty string |
| search_type | Optional, must be: hybrid, bm25, semantic, or exact |
| limit | Optional, 1-100 |
| title | Optional, string |
| return_text | Optional, boolean |

### Edition Segments by Span

| Rule | Validation |
|------|------------|
| edition_id | Required, must reference existing edition |
| span_start | Required, >= 0 |
| span_end | Required, >= 1, must be > span_start |

---

## Limitations and Considerations

### Current Limitations

1. **No Direct Segment Creation**
   - Segments must be created via segmentation/alignment annotations
   - Cannot create standalone segments

2. **No Segment Update**
   - Segments are part of annotations (immutable)
   - Update by recreating parent annotation

3. **No Segment Delete**
   - Delete parent annotation to remove segments
   - Cannot delete individual segments

4. **Search Dependency**
   - Search depends on external search API
   - Search index may lag behind content updates
   - Segmentation mapping requires both systems to be in sync

---

### Search Performance

**Factors Affecting Performance:**
- Query complexity
- Result limit
- return_text parameter (true is slower)
- Search type (semantic is slower than bm25)
- Title filter (speeds up search)

**Optimization:**
```bash
# Fast: Metadata only, limited results
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=wisdom&limit=5&return_text=false" \
  -H "X-API-Key: your_api_key"

# Slow: Full text, many results
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/search?query=wisdom&limit=100&return_text=true&search_type=semantic" \
  -H "X-API-Key: your_api_key"
```

---

## Real-World Applications

### Application 1: Parallel Translation Viewer

```javascript
class ParallelViewer {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api-l25bgmwqoa-uc.a.run.app';
  }
  
  async displayParallel(segmentId) {
    // Get source segment
    const sourceContent = await this.getContent(segmentId);
    
    // Get aligned segments
    const related = await this.getRelated(segmentId);
    
    // Build display
    const view = {
      source: sourceContent,
      translations: []
    };
    
    for (const seg of related) {
      const content = await this.getContent(seg.id);
      view.translations.push({
        manifestationId: seg.manifestation_id,
        content: content
      });
    }
    
    return view;
  }
  
  async getContent(segmentId) {
    const response = await fetch(
      `${this.baseUrl}/v2/segments/${segmentId}/content`,
      { headers: { 'X-API-Key': this.apiKey } }
    );
    return await response.json();
  }
  
  async getRelated(segmentId) {
    const response = await fetch(
      `${this.baseUrl}/v2/segments/${segmentId}/related`,
      { headers: { 'X-API-Key': this.apiKey } }
    );
    return await response.json();
  }
}

// Usage in web app
const viewer = new ParallelViewer('your_api_key');

// When user clicks a segment
document.getElementById('segment').addEventListener('click', async (e) => {
  const segmentId = e.target.dataset.segmentId;
  const parallel = await viewer.displayParallel(segmentId);
  
  // Display in UI
  document.getElementById('source').textContent = parallel.source;
  
  const translationsDiv = document.getElementById('translations');
  translationsDiv.innerHTML = '';
  
  parallel.translations.forEach(trans => {
    const div = document.createElement('div');
    div.className = 'translation';
    div.textContent = trans.content;
    div.dataset.manifestationId = trans.manifestationId;
    translationsDiv.appendChild(div);
  });
});
```

---

### Application 2: Search-Based Text Discovery

```python
class TextDiscovery:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def discover(self, query: str, search_type: str = "hybrid"):
        """Discover texts and their relationships through search."""
        # Search segments
        response = requests.get(
            f"{self.base_url}/v2/segments/search",
            headers=self.headers,
            params={
                "query": query,
                "search_type": search_type,
                "limit": 10,
                "return_text": True
            }
        )
        response.raise_for_status()
        data = response.json()
        
        discoveries = []
        
        for result in data['results']:
            # Get segmentation IDs
            if not result['segmentation_ids']:
                continue
            
            seg_id = result['segmentation_ids'][0]
            
            # Get segment info
            seg_response = requests.get(
                f"{self.base_url}/v2/segments/{seg_id}/related",
                headers=self.headers
            )
            
            if seg_response.ok:
                related = seg_response.json()
                
                # Get all manifestation IDs
                manifestations = set([seg_id])  # Source
                manifestations.update([r['manifestation_id'] for r in related])
                
                discoveries.append({
                    'query_match': result['entity']['text'],
                    'relevance': result['distance'],
                    'source_segment': seg_id,
                    'related_count': len(related),
                    'manifestations': list(manifestations)
                })
        
        return discoveries

# Usage
discovery = TextDiscovery("your_api_key")
results = discovery.discover("སྟོང་པ་ཉིད", "semantic")

print(f"Discovered {len(results)} text locations with alignments:\n")
for i, disc in enumerate(results, 1):
    print(f"{i}. Relevance: {disc['relevance']:.3f}")
    print(f"   Match: {disc['query_match'][:80]}...")
    print(f"   Related: {disc['related_count']} aligned segments")
    print(f"   Manifestations: {', '.join(disc['manifestations'])}")
```

---

### Application 3: Citation Generator

```python
import requests

class CitationGenerator:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def generate_citation(self, segment_id: str) -> dict:
        """Generate citation information for a segment."""
        # Get segment via related query (to get full info)
        related_response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/related",
            headers=self.headers
        )
        
        # This returns the segment in related list, extract manifestation/text IDs
        # Actually, we need to get the segment info differently
        # For now, use the related endpoint to demonstrate
        
        # Get segment content
        content_response = requests.get(
            f"{self.base_url}/v2/segments/{segment_id}/content",
            headers=self.headers
        )
        content = content_response.json()
        
        # You would typically also fetch:
        # - Edition metadata (for source info)
        # - Text metadata (for title, author)
        # - Page/folio if pagination exists
        
        return {
            'segment_id': segment_id,
            'content': content,
            'excerpt': content[:100] + '...' if len(content) > 100 else content
        }

# Usage
citation = CitationGenerator("your_api_key")
cite_info = citation.generate_citation("SEG001")

print(f"Segment: {cite_info['segment_id']}")
print(f"Excerpt: {cite_info['excerpt']}")
```

---

## Integration with Annotations

Segments are created and managed through annotations. Here's the relationship:

### Segmentation Annotation → Segments

```bash
# Create segmentation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "segmentation": {
      "segments": [
        {"lines": [{"start": 0, "end": 50}]},
        {"lines": [{"start": 50, "end": 100}]}
      ]
    }
  }'

# Get created segments
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=segmentation" \
  -H "X-API-Key: your_api_key" \
  | jq '.segmentations[0].segments'

# Use segment IDs
# segments[0].id, segments[1].id, etc.
```

### Alignment Annotation → Related Segments

```bash
# Create alignment (creates segments in both source and target)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I_SOURCE/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "alignment": {
      "target_id": "I_TARGET",
      "target_segments": [
        {"lines": [{"start": 0, "end": 50}]}
      ],
      "aligned_segments": [
        {"lines": [{"start": 0, "end": 40}], "alignment_indices": [0]}
      ]
    }
  }'

# Get source segment ID
SOURCE_SEG=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I_SOURCE/annotations?type=alignment" \
  -H "X-API-Key: your_api_key" \
  | jq -r '.alignments[0].aligned_segments[0].id')

# Query related segments
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/$SOURCE_SEG/related" \
  -H "X-API-Key: your_api_key"
```

---

## Frequently Asked Questions

### Q: How do I create a segment?

**A:** Segments are created as part of annotations. You cannot create standalone segments. Create a segmentation annotation via `POST /v2/editions/{edition_id}/annotations` with type `segmentation`.

### Q: Can I update a segment's span?

**A:** No, segments are immutable as part of their parent annotation. To change spans, you must delete and recreate the annotation.

### Q: How do I delete a segment?

**A:** Delete the parent annotation. Segments are deleted when their parent segmentation/alignment annotation is deleted.

### Q: What's the difference between search segment IDs and segmentation IDs?

**A:** 
- **Search segment ID** (`results[].id`): From external search system
- **Segmentation IDs** (`results[].segmentation_ids`): Your annotation segments that overlap with the search result

Use segmentation IDs to query related segments and content in your system.

### Q: Why does a segment have multiple spans (lines)?

**A:** Some segments represent non-contiguous text portions. For example, a verse might be split by commentary, so it would have multiple spans to represent the discontinuous verse text.

### Q: Can segments overlap?

**A:** Within a single segmentation annotation, segments typically don't overlap. However, different segmentation annotations can have overlapping segments (e.g., sentence vs paragraph segmentation).

### Q: How are related segments determined?

**A:** Through alignment annotations. When you create an alignment between two editions, it establishes relationships between their segments. The related segments query follows these alignment relationships.

### Q: Can I search only within specific editions?

**A:** The search endpoint searches across all indexed content. Use the `title` parameter to filter by text title, but there's no direct edition filter. After getting results, you can filter by checking the manifestation_id.

---

## See Also

- [Annotations API Documentation](./annotations-api.md) - Creating and managing segmentation and alignment
- [Editions API Documentation](./editions-api.md) - Edition content and management
- [Text Operations Specification](../text-operations-spec.md) - Span adjustment behavior
- OpenAPI Specification: `GET /v2/schema/openapi`
