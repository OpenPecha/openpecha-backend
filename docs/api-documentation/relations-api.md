# Relations API Documentation

This document provides comprehensive documentation for all Relations-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Relations Endpoints](#relations-endpoints)
   - [Get Relations for Expression](#get-relations-for-expression)
4. [Data Models](#data-models)
5. [Error Responses](#error-responses)
6. [Best Practices](#best-practices)
7. [Workflow Examples](#workflow-examples)

---

## Overview

The Relations API provides a unified view of all relationships for a given expression (text). It returns connections between texts such as translations, commentaries, and other text relationships.

### Key Concepts

- **Relation**: A directional connection between two expressions
- **Expression**: A text in the OpenPecha system (referenced by expression_id or text_id)
- **Relation Type**: The nature of the relationship (TRANSLATION_OF, COMMENTARY_OF)
- **Direction**: Whether the relation is incoming (`in`) or outgoing (`out`) from the query expression
- **Relation Graph**: The complete network of related expressions

### Relation Types

| Type | Description | Example |
|------|-------------|---------|
| `TRANSLATION_OF` | One text is a translation of another | English translation of Tibetan text |
| `COMMENTARY_OF` | One text is a commentary on another | Scholarly commentary on a sutra |

### Directions

| Direction | Meaning | Example |
|-----------|---------|---------|
| `in` | Incoming relation (other → this) | English text is translation of Tibetan text (Tibetan's perspective: incoming) |
| `out` | Outgoing relation (this → other) | Tibetan text has English translation (Tibetan's perspective: outgoing) |

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

## Relations Endpoints

### Get Relations for Expression

Retrieve all relationships for a given expression, including relation type, direction, and related expression IDs.

**Endpoint:**
```
GET /v2/relations/expressions/{expression_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `expression_id` | string | path | Yes | The ID of the expression to retrieve relations for |

**Response: 200 OK**

The response is an object where:
- **Keys**: Expression IDs (including the query expression and related expressions)
- **Values**: Arrays of relations for that expression

```json
{
  "13mxFPmsmIqktVTYVV8AL": [
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "RQKWt16H22fyxodz1Hpu8"
    },
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "zRsb99ocU5fM8wurAUfMQ"
    },
    {
      "type": "COMMENTARY_OF",
      "direction": "out",
      "otherId": "t0CzlMC69QUySNEBzSL4e"
    }
  ]
}
```

**Response Structure:**

The response contains the complete relation graph for the expression:
1. Primary key is the query expression ID
2. Relations array contains all direct relationships
3. Additional keys may be present for related expressions and their relationships

**Understanding the Response:**

For expression `13mxFPmsmIqktVTYVV8AL`:
- Has 2 incoming TRANSLATION_OF relations (this expression is a translation of 2 other texts)
- Has 1 outgoing COMMENTARY_OF relation (this expression is a commentary on another text)

**Empty Result:**

```json
{
  "13mxFPmsmIqktVTYVV8AL": []
}
```

**Error Responses:**
- `404 Not Found`: Expression does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all relations for an expression
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/13mxFPmsmIqktVTYVV8AL" \
  -H "X-API-Key: your_api_key"

# Pretty print with jq
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/13mxFPmsmIqktVTYVV8AL" \
  -H "X-API-Key: your_api_key" \
  | jq '.'
```

**Use Cases:**
- Display all translations of a text
- Show commentaries on a text
- Build navigation between related texts
- Construct relation graphs
- Find source texts for translations
- Discover related content

---

## Data Models

### Relation

Schema for individual relation:

```typescript
{
  type: "TRANSLATION_OF" | "COMMENTARY_OF";  // Relationship type
  direction: "in" | "out";                    // Direction relative to expression
  otherId: string;                            // Related expression ID
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Type of relationship |
| `direction` | string | Direction: `in` (incoming) or `out` (outgoing) |
| `otherId` | string | ID of the related expression |

**Example:**

```json
{
  "type": "TRANSLATION_OF",
  "direction": "in",
  "otherId": "SOURCE_TEXT_ID"
}
```

**Interpretation:** This expression is a translation of `SOURCE_TEXT_ID`

---

### RelationsResponse

Complete response schema:

```typescript
{
  [expression_id: string]: Relation[]
}
```

**Structure:**
- Keys are expression IDs
- Values are arrays of relations for that expression
- Primary key is always the query expression
- May include additional keys for related expressions

**Example:**

```json
{
  "QUERY_EXPRESSION_ID": [
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "SOURCE_TEXT_ID"
    },
    {
      "type": "COMMENTARY_OF",
      "direction": "out",
      "otherId": "COMMENTED_TEXT_ID"
    }
  ]
}
```

---

## Understanding Directions

### TRANSLATION_OF Relations

**Outgoing (`out`)**: This expression has been translated into other languages

```json
{
  "TIBETAN_TEXT_ID": [
    {
      "type": "TRANSLATION_OF",
      "direction": "out",
      "otherId": "ENGLISH_TEXT_ID"
    },
    {
      "type": "TRANSLATION_OF",
      "direction": "out",
      "otherId": "CHINESE_TEXT_ID"
    }
  ]
}
```

**Interpretation:** Tibetan text has English and Chinese translations

---

**Incoming (`in`)**: This expression is a translation of another expression

```json
{
  "ENGLISH_TEXT_ID": [
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "TIBETAN_TEXT_ID"
    }
  ]
}
```

**Interpretation:** English text is a translation of Tibetan text

---

### COMMENTARY_OF Relations

**Outgoing (`out`)**: This expression is a commentary on another expression

```json
{
  "COMMENTARY_TEXT_ID": [
    {
      "type": "COMMENTARY_OF",
      "direction": "out",
      "otherId": "ROOT_TEXT_ID"
    }
  ]
}
```

**Interpretation:** This text is a commentary on the root text

---

**Incoming (`in`)**: This expression has commentaries written about it

```json
{
  "ROOT_TEXT_ID": [
    {
      "type": "COMMENTARY_OF",
      "direction": "in",
      "otherId": "COMMENTARY_TEXT_ID_1"
    },
    {
      "type": "COMMENTARY_OF",
      "direction": "in",
      "otherId": "COMMENTARY_TEXT_ID_2"
    }
  ]
}
```

**Interpretation:** Root text has 2 commentaries

---

## Error Responses

### 404 Not Found

```json
{
  "error": "Resource was not found"
}
```

**Common Causes:**
- Expression ID does not exist
- Expression was deleted
- Invalid expression ID format

---

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

**Common Causes:**
- Database connection issues
- Internal server error
- Graph query timeout

---

## Best Practices

### 1. Filtering Relations

Filter relations by type or direction:

```python
import requests

def get_relations(expression_id: str, api_key: str) -> dict:
    """Get all relations for expression."""
    response = requests.get(
        f"https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/{expression_id}",
        headers={"X-API-Key": api_key}
    )
    response.raise_for_status()
    return response.json()

def filter_relations(relations: dict, expression_id: str, 
                    relation_type: str = None, direction: str = None) -> list:
    """Filter relations by type and/or direction."""
    if expression_id not in relations:
        return []
    
    filtered = relations[expression_id]
    
    if relation_type:
        filtered = [r for r in filtered if r['type'] == relation_type]
    
    if direction:
        filtered = [r for r in filtered if r['direction'] == direction]
    
    return filtered

# Usage
expression_id = "TEXT123"
all_relations = get_relations(expression_id, "your_api_key")

# Get only translations
translations = filter_relations(all_relations, expression_id, relation_type="TRANSLATION_OF")

# Get only outgoing relations (texts this one is translated into)
outgoing = filter_relations(all_relations, expression_id, direction="out")

# Get incoming commentaries
incoming_commentaries = filter_relations(
    all_relations, 
    expression_id, 
    relation_type="COMMENTARY_OF", 
    direction="in"
)
```

---

### 2. Building Navigation

Use relations to build navigation between texts:

```javascript
class TextNavigator {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api-l25bgmwqoa-uc.a.run.app';
  }
  
  async getNavigationLinks(expressionId) {
    // Get relations
    const response = await fetch(
      `${this.baseUrl}/v2/relations/expressions/${expressionId}`,
      {
        headers: { 'X-API-Key': this.apiKey }
      }
    );
    
    const relations = await response.json();
    const expressionRelations = relations[expressionId] || [];
    
    // Organize by type and direction
    const nav = {
      sourceTexts: [],       // This is a translation of...
      translations: [],      // Translations of this text
      rootTexts: [],        // This is a commentary on...
      commentaries: []      // Commentaries on this text
    };
    
    expressionRelations.forEach(rel => {
      if (rel.type === 'TRANSLATION_OF') {
        if (rel.direction === 'in') {
          nav.sourceTexts.push(rel.otherId);
        } else {
          nav.translations.push(rel.otherId);
        }
      } else if (rel.type === 'COMMENTARY_OF') {
        if (rel.direction === 'out') {
          nav.rootTexts.push(rel.otherId);
        } else {
          nav.commentaries.push(rel.otherId);
        }
      }
    });
    
    return nav;
  }
}

// Usage
const navigator = new TextNavigator('your_api_key');
const links = await navigator.getNavigationLinks('TEXT123');

console.log('Source texts:', links.sourceTexts);
console.log('Translations:', links.translations);
console.log('Root texts:', links.rootTexts);
console.log('Commentaries:', links.commentaries);
```

---

### 3. Caching Relations

Relations change infrequently and should be cached:

```python
from functools import lru_cache
from datetime import datetime, timedelta
import requests

class RelationsCache:
    def __init__(self, api_key: str, ttl_seconds: int = 3600):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.cache = {}
        self.ttl_seconds = ttl_seconds
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations with caching."""
        now = datetime.now()
        
        # Check cache
        if expression_id in self.cache:
            cached_data, cached_time = self.cache[expression_id]
            if now - cached_time < timedelta(seconds=self.ttl_seconds):
                return cached_data
        
        # Fetch from API
        response = requests.get(
            f"{self.base_url}/v2/relations/expressions/{expression_id}",
            headers={"X-API-Key": self.api_key}
        )
        response.raise_for_status()
        data = response.json()
        
        # Update cache
        self.cache[expression_id] = (data, now)
        
        return data
    
    def invalidate(self, expression_id: str):
        """Invalidate cache for expression."""
        if expression_id in self.cache:
            del self.cache[expression_id]

# Usage
cache = RelationsCache("your_api_key", ttl_seconds=3600)

# First call - fetches from API
relations1 = cache.get_relations("TEXT123")

# Second call - uses cache
relations2 = cache.get_relations("TEXT123")

# After creating/deleting text relationships, invalidate cache
cache.invalidate("TEXT123")
```

---

### 4. Enriching with Text Metadata

Combine relations with text metadata for rich display:

```python
import requests

class EnrichedRelations:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def get_enriched_relations(self, expression_id: str) -> dict:
        """Get relations with full text metadata."""
        # Get relations
        rel_response = requests.get(
            f"{self.base_url}/v2/relations/expressions/{expression_id}",
            headers=self.headers
        )
        rel_response.raise_for_status()
        relations_data = rel_response.json()
        
        if expression_id not in relations_data:
            return {"expression_id": expression_id, "relations": []}
        
        relations = relations_data[expression_id]
        
        # Enrich each relation with text metadata
        enriched = []
        for rel in relations:
            other_id = rel['otherId']
            
            # Get text metadata
            text_response = requests.get(
                f"{self.base_url}/v2/texts/{other_id}",
                headers=self.headers
            )
            
            if text_response.ok:
                text_data = text_response.json()
                enriched.append({
                    'type': rel['type'],
                    'direction': rel['direction'],
                    'otherId': other_id,
                    'title': text_data.get('title', {}),
                    'language': text_data.get('language', ''),
                    'contributions': text_data.get('contributions', [])
                })
            else:
                # Include without metadata if fetch fails
                enriched.append(rel)
        
        return {
            'expression_id': expression_id,
            'relations': enriched
        }

# Usage
enriched = EnrichedRelations("your_api_key")
result = enriched.get_enriched_relations("TEXT123")

print(f"Relations for {result['expression_id']}:\n")
for rel in result['relations']:
    print(f"  {rel['type']} ({rel['direction']})")
    print(f"    ID: {rel['otherId']}")
    if 'title' in rel:
        print(f"    Title: {rel['title']}")
        print(f"    Language: {rel['language']}")
```

---

## Workflow Examples

### Scenario 1: Display All Translations

```bash
#!/bin/bash

EXPRESSION_ID="TEXT123"

# Get relations
RELATIONS=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/$EXPRESSION_ID" \
  -H "X-API-Key: your_api_key")

echo "Translations of $EXPRESSION_ID:"

# Extract outgoing TRANSLATION_OF relations
echo "$RELATIONS" | jq -r ".[\"$EXPRESSION_ID\"][] | select(.type == \"TRANSLATION_OF\" and .direction == \"out\") | .otherId" | while read trans_id; do
  # Get translation metadata
  TRANS_INFO=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$trans_id" \
    -H "X-API-Key: your_api_key")
  
  TITLE=$(echo "$TRANS_INFO" | jq -r '.title.en // .title | to_entries[0].value')
  LANG=$(echo "$TRANS_INFO" | jq -r '.language')
  
  echo "  - [$LANG] $TITLE ($trans_id)"
done

echo -e "\nSource texts (if this is a translation):"

# Extract incoming TRANSLATION_OF relations
echo "$RELATIONS" | jq -r ".[\"$EXPRESSION_ID\"][] | select(.type == \"TRANSLATION_OF\" and .direction == \"in\") | .otherId" | while read source_id; do
  SOURCE_INFO=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$source_id" \
    -H "X-API-Key: your_api_key")
  
  TITLE=$(echo "$SOURCE_INFO" | jq -r '.title.bo // .title | to_entries[0].value')
  LANG=$(echo "$SOURCE_INFO" | jq -r '.language')
  
  echo "  - [$LANG] $TITLE ($source_id)"
done
```

---

### Scenario 2: Build Commentary Navigation

```python
import requests
from typing import List, Dict

class CommentaryNavigator:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def get_commentaries(self, expression_id: str) -> List[Dict]:
        """Get all commentaries on this text."""
        relations = self.get_relations(expression_id)
        
        if expression_id not in relations:
            return []
        
        # Filter incoming COMMENTARY_OF relations
        commentaries = [
            r for r in relations[expression_id]
            if r['type'] == 'COMMENTARY_OF' and r['direction'] == 'in'
        ]
        
        # Enrich with metadata
        enriched = []
        for comm in commentaries:
            text_info = self.get_text(comm['otherId'])
            if text_info:
                enriched.append({
                    'id': comm['otherId'],
                    'title': text_info.get('title', {}),
                    'language': text_info.get('language', ''),
                    'contributions': text_info.get('contributions', [])
                })
        
        return enriched
    
    def get_root_text(self, commentary_id: str) -> Dict:
        """Get the root text that this commentary explains."""
        relations = self.get_relations(commentary_id)
        
        if commentary_id not in relations:
            return None
        
        # Filter outgoing COMMENTARY_OF relations
        root_relations = [
            r for r in relations[commentary_id]
            if r['type'] == 'COMMENTARY_OF' and r['direction'] == 'out'
        ]
        
        if not root_relations:
            return None
        
        # Get root text info (typically just one)
        root_id = root_relations[0]['otherId']
        return self.get_text(root_id)
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        response = requests.get(
            f"{self.base_url}/v2/relations/expressions/{expression_id}",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_text(self, text_id: str) -> dict:
        """Get text metadata."""
        response = requests.get(
            f"{self.base_url}/v2/texts/{text_id}",
            headers=self.headers
        )
        if response.ok:
            return response.json()
        return None

# Usage
navigator = CommentaryNavigator("your_api_key")

# Get all commentaries on a root text
root_text_id = "ROOT_TEXT_001"
commentaries = navigator.get_commentaries(root_text_id)

print(f"Commentaries on {root_text_id}:\n")
for comm in commentaries:
    title = comm['title'].get('en', comm['title'].get('bo', 'Untitled'))
    print(f"  - {title} [{comm['language']}]")
    print(f"    ID: {comm['id']}")

# Get root text for a commentary
commentary_id = "COMM_001"
root_text = navigator.get_root_text(commentary_id)

if root_text:
    root_title = root_text['title'].get('bo', root_text['title'].get('en', 'Untitled'))
    print(f"\nCommentary on: {root_title}")
```

---

### Scenario 3: Building Relation Graph

```python
import requests
from typing import Set, Dict, List

class RelationGraphBuilder:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def build_graph(self, root_expression_id: str, max_depth: int = 2) -> Dict:
        """Build relation graph starting from root expression."""
        visited = set()
        graph = {
            'nodes': {},
            'edges': []
        }
        
        self._traverse(root_expression_id, visited, graph, depth=0, max_depth=max_depth)
        
        return graph
    
    def _traverse(self, expression_id: str, visited: Set, graph: Dict, depth: int, max_depth: int):
        """Recursively traverse relations."""
        if expression_id in visited or depth > max_depth:
            return
        
        visited.add(expression_id)
        
        # Get text info
        text_info = self.get_text(expression_id)
        if text_info:
            graph['nodes'][expression_id] = {
                'id': expression_id,
                'title': text_info.get('title', {}),
                'language': text_info.get('language', '')
            }
        
        # Get relations
        relations_data = self.get_relations(expression_id)
        
        if expression_id not in relations_data:
            return
        
        # Process each relation
        for rel in relations_data[expression_id]:
            other_id = rel['otherId']
            
            # Add edge
            graph['edges'].append({
                'from': expression_id if rel['direction'] == 'out' else other_id,
                'to': other_id if rel['direction'] == 'out' else expression_id,
                'type': rel['type']
            })
            
            # Traverse related expression
            self._traverse(other_id, visited, graph, depth + 1, max_depth)
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/relations/expressions/{expression_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except:
            return {}
    
    def get_text(self, text_id: str) -> dict:
        """Get text metadata."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/texts/{text_id}",
                headers=self.headers
            )
            if response.ok:
                return response.json()
        except:
            pass
        return None

# Usage
builder = RelationGraphBuilder("your_api_key")
graph = builder.build_graph("ROOT_TEXT_001", max_depth=2)

print(f"Graph has {len(graph['nodes'])} nodes and {len(graph['edges'])} edges\n")

print("Nodes:")
for node_id, node in graph['nodes'].items():
    title = node['title'].get('en', node['title'].get('bo', 'Untitled'))
    print(f"  {node_id}: {title} [{node['language']}]")

print("\nEdges:")
for edge in graph['edges']:
    print(f"  {edge['from']} --[{edge['type']}]--> {edge['to']}")
```

---

### Scenario 4: Find All Versions

```bash
#!/bin/bash

EXPRESSION_ID="TIBETAN_ROOT_TEXT"

echo "Finding all versions and related texts for: $EXPRESSION_ID"

# Get relations
RELATIONS=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/$EXPRESSION_ID" \
  -H "X-API-Key: your_api_key")

# Get source text info
SOURCE=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$EXPRESSION_ID" \
  -H "X-API-Key: your_api_key")

SOURCE_TITLE=$(echo "$SOURCE" | jq -r '.title.bo // .title.en // "Untitled"')
SOURCE_LANG=$(echo "$SOURCE" | jq -r '.language')

echo -e "\nSource Text:"
echo "  $SOURCE_TITLE [$SOURCE_LANG]"

# Get translations
echo -e "\nTranslations:"
echo "$RELATIONS" | jq -r ".[\"$EXPRESSION_ID\"][] | select(.type == \"TRANSLATION_OF\" and .direction == \"out\") | .otherId" | while read trans_id; do
  TRANS=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$trans_id" \
    -H "X-API-Key: your_api_key")
  
  TRANS_TITLE=$(echo "$TRANS" | jq -r '.title.en // .title | to_entries[0].value // "Untitled"')
  TRANS_LANG=$(echo "$TRANS" | jq -r '.language')
  
  echo "  - $TRANS_TITLE [$TRANS_LANG] ($trans_id)"
done

# Get commentaries
echo -e "\nCommentaries:"
echo "$RELATIONS" | jq -r ".[\"$EXPRESSION_ID\"][] | select(.type == \"COMMENTARY_OF\" and .direction == \"in\") | .otherId" | while read comm_id; do
  COMM=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$comm_id" \
    -H "X-API-Key: your_api_key")
  
  COMM_TITLE=$(echo "$COMM" | jq -r '.title.bo // .title.en // "Untitled"')
  COMM_LANG=$(echo "$COMM" | jq -r '.language')
  AUTHOR=$(echo "$COMM" | jq -r '.contributions[] | select(.role == "author") | .person_id' | head -1)
  
  echo "  - $COMM_TITLE [$COMM_LANG] by $AUTHOR ($comm_id)"
done
```

---

## Complete Examples

### Example 1: Text Relationship Browser

```javascript
class TextRelationshipBrowser {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api-l25bgmwqoa-uc.a.run.app';
    this.cache = new Map();
  }
  
  async exploreText(expressionId) {
    const text = await this.getText(expressionId);
    const relations = await this.getRelations(expressionId);
    
    return {
      text: text,
      relationships: await this.processRelations(expressionId, relations)
    };
  }
  
  async processRelations(expressionId, relationsData) {
    const relations = relationsData[expressionId] || [];
    
    const processed = {
      sourceTexts: [],
      translations: [],
      rootTexts: [],
      commentaries: []
    };
    
    for (const rel of relations) {
      const relatedText = await this.getText(rel.otherId);
      const item = {
        id: rel.otherId,
        title: this.getTitle(relatedText),
        language: relatedText.language
      };
      
      if (rel.type === 'TRANSLATION_OF') {
        if (rel.direction === 'in') {
          processed.sourceTexts.push(item);
        } else {
          processed.translations.push(item);
        }
      } else if (rel.type === 'COMMENTARY_OF') {
        if (rel.direction === 'out') {
          processed.rootTexts.push(item);
        } else {
          processed.commentaries.push(item);
        }
      }
    }
    
    return processed;
  }
  
  async getRelations(expressionId) {
    const response = await fetch(
      `${this.baseUrl}/v2/relations/expressions/${expressionId}`,
      {
        headers: { 'X-API-Key': this.apiKey }
      }
    );
    return await response.json();
  }
  
  async getText(textId) {
    if (this.cache.has(textId)) {
      return this.cache.get(textId);
    }
    
    const response = await fetch(
      `${this.baseUrl}/v2/texts/${textId}`,
      {
        headers: { 'X-API-Key': this.apiKey }
      }
    );
    
    const text = await response.json();
    this.cache.set(textId, text);
    return text;
  }
  
  getTitle(text) {
    if (typeof text.title === 'string') {
      return text.title;
    }
    return text.title.en || text.title.bo || text.title.sa || 'Untitled';
  }
  
  renderHTML(data) {
    let html = `
      <div class="text-browser">
        <h1>${this.getTitle(data.text)}</h1>
        <p>Language: ${data.text.language}</p>
    `;
    
    if (data.relationships.sourceTexts.length > 0) {
      html += '<h2>Translation of:</h2><ul>';
      data.relationships.sourceTexts.forEach(text => {
        html += `<li><a href="#${text.id}">${text.title}</a> [${text.language}]</li>`;
      });
      html += '</ul>';
    }
    
    if (data.relationships.translations.length > 0) {
      html += '<h2>Available Translations:</h2><ul>';
      data.relationships.translations.forEach(text => {
        html += `<li><a href="#${text.id}">${text.title}</a> [${text.language}]</li>`;
      });
      html += '</ul>';
    }
    
    if (data.relationships.rootTexts.length > 0) {
      html += '<h2>Commentary on:</h2><ul>';
      data.relationships.rootTexts.forEach(text => {
        html += `<li><a href="#${text.id}">${text.title}</a> [${text.language}]</li>`;
      });
      html += '</ul>';
    }
    
    if (data.relationships.commentaries.length > 0) {
      html += '<h2>Commentaries:</h2><ul>';
      data.relationships.commentaries.forEach(text => {
        html += `<li><a href="#${text.id}">${text.title}</a> [${text.language}]</li>`;
      });
      html += '</ul>';
    }
    
    html += '</div>';
    return html;
  }
}

// Usage
const browser = new TextRelationshipBrowser('your_api_key');

// Explore a text
const data = await browser.exploreText('TEXT123');

// Render to HTML
const html = browser.renderHTML(data);
document.getElementById('content').innerHTML = html;
```

---

### Example 2: Translation Chain Finder

```python
import requests
from collections import deque

class TranslationChainFinder:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def find_translation_chain(self, source_id: str, target_id: str) -> List[str]:
        """Find translation chain from source to target using BFS."""
        # BFS to find shortest path
        queue = deque([(source_id, [source_id])])
        visited = {source_id}
        
        while queue:
            current_id, path = queue.popleft()
            
            if current_id == target_id:
                return path
            
            # Get relations
            relations = self.get_relations(current_id)
            
            if current_id not in relations:
                continue
            
            # Find TRANSLATION_OF relations
            for rel in relations[current_id]:
                if rel['type'] != 'TRANSLATION_OF':
                    continue
                
                other_id = rel['otherId']
                
                if other_id not in visited:
                    visited.add(other_id)
                    queue.append((other_id, path + [other_id]))
        
        return None  # No path found
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/relations/expressions/{expression_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except:
            return {}
    
    def describe_chain(self, chain: List[str]) -> List[Dict]:
        """Get metadata for each text in chain."""
        described = []
        
        for text_id in chain:
            response = requests.get(
                f"{self.base_url}/v2/texts/{text_id}",
                headers=self.headers
            )
            
            if response.ok:
                text = response.json()
                title = text.get('title', {})
                if isinstance(title, dict):
                    title = title.get('en', title.get('bo', 'Untitled'))
                
                described.append({
                    'id': text_id,
                    'title': title,
                    'language': text.get('language', 'unknown')
                })
        
        return described

# Usage
finder = TranslationChainFinder("your_api_key")

# Find translation chain
source = "TIBETAN_TEXT_001"
target = "ENGLISH_TEXT_001"

chain = finder.find_translation_chain(source, target)

if chain:
    print(f"Translation chain found ({len(chain)} steps):\n")
    described = finder.describe_chain(chain)
    
    for i, text in enumerate(described):
        arrow = " -> " if i < len(described) - 1 else ""
        print(f"{text['title']} [{text['language']}]{arrow}", end="")
    print()
else:
    print("No translation chain found")
```

---

### Example 3: Multi-Language Navigation

```python
import requests
from typing import Dict, List

class MultiLanguageNavigator:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def get_all_language_versions(self, expression_id: str) -> Dict[str, List[Dict]]:
        """Get all language versions of a text."""
        # Start with the query text
        versions = {}
        processed = set()
        to_process = [expression_id]
        
        while to_process:
            current_id = to_process.pop()
            
            if current_id in processed:
                continue
            
            processed.add(current_id)
            
            # Get text info
            text = self.get_text(current_id)
            if not text:
                continue
            
            lang = text.get('language', 'unknown')
            if lang not in versions:
                versions[lang] = []
            
            versions[lang].append({
                'id': current_id,
                'title': text.get('title', {}),
                'contributions': text.get('contributions', [])
            })
            
            # Get relations
            relations = self.get_relations(current_id)
            
            if current_id not in relations:
                continue
            
            # Find all translation relations
            for rel in relations[current_id]:
                if rel['type'] == 'TRANSLATION_OF':
                    other_id = rel['otherId']
                    if other_id not in processed:
                        to_process.append(other_id)
        
        return versions
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/relations/expressions/{expression_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except:
            return {}
    
    def get_text(self, text_id: str) -> dict:
        """Get text metadata."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/texts/{text_id}",
                headers=self.headers
            )
            if response.ok:
                return response.json()
        except:
            pass
        return None

# Usage
navigator = MultiLanguageNavigator("your_api_key")
versions = navigator.get_all_language_versions("TIBETAN_ROOT_TEXT")

print("Available language versions:\n")
for lang, texts in sorted(versions.items()):
    print(f"{lang.upper()}:")
    for text in texts:
        title = text['title']
        if isinstance(title, dict):
            title = title.get(lang, title.get('en', 'Untitled'))
        print(f"  - {title} ({text['id']})")
```

---

## Integration Patterns

### Pattern 1: Text Detail Page

Complete text detail page with all relationships:

```
1. Load text metadata: GET /v2/texts/{text_id}
2. Load relations: GET /v2/relations/expressions/{text_id}
3. For each relation, load related text metadata
4. Display organized by relationship type
5. Provide navigation links to related texts
```

---

### Pattern 2: Translation Selector

Allow users to switch between language versions:

```
1. Load current text: GET /v2/texts/{text_id}
2. Get relations: GET /v2/relations/expressions/{text_id}
3. Filter TRANSLATION_OF relations (both in and out)
4. Build language selector dropdown
5. On selection, navigate to selected translation
```

---

### Pattern 3: Commentary View

Display text with its commentaries:

```
1. Load root text: GET /v2/texts/{root_id}
2. Get relations: GET /v2/relations/expressions/{root_id}
3. Filter incoming COMMENTARY_OF relations
4. Load each commentary metadata
5. Display root text with commentary links
6. On click, load and display commentary
```

---

## Relation Creation

Relations are created through the Texts API when creating texts with relationships:

### Creating Translation

```bash
# Create translation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"en": "English Translation"},
    "language": "en",
    "license": "public",
    "translations": [
      {
        "expression_id": "TIBETAN_SOURCE_ID"
      }
    ]
  }'

# Query relations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/TIBETAN_SOURCE_ID" \
  -H "X-API-Key: your_api_key"
```

The relation will show:
- From Tibetan perspective: Outgoing TRANSLATION_OF to English
- From English perspective: Incoming TRANSLATION_OF from Tibetan

---

### Creating Commentary

```bash
# Create commentary
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "འགྲེལ་པ།"},
    "language": "bo",
    "license": "public",
    "commentaries": [
      {
        "expression_id": "ROOT_TEXT_ID"
      }
    ],
    "contributions": [
      {
        "person_id": "SCHOLAR_ID",
        "role": "author"
      }
    ]
  }'

# Query relations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/ROOT_TEXT_ID" \
  -H "X-API-Key: your_api_key"
```

The relation will show:
- From root text perspective: Incoming COMMENTARY_OF from commentary
- From commentary perspective: Outgoing COMMENTARY_OF to root text

---

## Advanced Usage

### Recursive Relation Discovery

```python
import requests
from typing import Set, Dict

class RelationDiscovery:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def discover_all_related(self, expression_id: str, 
                            relation_type: str = None,
                            max_depth: int = 5) -> Set[str]:
        """Recursively discover all related expressions."""
        related = set()
        to_visit = [(expression_id, 0)]
        visited = set()
        
        while to_visit:
            current_id, depth = to_visit.pop(0)
            
            if current_id in visited or depth > max_depth:
                continue
            
            visited.add(current_id)
            
            # Get relations
            relations_data = self.get_relations(current_id)
            
            if current_id not in relations_data:
                continue
            
            # Process relations
            for rel in relations_data[current_id]:
                if relation_type and rel['type'] != relation_type:
                    continue
                
                other_id = rel['otherId']
                related.add(other_id)
                
                if other_id not in visited:
                    to_visit.append((other_id, depth + 1))
        
        return related
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/relations/expressions/{expression_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except:
            return {}

# Usage
discovery = RelationDiscovery("your_api_key")

# Find all texts in translation network
all_related = discovery.discover_all_related("TIBETAN_TEXT_001")
print(f"Found {len(all_related)} related texts in translation network")

# Find only commentary-related texts
commentaries = discovery.discover_all_related(
    "ROOT_TEXT_001", 
    relation_type="COMMENTARY_OF"
)
print(f"Found {len(commentaries)} texts in commentary network")
```

---

### Bidirectional Relation Handling

```python
import requests

class BidirectionalRelations:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def get_bidirectional_pairs(self, expression_id: str, 
                                relation_type: str = "TRANSLATION_OF") -> List[tuple]:
        """Get pairs of related expressions."""
        relations = self.get_relations(expression_id)
        
        if expression_id not in relations:
            return []
        
        pairs = []
        
        for rel in relations[expression_id]:
            if rel['type'] != relation_type:
                continue
            
            if rel['direction'] == 'out':
                # This -> Other
                pairs.append((expression_id, rel['otherId']))
            else:
                # Other -> This
                pairs.append((rel['otherId'], expression_id))
        
        return pairs
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        response = requests.get(
            f"{self.base_url}/v2/relations/expressions/{expression_id}",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()

# Usage
handler = BidirectionalRelations("your_api_key")

# Get translation pairs
pairs = handler.get_bidirectional_pairs("TEXT123", "TRANSLATION_OF")

for source_id, target_id in pairs:
    print(f"{source_id} --[translates to]--> {target_id}")
```

---

## Troubleshooting

### Common Issues

**Issue: "Resource was not found" when querying relations**

**Solution:**
- Expression ID does not exist
- Verify expression ID from texts list
- Check if text was deleted

```bash
# Verify expression exists
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/YOUR_EXPRESSION_ID" \
  -H "X-API-Key: your_api_key"
```

---

**Issue: Relations query returns empty array**

**Solution:**
- Expression has no relationships
- Relationships not created yet
- Check if translations/commentaries were properly created

```bash
# Check text metadata for relationship hints
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/YOUR_EXPRESSION_ID" \
  -H "X-API-Key: your_api_key" \
  | jq '{translations, commentaries}'
```

---

**Issue: Relations appear one-directional**

**Solution:**
- Relations are stored bidirectionally in the system
- Query both expressions to see both perspectives
- Use direction field to determine relationship orientation

```bash
# Query from source perspective
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/TIBETAN_TEXT" \
  -H "X-API-Key: your_api_key"

# Query from target perspective
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/ENGLISH_TEXT" \
  -H "X-API-Key: your_api_key"
```

---

**Issue: Too many relations to process efficiently**

**Solution:**
- Cache relations data
- Filter by type/direction before processing
- Paginate display of related texts
- Load metadata on-demand (lazy loading)

---

## Performance Optimization

### Caching Strategy

Relations change infrequently and benefit from caching:

```python
import requests
from datetime import datetime, timedelta
from typing import Optional

class CachedRelationsClient:
    def __init__(self, api_key: str, cache_ttl: int = 3600):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
        self.cache = {}
        self.cache_ttl = cache_ttl
    
    def get_relations(self, expression_id: str, force_refresh: bool = False) -> dict:
        """Get relations with caching."""
        cache_key = f"relations:{expression_id}"
        
        # Check cache
        if not force_refresh and cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            age = (datetime.now() - timestamp).total_seconds()
            
            if age < self.cache_ttl:
                return data
        
        # Fetch from API
        response = requests.get(
            f"{self.base_url}/v2/relations/expressions/{expression_id}",
            headers=self.headers
        )
        response.raise_for_status()
        data = response.json()
        
        # Cache result
        self.cache[cache_key] = (data, datetime.now())
        
        return data
    
    def invalidate(self, expression_id: str):
        """Invalidate cache for expression."""
        cache_key = f"relations:{expression_id}"
        if cache_key in self.cache:
            del self.cache[cache_key]
    
    def clear_cache(self):
        """Clear entire cache."""
        self.cache.clear()

# Usage
client = CachedRelationsClient("your_api_key", cache_ttl=3600)

# First call - fetches from API
relations = client.get_relations("TEXT123")

# Subsequent calls - uses cache
relations = client.get_relations("TEXT123")

# Force refresh
relations = client.get_relations("TEXT123", force_refresh=True)

# Invalidate specific entry
client.invalidate("TEXT123")
```

---

### Batch Relation Loading

Load relations for multiple expressions efficiently:

```python
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_relations_batch(expression_ids: List[str], api_key: str, 
                       max_workers: int = 5) -> Dict[str, dict]:
    """Get relations for multiple expressions in parallel."""
    base_url = "https://api-l25bgmwqoa-uc.a.run.app"
    headers = {"X-API-Key": api_key}
    
    def fetch_relations(expr_id):
        try:
            response = requests.get(
                f"{base_url}/v2/relations/expressions/{expr_id}",
                headers=headers
            )
            response.raise_for_status()
            return expr_id, response.json()
        except Exception as e:
            return expr_id, {"error": str(e)}
    
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_relations, expr_id): expr_id 
                  for expr_id in expression_ids}
        
        for future in as_completed(futures):
            expr_id, data = future.result()
            results[expr_id] = data
    
    return results

# Usage
expression_ids = ["TEXT001", "TEXT002", "TEXT003", "TEXT004", "TEXT005"]
all_relations = get_relations_batch(expression_ids, "your_api_key")

for expr_id, relations in all_relations.items():
    if 'error' in relations:
        print(f"{expr_id}: Error - {relations['error']}")
    elif expr_id in relations:
        rel_count = len(relations[expr_id])
        print(f"{expr_id}: {rel_count} relations")
    else:
        print(f"{expr_id}: No relations")
```

---

## Visualizations

### Graph Visualization

Generate graph data for visualization tools:

```python
import requests
import json

class RelationGraphVisualizer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def generate_graph_json(self, expression_id: str, max_depth: int = 2) -> dict:
        """Generate graph data for visualization (D3.js, Cytoscape, etc.)."""
        nodes = {}
        edges = []
        visited = set()
        
        self._build_graph(expression_id, nodes, edges, visited, depth=0, max_depth=max_depth)
        
        # Convert to standard graph format
        return {
            'nodes': list(nodes.values()),
            'edges': edges
        }
    
    def _build_graph(self, expr_id: str, nodes: dict, edges: list, 
                     visited: Set, depth: int, max_depth: int):
        """Recursively build graph structure."""
        if expr_id in visited or depth > max_depth:
            return
        
        visited.add(expr_id)
        
        # Get text info
        text = self.get_text(expr_id)
        if text:
            title = text.get('title', {})
            if isinstance(title, dict):
                title = title.get('en', title.get('bo', 'Untitled'))
            
            nodes[expr_id] = {
                'id': expr_id,
                'label': title,
                'language': text.get('language', ''),
                'type': 'expression'
            }
        
        # Get relations
        relations = self.get_relations(expr_id)
        
        if expr_id not in relations:
            return
        
        # Process relations
        for rel in relations[expr_id]:
            other_id = rel['otherId']
            
            # Add edge
            if rel['direction'] == 'out':
                edges.append({
                    'source': expr_id,
                    'target': other_id,
                    'type': rel['type'],
                    'label': self._format_label(rel['type'])
                })
            else:
                edges.append({
                    'source': other_id,
                    'target': expr_id,
                    'type': rel['type'],
                    'label': self._format_label(rel['type'])
                })
            
            # Recurse
            self._build_graph(other_id, nodes, edges, visited, depth + 1, max_depth)
    
    def _format_label(self, rel_type: str) -> str:
        """Format relation type for display."""
        if rel_type == 'TRANSLATION_OF':
            return 'translation'
        elif rel_type == 'COMMENTARY_OF':
            return 'commentary'
        return rel_type.lower()
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/relations/expressions/{expression_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except:
            return {}
    
    def get_text(self, text_id: str) -> dict:
        """Get text metadata."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/texts/{text_id}",
                headers=self.headers
            )
            if response.ok:
                return response.json()
        except:
            pass
        return None

# Usage
visualizer = RelationGraphVisualizer("your_api_key")
graph = visualizer.generate_graph_json("ROOT_TEXT_001", max_depth=2)

# Export for D3.js
with open('graph.json', 'w', encoding='utf-8') as f:
    json.dump(graph, f, ensure_ascii=False, indent=2)

print(f"Generated graph with {len(graph['nodes'])} nodes and {len(graph['edges'])} edges")
```

**D3.js Integration Example:**

```html
<!DOCTYPE html>
<html>
<head>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    .node { fill: #69b3a2; stroke: #333; stroke-width: 2px; }
    .link { stroke: #999; stroke-opacity: 0.6; marker-end: url(#arrow); }
    .label { font-family: sans-serif; font-size: 12px; }
  </style>
</head>
<body>
  <svg id="graph" width="800" height="600"></svg>
  
  <script>
    // Load graph data from API
    async function loadGraph() {
      const response = await fetch(
        'https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/TEXT123',
        {
          headers: { 'X-API-Key': 'your_api_key' }
        }
      );
      
      const relationsData = await response.json();
      
      // Convert to D3 format
      const nodes = [];
      const links = [];
      const nodeIds = new Set();
      
      for (const [exprId, relations] of Object.entries(relationsData)) {
        if (!nodeIds.has(exprId)) {
          nodes.push({ id: exprId, label: exprId });
          nodeIds.add(exprId);
        }
        
        relations.forEach(rel => {
          if (!nodeIds.has(rel.otherId)) {
            nodes.push({ id: rel.otherId, label: rel.otherId });
            nodeIds.add(rel.otherId);
          }
          
          if (rel.direction === 'out') {
            links.push({
              source: exprId,
              target: rel.otherId,
              type: rel.type
            });
          }
        });
      }
      
      return { nodes, links };
    }
    
    // Render graph
    async function renderGraph() {
      const { nodes, links } = await loadGraph();
      
      const svg = d3.select("#graph");
      const width = 800;
      const height = 600;
      
      // Create force simulation
      const simulation = d3.forceSimulation(nodes)
        .force("link", d3.forceLink(links).id(d => d.id))
        .force("charge", d3.forceManyBody().strength(-300))
        .force("center", d3.forceCenter(width / 2, height / 2));
      
      // Draw links
      const link = svg.append("g")
        .selectAll("line")
        .data(links)
        .enter().append("line")
        .attr("class", "link");
      
      // Draw nodes
      const node = svg.append("g")
        .selectAll("circle")
        .data(nodes)
        .enter().append("circle")
        .attr("class", "node")
        .attr("r", 8);
      
      // Add labels
      const label = svg.append("g")
        .selectAll("text")
        .data(nodes)
        .enter().append("text")
        .attr("class", "label")
        .text(d => d.label);
      
      // Update positions
      simulation.on("tick", () => {
        link
          .attr("x1", d => d.source.x)
          .attr("y1", d => d.source.y)
          .attr("x2", d => d.target.x)
          .attr("y2", d => d.target.y);
        
        node
          .attr("cx", d => d.x)
          .attr("cy", d => d.y);
        
        label
          .attr("x", d => d.x + 12)
          .attr("y", d => d.y + 4);
      });
    }
    
    renderGraph();
  </script>
</body>
</html>
```

---

## Real-World Applications

### Application 1: Translation Selector Component

```javascript
class TranslationSelector {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api-l25bgmwqoa-uc.a.run.app';
  }
  
  async getAvailableLanguages(expressionId) {
    // Get relations
    const relations = await this.getRelations(expressionId);
    const expressionRelations = relations[expressionId] || [];
    
    // Get current text
    const currentText = await this.getText(expressionId);
    
    // Build language list
    const languages = [
      {
        id: expressionId,
        language: currentText.language,
        title: this.getTitle(currentText),
        current: true
      }
    ];
    
    // Add translations
    for (const rel of expressionRelations) {
      if (rel.type !== 'TRANSLATION_OF') {
        continue;
      }
      
      const otherId = rel.otherId;
      const otherText = await this.getText(otherId);
      
      if (otherText) {
        languages.push({
          id: otherId,
          language: otherText.language,
          title: this.getTitle(otherText),
          current: false
        });
      }
    }
    
    return languages;
  }
  
  async getRelations(expressionId) {
    const response = await fetch(
      `${this.baseUrl}/v2/relations/expressions/${expressionId}`,
      {
        headers: { 'X-API-Key': this.apiKey }
      }
    );
    return await response.json();
  }
  
  async getText(textId) {
    const response = await fetch(
      `${this.baseUrl}/v2/texts/${textId}`,
      {
        headers: { 'X-API-Key': this.apiKey }
      }
    );
    return await response.json();
  }
  
  getTitle(text) {
    const title = text.title;
    if (typeof title === 'string') {
      return title;
    }
    return title[text.language] || title.en || title.bo || 'Untitled';
  }
  
  renderSelector(languages) {
    const select = document.createElement('select');
    select.id = 'language-selector';
    
    languages.forEach(lang => {
      const option = document.createElement('option');
      option.value = lang.id;
      option.textContent = `${this.formatLanguage(lang.language)}: ${lang.title}`;
      option.selected = lang.current;
      select.appendChild(option);
    });
    
    select.addEventListener('change', (e) => {
      window.location.href = `/texts/${e.target.value}`;
    });
    
    return select;
  }
  
  formatLanguage(code) {
    const names = {
      'bo': 'Tibetan',
      'en': 'English',
      'zh': 'Chinese',
      'sa': 'Sanskrit'
    };
    return names[code] || code.toUpperCase();
  }
}

// Usage
const selector = new TranslationSelector('your_api_key');
const languages = await selector.getAvailableLanguages('TEXT123');
const selectorElement = selector.renderSelector(languages);
document.getElementById('language-selector-container').appendChild(selectorElement);
```

---

### Application 2: Commentary Sidebar

```python
import requests
from typing import List, Dict

class CommentarySidebar:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def get_commentary_data(self, root_text_id: str) -> Dict:
        """Get all commentary data for sidebar display."""
        # Get root text
        root_text = self.get_text(root_text_id)
        
        # Get relations
        relations = self.get_relations(root_text_id)
        
        if root_text_id not in relations:
            return {
                'root_text': root_text,
                'commentaries': []
            }
        
        # Filter incoming COMMENTARY_OF relations
        commentaries = []
        for rel in relations[root_text_id]:
            if rel['type'] == 'COMMENTARY_OF' and rel['direction'] == 'in':
                comm = self.get_text(rel['otherId'])
                if comm:
                    # Get author info
                    author = self._get_author(comm)
                    
                    commentaries.append({
                        'id': rel['otherId'],
                        'title': comm.get('title', {}),
                        'language': comm.get('language', ''),
                        'author': author,
                        'license': comm.get('license', '')
                    })
        
        return {
            'root_text': root_text,
            'commentaries': commentaries
        }
    
    def _get_author(self, text: dict) -> Dict:
        """Extract author info from contributions."""
        contributions = text.get('contributions', [])
        
        for contrib in contributions:
            if contrib.get('role') == 'author':
                person_id = contrib.get('person_id')
                if person_id:
                    person = self.get_person(person_id)
                    if person:
                        return {
                            'id': person_id,
                            'name': person.get('name', {})
                        }
        
        return None
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        response = requests.get(
            f"{self.base_url}/v2/relations/expressions/{expression_id}",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_text(self, text_id: str) -> dict:
        """Get text metadata."""
        response = requests.get(
            f"{self.base_url}/v2/texts/{text_id}",
            headers=self.headers
        )
        if response.ok:
            return response.json()
        return None
    
    def get_person(self, person_id: str) -> dict:
        """Get person metadata."""
        response = requests.get(
            f"{self.base_url}/v2/persons/{person_id}",
            headers=self.headers
        )
        if response.ok:
            return response.json()
        return None

# Usage
sidebar = CommentarySidebar("your_api_key")
data = sidebar.get_commentary_data("ROOT_TEXT_001")

print(f"Root Text: {data['root_text']['title']}\n")
print(f"Commentaries ({len(data['commentaries'])}):")

for comm in data['commentaries']:
    title = comm['title'].get('bo', comm['title'].get('en', 'Untitled'))
    print(f"\n  {title}")
    print(f"    Language: {comm['language']}")
    print(f"    ID: {comm['id']}")
    
    if comm['author']:
        author_name = comm['author']['name'].get('bo', comm['author']['name'].get('en', 'Unknown'))
        print(f"    Author: {author_name}")
```

---

### Application 3: Translation Network Explorer

```python
import requests
from collections import defaultdict

class TranslationNetworkExplorer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api-l25bgmwqoa-uc.a.run.app"
        self.headers = {"X-API-Key": api_key}
    
    def analyze_network(self, expression_id: str) -> Dict:
        """Analyze translation network for an expression."""
        # Find all texts in network
        network_texts = self._discover_network(expression_id)
        
        # Analyze network
        analysis = {
            'total_texts': len(network_texts),
            'languages': defaultdict(int),
            'hub_texts': [],
            'leaf_texts': []
        }
        
        # Process each text
        for text_id in network_texts:
            text = self.get_text(text_id)
            if text:
                analysis['languages'][text.get('language', 'unknown')] += 1
            
            # Get relations to determine hub vs leaf
            relations = self.get_relations(text_id)
            rel_count = len(relations.get(text_id, []))
            
            if rel_count >= 3:
                analysis['hub_texts'].append({
                    'id': text_id,
                    'relation_count': rel_count
                })
            elif rel_count == 0:
                analysis['leaf_texts'].append(text_id)
        
        return analysis
    
    def _discover_network(self, expression_id: str, max_depth: int = 5) -> Set[str]:
        """Discover all texts in translation network."""
        network = set()
        to_visit = [(expression_id, 0)]
        visited = set()
        
        while to_visit:
            current_id, depth = to_visit.pop(0)
            
            if current_id in visited or depth > max_depth:
                continue
            
            visited.add(current_id)
            network.add(current_id)
            
            # Get relations
            relations = self.get_relations(current_id)
            
            if current_id not in relations:
                continue
            
            # Follow translation relations
            for rel in relations[current_id]:
                if rel['type'] == 'TRANSLATION_OF':
                    other_id = rel['otherId']
                    if other_id not in visited:
                        to_visit.append((other_id, depth + 1))
        
        return network
    
    def get_relations(self, expression_id: str) -> dict:
        """Get relations for expression."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/relations/expressions/{expression_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except:
            return {}
    
    def get_text(self, text_id: str) -> dict:
        """Get text metadata."""
        try:
            response = requests.get(
                f"{self.base_url}/v2/texts/{text_id}",
                headers=self.headers
            )
            if response.ok:
                return response.json()
        except:
            pass
        return None

# Usage
explorer = TranslationNetworkExplorer("your_api_key")
analysis = explorer.analyze_network("TIBETAN_TEXT_001")

print("Translation Network Analysis:")
print(f"  Total texts: {analysis['total_texts']}")
print(f"\n  Languages:")
for lang, count in analysis['languages'].items():
    print(f"    {lang}: {count} texts")

print(f"\n  Hub texts (3+ connections):")
for hub in analysis['hub_texts']:
    print(f"    {hub['id']} ({hub['relation_count']} relations)")

print(f"\n  Leaf texts (no connections): {len(analysis['leaf_texts'])}")
```

---

## Quick Reference

### HTTP Methods Summary

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/v2/relations/expressions/{expression_id}` | Get all relations for expression |

### Query Parameters

No query parameters for the relations endpoint.

### Response Structure

```
{
  "expression_id": [
    { type, direction, otherId },
    { type, direction, otherId }
  ]
}
```

### Relation Types

| Type | Description |
|------|-------------|
| `TRANSLATION_OF` | Translation relationship |
| `COMMENTARY_OF` | Commentary relationship |

### Directions

| Direction | Meaning |
|-----------|---------|
| `in` | Incoming relation (other → this) |
| `out` | Outgoing relation (this → other) |

### Status Codes

| Status | Meaning | Common Use |
|--------|---------|------------|
| 200 | OK | Relations retrieved successfully |
| 404 | Not Found | Expression doesn't exist |
| 500 | Server Error | Internal error |

---

## Integration with Other APIs

### With Texts API

Relations complement text metadata:

```bash
# Get text
TEXT=$(curl -s -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/TEXT123" \
  -H "X-API-Key: your_api_key")

# Get relations
RELATIONS=$(curl -s -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/TEXT123" \
  -H "X-API-Key: your_api_key")

# Combine for complete view
echo "$TEXT" | jq ". + {relations: $(echo "$RELATIONS" | jq '.[\"TEXT123\"]')}"
```

---

### With Editions API

Relations link texts; editions provide content:

```python
import requests

def get_translated_content(source_edition_id: str, target_language: str, api_key: str) -> str:
    """Get translation content for edition."""
    headers = {"X-API-Key": api_key}
    base_url = "https://api-l25bgmwqoa-uc.a.run.app"
    
    # Get source edition to get expression_id
    edition = requests.get(
        f"{base_url}/v2/editions/{source_edition_id}",
        headers=headers
    ).json()
    
    source_expression_id = edition['text_id']
    
    # Get relations
    relations = requests.get(
        f"{base_url}/v2/relations/expressions/{source_expression_id}",
        headers=headers
    ).json()
    
    # Find translation in target language
    for rel in relations.get(source_expression_id, []):
        if rel['type'] != 'TRANSLATION_OF' or rel['direction'] != 'out':
            continue
        
        trans_text = requests.get(
            f"{base_url}/v2/texts/{rel['otherId']}",
            headers=headers
        ).json()
        
        if trans_text.get('language') == target_language:
            # Get editions for this translation
            editions = requests.get(
                f"{base_url}/v2/texts/{rel['otherId']}/editions",
                headers=headers
            ).json()
            
            if editions:
                # Get first edition content
                first_edition_id = editions[0]['id']
                content = requests.get(
                    f"{base_url}/v2/editions/{first_edition_id}/content",
                    headers=headers
                ).json()
                
                return content
    
    return None

# Usage
english_translation = get_translated_content(
    "TIBETAN_EDITION_001",
    "en",
    "your_api_key"
)

if english_translation:
    print("Translation:", english_translation[:200], "...")
else:
    print("No translation available in specified language")
```

---

### With Segments API

Combine relations and segments for aligned content:

```bash
# Get source segment
SOURCE_CONTENT=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/SEG_SOURCE/content" \
  -H "X-API-Key: your_api_key")

# Get segment info (from related query to get manifestation_id)
RELATED=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/segments/SEG_SOURCE/related" \
  -H "X-API-Key: your_api_key")

# Extract manifestation_id and get edition
MANIFESTATION_ID=$(echo "$RELATED" | jq -r '.[0].manifestation_id')

# Get edition to find expression_id
EDITION=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$MANIFESTATION_ID" \
  -H "X-API-Key: your_api_key")

EXPRESSION_ID=$(echo "$EDITION" | jq -r '.text_id')

# Get relations for this expression
RELATIONS=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/$EXPRESSION_ID" \
  -H "X-API-Key: your_api_key")

echo "Source segment: $SOURCE_CONTENT"
echo -e "\nRelated expressions: $(echo "$RELATIONS" | jq -r ".[\"$EXPRESSION_ID\"] | length")"
```

---

## Frequently Asked Questions

### Q: How do I create a relation between texts?

**A:** Relations are created implicitly when you create texts with translations or commentaries:

```bash
# Create translation (creates TRANSLATION_OF relation)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"en": "English Translation"},
    "language": "en",
    "translations": [{"expression_id": "TIBETAN_TEXT_ID"}]
  }'

# Create commentary (creates COMMENTARY_OF relation)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "Commentary"},
    "language": "bo",
    "commentaries": [{"expression_id": "ROOT_TEXT_ID"}]
  }'
```

### Q: Can I delete a relation?

**A:** Relations cannot be deleted directly. They are managed through the Texts API. To remove a relation, you would need to update or delete the related text.

### Q: Are relations bidirectional?

**A:** Yes, conceptually. When you create a translation relation, both texts can see the relationship from their perspective (using `direction` field). Query each expression to see its view of the relationship.

### Q: What's the difference between `in` and `out` directions?

**A:**
- **`out`**: This expression relates TO another (e.g., "this is a translation of that")
- **`in`**: Another expression relates TO this (e.g., "that is a translation of this")

From Tibetan text perspective:
- English translation: `out` (Tibetan has English translation)

From English text perspective:
- Tibetan source: `in` (English is translation of Tibetan)

### Q: Can a text have multiple relation types?

**A:** Yes, a text can be both a translation and have commentaries, or any other combination of supported relation types.

### Q: How many levels of relations should I traverse?

**A:** Depends on your use case:
- **Direct relationships**: 0 levels (just query expression)
- **Translations + their commentaries**: 1-2 levels
- **Full network**: 3-5 levels (be mindful of performance)

### Q: Can relations form cycles?

**A:** In theory, yes (e.g., A translates to B, B translates to C, C translates back to A), though this is unlikely in practice. Use a visited set when traversing to avoid infinite loops.

### Q: Are there other relation types besides TRANSLATION_OF and COMMENTARY_OF?

**A:** Currently, only `TRANSLATION_OF` and `COMMENTARY_OF` are supported. Future relation types may be added.

---

## Validation Rules

### Get Relations

| Rule | Validation |
|------|------------|
| expression_id | Required, must reference existing expression |

---

## Performance Considerations

### API Call Optimization

**Minimize Calls:**
- Cache relations (change infrequently)
- Batch text metadata fetches
- Use relation data to avoid redundant queries

**Example - Efficient Loading:**

```python
# Inefficient: Multiple sequential calls
for rel in relations:
    text = get_text(rel['otherId'])  # Sequential
    # process...

# Efficient: Batch parallel calls
other_ids = [rel['otherId'] for rel in relations]
texts = get_texts_batch(other_ids)  # Parallel
```

---

### Cache Strategy

**Cache Keys:**
```
relations:TEXT123
text:TEXT123
person:P123
```

**TTL Recommendations:**
- Relations: 1-24 hours (stable)
- Text metadata: 1-6 hours (relatively stable)
- Person metadata: 6-24 hours (very stable)

**Invalidation:**
- Invalidate on text creation with relationships
- Invalidate on text deletion
- Invalidate on text update if relationships change

---

## Migration Notes

### From Text API Relationships

Previously, relationship data was embedded in text responses (`translations`, `commentaries` arrays). The Relations API provides:

**Advantages:**
- Unified view of all relationships
- Bidirectional perspective
- Graph traversal support
- Separate caching strategy
- Better performance for relation-heavy queries

**Migration:**

```python
# Old approach: Get from text metadata
text = requests.get(f"{base_url}/v2/texts/{text_id}").json()
translations = text.get('translations', [])
commentaries = text.get('commentaries', [])

# New approach: Get from relations endpoint
relations_data = requests.get(f"{base_url}/v2/relations/expressions/{text_id}").json()
relations = relations_data.get(text_id, [])

# Filter translations
translations = [
    r['otherId'] for r in relations 
    if r['type'] == 'TRANSLATION_OF' and r['direction'] == 'out'
]

# Filter commentaries
commentaries = [
    r['otherId'] for r in relations 
    if r['type'] == 'COMMENTARY_OF' and r['direction'] == 'in'
]
```

---

## Limitations and Considerations

### Current Limitations

1. **No Direct Relation Creation**
   - Relations created via Texts API
   - Cannot create standalone relations

2. **No Relation Update**
   - Update by modifying source text
   - No direct relation mutation endpoint

3. **No Relation Delete**
   - Delete by removing relationship from text
   - Or delete related text

4. **Limited Relation Types**
   - Only TRANSLATION_OF and COMMENTARY_OF
   - No custom relation types

5. **No Relation Metadata**
   - Cannot add notes or metadata to relations
   - No relation creation timestamps
   - No relation creator tracking

---

## Semantic Meaning

### Translation Relations

**TRANSLATION_OF** establishes equivalence across languages:

```
Tibetan Text --[TRANSLATION_OF]--> English Text
```

**Semantics:**
- Content equivalence (same meaning, different language)
- One-to-many possible (multiple translations)
- Direction: source → translation

---

### Commentary Relations

**COMMENTARY_OF** establishes explanatory relationships:

```
Commentary --[COMMENTARY_OF]--> Root Text
```

**Semantics:**
- Explanatory relationship (commentary explains root)
- One-to-many possible (multiple commentaries on same text)
- Direction: commentary → root text

---

### Chained Relations

Relations can chain across texts:

```
Tibetan Root
  ├─[translation]→ English Translation
  │                 └─[commentary]→ English Commentary
  └─[translation]→ Chinese Translation
                    └─[commentary]→ Chinese Commentary
```

Use recursive queries to discover full relationship networks.

---

## See Also

- [Texts API Documentation](./texts-api.md) - Creating texts with relationships
- [Editions API Documentation](./editions-api.md) - Edition content and management
- [Segments API Documentation](./segments-api.md) - Segment-level content alignment
- [Annotations API Documentation](./annotations-api.md) - Alignment annotations
- OpenAPI Specification: `GET /v2/schema/openapi`
