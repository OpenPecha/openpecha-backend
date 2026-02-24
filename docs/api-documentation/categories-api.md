# Categories API Documentation

This document provides comprehensive documentation for all Categories-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Category Endpoints](#category-endpoints)
   - [List Categories](#list-categories)
   - [Create New Category](#create-new-category)
4. [Hierarchical Structure](#hierarchical-structure)
   - [Root Categories](#root-categories)
   - [Child Categories](#child-categories)
   - [Navigating Hierarchies](#navigating-hierarchies)
5. [Application Context](#application-context)
6. [Data Models](#data-models)
7. [Error Responses](#error-responses)
8. [Best Practices](#best-practices)

---

## Overview

Categories provide a hierarchical taxonomy system for organizing texts. Each category can have localized titles in multiple languages and can be part of a parent-child hierarchy for nested organization.

### Key Concepts

- **Category**: A classification or topic used to organize texts
- **Hierarchy**: Tree structure where categories can have parent and child categories
- **Application Context**: Categories are scoped to specific applications via the `X-Application` header
- **Localized Titles**: Support for category names in multiple languages

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

## Category Endpoints

### List Categories

Get categories filtered by application (via `X-Application` header) and optional parent.

**Endpoint:**
```
GET /v2/categories
```

**Headers:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `X-API-Key` | string | Yes | API authentication key |
| `X-Application` | string | Yes | Application context for categories |

**Parameters:**

| Name | Type | Location | Required | Default | Description |
|------|------|----------|----------|---------|-------------|
| `parent_id` | string | query | No | null | Parent category ID (null or omitted returns root categories) |
| `language` | string | query | No | "bo" | Language code for filtering |

**Response: 200 OK (Root Categories)**

```json
[
  {
    "id": "CAT12345678",
    "parent_id": null,
    "title": {
      "en": "Literature",
      "bo": "རྩོམ་རིག"
    },
    "children": ["CAT87654321"]
  },
  {
    "id": "CAT23456789",
    "parent_id": null,
    "title": {
      "en": "History",
      "bo": "ལོ་རྒྱུས"
    },
    "children": []
  }
]
```

**Response: 200 OK (Child Categories)**

```json
[
  {
    "id": "CAT87654321",
    "parent_id": "CAT12345678",
    "title": {
      "en": "Poetry",
      "bo": "སྙན་ངག"
    },
    "children": []
  }
]
```

**Error Responses:**
- `400 Bad Request`: Invalid parameters or missing X-Application header
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all root categories
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Get child categories of a specific parent
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=CAT12345678" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Get categories with specific language filter
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?language=en" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Get child categories with language filter
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=CAT12345678&language=bo" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"
```

---

### Create New Category

Create a category with localized title and optional parent relationship. Application context is provided via `X-Application` header.

**Endpoint:**
```
POST /v2/categories
```

**Headers:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `X-API-Key` | string | Yes | API authentication key |
| `X-Application` | string | Yes | Application context for the category |

**Request Body:**

```json
{
  "title": {
    "en": "Literature",
    "bo": "རྩོམ་རིག"
  },
  "description": {
    "en": "Literary works and compositions",
    "bo": "རྩོམ་རིག་གི་བརྩམས་ཆོས།"
  },
  "parent_id": "CAT12345678"
}
```

**Required Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `title` | object | Localized category titles (language code → text mapping) |

**Optional Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `description` | object | Localized category descriptions (language code → text mapping) |
| `parent_id` | string | Parent category ID (null for root categories) |

**Response: 201 Created**

```json
{
  "id": "CAT12345678"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request parameters or missing X-Application header
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Create root category
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Literature",
      "bo": "རྩོམ་རིག"
    }
  }'

# Create child category with parent
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Poetry",
      "bo": "སྙན་ངག"
    },
    "parent_id": "CAT12345678"
  }'

# Create category with description
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Buddhist Philosophy",
      "bo": "ནང་པའི་གྲུབ་མཐའ།"
    },
    "description": {
      "en": "Philosophical texts from Buddhist traditions",
      "bo": "ནང་པའི་ལུགས་ཀྱི་གྲུབ་མཐའི་གཞུང་ལུགས།"
    }
  }'
```

---

## Hierarchical Structure

Categories support a hierarchical tree structure with parent-child relationships.

### Root Categories

Root categories have no parent and serve as top-level classification nodes.

**Characteristics:**
- `parent_id` is `null`
- Top-level categories in the taxonomy
- Can have child categories

**Example:**

```json
{
  "id": "CAT_ROOT_001",
  "parent_id": null,
  "title": {
    "en": "Buddhist Canon",
    "bo": "བཀའ་འགྱུར།"
  },
  "children": ["CAT_CHILD_001", "CAT_CHILD_002"]
}
```

**Get Root Categories:**

```bash
# Omit parent_id parameter or set it to null
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"
```

---

### Child Categories

Child categories have a parent and represent subcategories or refinements of classification.

**Characteristics:**
- `parent_id` references another category
- Can themselves have child categories (multi-level hierarchy)
- Listed in parent's `children` array

**Example:**

```json
{
  "id": "CAT_CHILD_001",
  "parent_id": "CAT_ROOT_001",
  "title": {
    "en": "Sutras",
    "bo": "མདོ་སྡེ།"
  },
  "children": ["CAT_GRANDCHILD_001"]
}
```

**Get Child Categories:**

```bash
# Specify parent_id to get children
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=CAT_ROOT_001" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"
```

---

### Navigating Hierarchies

Build complete category trees by recursively querying children.

**Example Hierarchy:**

```
Buddhist Canon (CAT_ROOT_001)
├── Sutras (CAT_SUTRAS)
│   ├── Prajnaparamita (CAT_PRAJNAPARA)
│   └── Avatamsaka (CAT_AVATAMSAKA)
├── Tantras (CAT_TANTRAS)
│   ├── Yoga Tantra (CAT_YOGA)
│   └── Anuttarayoga (CAT_ANUTTARA)
└── Commentaries (CAT_COMMENTARIES)
```

**Recursive Query Script:**

```bash
#!/bin/bash

function get_category_tree() {
  local parent_id=$1
  local indent=$2
  
  # Get categories for this level
  if [ -z "$parent_id" ]; then
    CATEGORIES=$(curl -s -X GET \
      "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
      -H "X-API-Key: your_api_key" \
      -H "X-Application: webuddhist")
  else
    CATEGORIES=$(curl -s -X GET \
      "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=$parent_id" \
      -H "X-API-Key: your_api_key" \
      -H "X-Application: webuddhist")
  fi
  
  # Process each category
  echo "$CATEGORIES" | jq -c '.[]' | while read category; do
    id=$(echo "$category" | jq -r '.id')
    title=$(echo "$category" | jq -r '.title.en')
    children=$(echo "$category" | jq -r '.children | length')
    
    echo "${indent}${title} (${id})"
    
    # Recurse for children
    if [ "$children" -gt 0 ]; then
      get_category_tree "$id" "${indent}  "
    fi
  done
}

# Start from root
get_category_tree "" ""
```

---

## Application Context

Categories are scoped to specific applications using the `X-Application` header. This allows different applications to maintain separate category taxonomies.

### X-Application Header

**Required for all category operations:**
- Category retrieval: `GET /v2/categories`
- Category creation: `POST /v2/categories`

**Example Applications:**
- `webuddhist`: Main Buddhist text application
- `monlam`: Monlam application
- `lotsawa`: Translation application
- Custom application names (lowercase)

**Example:**

```bash
# Categories for webuddhist application
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Categories for monlam application
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: monlam"
```

**Note:** Categories in different applications are completely separate. A category with the same title can exist independently in multiple applications.

---

## Data Models

### CategoryInput

Schema for creating a new category:

```typescript
{
  title: {                       // Required
    [language: string]: string;  // At least one language required
  };
  description?: {                // Optional
    [language: string]: string;
  } | null;
  parent_id?: string | null;     // Optional: null for root categories
}
```

**Examples:**

Root category without description:
```json
{
  "title": {
    "en": "Buddhist Canon",
    "bo": "བཀའ་འགྱུར།"
  }
}
```

Child category with description:
```json
{
  "title": {
    "en": "Mahayana Sutras",
    "bo": "ཐེག་ཆེན་མདོ་སྡེ།",
    "sa": "महायानसूत्र"
  },
  "description": {
    "en": "Sutras of the Great Vehicle",
    "bo": "ཐེག་པ་ཆེན་པོའི་མདོ།"
  },
  "parent_id": "CAT_SUTRAS"
}
```

---

### CategoryOutput

Schema for category response data:

```typescript
{
  id: string;                    // Category ID
  title: {                       // Localized titles
    [language: string]: string;
  };
  description?: {                // Optional localized descriptions
    [language: string]: string;
  } | null;
  parent_id: string | null;      // Parent category ID (null for root)
  children: string[];            // Array of child category IDs
}
```

**Example:**

```json
{
  "id": "CAT12345678",
  "parent_id": "CAT_ROOT",
  "title": {
    "en": "Perfection of Wisdom",
    "bo": "ཤེར་ཕྱིན།",
    "sa": "प्रज्ञापारमिता"
  },
  "description": {
    "en": "Texts on the perfection of wisdom",
    "bo": "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་གཞུང་ལུགས།"
  },
  "children": [
    "CAT_PRAJNAPARA_SHORT",
    "CAT_PRAJNAPARA_MEDIUM",
    "CAT_PRAJNAPARA_LONG"
  ]
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
- Missing `X-Application` header
- Invalid `X-Application` value
- Invalid `parent_id` (references non-existent category)
- Invalid query parameters

### 422 Validation Error

```json
{
  "error": "Validation error",
  "details": [
    {
      "loc": ["body", "title"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

**Common Causes:**
- Missing required `title` field
- Empty title object
- Invalid field types
- Title object without any language entries

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

---

## Best Practices

### 1. Creating Categories

**Do:**
- Provide titles in multiple languages (especially Tibetan and English for Buddhist texts)
- Use descriptive titles that clearly indicate the category's scope
- Plan hierarchy structure before creating categories
- Use consistent naming conventions across languages
- Add descriptions for clarity when category purpose is not obvious

**Don't:**
- Create duplicate categories with identical purposes
- Use overly broad or vague category names
- Create deeply nested hierarchies without clear organization
- Forget to specify X-Application header
- Create circular parent-child relationships

---

### 2. Organizing Hierarchies

**Do:**
- Start with broad root categories
- Create logical subcategories under appropriate parents
- Keep hierarchies balanced (avoid very deep or very flat structures)
- Use consistent granularity at each level
- Document category purposes for complex taxonomies

**Don't:**
- Create orphaned categories with non-existent parents
- Mix different classification systems in same hierarchy
- Create too many levels (3-5 levels is usually sufficient)
- Create single-child categories unnecessarily

**Recommended Structure:**

```
Level 1 (Root): Broad domains (Literature, Philosophy, History)
Level 2: Major subdivisions (Poetry, Prose, Drama)
Level 3: Specific genres (Epic Poetry, Lyric Poetry)
Level 4: Fine-grained classifications (Religious Epics, Heroic Epics)
```

---

### 3. Multi-Language Support

**Do:**
- Provide titles in primary languages (Tibetan, English for Buddhist texts)
- Include Sanskrit terms for traditional Buddhist categories
- Use traditional terms in Tibetan alongside English translations
- Add Chinese translations for broader accessibility

**Don't:**
- Mix languages within a single title entry
- Use English-only titles for traditional Buddhist categories
- Leave traditional language titles empty

**Example - Buddhist Category:**

```json
{
  "title": {
    "bo": "མདོ་སྡེ།",
    "en": "Sutras",
    "sa": "सूत्र",
    "zh": "经藏"
  },
  "description": {
    "bo": "སངས་རྒྱས་ཀྱི་གསུང་རབ་མདོ་སྡེ་ཕྱོགས།",
    "en": "Collections of Buddha's discourses"
  }
}
```

---

### 4. Application Context

**Do:**
- Use consistent application names across your system
- Create applications via `POST /v2/applications` before using them
- Use lowercase application names
- Document which applications your system uses

**Don't:**
- Mix categories from different applications
- Forget X-Application header (will cause 400 error)
- Use uppercase or mixed-case application names
- Create categories without establishing application context

---

## Workflow Examples

### Scenario 1: Building Buddhist Text Taxonomy

```bash
# Step 1: Create application (if not exists)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "webuddhist"
  }'

# Step 2: Create root categories
CANON_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Buddhist Canon",
      "bo": "བཀའ་འགྱུར།"
    },
    "description": {
      "en": "The Tibetan Buddhist Canon",
      "bo": "བོད་ཀྱི་བཀའ་འགྱུར།"
    }
  }' | jq -r '.id')

# Step 3: Create subcategories
SUTRA_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Sutras",
      "bo": "མདོ་སྡེ།",
      "sa": "सूत्र"
    },
    "parent_id": "'$CANON_ID'"
  }' | jq -r '.id')

TANTRA_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Tantras",
      "bo": "རྒྱུད་སྡེ།",
      "sa": "तन्त्र"
    },
    "parent_id": "'$CANON_ID'"
  }' | jq -r '.id')

# Step 4: Create third-level categories
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Prajnaparamita",
      "bo": "ཤེར་ཕྱིན།",
      "sa": "प्रज्ञापारमिता"
    },
    "description": {
      "en": "Perfection of Wisdom texts"
    },
    "parent_id": "'$SUTRA_ID'"
  }'

# Step 5: Verify hierarchy
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"
```

---

### Scenario 2: Creating Category Tree for Different Applications

```bash
# Create categories for webuddhist application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Buddhist Literature", "bo": "ནང་ཆོས་རྩོམ་རིག"}
  }'

# Create separate categories for monlam application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: monlam" \
  -d '{
    "title": {"en": "Modern Tibetan Literature", "bo": "བོད་ཀྱི་རྩོམ་རིག་གསར་པ།"}
  }'

# These are completely separate taxonomies
```

---

### Scenario 3: Browsing Category Hierarchy

```bash
# Step 1: Get root categories
ROOT_CATEGORIES=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist")

echo "Root categories:"
echo "$ROOT_CATEGORIES" | jq -r '.[] | "\(.id): \(.title.en)"'

# Step 2: Select a category and get its children
SELECTED_ID=$(echo "$ROOT_CATEGORIES" | jq -r '.[0].id')

CHILD_CATEGORIES=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=$SELECTED_ID" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist")

echo "Child categories of $SELECTED_ID:"
echo "$CHILD_CATEGORIES" | jq -r '.[] | "  \(.id): \(.title.en)"'
```

---

### Scenario 4: Finding Texts in Category

```bash
# Step 1: Get category ID
CATEGORY_ID="CAT_PRAJNAPARA"

# Step 2: Get all texts in this category
TEXTS=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?category_id=$CATEGORY_ID" \
  -H "X-API-Key: your_api_key")

echo "Texts in category $CATEGORY_ID:"
echo "$TEXTS" | jq -r '.[] | "\(.id): \(.title.en)"'

# Step 3: Get text details
TEXT_ID=$(echo "$TEXTS" | jq -r '.[0].id')
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$TEXT_ID" \
  -H "X-API-Key: your_api_key" \
  | jq '.'
```

---

## Advanced Usage

### Multi-Lingual Category Names

Categories support rich multilingual metadata:

```json
{
  "title": {
    "bo": "མདོ་སྡེ།",
    "en": "Sutras",
    "sa": "सूत्र",
    "zh": "经部",
    "pi": "Sutta"
  },
  "description": {
    "bo": "སངས་རྒྱས་ཀྱི་གསུང་རབ་མདོ་སྡེའི་སྡེ་ཚན།",
    "en": "Discourses and teachings of the Buddha",
    "sa": "बुद्धभाषित सूत्र",
    "zh": "佛陀的教诲经典"
  }
}
```

**Benefits:**
- Better discoverability across languages
- Support for multilingual applications
- Cultural and traditional term preservation

---

### Language-Specific Filtering

Filter categories by language to get only those with titles in specific languages:

```bash
# Get categories with Tibetan titles
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?language=bo" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Get categories with English titles
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?language=en" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Get categories with Sanskrit titles
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?language=sa" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"
```

**Note:** The language filter returns categories that have a title in the specified language. Default is `"bo"` (Tibetan).

---

### Building Category Breadcrumbs

Generate breadcrumb navigation from child to root:

```python
import requests

API_KEY = "your_api_key"
BASE_URL = "https://api-l25bgmwqoa-uc.a.run.app"
APPLICATION = "webuddhist"

headers = {
    "X-API-Key": API_KEY,
    "X-Application": APPLICATION
}

def get_breadcrumbs(category_id):
    """Get breadcrumb trail from category to root."""
    breadcrumbs = []
    current_id = category_id
    
    # Cache all categories to avoid repeated API calls
    all_categories = {}
    response = requests.get(
        f"{BASE_URL}/v2/categories",
        headers=headers
    )
    
    # Build category lookup
    def fetch_children(parent_id=None):
        params = {}
        if parent_id:
            params['parent_id'] = parent_id
        
        response = requests.get(
            f"{BASE_URL}/v2/categories",
            headers=headers,
            params=params
        )
        categories = response.json()
        
        for cat in categories:
            all_categories[cat['id']] = cat
            if cat['children']:
                for child_id in cat['children']:
                    fetch_children(child_id)
    
    fetch_children()
    
    # Build breadcrumb trail
    while current_id:
        if current_id in all_categories:
            cat = all_categories[current_id]
            breadcrumbs.insert(0, {
                'id': cat['id'],
                'title': cat['title']
            })
            current_id = cat['parent_id']
        else:
            break
    
    return breadcrumbs

# Example usage
breadcrumbs = get_breadcrumbs("CAT_PRAJNAPARA")
print(" > ".join([b['title']['en'] for b in breadcrumbs]))
# Output: Buddhist Canon > Sutras > Prajnaparamita
```

---

### Category-Based Text Organization

```bash
# Step 1: Create category hierarchy
ROOT_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{"title": {"en": "Philosophy", "bo": "གྲུབ་མཐའ།"}}' \
  | jq -r '.id')

SUB_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Madhyamaka", "bo": "དབུ་མ།"},
    "parent_id": "'$ROOT_ID'"
  }' | jq -r '.id')

# Step 2: Create text in leaf category
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "དབུ་མ་རྩ་བའི་འགྲེལ་པ།"},
    "language": "bo",
    "category_id": "'$SUB_ID'",
    "contributions": [{"person_bdrc_id": "P2816", "role": "author"}]
  }'

# Step 3: Query texts in category
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?category_id=$SUB_ID" \
  -H "X-API-Key: your_api_key"
```

---

## Integration with Other APIs

### Categories and Texts

Texts must belong to a category:

```
1. GET /v2/categories → Find or create category
2. POST /v2/texts with category_id → Create text in category
3. GET /v2/texts?category_id={id} → Query texts in category
```

**Example:**

```bash
# Get category
CATEGORY=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist")

CATEGORY_ID=$(echo "$CATEGORY" | jq -r '.[0].id')

# Create text in category
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "གཞུང་།"},
    "language": "bo",
    "category_id": "'$CATEGORY_ID'",
    "contributions": [{"person_bdrc_id": "P2816", "role": "author"}]
  }'
```

---

### Categories and Applications

Create application before creating categories:

```
1. POST /v2/applications → Create application
2. POST /v2/categories with X-Application header → Create categories
```

**Example:**

```bash
# Create application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "name": "lotsawa"
  }'

# Create categories for this application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: lotsawa" \
  -d '{
    "title": {"en": "Translation Projects", "bo": "སྒྱུར་རྩོམ།"}
  }'
```

---

## Common Scenarios

### Scenario 1: Traditional Buddhist Classification

Create traditional Buddhist text classification system:

```bash
# Root: Buddhist Canon
CANON=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Buddhist Canon", "bo": "བཀའ་འགྱུར།", "sa": "त्रिपिटक"}
  }' | jq -r '.id')

# Level 2: Three Baskets
SUTRA=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Sutra Basket", "bo": "མདོ་སྡེ།", "sa": "सूत्रपिटक"},
    "parent_id": "'$CANON'"
  }' | jq -r '.id')

VINAYA=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Vinaya Basket", "bo": "འདུལ་བ།", "sa": "विनयपिटक"},
    "parent_id": "'$CANON'"
  }' | jq -r '.id')

ABHIDHARMA=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Abhidharma Basket", "bo": "མངོན་པ།", "sa": "अभिधर्मपिटक"},
    "parent_id": "'$CANON'"
  }' | jq -r '.id')

# Level 3: Sutra subdivisions
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Perfection of Wisdom", "bo": "ཤེར་ཕྱིན།", "sa": "प्रज्ञापारमिता"},
    "parent_id": "'$SUTRA'"
  }'
```

---

### Scenario 2: Modern Library Classification

Create modern library-style classification:

```bash
# Root categories
LITERATURE=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: library" \
  -d '{
    "title": {"en": "Literature", "bo": "རྩོམ་རིག"}
  }' | jq -r '.id')

HISTORY=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: library" \
  -d '{
    "title": {"en": "History", "bo": "ལོ་རྒྱུས"}
  }' | jq -r '.id')

RELIGION=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: library" \
  -d '{
    "title": {"en": "Religion", "bo": "ཆོས་ལུགས།"}
  }' | jq -r '.id')

# Literature subcategories
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: library" \
  -d '{
    "title": {"en": "Poetry", "bo": "སྙན་ངག"},
    "parent_id": "'$LITERATURE'"
  }'

curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: library" \
  -d '{
    "title": {"en": "Prose", "bo": "རྩོམ་རིག"},
    "parent_id": "'$LITERATURE'"
  }'
```

---

## Validation Rules

### Category Creation

| Field | Validation |
|-------|------------|
| `title` | Required, must be object with at least one language entry |
| `title[language]` | Non-empty string |
| `description` | Optional, if provided must be object with at least one language entry |
| `description[language]` | Non-empty string if language provided |
| `parent_id` | Optional, must reference existing category if provided |
| `X-Application` header | Required, non-empty string |

### Category Retrieval

| Field | Validation |
|-------|------------|
| `parent_id` | Optional, null or omitted returns root categories |
| `language` | Optional, defaults to "bo" |
| `X-Application` header | Required, non-empty string |

---

## Troubleshooting

### Common Issues

**Issue: "There was an error with the request" (400)**

**Solution:**
- Verify `X-Application` header is present
- Check `X-Application` value is valid (application must exist)
- Ensure `parent_id` references existing category if provided

```bash
# ✗ Wrong - missing X-Application
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key"

# ✓ Correct
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"
```

---

**Issue: "Validation error: field required" on title**

**Solution:**
- Ensure `title` field is provided
- Verify `title` is an object (not a string)
- Include at least one language entry in title object

```json
// ✗ Wrong - title is string
{
  "title": "Buddhist Canon"
}

// ✗ Wrong - empty title object
{
  "title": {}
}

// ✓ Correct
{
  "title": {
    "en": "Buddhist Canon"
  }
}
```

---

**Issue: Parent category not found**

**Solution:**
- Verify parent category exists before creating child
- Check `parent_id` spelling
- Ensure parent belongs to same application context

```bash
# Step 1: Verify parent exists
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  | jq -r '.[] | select(.id == "CAT_PARENT")'

# Step 2: Create child only if parent exists
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Child Category"},
    "parent_id": "CAT_PARENT"
  }'
```

---

**Issue: Cannot find categories for application**

**Solution:**
- Application may not exist yet
- Create application first using `POST /v2/applications`
- Verify application name is lowercase

```bash
# Create application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "name": "myapp"
  }'

# Now create categories for this application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: myapp" \
  -d '{
    "title": {"en": "First Category"}
  }'
```

---

**Issue: Wrong X-Application context**

**Solution:**
- Categories are scoped per application
- Ensure you're querying with the correct X-Application header
- Categories created in one application won't appear in another

```bash
# Categories in webuddhist
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Different categories in monlam
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: monlam"
```

---

## Category Design Patterns

### Pattern 1: Traditional Buddhist Classification

Based on classical Buddhist text organization:

```
Buddhist Canon (བཀའ་འགྱུར།)
├── Sutras (མདོ་སྡེ།)
│   ├── Prajnaparamita (ཤེར་ཕྱིན།)
│   ├── Avatamsaka (ཕལ་ཆེན།)
│   └── Ratnakuta (དཀོན་བརྩེགས།)
├── Vinaya (འདུལ་བ།)
│   ├── Monastic Rules (སོ་སོར་ཐར་པ།)
│   └── Ordination (གསོ་སྦྱོང་།)
└── Abhidharma (མངོན་པ།)
    ├── Abhidharmakosha (མངོན་མཛོད།)
    └── Abhidharmasamuccaya (མངོན་ཀུན།)
```

---

### Pattern 2: Genre-Based Classification

Organized by literary genre:

```
Literature (རྩོམ་རིག)
├── Poetry (སྙན་ངག)
│   ├── Religious Poetry (ཆོས་སྙན་ངག)
│   └── Secular Poetry (འཇིག་རྟེན་སྙན་ངག)
├── Prose (རྩོམ་ཡིག)
│   ├── Historical (ལོ་རྒྱུས་རྩོམ་ཡིག)
│   └── Biographical (རྣམ་ཐར།)
└── Drama (ཟློས་གར།)
```

---

### Pattern 3: Thematic Classification

Organized by subject matter:

```
Buddhist Studies (ནང་ཆོས་རིག་གནས།)
├── Philosophy (གྲུབ་མཐའ།)
│   ├── Madhyamaka (དབུ་མ།)
│   ├── Yogacara (སེམས་ཙམ།)
│   └── Pramana (ཚད་མ།)
├── Practice (སྒོམ་ཁྲིད།)
│   ├── Meditation (བསམ་གཏན།)
│   └── Ritual (ཆོ་ག)
└── History (ཆོས་འབྱུང་།)
```

---

### Pattern 4: Period-Based Classification

Organized by historical period:

```
Tibetan Literature (བོད་ཀྱི་རྩོམ་རིག)
├── Imperial Period (བཙན་པོའི་དུས་སྐབས།)
├── Renaissance Period (ཕྱི་དར།)
│   ├── Early Renaissance (ཕྱི་དར་སྔ་མ།)
│   └── Later Renaissance (ཕྱི་དར་ཕྱི་མ།)
└── Modern Period (དེང་རབས།)
    ├── 20th Century (དུས་རབས་ ༢༠།)
    └── Contemporary (ད་ལྟའི་དུས་སྐབས།)
```

---

## Testing and Development

### Test Category Creation

```bash
# Create test category
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: test_app" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Test Category",
      "bo": "དཔེ་མཚོན་དབྱེ་བ།"
    }
  }' -w "\nStatus: %{http_code}\n"
```

### Test Category Retrieval

```bash
# Get all root categories
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: test_app" \
  | jq '.'

# Get specific category's children
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=CAT_TEST" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: test_app" \
  | jq '.'
```

### Test Hierarchy Navigation

```bash
# Create parent
PARENT=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: test_app" \
  -d '{"title": {"en": "Parent"}}' | jq -r '.id')

# Create child
CHILD=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: test_app" \
  -d '{"title": {"en": "Child"}, "parent_id": "'$PARENT'"}' | jq -r '.id')

# Verify parent shows child
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: test_app" \
  | jq '.[] | select(.id == "'$PARENT'") | .children'

# Verify child shows parent
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=$PARENT" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: test_app" \
  | jq '.[] | select(.id == "'$CHILD'") | .parent_id'
```

---

## Complete TypeScript Types

```typescript
// Input types
interface CategoryInput {
  title: {                       // Required
    [language: string]: string;  // At least one language required
  };
  description?: {                // Optional
    [language: string]: string;
  } | null;
  parent_id?: string | null;     // Optional: null for root categories
}

// Output types
interface CategoryOutput {
  id: string;                    // Category ID
  title: {                       // Localized titles
    [language: string]: string;
  };
  description?: {                // Optional localized descriptions
    [language: string]: string;
  } | null;
  parent_id: string | null;      // Parent category ID (null for root)
  children: string[];            // Array of child category IDs
}
```

---

## Quick Reference

### HTTP Methods Summary

| Method | Endpoint | Purpose | Required Headers |
|--------|----------|---------|------------------|
| GET | `/v2/categories` | List categories | X-API-Key, X-Application |
| POST | `/v2/categories` | Create new category | X-API-Key, X-Application, Content-Type |

### Query Parameters Summary

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `parent_id` | string | null | Filter by parent (null = root categories) |
| `language` | string | "bo" | Filter by language |

### Status Codes

| Status | Meaning | Common Use |
|--------|---------|------------|
| 200 | OK | Successful GET |
| 201 | Created | Successful POST |
| 400 | Bad Request | Missing X-Application, invalid parent_id |
| 422 | Validation Error | Missing title, invalid format |
| 500 | Server Error | Internal error |

---

## Real-World Examples

### Example 1: Complete Kangyur Classification

```bash
# Create comprehensive Buddhist Canon structure
APP="webuddhist"

# Root: Kangyur
KANGYUR=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {
      "en": "Kangyur - Translated Words of the Buddha",
      "bo": "བཀའ་འགྱུར།"
    },
    "description": {
      "en": "The Tibetan Buddhist Canon containing the Buddha'\''s teachings"
    }
  }' | jq -r '.id')

# Major divisions
VINAYA=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Vinaya", "bo": "འདུལ་བ།", "sa": "विनय"},
    "description": {"en": "Monastic discipline and ethical codes"},
    "parent_id": "'$KANGYUR'"
  }' | jq -r '.id')

PRAJNAPARA=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Prajnaparamita", "bo": "ཤེར་ཕྱིན།", "sa": "प्रज्ञापारमिता"},
    "description": {"en": "Perfection of Wisdom sutras"},
    "parent_id": "'$KANGYUR'"
  }' | jq -r '.id')

AVATAMSAKA=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Avatamsaka", "bo": "ཕལ་ཆེན།", "sa": "अवतंसक"},
    "description": {"en": "Flower Ornament sutras"},
    "parent_id": "'$KANGYUR'"
  }' | jq -r '.id')

RATNAKUTA=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Ratnakuta", "bo": "དཀོན་བརྩེགས།", "sa": "रत्नकूट"},
    "description": {"en": "Heap of Jewels collection"},
    "parent_id": "'$KANGYUR'"
  }' | jq -r '.id')

# Verify structure
echo "Root categories:"
curl -s -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  | jq -r '.[] | "\(.title.en) (\(.id))"'

echo "\nChildren of Kangyur:"
curl -s -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories?parent_id=$KANGYUR" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  | jq -r '.[] | "  \(.title.en) (\(.id))"'
```

---

### Example 2: Modern Tibetan Library

```bash
# Create modern classification system
APP="tibetan_library"

# Create application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -d '{"name": "tibetan_library"}'

# Root categories by Dewey-like system
PHILOSOPHY=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Philosophy & Religion", "bo": "ལྟ་གྲུབ་དང་ཆོས་ལུགས།"}
  }' | jq -r '.id')

SOCIAL_SCIENCE=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Social Sciences", "bo": "སྤྱི་ཚོགས་རིག་གནས།"}
  }' | jq -r '.id')

LITERATURE=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Literature", "bo": "རྩོམ་རིག"}
  }' | jq -r '.id')

# Subcategories under Literature
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Classical Poetry", "bo": "སྔ་རབས་སྙན་ངག"},
    "parent_id": "'$LITERATURE'"
  }'

curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: $APP" \
  -d '{
    "title": {"en": "Modern Literature", "bo": "དེང་རབས་རྩོམ་རིག"},
    "parent_id": "'$LITERATURE'"
  }'
```

---

### Example 3: Multi-Application Categories

```bash
# Application 1: webuddhist (traditional Buddhist texts)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  -d '{
    "title": {"en": "Sutras", "bo": "མདོ་སྡེ།"}
  }'

# Application 2: monlam (modern digital texts)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: monlam" \
  -d '{
    "title": {"en": "Digital Translations", "bo": "གློག་རིག་སྒྱུར་རྩོམ།"}
  }'

# These are separate taxonomies
```

---

## Category Tree Utilities

### Python Helper Class

```python
import requests
from typing import List, Dict, Optional

class CategoryManager:
    def __init__(self, api_key: str, base_url: str, application: str):
        self.api_key = api_key
        self.base_url = base_url
        self.application = application
        self.headers = {
            "X-API-Key": api_key,
            "X-Application": application,
            "Content-Type": "application/json"
        }
    
    def get_categories(self, parent_id: Optional[str] = None, language: str = "bo") -> List[Dict]:
        """Get categories, optionally filtered by parent."""
        params = {"language": language}
        if parent_id:
            params["parent_id"] = parent_id
        
        response = requests.get(
            f"{self.base_url}/v2/categories",
            headers=self.headers,
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    def create_category(self, title: Dict[str, str], 
                       description: Optional[Dict[str, str]] = None,
                       parent_id: Optional[str] = None) -> str:
        """Create a category and return its ID."""
        data = {"title": title}
        if description:
            data["description"] = description
        if parent_id:
            data["parent_id"] = parent_id
        
        response = requests.post(
            f"{self.base_url}/v2/categories",
            headers=self.headers,
            json=data
        )
        response.raise_for_status()
        return response.json()["id"]
    
    def get_category_tree(self, parent_id: Optional[str] = None, level: int = 0) -> None:
        """Print category tree recursively."""
        categories = self.get_categories(parent_id)
        
        for cat in categories:
            indent = "  " * level
            title = cat['title'].get('en', list(cat['title'].values())[0])
            print(f"{indent}{title} ({cat['id']})")
            
            if cat['children']:
                self.get_category_tree(cat['id'], level + 1)
    
    def get_breadcrumbs(self, category_id: str) -> List[Dict]:
        """Get breadcrumb trail from category to root."""
        breadcrumbs = []
        current_id = category_id
        
        # Build category lookup by fetching all
        all_cats = {}
        
        def fetch_recursive(parent_id=None):
            cats = self.get_categories(parent_id)
            for cat in cats:
                all_cats[cat['id']] = cat
                if cat['children']:
                    for child_id in cat['children']:
                        fetch_recursive(child_id)
        
        fetch_recursive()
        
        # Build trail
        while current_id and current_id in all_cats:
            cat = all_cats[current_id]
            breadcrumbs.insert(0, {
                'id': cat['id'],
                'title': cat['title']
            })
            current_id = cat['parent_id']
        
        return breadcrumbs

# Example usage
manager = CategoryManager(
    api_key="your_api_key",
    base_url="https://api-l25bgmwqoa-uc.a.run.app",
    application="webuddhist"
)

# Print full tree
print("Category Tree:")
manager.get_category_tree()

# Get breadcrumbs
breadcrumbs = manager.get_breadcrumbs("CAT_SPECIFIC")
print(" > ".join([b['title']['en'] for b in breadcrumbs]))
```

---

## Data Quality Guidelines

### Naming Conventions

**Titles:**
- Use clear, descriptive names
- Follow traditional terminology for Buddhist categories
- Include proper diacriticals in romanized Sanskrit terms
- Use sentence case in English (not title case)

**Examples:**

```json
// ✓ Good - Clear and traditional
{
  "title": {
    "en": "Perfection of Wisdom",
    "bo": "ཤེར་ཕྱིན།",
    "sa": "प्रज्ञापारमिता"
  }
}

// ✗ Avoid - Vague or inconsistent
{
  "title": {
    "en": "WISDOM TEXTS",
    "bo": "wisdom"
  }
}
```

---

### Hierarchy Design

**Depth:**
- Keep hierarchies between 2-5 levels
- Avoid single-child categories
- Balance breadth vs depth

**Organization:**
- Use consistent classification principles at each level
- Group related categories together
- Consider user navigation patterns

**Example Structure:**

```
Level 1: Domain (Philosophy, Literature, History)
Level 2: Tradition/Genre (Buddhist, Bon, Secular)
Level 3: School/Period (Madhyamaka, Yogacara, Nyingma)
Level 4: Specific Topics (Emptiness, Consciousness, Ritual)
```

---

### Localization

**Required Languages:**
- Tibetan (`bo`): For traditional Buddhist texts
- English (`en`): For international accessibility

**Optional Languages:**
- Sanskrit (`sa`): For Indian Buddhist categories
- Chinese (`zh`): For broader Asian context
- Pali (`pi`): For Theravada categories

---

## Limitations and Constraints

### Current API Limitations

1. **No Category Update Endpoint**
   - Categories cannot be updated once created
   - To modify: create new category, migrate texts, delete old category (requires manual process)

2. **No Category Delete Endpoint**
   - Categories cannot be deleted via API
   - Manual database cleanup required for category removal

3. **No Category Move Operation**
   - Cannot change parent_id after creation
   - To restructure: create new categories and migrate content

4. **No Bulk Operations**
   - Must create categories one at a time
   - No batch create endpoint

### Workarounds

**To update a category:**
```
1. Create new category with correct values
2. Update all texts to use new category_id
3. Contact administrators to remove old category
```

**To restructure hierarchy:**
```
1. Plan new structure
2. Create new categories with correct parent relationships
3. Migrate texts to new categories
4. Contact administrators to clean up old categories
```

---

## Integration Patterns

### Pattern 1: Category-First Text Creation

```bash
# 1. Ensure category exists
CATEGORY_ID=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist" \
  | jq -r '.[0].id')

# 2. Create text in category
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "གཞུང་།"},
    "language": "bo",
    "category_id": "'$CATEGORY_ID'",
    "contributions": [{"person_bdrc_id": "P2816", "role": "author"}]
  }'
```

---

### Pattern 2: Dynamic Category Discovery

```javascript
// Frontend example: Build category selector
async function getCategoryOptions(applicationName) {
  const headers = {
    'X-API-Key': 'your_api_key',
    'X-Application': applicationName
  };
  
  // Get root categories
  const response = await fetch(
    'https://api-l25bgmwqoa-uc.a.run.app/v2/categories',
    { headers }
  );
  
  const rootCategories = await response.json();
  
  // Build options recursively
  const buildOptions = async (parentId = null, prefix = '') => {
    const params = new URLSearchParams();
    if (parentId) params.set('parent_id', parentId);
    
    const res = await fetch(
      `https://api-l25bgmwqoa-uc.a.run.app/v2/categories?${params}`,
      { headers }
    );
    const categories = await res.json();
    
    let options = [];
    for (const cat of categories) {
      const title = cat.title.en || Object.values(cat.title)[0];
      options.push({
        value: cat.id,
        label: `${prefix}${title}`
      });
      
      if (cat.children.length > 0) {
        const childOptions = await buildOptions(cat.id, `${prefix}  `);
        options = options.concat(childOptions);
      }
    }
    return options;
  };
  
  return await buildOptions();
}

// Usage
const categoryOptions = await getCategoryOptions('webuddhist');
console.log(categoryOptions);
```

---

## Frequently Asked Questions

### Q: Can I update a category after creation?

**A:** No, the API currently doesn't support category updates. Categories are immutable once created. To change a category, you would need to create a new one and migrate texts.

### Q: Can I delete a category?

**A:** No, there's no DELETE endpoint for categories. Category deletion requires administrative database access.

### Q: How many levels of hierarchy are supported?

**A:** The API doesn't impose a hard limit, but we recommend 3-5 levels for usability. Deeper hierarchies become difficult to navigate.

### Q: Can categories have multiple parents?

**A:** No, each category can have only one parent (or none for root categories). The structure is a tree, not a graph.

### Q: What happens if I specify a non-existent parent_id?

**A:** The API will return a validation error. Always verify the parent category exists before creating a child.

### Q: Are category IDs unique across applications?

**A:** Yes, category IDs are globally unique across all applications, but category data is scoped per application via the X-Application header.

### Q: What's the difference between the language parameter and title languages?

**A:** 
- **language parameter**: Filters which categories are returned (only those with title in that language)
- **title object**: Contains the actual localized titles for the category

### Q: Can I query all texts recursively including child categories?

**A:** Not directly via the API. You need to:
1. Get category and its children recursively
2. Query texts for each category ID
3. Aggregate results client-side

---

## Performance Considerations

### Query Optimization

**Do:**
- Cache category hierarchies (they change infrequently)
- Fetch entire tree once and navigate locally
- Use language filter to reduce response size if only need specific language

**Don't:**
- Query categories repeatedly for same application
- Make recursive API calls for every breadcrumb generation
- Fetch categories every time you need to display taxonomy

### Recommended Approach

```javascript
// Cache category tree at application startup
class CategoryCache {
  constructor(apiKey, application) {
    this.apiKey = apiKey;
    this.application = application;
    this.cache = new Map();
    this.lastFetch = null;
  }
  
  async refresh() {
    // Fetch all categories for application
    const headers = {
      'X-API-Key': this.apiKey,
      'X-Application': this.application
    };
    
    // Fetch recursively and cache
    const fetchLevel = async (parentId = null) => {
      const params = parentId ? `?parent_id=${parentId}` : '';
      const response = await fetch(
        `https://api-l25bgmwqoa-uc.a.run.app/v2/categories${params}`,
        { headers }
      );
      const categories = await response.json();
      
      for (const cat of categories) {
        this.cache.set(cat.id, cat);
        if (cat.children.length > 0) {
          for (const childId of cat.children) {
            await fetchLevel(childId);
          }
        }
      }
    };
    
    await fetchLevel();
    this.lastFetch = Date.now();
  }
  
  get(categoryId) {
    return this.cache.get(categoryId);
  }
  
  getChildren(categoryId) {
    const category = this.cache.get(categoryId);
    return category ? category.children.map(id => this.cache.get(id)) : [];
  }
  
  getRootCategories() {
    return Array.from(this.cache.values()).filter(cat => !cat.parent_id);
  }
}

// Usage
const cache = new CategoryCache('your_api_key', 'webuddhist');
await cache.refresh();

// Now all category operations are local
const roots = cache.getRootCategories();
const children = cache.getChildren('CAT_ID');
```

---

## Migration and Import

### Importing Taxonomy from File

```python
import requests
import json

API_KEY = "your_api_key"
BASE_URL = "https://api-l25bgmwqoa-uc.a.run.app"
APPLICATION = "webuddhist"

headers = {
    "X-API-Key": API_KEY,
    "X-Application": APPLICATION,
    "Content-Type": "application/json"
}

# Category tree definition
taxonomy = {
    "title": {"en": "Buddhist Canon", "bo": "བཀའ་འགྱུར།"},
    "children": [
        {
            "title": {"en": "Sutras", "bo": "མདོ་སྡེ།"},
            "children": [
                {"title": {"en": "Prajnaparamita", "bo": "ཤེར་ཕྱིན།"}},
                {"title": {"en": "Avatamsaka", "bo": "ཕལ་ཆེན།"}}
            ]
        },
        {
            "title": {"en": "Tantras", "bo": "རྒྱུད་སྡེ།"},
            "children": [
                {"title": {"en": "Kriya Tantra", "bo": "བྱ་རྒྱུད།"}},
                {"title": {"en": "Yoga Tantra", "bo": "རྣལ་འབྱོར་རྒྱུད།"}}
            ]
        }
    ]
}

def create_category_tree(node, parent_id=None):
    """Recursively create category tree."""
    data = {"title": node["title"]}
    if "description" in node:
        data["description"] = node["description"]
    if parent_id:
        data["parent_id"] = parent_id
    
    response = requests.post(
        f"{BASE_URL}/v2/categories",
        headers=headers,
        json=data
    )
    response.raise_for_status()
    category_id = response.json()["id"]
    
    print(f"Created: {node['title']['en']} ({category_id})")
    
    # Create children
    if "children" in node:
        for child in node["children"]:
            create_category_tree(child, category_id)
    
    return category_id

# Import taxonomy
root_id = create_category_tree(taxonomy)
print(f"Root category ID: {root_id}")
```

---

## Complete Workflow Example

### Building and Using Category System

```bash
#!/bin/bash

API_KEY="your_api_key"
BASE_URL="https://api-l25bgmwqoa-uc.a.run.app"
APP="webuddhist"

# Helper function to create category
create_category() {
  local title_en=$1
  local title_bo=$2
  local parent_id=$3
  
  local data='{"title": {"en": "'$title_en'", "bo": "'$title_bo'"}}'
  if [ -n "$parent_id" ]; then
    data='{"title": {"en": "'$title_en'", "bo": "'$title_bo'"}, "parent_id": "'$parent_id'"}'
  fi
  
  curl -s -X POST "$BASE_URL/v2/categories" \
    -H "X-API-Key: $API_KEY" \
    -H "X-Application: $APP" \
    -H "Content-Type: application/json" \
    -d "$data" | jq -r '.id'
}

# Build taxonomy
echo "Creating Buddhist text taxonomy..."

# Root
CANON=$(create_category "Buddhist Canon" "བཀའ་འགྱུར།" "")
echo "Created root: $CANON"

# Level 2
SUTRAS=$(create_category "Sutras" "མདོ་སྡེ།" "$CANON")
TANTRAS=$(create_category "Tantras" "རྒྱུད་སྡེ།" "$CANON")
echo "Created level 2: Sutras=$SUTRAS, Tantras=$TANTRAS"

# Level 3
PRAJNAPARA=$(create_category "Prajnaparamita" "ཤེར་ཕྱིན།" "$SUTRAS")
AVATAMSAKA=$(create_category "Avatamsaka" "ཕལ་ཆེན།" "$SUTRAS")
echo "Created level 3: Prajnaparamita=$PRAJNAPARA, Avatamsaka=$AVATAMSAKA"

# Verify
echo -e "\nCategory tree:"
curl -s -X GET "$BASE_URL/v2/categories" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Application: $APP" \
  | jq -r '.[] | "\(.title.en) (\(.id)) - children: \(.children | length)"'

# Create text in leaf category
echo -e "\nCreating text in Prajnaparamita category..."
TEXT_ID=$(curl -s -X POST "$BASE_URL/v2/texts" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {"bo": "ཤེར་ཕྱིན་སྡུད་པ།", "en": "Heart Sutra"},
    "language": "bo",
    "category_id": "'$PRAJNAPARA'",
    "contributions": [{"person_bdrc_id": "P2816", "role": "author"}]
  }' | jq -r '.id')

echo "Created text: $TEXT_ID"

# Query texts in category
echo -e "\nTexts in Prajnaparamita category:"
curl -s -X GET "$BASE_URL/v2/texts?category_id=$PRAJNAPARA" \
  -H "X-API-Key: $API_KEY" \
  | jq -r '.[] | "\(.id): \(.title.en)"'
```

---

## Category Usage Statistics

Track category usage to optimize taxonomy:

```python
import requests
from collections import Counter

API_KEY = "your_api_key"
BASE_URL = "https://api-l25bgmwqoa-uc.a.run.app"
APPLICATION = "webuddhist"

headers = {
    "X-API-Key": API_KEY,
    "X-Application": APPLICATION
}

# Get all texts
response = requests.get(f"{BASE_URL}/v2/texts", headers={"X-API-Key": API_KEY})
texts = response.json()

# Count texts per category
category_counts = Counter([text['category_id'] for text in texts])

# Get category names
response = requests.get(
    f"{BASE_URL}/v2/categories",
    headers=headers
)
categories = response.json()
category_names = {cat['id']: cat['title']['en'] for cat in categories}

# Print statistics
print("Texts per category:")
for cat_id, count in category_counts.most_common():
    name = category_names.get(cat_id, cat_id)
    print(f"{name}: {count} texts")
```

---

## See Also

- [Texts API Documentation](./texts-api.md) - Using categories in text creation
- [Editions API Documentation](./editions-api.md) - Edition management
- [Persons API Documentation](./persons-api.md) - Person management
- Applications API: `POST /v2/applications` - Creating applications
- OpenAPI Specification: `GET /v2/schema/openapi`
