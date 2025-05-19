#!/usr/bin/env python
"""
Script to fix metadata records by:
1. Replacing string 'None' in source_url with proper null value
2. Adding 'en' key with value 'None' to title fields that only have 'bo'
"""

import logging

import firebase_admin
from firebase_admin import credentials, firestore

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def main():
    # Explicitly set the project ID here
    project_id = "pecha-backend-dev"  # Change to "pecha-backend-dev" for dev environment

    try:
        firebase_admin.get_app()  # Check if Firebase is already initialized
    except ValueError:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {"projectId": project_id})

    logger.info("Using project ID: %s", project_id)

    # Get Firestore client
    db = firestore.client()

    # Query all metadata documents in the collection
    metadata_collection = db.collection("metadata")
    all_metadata = metadata_collection.get()

    # Track statistics
    total_docs = 0
    updated_docs = 0
    source_url_fixes = 0
    en_title_fixes = 0
    bo_title_fixes = 0

    # Process each document
    for doc in all_metadata:
        total_docs += 1
        doc_id = doc.id
        data = doc.to_dict()
        needs_update = False

        # Check if the document has source_url="None"
        if "source_url" in data and data["source_url"] == "None":
            logger.info("Fixing document %s: source_url is string 'None'", doc_id)
            data["source_url"] = None
            needs_update = True
            source_url_fixes += 1

        # Check if title is missing either 'en' or 'bo' key and add it with 'None' value
        if "title" in data and isinstance(data["title"], dict):
            title = data["title"]

            # Missing 'en' key
            if "bo" in title and "en" not in title:
                logger.info("Fixing document %s: Adding missing 'en' key to title", doc_id)
                title["en"] = "None"
                needs_update = True
                en_title_fixes += 1

            # Missing 'bo' key
            elif "en" in title and "bo" not in title:
                logger.info("Fixing document %s: Adding missing 'bo' key to title", doc_id)
                title["bo"] = "None"
                needs_update = True
                bo_title_fixes += 1

        # Write back to Firestore if changes were made
        if needs_update:
            metadata_collection.document(doc_id).set(data)
            updated_docs += 1

    # Print detailed summary
    logger.info("Summary statistics:")
    logger.info("  - Total documents processed: %d", total_docs)
    logger.info("  - Documents updated: %d", updated_docs)
    logger.info("  - Source URL fixes: %d", source_url_fixes)
    logger.info("  - Title field fixes (en): %d", en_title_fixes)
    logger.info("  - Title field fixes (bo): %d", bo_title_fixes)
    logger.info("  - Total title field fixes: %d", en_title_fixes + bo_title_fixes)


if __name__ == "__main__":
    main()
