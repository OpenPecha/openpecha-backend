"""
Migration script: Firebase Storage → S3

Copies all base text files from a Firebase Storage bucket to an AWS S3 bucket.
The path structure is identical in both systems: base_texts/{text_id}/{edition_id}.txt

S3 credentials and bucket config are loaded from .env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_S3_BUCKET, AWS_REGION).

Prerequisites:
  - Firebase credentials: Run `gcloud auth application-default login` to authenticate
    with a Google account that has access to the Firebase project's storage bucket.
  - AWS credentials: Configured in .env (see .env.example).

Usage:
  python scripts/migrate_firebase_to_s3.py \
      --firebase-bucket <firebase-bucket-name> \
      [--prefix base_texts/] \
      [--dry-run]

Examples:
  # Dry run (list files without uploading):
  python scripts/migrate_firebase_to_s3.py \
      --firebase-bucket pecha-backend-dev.appspot.com \
      --dry-run

  # Actual migration:
  python scripts/migrate_firebase_to_s3.py \
      --firebase-bucket pecha-backend-dev.appspot.com
"""

import argparse
import hashlib
import logging
import os
import sys

import boto3
import firebase_admin
from dotenv import load_dotenv
from firebase_admin import credentials, storage
from google.cloud.storage import Blob, Bucket
from mypy_boto3_s3 import S3Client

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def init_firebase(bucket_name: str) -> Bucket:
    """Initialize Firebase Admin SDK and return the storage bucket."""
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {"storageBucket": bucket_name})

    return storage.bucket(bucket_name)


def init_s3(region: str) -> S3Client:
    """Initialize and return a boto3 S3 client using credentials from .env."""
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=region,
    )
    return session.client("s3")


def list_firebase_blobs(bucket: Bucket, prefix: str) -> list[Blob]:
    """List all blobs under the given prefix in Firebase Storage."""
    blobs = list(bucket.list_blobs(prefix=prefix))
    logger.info("Found %d blobs under prefix '%s'", len(blobs), prefix)
    return blobs


def migrate(
    firebase_bucket_name: str,
    prefix: str,
    *,
    dry_run: bool,
) -> None:
    s3_bucket_name = os.environ.get("AWS_S3_BUCKET", "")
    s3_region = os.environ.get("AWS_REGION", "")

    if not s3_bucket_name or not s3_region:
        logger.error("AWS_S3_BUCKET and AWS_REGION must be set in .env")
        sys.exit(1)

    firebase_bucket = init_firebase(firebase_bucket_name)
    s3_client = init_s3(s3_region)

    blobs = list_firebase_blobs(firebase_bucket, prefix)

    if not blobs:
        logger.info("No files to migrate.")
        return

    # Filter out directory markers
    file_blobs = [b for b in blobs if b.name and not b.name.endswith("/")]
    total = len(file_blobs)
    logger.info("Files to process: %d", total)

    migrated = 0
    skipped = 0
    failed = 0

    for i, blob in enumerate(file_blobs, start=1):
        if blob.name is None:
            continue
        s3_key: str = blob.name
        counter = f"[{i}/{total}]"

        if dry_run:
            logger.info("%s [DRY RUN] Would migrate: %s", counter, blob.name)
            migrated += 1
            continue

        try:
            data = blob.download_as_bytes()
            local_md5 = hashlib.md5(data).hexdigest()  # noqa: S324

            # Check if S3 already has identical content via ETag
            try:
                head = s3_client.head_object(Bucket=s3_bucket_name, Key=s3_key)
                remote_etag = head["ETag"].strip('"')
                if remote_etag == local_md5:
                    logger.info("%s SKIP (identical): %s", counter, s3_key)
                    skipped += 1
                    continue
            except s3_client.exceptions.ClientError as err:
                if err.response.get("Error", {}).get("Code") != "404":
                    raise

            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=s3_key,
                Body=data,
                ContentType="text/plain; charset=utf-8",
                CacheControl="public, max-age=0, must-revalidate",
            )
            logger.info("%s UPLOADED: %s (%d bytes)", counter, s3_key, len(data))
            migrated += 1

        except Exception:
            logger.exception("%s FAILED: %s", counter, blob.name)
            failed += 1

    logger.info(
        "Migration complete. Migrated: %d, Skipped: %d, Failed: %d",
        migrated,
        skipped,
        failed,
    )

    if failed > 0:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate base texts from Firebase Storage to S3")
    parser.add_argument(
        "--firebase-bucket",
        required=True,
        help="Firebase Storage bucket name (e.g. pecha-backend-dev.appspot.com)",
    )
    parser.add_argument(
        "--prefix",
        default="base_texts/",
        help="Firebase Storage prefix to migrate (default: base_texts/)",
    )
    parser.add_argument("--dry-run", action="store_true", help="List files without uploading")
    args = parser.parse_args()

    migrate(
        firebase_bucket_name=args.firebase_bucket,
        prefix=args.prefix,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
