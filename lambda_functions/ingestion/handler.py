"""Data Ingestion Lambda.

Scheduled daily via EventBridge to download YouTube trending data
and upload to S3 raw landing zone.
"""

import csv
import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
RAW_BUCKET = os.environ["RAW_BUCKET"]

# Kaggle dataset regions
REGIONS = ["US", "GB", "CA", "DE", "FR", "IN", "JP", "KR", "MX", "RU"]


def lambda_handler(event, context):
    """Ingest YouTube trending data to S3.

    For the initial load, this processes the Kaggle dataset files
    that should be pre-uploaded to s3://{RAW_BUCKET}/staging/.

    For incremental loads, this could be extended to use the YouTube Data API v3.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("Starting ingestion for date: %s", today)

    results = {"processed": [], "errors": []}

    for region in REGIONS:
        try:
            # Check for staged CSV file
            csv_key = f"staging/{region}videos.csv"
            json_key = f"staging/{region}_category_id.json"

            # Process CSV
            if _file_exists(csv_key):
                processed_key = f"csv/{region}/{today}/{region}videos.csv"
                _copy_and_validate_csv(csv_key, processed_key, region)
                results["processed"].append({"region": region, "type": "csv"})
                logger.info("Processed CSV for region: %s", region)

            # Process JSON category mapping
            if _file_exists(json_key):
                processed_key = f"json/{region}/{today}/{region}_category_id.json"
                _copy_and_validate_json(json_key, processed_key, region)
                results["processed"].append({"region": region, "type": "json"})
                logger.info("Processed JSON for region: %s", region)

        except Exception as e:
            logger.error("Failed processing region %s: %s", region, str(e))
            results["errors"].append({"region": region, "error": str(e)})

    # Write ingestion manifest
    manifest = {
        "ingestion_date": today,
        "regions_processed": len(results["processed"]),
        "errors": len(results["errors"]),
        "details": results,
    }

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=f"manifests/{today}/ingestion_manifest.json",
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
    )

    logger.info("Ingestion complete: %s", json.dumps(manifest))
    return manifest


def _file_exists(key: str) -> bool:
    """Check if a file exists in the raw bucket."""
    try:
        s3_client.head_object(Bucket=RAW_BUCKET, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False


def _copy_and_validate_csv(source_key: str, dest_key: str, region: str) -> None:
    """Copy CSV file with validation.

    Validates:
    - File is valid CSV
    - Has expected columns
    - Adds region metadata column
    """
    response = s3_client.get_object(Bucket=RAW_BUCKET, Key=source_key)
    content = response["Body"].read().decode("utf-8", errors="replace")

    # Validate CSV structure
    reader = csv.DictReader(io.StringIO(content))
    expected_columns = {"video_id", "title", "channel_title", "category_id", "views", "likes"}

    if not expected_columns.issubset(set(reader.fieldnames or [])):
        missing = expected_columns - set(reader.fieldnames or [])
        raise ValueError(f"Missing expected columns for {region}: {missing}")

    # Add metadata and re-upload
    output = io.StringIO()
    fieldnames = list(reader.fieldnames) + ["region", "ingestion_date"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row_count = 0
    for row in csv.DictReader(io.StringIO(content)):
        row["region"] = region
        row["ingestion_date"] = today
        writer.writerow(row)
        row_count += 1

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=dest_key,
        Body=output.getvalue().encode("utf-8"),
        ContentType="text/csv",
        Metadata={"region": region, "row_count": str(row_count)},
    )

    logger.info("Validated and copied CSV: %s (%d rows)", dest_key, row_count)


def _copy_and_validate_json(source_key: str, dest_key: str, region: str) -> None:
    """Copy JSON category mapping with validation."""
    response = s3_client.get_object(Bucket=RAW_BUCKET, Key=source_key)
    content = response["Body"].read().decode("utf-8")

    # Validate JSON structure
    data = json.loads(content)

    if "items" not in data:
        raise ValueError(f"Invalid category JSON for {region}: missing 'items' key")

    # Flatten and enrich
    categories = []
    for item in data["items"]:
        categories.append(
            {
                "category_id": int(item["id"]),
                "category_title": item["snippet"]["title"],
                "assignable": item["snippet"].get("assignable", False),
                "region": region,
            }
        )

    enriched = {
        "region": region,
        "category_count": len(categories),
        "categories": categories,
    }

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=dest_key,
        Body=json.dumps(enriched, indent=2),
        ContentType="application/json",
        Metadata={"region": region, "category_count": str(len(categories))},
    )

    logger.info("Validated and copied JSON: %s (%d categories)", dest_key, len(categories))
