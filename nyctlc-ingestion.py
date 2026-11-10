# Copyright © Sachin Chandrashekhar - Data Engineering Hub. All Rights Reserved.
#
# This code is provided as part of a paid training program.
# Unauthorized copying, distribution, or
# publication of this code—either in whole or in part—
# is strictly prohibited.
#
# This code may NOT be:
# - Uploaded to public repositories (GitHub, GitLab, etc.) unless approved by Data Engineering Hub
# - Shared with third parties
# - Used for commercial purposes
#
# Licensed for personal educational use only.
#
# If this code is found on a public repository or distributed without
# authorization, please notify: legal@dataengineeringhub.in

"""
AWS Glue Python Shell - NYCTLC Lyft (HVFHV) Data Ingestion
================================================================
Backup script for DEH Silver Bootcamp Mission 2.
Downloads the latest NYCTLC HVFHV (High Volume For-Hire Vehicle) parquet file
and taxi zone lookup from CloudFront and uploads to S3 raw layer.

Usage: Deploy as a Glue Python Shell job. Bucket name is auto-detected from account ID.
"""

import sys
import requests
import boto3
from datetime import datetime, timedelta
import time
from datetime import datetime

# ---------------------------------------------------------
# Configuration — update with your bucket name
# ---------------------------------------------------------
ACCOUNT_ID = boto3.client('sts').get_caller_identity()['Account']
BUCKET_NAME = f"mission-deh-hof-nyctlc-{ACCOUNT_ID}"

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
CLOUDFRONT_BASE = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_PATH = "/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet"
ZONE_LOOKUP_URL = f"{CLOUDFRONT_BASE}/misc/taxi+_zone_lookup.csv"

S3_RAW_TRIP_PREFIX = "raw/nyctlc/fhvhv_tripdata/"
S3_RAW_ZONE_PREFIX = "raw/nyctlc/taxi_zone_lookup/"

s3_client = boto3.client('s3')


def find_available_months(num_months=6):
    """
    NYCTLC data is typically available ~2 months after the trip month.
    Find the latest available month, then go back num_months from there.
    Returns list of (year, month, url) tuples for available months.
    """
    today = datetime.now()
    available_months = []
    
    # Find the latest available month first
    latest_month_offset = None
    for months_back in [2, 3, 4]:
        target_date = today - timedelta(days=30 * months_back)
        year = target_date.year
        month = target_date.month
        
        url = f"{CLOUDFRONT_BASE}{TRIP_DATA_PATH.format(year=year, month=month)}"
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            latest_month_offset = months_back
            print(f"✅ Latest available month: {year}-{month:02d}")
            break
    
    if latest_month_offset is None:
        raise Exception("Could not find any available NYCTLC HVFHV data!")
    
    # Now collect num_months going back from the latest
    for i in range(num_months):
        months_back = latest_month_offset + i
        target_date = today - timedelta(days=30 * months_back)
        year = target_date.year
        month = target_date.month
        
        url = f"{CLOUDFRONT_BASE}{TRIP_DATA_PATH.format(year=year, month=month)}"
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            available_months.append((year, month, url))
            print(f"   ✅ Available: {year}-{month:02d}")
        else:
            print(f"   ⚠️  Not available: {year}-{month:02d} (skipping)")
    
    print(f"\n   Total months found: {len(available_months)}")
    return available_months


def download_and_upload(source_url, s3_key, stream=False):
    """Download file from URL and upload to S3. Uses streaming multipart upload for large files."""
    print(f"⬇️  Downloading: {source_url}")

    if not stream:
        # Small files — download fully into memory
        response = requests.get(source_url, timeout=300)
        response.raise_for_status()
        file_size_mb = len(response.content) / (1024 * 1024)
        print(f"   Downloaded: {file_size_mb:.1f} MB")

        print(f"⬆️  Uploading to: s3://{BUCKET_NAME}/{s3_key}")
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=response.content)
        print(f"   ✅ Upload complete!")
    else:
        # Large files — stream in chunks via S3 multipart upload
        CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB

        response = requests.get(source_url, timeout=300, stream=True)
        response.raise_for_status()

        content_length = response.headers.get("Content-Length", "unknown")
        print(f"   File size: {content_length} bytes")

        mpu = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=s3_key)
        upload_id = mpu["UploadId"]

        parts = []
        part_number = 1
        downloaded = 0
        buffer = b""

        try:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    buffer += chunk
                    downloaded += len(chunk)

                    if len(buffer) >= CHUNK_SIZE:
                        print(f"   Uploading part {part_number} ({downloaded / (1024*1024):.1f} MB downloaded)...")
                        part = s3_client.upload_part(
                            Bucket=BUCKET_NAME,
                            Key=s3_key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=buffer
                        )
                        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        part_number += 1
                        buffer = b""

            # Upload remaining buffer
            if buffer:
                print(f"   Uploading final part {part_number} ({downloaded / (1024*1024):.1f} MB total)...")
                part = s3_client.upload_part(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

            s3_client.complete_multipart_upload(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )
            print(f"   ✅ Upload complete: s3://{BUCKET_NAME}/{s3_key}")

        except Exception as e:
            print(f"   ❌ Error during upload, aborting: {e}")
            s3_client.abort_multipart_upload(Bucket=BUCKET_NAME, Key=s3_key, UploadId=upload_id)
            raise


def main():
    print("=" * 60)
    print("NYCTLC Lyft (HVFHV) Data Ingestion — 6 Months")
    print(f"Target bucket: {BUCKET_NAME}")
    print("=" * 60)
    
    # Step 1: Find available months (last 6)
    print("\n📦 Step 1: Finding last 6 months of available HVFHV trip data...")
    available_months = find_available_months(num_months=6)
    
    if not available_months:
        raise Exception("No available months found!")
    
    # Step 2: Download and upload each month
    print("\n📦 Step 2: Downloading and uploading trip data files...")
    files_downloaded = 0
    for year, month, trip_url in available_months:
        trip_filename = f"fhvhv_tripdata_{year}-{month:02d}.parquet"
        trip_s3_key = f"{S3_RAW_TRIP_PREFIX}{trip_filename}"
        print(f"\n--- Processing: {trip_filename} ---")
        download_and_upload(trip_url, trip_s3_key, stream=True)
        files_downloaded += 1
    
    # Step 3: Download taxi zone lookup
    print("\n📦 Step 3: Downloading taxi zone lookup table...")
    zone_s3_key = f"{S3_RAW_ZONE_PREFIX}taxi_zone_lookup.csv"
    download_and_upload(ZONE_LOOKUP_URL, zone_s3_key)
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ INGESTION COMPLETE!")
    print(f"   Files downloaded: {files_downloaded} months of trip data + zone lookup")
    print(f"   Location: s3://{BUCKET_NAME}/{S3_RAW_TRIP_PREFIX}")
    print("=" * 60)

    return files_downloaded


def write_audit_record(status, files_downloaded, duration, error_message=""):
    """Write an audit record to the mission-deh-hof-job-audit DynamoDB table."""
    dynamodb = boto3.client('dynamodb')
    dynamodb.put_item(
        TableName='mission-deh-hof-job-audit',
        Item={
            'job_name': {'S': 'mission-deh-hof-nyctlc-ingestion'},
            'run_timestamp': {'S': datetime.now().isoformat()},
            'layer': {'S': 'raw'},
            'records_read': {'N': str(files_downloaded)},
            'records_written': {'N': str(files_downloaded)},
            'records_rejected': {'N': '0'},
            'duration_seconds': {'N': str(duration)},
            'status': {'S': status},
            'error_message': {'S': error_message},
        }
    )
    print(f"Audit record written ({status}). Duration: {duration}s")


if __name__ == "__main__":
    start_time = time.time()
    files_downloaded = 0
    try:
        files_downloaded = main()
        duration = int(time.time() - start_time)
        write_audit_record("SUCCESS", files_downloaded, duration)
    except Exception as e:
        duration = int(time.time() - start_time)
        write_audit_record("FAILED", files_downloaded, duration, error_message=str(e))
        print(f"❌ Job failed: {e}")
        raise
