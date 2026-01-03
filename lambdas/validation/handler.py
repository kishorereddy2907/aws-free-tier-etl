import csv
import io
import json
import boto3

s3 = boto3.client("s3")

REQUIRED_COLUMNS = ["email", "country"]
MIN_DATA_ROWS = 1


def lambda_handler(event, context):
    """
    Validation responsibilities:
    - Header validation (required columns)
    - Row count validation (non-empty file)
    """

    bucket = event["bucket"]
    key = event["key"]

    csv_content = read_csv_from_s3(bucket, key)
    header, rows = parse_csv(csv_content)

    validate_header(header)
    validate_row_count(rows)

    return {
        "bucket": bucket,
        "key": key,
        "valid": True,
        "metrics": {
            "total_rows": len(rows),
            "column_count": len(header)
        }
    }


# -----------------------------
# Helper functions
# -----------------------------

def read_csv_from_s3(bucket, key):
    """Read CSV file from S3 bucket."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    except Exception as e:
        raise ValueError(f"Failed to read CSV from S3: {str(e)}")


def parse_csv(csv_content):
    """Parse CSV content and return header and data rows."""
    reader = csv.reader(io.StringIO(csv_content))
    rows = list(reader)
    
    if not rows:
        raise ValueError("CSV file is empty")
    
    header = rows[0]
    data_rows = rows[1:]
    
    return header, data_rows


def validate_header(header):
    """Validate that all required columns are present in the header."""
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in header]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")


def validate_row_count(rows):
    """Validate that the CSV has at least the minimum required data rows."""
    if len(rows) < MIN_DATA_ROWS:
        raise ValueError(f"CSV must have at least {MIN_DATA_ROWS} data row(s), found {len(rows)}")