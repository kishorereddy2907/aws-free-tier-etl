import csv
import io
import json
import boto3

s3 = boto3.client("s3")

REQUIRED_COLUMNS = ["email", "country"]
EXPECTED_MIN_COLUMNS = 2
MAX_EMPTY_FILE_ROWS = 0


def lambda_handler(event, context):
    """
    Validation responsibilities:
    - CSV format check
    - Header check
    - Column count validation
    - Record count validation
    - Email duplicate detection
    """

    bucket = event["bucket"]
    key = event["key"]

    csv_content = read_csv_from_s3(bucket, key)
    header, rows = parse_csv(csv_content)

    validate_header(header)
    validate_column_count(header)
    validate_record_count(rows)

    good_records, bad_records = split_duplicates(rows, header.index("email"))

    result = {
        "bucket": bucket,
        "key": key,
        "metrics": {
            "total_records": len(rows),
            "valid_records": len(good_records),
            "duplicate_records": len(bad_records),
            "column_count": len(header)
        },
        "good_data": good_records,
        "bad_data": bad_records
    }

    return result


# -----------------------------
# Helper functions
# -----------------------------

def read_csv_from_s3(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    except Exception as e:
        raise ValueError(f"Failed to read CSV from S3: {str(e)}")


def parse_csv(csv_text):
    try:
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)
    except Exception:
        raise ValueError("File is not valid CSV format")

    if not rows:
        raise ValueError("CSV file is empty")

    header = rows[0]
    data_rows = rows[1:]

    return header, data_rows


def validate_header(header):
    missing = [col for col in REQUIRED_COLUMNS if col not in header]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_column_count(header):
    if len(header) < EXPECTED_MIN_COLUMNS:
        raise ValueError("Invalid column count in CSV header")


def validate_record_count(rows):
    if len(rows) <= MAX_EMPTY_FILE_ROWS:
        raise ValueError("CSV file contains no data rows")


def split_duplicates(rows, email_index):
    seen_emails = set()
    good = []
    bad = []

    for row in rows:
        email = row[email_index].strip().lower()

        if not email:
            bad.append(row)
            continue

        if email in seen_emails:
            bad.append(row)
        else:
            seen_emails.add(email)
            good.append(row)

    return good, bad
