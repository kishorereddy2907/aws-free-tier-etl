import json
import boto3
import pandas as pd
import io
import datetime

s3 = boto3.client("s3")
CURATED_BUCKET = "aws-free-tier-etl-curated-446072489762"

def lambda_handler(event, context):
    """
    Transformation responsibilities:
    - Read input data from event
    - Convert to Pandas DataFrame
    - Write to Parquet format
    - Upload to Curated S3 Bucket with timestamp
    """
    try:
        # 1. Parse Input
        # Assuming event comes from SQS -> Lambda, records are in event["Records"]
        # And the "body" of the SQS message contains the actual data payload
        
        all_records = []
        
        for record in event.get("Records", []):
            body = json.loads(record["body"])
            # Flatten or extract relevant data. 
            # Assuming 'record' key in body holds the row data based on previous context.
            if "record" in body:
                all_records.append(body["record"])
            else:
                # Fallback if the whole body is the record
                all_records.append(body)
        
        if not all_records:
            print("No records to process.")
            return {"status": "skipped", "reason": "no_records"}

        # 2. Convert to DataFrame
        df = pd.DataFrame(all_records)
        
        # 3. Convert to Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_content = parquet_buffer.getvalue()

        # 4. Generate Timestamped Key
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # Use first record ID or generic name for the batch
        file_name = f"data_{timestamp}.parquet"
        
        # 5. Upload to S3
        s3.put_object(
            Bucket=CURATED_BUCKET,
            Key=file_name,
            Body=parquet_content,
            ContentType="application/x-parquet"
        )
        
        print(f"Successfully uploaded {file_name} to {CURATED_BUCKET}")
        
        return {
            "status": "success", 
            "file": file_name, 
            "record_count": len(all_records)
        }

    except Exception as e:
        print(f"Transformation failed: {str(e)}")
        raise e
