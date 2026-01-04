import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime

# Best practice: Pass these as arguments, but for simplicity here we define them
# matching the CloudFormation stack resources.
RAW_BUCKET = "aws-free-tier-etl-raw-446072489762"
CURATED_BUCKET = "aws-free-tier-etl-curated-446072489762"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Read CSV from Raw Bucket
# recurse=True allows reading all files in the bucket/folder
source_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [f"s3://{RAW_BUCKET}/"], "recurse": True},
    transformation_ctx="source_dynamic_frame"
)

# 2. Write to Parquet in Curated Bucket
# Using a timestamped folder for each run to avoid collisions
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"s3://{CURATED_BUCKET}/output_{timestamp}/"

sink_dynamic_frame = glueContext.write_dynamic_frame.from_options(
    frame=source_dynamic_frame,
    connection_type="s3",
    format="parquet",
    connection_options={"path": output_path},
    transformation_ctx="sink_dynamic_frame"
)

job.commit()
