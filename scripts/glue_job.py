from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", "true") \
    .csv("s3://aws-free-tier-etl-raw-446072489762/employees_global/")

df_dedup = df.dropDuplicates(["email"], ["country"])

df_dedup.write.mode("overwrite") \
    .partitionBy("country") \
    .parquet("s3://aws-free-tier-etl-curated-446072489762/data/")
