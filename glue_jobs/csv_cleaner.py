"""AWS Glue ETL Job 1: CSV Cleaning + Schema Enforcement.

Reads raw YouTube trending CSV data, cleans and normalizes columns,
enforces schema, and writes to processed zone as Parquet.
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
)

# Initialize Glue context
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET",
        "PROCESSED_BUCKET",
        "GLUE_DATABASE",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Configuration
RAW_BUCKET = args["RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
GLUE_DATABASE = args["GLUE_DATABASE"]

# Expected schema for YouTube trending CSVs
TRENDING_SCHEMA = StructType(
    [
        StructField("video_id", StringType(), False),
        StructField("trending_date", StringType(), True),
        StructField("title", StringType(), True),
        StructField("channel_title", StringType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("publish_time", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("views", LongType(), True),
        StructField("likes", LongType(), True),
        StructField("dislikes", LongType(), True),
        StructField("comment_count", LongType(), True),
        StructField("thumbnail_link", StringType(), True),
        StructField("comments_disabled", BooleanType(), True),
        StructField("ratings_disabled", BooleanType(), True),
        StructField("video_error_or_removed", BooleanType(), True),
        StructField("description", StringType(), True),
        StructField("region", StringType(), True),
        StructField("ingestion_date", StringType(), True),
    ]
)


def clean_csv_data():
    """Main cleaning pipeline for CSV trending data."""
    # Read raw CSV data
    raw_path = f"s3://{RAW_BUCKET}/csv/"
    print(f"Reading raw CSV data from: {raw_path}")

    df = spark.read.option("header", "true").option("quote", '"').option(
        "escape", '"'
    ).option("multiLine", "true").csv(raw_path)

    print(f"Raw record count: {df.count()}")
    print(f"Raw schema: {df.schema.simpleString()}")

    # === CLEANING STEPS ===

    # 1. Drop duplicates based on video_id + trending_date + region
    df = df.dropDuplicates(["video_id", "trending_date", "region"])

    # 2. Cast numeric columns
    df = (
        df.withColumn("views", F.col("views").cast(LongType()))
        .withColumn("likes", F.col("likes").cast(LongType()))
        .withColumn("dislikes", F.col("dislikes").cast(LongType()))
        .withColumn("comment_count", F.col("comment_count").cast(LongType()))
        .withColumn("category_id", F.col("category_id").cast(IntegerType()))
    )

    # 3. Parse trending_date (format: YY.DD.MM → YYYY-MM-DD)
    df = df.withColumn(
        "trending_date_parsed",
        F.to_date(
            F.concat(
                F.lit("20"),
                F.substring("trending_date", 1, 2),
                F.lit("-"),
                F.substring("trending_date", 7, 2),
                F.lit("-"),
                F.substring("trending_date", 4, 2),
            ),
            "yyyy-MM-dd",
        ),
    )

    # 4. Parse publish_time to timestamp
    df = df.withColumn(
        "published_at",
        F.to_timestamp("publish_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
    )

    # 5. Clean text fields
    df = (
        df.withColumn("title", F.trim(F.col("title")))
        .withColumn("channel_title", F.trim(F.col("channel_title")))
        .withColumn(
            "description",
            F.when(F.col("description").isNull(), F.lit("")).otherwise(
                F.trim(F.col("description"))
            ),
        )
    )

    # 6. Parse tags (pipe-separated → array)
    df = df.withColumn(
        "tags_array",
        F.when(
            F.col("tags") == "[none]", F.array()
        ).otherwise(F.split(F.col("tags"), "\\|")),
    )
    df = df.withColumn("tag_count", F.size("tags_array"))

    # 7. Compute derived metrics
    df = (
        df.withColumn(
            "engagement_rate",
            F.when(
                F.col("views") > 0,
                (F.col("likes") + F.col("dislikes") + F.col("comment_count"))
                / F.col("views")
                * 100,
            ).otherwise(0.0),
        )
        .withColumn(
            "like_ratio",
            F.when(
                (F.col("likes") + F.col("dislikes")) > 0,
                F.col("likes") / (F.col("likes") + F.col("dislikes")) * 100,
            ).otherwise(0.0),
        )
        .withColumn("title_length", F.length("title"))
        .withColumn("description_length", F.length("description"))
    )

    # 8. Add processing metadata
    df = df.withColumn("processed_at", F.current_timestamp())

    # 9. Extract partitioning columns
    df = (
        df.withColumn(
            "year", F.year("trending_date_parsed")
        )
        .withColumn("month", F.month("trending_date_parsed"))
    )

    # 10. Select final columns
    cleaned_df = df.select(
        "video_id",
        "trending_date_parsed",
        "title",
        "channel_title",
        "category_id",
        "published_at",
        "tags_array",
        "tag_count",
        "views",
        "likes",
        "dislikes",
        "comment_count",
        "comments_disabled",
        "ratings_disabled",
        "video_error_or_removed",
        "description",
        "title_length",
        "description_length",
        "engagement_rate",
        "like_ratio",
        "region",
        "processed_at",
        "year",
        "month",
    ).withColumnRenamed("trending_date_parsed", "trending_date")

    # Filter out obviously bad records
    cleaned_df = cleaned_df.filter(
        (F.col("video_id").isNotNull())
        & (F.col("views").isNotNull())
        & (F.col("views") >= 0)
    )

    print(f"Cleaned record count: {cleaned_df.count()}")

    # Write to processed zone as Parquet with partitioning
    output_path = f"s3://{PROCESSED_BUCKET}/parquet/trending_videos/"
    print(f"Writing cleaned data to: {output_path}")

    cleaned_df.write.mode("append").partitionBy("year", "month", "region").parquet(
        output_path
    )

    print("CSV cleaning job complete!")


# Run
clean_csv_data()
job.commit()
