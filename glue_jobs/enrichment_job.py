"""AWS Glue ETL Job 3: Join + Enrich.

Joins cleaned trending video data with category mappings,
computes additional analytics columns, and writes the final
enriched dataset as optimized Parquet.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_BUCKET", "PROCESSED_BUCKET", "GLUE_DATABASE"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

PROCESSED_BUCKET = args["PROCESSED_BUCKET"]


def enrich_data():
    """Join trending videos with categories and compute analytics."""

    # Read cleaned trending data
    videos_path = f"s3://{PROCESSED_BUCKET}/parquet/trending_videos/"
    videos_df = spark.read.parquet(videos_path)
    print(f"Trending videos count: {videos_df.count()}")

    # Read category mapping
    categories_path = f"s3://{PROCESSED_BUCKET}/parquet/category_mapping/"
    categories_df = spark.read.parquet(categories_path)
    print(f"Category mappings count: {categories_df.count()}")

    # === JOIN: Videos + Categories ===
    enriched = videos_df.join(
        categories_df.select("category_id", "category_title", F.col("region").alias("cat_region")),
        on=[
            videos_df.category_id == categories_df.category_id,
            videos_df.region == categories_df.cat_region,
        ],
        how="left",
    ).drop(categories_df.category_id).drop("cat_region")

    # === ANALYTICS COLUMNS ===

    # 1. Days between publish and trending
    enriched = enriched.withColumn(
        "days_to_trend",
        F.datediff("trending_date", F.to_date("published_at")),
    )

    # 2. Publish hour (for time-of-day analysis)
    enriched = enriched.withColumn(
        "publish_hour", F.hour("published_at")
    ).withColumn(
        "publish_day_of_week", F.dayofweek("published_at")
    )

    # 3. Views per day on trending
    enriched = enriched.withColumn(
        "views_per_day",
        F.when(
            F.col("days_to_trend") > 0,
            F.col("views") / F.col("days_to_trend"),
        ).otherwise(F.col("views")),
    )

    # 4. Ranking within region+date
    region_date_window = Window.partitionBy("region", "trending_date").orderBy(
        F.desc("views")
    )
    enriched = enriched.withColumn(
        "trending_rank", F.row_number().over(region_date_window)
    )

    # 5. Category ranking within region+date
    category_window = Window.partitionBy(
        "region", "trending_date", "category_title"
    ).orderBy(F.desc("views"))
    enriched = enriched.withColumn(
        "category_rank", F.row_number().over(category_window)
    )

    # 6. Channel trending frequency
    channel_window = Window.partitionBy("region", "channel_title")
    enriched = enriched.withColumn(
        "channel_trending_count", F.count("*").over(channel_window)
    )

    # 7. Views percentile within region
    enriched = enriched.withColumn(
        "views_percentile",
        F.percent_rank().over(
            Window.partitionBy("region", "trending_date").orderBy("views")
        ),
    )

    # 8. Engagement tier classification
    enriched = enriched.withColumn(
        "engagement_tier",
        F.when(F.col("engagement_rate") >= 10, "viral")
        .when(F.col("engagement_rate") >= 5, "high")
        .when(F.col("engagement_rate") >= 2, "medium")
        .otherwise("low"),
    )

    # 9. Content type classification
    enriched = enriched.withColumn(
        "content_type",
        F.when(F.col("description_length") > 1000, "long_form")
        .when(F.col("description_length") > 200, "standard")
        .otherwise("minimal"),
    )

    # Add final metadata
    enriched = enriched.withColumn("enriched_at", F.current_timestamp())

    print(f"Enriched record count: {enriched.count()}")
    enriched.printSchema()

    # Write final enriched dataset
    output_path = f"s3://{PROCESSED_BUCKET}/parquet/enriched_trending/"
    print(f"Writing enriched data to: {output_path}")

    enriched.coalesce(10).write.mode("overwrite").partitionBy(
        "year", "month", "region"
    ).option("compression", "snappy").parquet(output_path)

    # Write summary statistics
    summary = (
        enriched.groupBy("region", "year", "month", "category_title")
        .agg(
            F.count("*").alias("video_count"),
            F.sum("views").alias("total_views"),
            F.avg("views").alias("avg_views"),
            F.avg("engagement_rate").alias("avg_engagement_rate"),
            F.avg("days_to_trend").alias("avg_days_to_trend"),
            F.countDistinct("channel_title").alias("unique_channels"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    summary_path = f"s3://{PROCESSED_BUCKET}/parquet/trending_summary/"
    summary.write.mode("overwrite").partitionBy("year", "month").parquet(summary_path)

    print("Enrichment job complete!")


enrich_data()
job.commit()
