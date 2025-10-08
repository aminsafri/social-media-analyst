import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_bucket'])
job.init(args['JOB_NAME'], args)

def create_date_dimension():
    """Create date dimension"""
    start_date = "2024-01-01"
    end_date = "2026-12-31"

    date_df = spark.sql(f"""
        SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_array
    """).select(F.explode(F.col("date_array")).alias("full_date"))

    date_dim = date_df.select(
        F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date").cast("date"),
        F.year(F.col("full_date")).cast("int").alias("year"),
        F.quarter(F.col("full_date")).cast("int").alias("quarter"),
        F.month(F.col("full_date")).cast("int").alias("month"),
        F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
        F.dayofweek(F.col("full_date")).cast("int").alias("day_of_week"),
        F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
        F.when(F.dayofweek(F.col("full_date")).isin([1, 7]), True).otherwise(False).alias("is_weekend")
    )

    return date_dim

def process_reddit_data():
    """Process Reddit data into dimensional model"""
    
    reddit_df = glueContext.create_dynamic_frame.from_catalog(
        database="social_media_db",
        table_name="reddit"
    ).toDF()
    
    # Content dimension
    content_dim = reddit_df.select(
        F.abs(F.hash(F.col("post_id"))).alias("content_key"),
        F.col("post_id"),
        F.col("title"),
        F.col("subreddit"),
        F.col("author"),
        F.lit("reddit_post").alias("content_type"),
        to_timestamp(F.from_unixtime(F.col("created_utc"))).alias("created_at")
    ).distinct()
    
    # Engagement fact
    fact_engagement = reddit_df.select(
        F.abs(F.hash(F.col("post_id"))).alias("content_key"),
        F.date_format(F.from_unixtime(F.col("created_utc")), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("score").alias("upvotes"),
        F.col("num_comments").alias("comments_count"),
        (F.col("score") * 0.6 + F.col("num_comments") * 0.4).alias("engagement_score"),
        F.col("upvote_ratio")
    )
    
    return content_dim, fact_engagement


def process_news_data():
    """Process News data"""
    
    news_df = glueContext.create_dynamic_frame.from_catalog(
        database="social_media_db",
        table_name="news"
    ).toDF()
    
    # News dimension
    news_dim = news_df.select(
        F.abs(F.hash(F.concat(F.col("title"), F.col("source_name")))).alias("article_key"),
        F.col("title"),
        F.col("description"),
        F.col("source_name"),
        F.col("author"),
        F.col("category"),
        F.col("country"),
        to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("published_at")
    ).distinct()
    
    # News fact
    news_fact = news_df.select(
        F.abs(F.hash(F.concat(F.col("title"), F.col("source_name")))).alias("article_key"),
        F.date_format(F.col("published_at"), "yyyyMMdd").cast("int").alias("date_key"),
        F.length(F.col("description")).alias("description_length"),
        F.when(F.col("description").contains("positive"), 1)
         .when(F.col("description").contains("negative"), -1)
         .otherwise(0).alias("sentiment_score")
    )
    
    return news_dim, news_fact

def process_crypto_data():
    """Process Crypto data"""
    
    crypto_df = glueContext.create_dynamic_frame.from_catalog(
        database="social_media_db",
        table_name="crypto"
    ).toDF()
    
    # Crypto dimension
    crypto_dim = crypto_df.select(
        F.abs(F.hash(F.col("coin_id"))).alias("crypto_key"),
        F.col("coin_id"),
        F.col("symbol"),
        F.col("name"),
        F.col("market_cap_rank")
    ).distinct()
    
    # Crypto fact
    crypto_fact = crypto_df.select(
        F.abs(F.hash(F.col("coin_id"))).alias("crypto_key"),
        F.date_format(F.to_timestamp(F.col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"), 
                      "yyyyMMdd").cast("int").alias("date_key"),
        F.round(F.col("current_price").getItem("double").cast(DecimalType(18, 2)), 2).alias("current_price"),
        F.col("market_cap").cast(LongType()).alias("market_cap"),
        F.col("total_volume").getItem("long").alias("total_volume"),
        F.round(F.col("price_change_24h").cast(DecimalType(18, 2)), 2).alias("price_change_24h"),
        F.round(F.col("price_change_percentage_24h").cast(DecimalType(10, 2)), 2).alias("price_change_percentage_24h")
    )
    
    return crypto_dim, crypto_fact

    
def main():
    output_bucket = args['output_bucket']
    try:
        print("Creating date dimension...")
        date_dim = create_date_dimension()

        print("Processing Reddit data...")
        content_dim, fact_engagement = process_reddit_data()

        print("Processing News data...")
        news_dim, news_fact = process_news_data()

        print("Processing Crypto data...")
        crypto_dim, crypto_fact = process_crypto_data()

        print("Writing data to S3...")

        # Dimensions
        date_dim.write.mode("overwrite").parquet(f"s3://{output_bucket}/processed-data/dim_date/")
        content_dim.write.mode("overwrite").parquet(f"s3://{output_bucket}/processed-data/dim_content/")
        news_dim.write.mode("overwrite").parquet(f"s3://{output_bucket}/processed-data/dim_news/")
        crypto_dim.write.mode("overwrite").parquet(f"s3://{output_bucket}/processed-data/dim_crypto/")

        # Facts
        fact_engagement.write.mode("overwrite").parquet(f"s3://{output_bucket}/processed-data/fact_engagement/")
        news_fact.write.mode("overwrite").parquet(f"s3://{output_bucket}/processed-data/fact_news/")
        crypto_fact.write.mode("overwrite").parquet(f"s3://{output_bucket}/processed-data/fact_crypto_prices/")

        print("ETL job completed successfully!")

    except Exception as e:
        print(f"Error in ETL job: {str(e)}")
        raise

if __name__ == "__main__":
    main()

job.commit()
