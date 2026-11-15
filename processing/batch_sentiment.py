# Topic-aware bounded batch sentiment processing from Kafka.
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, stddev, count
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType,
    ArrayType,
)
from pyspark.sql.functions import pandas_udf

POST_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", LongType(), True),
    StructField("ecosystem", StringType(), True),
    StructField("source", StringType(), True),
    StructField("analysis_topic", StringType(), True),
    StructField("matched_keywords", ArrayType(StringType()), True),
])

SCHISM_THRESHOLD = 0.1


@pandas_udf(FloatType())
def sentiment_udf(texts: pd.Series) -> pd.Series:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    return texts.fillna("").apply(
        lambda value: float(analyzer.polarity_scores(value)["compound"]) if value else 0.0
    )


def main():
    spark = (
        SparkSession.builder
        .appName("RedditTopicAwareBatchSentiment")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.driver.extraJavaOptions",
            "--add-exports=java.base/sun.security.action=ALL-UNNAMED "
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "--add-exports=java.base/sun.security.action=ALL-UNNAMED "
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "reddit.enthusiasts,reddit.critics,reddit.public")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        parsed = (
            raw.select(from_json(col("value").cast("string"), POST_SCHEMA).alias("data"))
            .select("data.*")
            # Exclude messages produced by the old, non-topic-aware pipeline.
            .filter(col("text").isNotNull() & col("analysis_topic").isNotNull())
        )

        with_sentiment = parsed.withColumn("sentiment", sentiment_udf(col("text")))

        # Critical correction: compare ecosystems only within the same analysis topic.
        scores = (
            with_sentiment.groupBy("analysis_topic", "ecosystem")
            .agg(
                avg("sentiment").alias("avg_sentiment"),
                stddev("sentiment").alias("std_sentiment"),
                count("post_id").alias("post_count"),
            )
        )

        print("\n=== Topic-wise Ecosystem Sentiment ===")
        results = scores.orderBy("analysis_topic", "ecosystem").collect()
        topic_maps = {}
        for row in results:
            topic_maps.setdefault(row["analysis_topic"], {})[row["ecosystem"]] = row["avg_sentiment"]
            print(
                f"{row['analysis_topic']} | {row['ecosystem']:12s} | "
                f"mean={row['avg_sentiment']:+.4f} | posts={row['post_count']}"
            )

        print("\n=== Topic-wise Schism Detection ===")
        for topic, score_map in topic_maps.items():
            ecosystems = sorted(score_map)
            print(f"\nTopic: {topic}")
            for i in range(len(ecosystems)):
                for j in range(i + 1, len(ecosystems)):
                    e1, e2 = ecosystems[i], ecosystems[j]
                    difference = abs(score_map[e1] - score_map[e2])
                    status = "SCHISM" if difference >= SCHISM_THRESHOLD else "No schism"
                    print(f"  {status}: {e1} vs {e2} | divergence: {difference:.4f}")

        with_sentiment.write.mode("overwrite").parquet("../output/sentiment_scored")
        scores.write.mode("overwrite").parquet("../output/sentiment_summary")
        print("\nTopic-aware results saved to ../output/")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
