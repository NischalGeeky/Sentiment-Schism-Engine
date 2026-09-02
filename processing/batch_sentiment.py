# processing/batch_sentiment.py
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, stddev, from_unixtime, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType
)
from pyspark.sql.functions import pandas_udf

POST_SCHEMA = StructType([
    StructField("post_id",      StringType(),  True),
    StructField("title",        StringType(),  True),
    StructField("text",         StringType(),  True),
    StructField("author",       StringType(),  True),
    StructField("subreddit",    StringType(),  True),
    StructField("score",        IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc",  LongType(),    True),
    StructField("ecosystem",    StringType(),  True),
    StructField("source",       StringType(),  True),
])

@pandas_udf(FloatType())
def sentiment_udf(texts: pd.Series) -> pd.Series:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    return texts.apply(
        lambda t: float(analyzer.polarity_scores(t)['compound']) if t else 0.0
    )

SCHISM_THRESHOLD = 0.1

def main():
    spark = (
        SparkSession.builder
        .appName("RedditBatchSentiment")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions",
                "--add-exports=java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions",
                "--add-exports=java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # read ALL messages from kafka at once — batch mode
    raw = (
        spark.read       # <-- not readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "reddit.enthusiasts,reddit.critics,reddit.public")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed = (
        raw
        .select(from_json(col("value").cast("string"), POST_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("text").isNotNull())
    )

    with_sentiment = (
        parsed
        .withColumn("sentiment", sentiment_udf(col("text")))
    )

    # overall schism detection
    scores = (
        with_sentiment
        .groupBy("ecosystem")
        .agg(
            avg("sentiment").alias("avg_sentiment"),
            stddev("sentiment").alias("std_sentiment"),
        )
    )

    print("\n=== Ecosystem Sentiment Scores ===")
    results = scores.collect()
    score_map = {r['ecosystem']: r['avg_sentiment'] for r in results}
    for eco, score in score_map.items():
        print(f"  {eco:15s}: {score:+.4f}")

    print("\n=== Schism Detection ===")
    ecosystems = list(score_map.keys())
    for i in range(len(ecosystems)):
        for j in range(i + 1, len(ecosystems)):
            e1, e2 = ecosystems[i], ecosystems[j]
            diff = abs(score_map[e1] - score_map[e2])
            if diff >= SCHISM_THRESHOLD:
                print(f"  ⚡ SCHISM: {e1} vs {e2} | divergence: {diff:.4f}")
            else:
                print(f"  ✓ No schism: {e1} vs {e2} | divergence: {diff:.4f}")

    # save results to parquet for dashboard
    with_sentiment.write.mode("overwrite").parquet("../output/sentiment_scored")
    scores.write.mode("overwrite").parquet("../output/sentiment_summary")
    print("\nResults saved to ../output/")

if __name__ == "__main__":
    main()
