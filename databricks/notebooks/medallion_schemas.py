# Databricks notebook source
# DevRadar — Schemas Bronze (posts / comments)

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BRONZE_POSTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("title", StringType(), True),
        StructField("selftext", StringType(), True),
        StructField("author", StringType(), True),
        StructField("score", LongType(), True),
        StructField("upvote_ratio", DoubleType(), True),
        StructField("num_comments", LongType(), True),
        StructField("created_utc", DoubleType(), True),
        StructField("created_date", StringType(), True),
        StructField("permalink", StringType(), True),
        StructField("url", StringType(), True),
        StructField("flair", StringType(), True),
        StructField("is_self", BooleanType(), True),
    ]
)

BRONZE_COMMENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("author", StringType(), True),
        StructField("body", StringType(), True),
        StructField("score", LongType(), True),
        StructField("depth", LongType(), True),
        StructField("created_utc", DoubleType(), True),
        StructField("created_date", StringType(), True),
    ]
)
