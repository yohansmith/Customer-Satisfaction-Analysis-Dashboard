from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# BUSINESS SCHEMA (simplified)
business_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("review_count", LongType(), True),
    StructField("categories", StringType(), True)
])

# USER SCHEMA (simplified)
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("review_count", LongType(), True),
    StructField("average_stars", DoubleType(), True),
    StructField("yelping_since", StringType(), True)
])

# REVIEW SCHEMA
review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("date", StringType(), True)
])

# TIP SCHEMA
tip_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True)
])

# CHECKIN SCHEMA
checkin_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("date", StringType(), True)
])
