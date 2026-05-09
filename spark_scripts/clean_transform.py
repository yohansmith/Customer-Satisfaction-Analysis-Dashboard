import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, split, explode, to_timestamp, year, month, dayofmonth, trim

from schemas import business_schema, user_schema, review_schema, tip_schema, checkin_schema

spark = (SparkSession.builder
         .appName("Yelp_Cleaning_Transformation")
         .config("spark.executor.heartbeatInterval", "30s")
         .config("spark.network.timeout", "120s")
         .getOrCreate())

DATA_PATH = r"E:\ESOFT\TopUp\BDV\bdv_project\data"

# Load data with explicit schemas
business_df = spark.read.schema(business_schema).json(DATA_PATH + r"\yelp_academic_dataset_business.json")
user_df     = spark.read.schema(user_schema).json(DATA_PATH + r"\yelp_academic_dataset_user.json")
review_df   = spark.read.schema(review_schema).json(DATA_PATH + r"\yelp_academic_dataset_review.json")

# ----------------------------
# 4.1 SELECT USEFUL COLUMNS
# ----------------------------
business_clean = business_df.select(
    "business_id", "name", "city", "state", "stars", "review_count", "categories"
)

user_clean = user_df.select(
    "user_id", "name", "review_count", "average_stars"
)

review_clean = review_df.select(
    "review_id", "user_id", "business_id", "stars", "date"
)

# ----------------------------
# 4.2 EXPLODE CATEGORIES
# ----------------------------
business_clean = business_clean.withColumn(
    "category",
    explode(split(col("categories"), ","))
)

business_clean = business_clean.withColumn("category", trim(col("category")))

# ----------------------------
# 4.3 CLEAN DATE FORMAT
# ----------------------------
review_clean = review_clean.withColumn(
    "timestamp",
    F.try_to_timestamp(F.col("date"), F.lit("yyyy-MM-dd HH:mm:ss"))
)


review_clean = review_clean.withColumn("year", year("timestamp"))
review_clean = review_clean.withColumn("month", month("timestamp"))
review_clean = review_clean.withColumn("day", dayofmonth("timestamp"))

# ----------------------------
# 4.4 TAG NEGATIVE REVIEWS
# ----------------------------
review_clean = review_clean.withColumn(
    "is_negative",
    (col("stars") <= 2).cast("int")
)

# ----------------------------
# 4.5 REMOVE NULLS
# ----------------------------
review_clean = review_clean.dropna(subset=["business_id", "user_id", "stars"])

# Show sample outputs
print("=== Cleaned Business ===")
business_clean.show(5, truncate=False)

print("=== Cleaned Review ===")
review_clean.show(5, truncate=False)

# === SAVE CLEANED OUTPUTS ===
business_clean.write.mode("overwrite").parquet("processed/business_clean.parquet")
review_clean.write.mode("overwrite").parquet("processed/review_clean.parquet")

print("Cleaned datasets saved to /processed folder.")

spark.stop()
