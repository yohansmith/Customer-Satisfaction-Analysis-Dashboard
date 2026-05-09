from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("YelpAnalytics").getOrCreate()

# Load cleaned data
business = spark.read.parquet("processed/business_clean.parquet")
review = spark.read.parquet("processed/review_clean.parquet")

print("=== 1. Top 10 Most Reviewed Businesses ===")
top_business = (
    review.groupBy("business_id")
    .agg(F.count("*").alias("review_count"))
    .orderBy(F.desc("review_count"))
)
top_business.show(10, truncate=False)


print("=== 2. Average Star Ratings by City ===")
avg_city = (
    business.groupBy("city")
    .agg(F.avg("stars").alias("avg_rating"))
    .orderBy(F.desc("avg_rating"))
)
avg_city.show(10, truncate=False)


print("=== 3. Most Popular Categories (Exploded) ===")
popular_cat = (
    business.groupBy("category")
    .agg(F.count("*").alias("total"))
    .orderBy(F.desc("total"))
)
popular_cat.show(10, truncate=False)


print("=== 4. Review Volume by Year ===")
reviews_by_year = (
    review.groupBy("year")
    .agg(F.count("*").alias("reviews"))
    .orderBy("year")
)
reviews_by_year.show()


print("=== 5. Distribution of Negative Reviews ===")
neg_dist = (
    review.groupBy("is_negative")
    .agg(F.count("*").alias("count"))
)
neg_dist.show()
