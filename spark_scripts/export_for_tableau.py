from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExportForTableau").getOrCreate()

# Load cleaned datasets
business = spark.read.parquet("processed/business_clean.parquet")
review = spark.read.parquet("processed/review_clean.parquet")

# Export folder
export_path = "export"

# Write as CSV
business.write.mode("overwrite").option("header", True).csv(f"{export_path}/business_clean_csv")
review.write.mode("overwrite").option("header", True).csv(f"{export_path}/review_clean_csv")

print("✅ CSV export complete! Files saved in /export folder.")

spark.stop()
