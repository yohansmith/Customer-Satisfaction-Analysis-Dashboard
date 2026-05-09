import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from schemas import review_schema

spark = SparkSession.builder.appName("Debug").getOrCreate()

DATA_PATH = r"E:\ESOFT\TopUp\BDV\bdv_project\data"

df = spark.read.schema(review_schema).json(DATA_PATH + r"\yelp_academic_dataset_review.json")

print("=== Columns in review dataset ===")
print(df.columns)

df.printSchema()

spark.stop()
