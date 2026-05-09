import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from schemas import business_schema, user_schema, review_schema, tip_schema, checkin_schema

spark = (SparkSession.builder
         .appName("Yelp_Load_Explicit")
         .config("spark.executor.heartbeatInterval", "30s")
         .config("spark.network.timeout", "120s")
         .getOrCreate())

DATA_PATH = r"E:\ESOFT\TopUp\BDV\bdv_project\data"

business_df = spark.read.schema(business_schema).json(DATA_PATH + r"\yelp_academic_dataset_business.json")
user_df     = spark.read.schema(user_schema).json(DATA_PATH + r"\yelp_academic_dataset_user.json")
review_df   = spark.read.schema(review_schema).json(DATA_PATH + r"\yelp_academic_dataset_review.json")
checkin_df  = spark.read.schema(checkin_schema).json(DATA_PATH + r"\yelp_academic_dataset_checkin.json")
tip_df      = spark.read.schema(tip_schema).json(DATA_PATH + r"\yelp_academic_dataset_tip.json")

print("Schemas loaded successfully:")
business_df.printSchema()
user_df.printSchema()
review_df.printSchema()
checkin_df.printSchema()
tip_df.printSchema()

spark.stop()
