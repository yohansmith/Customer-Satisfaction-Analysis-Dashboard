import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("Yelp_Load")
         .config("spark.executor.heartbeatInterval", "30s")
         .config("spark.network.timeout", "120s")
         .getOrCreate())
DATA_PATH = r"E:\ESOFT\TopUp\BDV\bdv_project\data"

# 1. Business dataset
business_path = DATA_PATH + r"\yelp_academic_dataset_business.json"
business_df = spark.read.json(business_path)
print("\n=== BUSINESS SCHEMA ===")
business_df.printSchema()

# 2. User dataset
user_path = DATA_PATH + r"\yelp_academic_dataset_user.json"
user_df = spark.read.json(user_path)
print("\n=== USER SCHEMA ===")
user_df.printSchema()

# 3. Review dataset
review_path = DATA_PATH + r"\yelp_academic_dataset_review.json"
review_df = spark.read.json(review_path)
print("\n=== REVIEW SCHEMA ===")
review_df.printSchema()

# 4. Checkin dataset
checkin_path = DATA_PATH + r"\yelp_academic_dataset_checkin.json"
checkin_df = spark.read.json(checkin_path)
print("\n=== CHECKIN SCHEMA ===")
checkin_df.printSchema()

# 5. Tip dataset
tip_path = DATA_PATH + r"\yelp_academic_dataset_tip.json"
tip_df = spark.read.json(tip_path)
print("\n=== TIP SCHEMA ===")
tip_df.printSchema()

print("\nLoaded 5 Yelp datasets successfully.")
spark.stop()
