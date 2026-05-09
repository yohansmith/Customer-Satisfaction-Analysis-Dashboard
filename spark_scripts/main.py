import findspark
findspark.init(r"C:\spark")   # Make sure this matches your SPARK_HOME

from pyspark.sql import SparkSession

# Fix heartbeat warnings (recommended)
spark = (SparkSession.builder
         .appName("BDV_Assignment")
         .config("spark.executor.heartbeatInterval", "30s")
         .config("spark.network.timeout", "120s")
         .getOrCreate())

print("Spark version:", spark.version)

# Test session
df = spark.range(5)
df.show()

spark.stop()