from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "iis_log") \
  .load()

schema = StructType([
    StructField("message", StringType()),
    StructField("@metadata", StructType([
        StructField("@timestamp", StringType())
    ]))
])
df = df.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json(df.json, schema).alias("data")).select("data.*")
df = df.select("message", df["@metadata.@timestamp"].alias("timestamp"))



# HDFS뿐 아니라 모니터링을 위해 화면에도 표시
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
"""
query2 = df \
  .writeStream \
  .outputMode("append") \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://localhost:5432/online_judge") \
  .option("dbtable", "your_table") \
  .option("user", "your_username") \
  .option("password", "your_password") \
  .option("checkpointLocation", "hdfs://localhost:9000/user/hadoop/checkpoint") \
  .trigger(processingTime='10 seconds') \
  .start()
"""

spark.streams.awaitAnyTermination()

