from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, split
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "iis_log") \
  .load()

df = df.selectExpr("CAST(value AS STRING) as json")

schema = StructType([
    StructField("message", StringType()),
    StructField("@timestamp", StringType())
])

df = df.select(from_json(df.json, schema).alias("data")).select("data.*")

df = df.withColumn("message_split", split(df["message"], " "))
df = df.withColumn("client_ip", df["message_split"].getItem(2))
df = df.withColumn("url", df["message_split"].getItem(4))
df = df.withColumn("timestamp", df["@timestamp"])

df = df.select("client_ip", "url", "timestamp")

# HDFS뿐 아니라 모니터링을 위해 화면에도 표시
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query2 = df \
  .writeStream \
  .outputMode("append") \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://localhost:5432/online_judge") \
  .option("dbtable", "iis_log") \
  .option("user", "online_judge_admin") \
  .option("password", "abcd123$") \
  .trigger(processingTime='10 seconds') \
  .start()

spark.streams.awaitAnyTermination()

