from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "iis_log") \
  .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# HDFS뿐 아니라 모니터링을 위해 화면에도 표시
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query2 = df \
  .writeStream \
  .outputMode("append") \
  .format("csv") \
  .option("path", "hdfs://localhost:9000/user/hadoop/iis_log") \
  .option("checkpointLocation", "hdfs://localhost:9000/user/hadoop/checkpoint") \
  .trigger(processingTime='10 seconds') \
  .start()

spark.streams.awaitAnyTermination()

