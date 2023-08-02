from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, split
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_timestamp



# Spark 세션 생성
spark = SparkSession.builder.getOrCreate()

# 스키마 수동 지정
schema1 = StructType([
    StructField("client_ip", StringType()),
    StructField("url", StringType()),
    StructField("timestamp", StringType())
])

# Kafka에서 데이터 읽기
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iis_log") \
    .load()

# JSON 형식으로 변환
df = df.selectExpr("CAST(value AS STRING) as json")

# 스키마 정의
schema2 = StructType([
    StructField("message", StringType()),
    StructField("@timestamp", StringType())
])

# 데이터 추출
df = df.select(from_json(df.json, schema2).alias("data")).select("data.*")

# 메시지 분리
df = df.withColumn("message_split", split(df["message"], " "))
df = df.withColumn("client_ip", df["message_split"].getItem(2))
df = df.withColumn("url", df["message_split"].getItem(4))
df = df.withColumn("timestamp", to_timestamp(df["@timestamp"]))
#df = df.withColumn("timestamp", df["@timestamp"])

# 필요한 컬럼 선택
df = df.select("client_ip", "url", "timestamp")

# 화면에 출력
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# 중간 저장소로 Parquet 형식으로 쓰기
query2 = df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/hadoop/iis_log_temp") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/hadoop/checkpoint_temp") \
    .trigger(processingTime='10 seconds') \
    .start()

# 배치 작업으로 데이터를 PostgreSQL에 쓰기
def write_to_postgres():
    df_temp = spark.read.schema(schema1).parquet("hdfs://localhost:9000/user/hadoop/iis_log_temp")
    df_temp.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/online_judge") \
        .option("dbtable", "iis_log") \
        .option("user", "online_judge_admin") \
        .option("password", "abcd123$") \
        .mode("append") \
        .save()

# 배치 작업 실행
while True:
    write_to_postgres()

# 스트리밍 작업 중지
query.stop()

# 스트리밍 작업 대기
spark.streams.awaitAnyTermination()

