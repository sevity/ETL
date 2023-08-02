from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext("local[*]", "KafkaExample")
ssc = StreamingContext(sc, 10)  # 10초 단위로 처리
# KafkaUtils.createStream은 kafka broker가 아닌 zookeeper주소와 포트사용(2181)    
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'group1', {'iis_log':1})

kafkaStream.foreachRDD(lambda rdd: rdd.saveAsTextFile('hdfs://localhost:9000/spark_stream/iis_log'))

ssc.start()
ssc.awaitTermination()

