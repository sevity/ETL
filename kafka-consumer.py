from kafka import KafkaConsumer

# Kafka 서버 주소 설정
consumer = KafkaConsumer('iis_log',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',  # 이 옵션을 주면 쌓여있는 메시지중 가장 처음 부터 읽음
                         enable_auto_commit=True  # 이 옵션을 주면 마지막 읽은 위치 다음위치 부터 읽게해줌
                         )

for message in consumer:
    # 메시지 출력
    print (message)

