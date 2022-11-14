from kafka import KafkaConsumer
import json 


brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topicName = "trips"
consumer = KafkaConsumer(topicName, bootstrap_servers=brokers)

for messages in consumer:
    row = json.loads(messages.value.decode())
    # messages는 json형태이고 보내는 값의 value만 가지고 오고 decode해서 python object로 변환 

    if float(row[11]) > 10:
        print("--over 10 --")
        print(f"{row[10]} - {row[11]}")
    # 요금이 10불 이상인 것 중 payment type과 fare amount만 출력 

