from kafka import KafkaProducer
import csv 
import json 
# json형식으로 내보내기 위해서 
import time 
# stream으로 데이터를 하나씩 보낼 때 일정시간 딜레이를 주기 위해 사용 

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producer = KafkaProducer(bootstrap_servers = brokers)
topicName = "trips"

with open("/Users/sig6774/Desktop/Data_Engineering/data-engineering-main/01-spark/data/tripdata_2021_1.csv", "r") as file:
    reader = csv.reader(file)
    # 특정 csv 데이터를 읽어옴 
    headings = next(reader)
    # next함수를 활용해서 header만 추출 

    for row in reader:
        producer.send(topicName, json.dumps(row).encode("utf-8"))
        # producer를 통해 message를 보냄 
        # csv 형태의 데이터를 json 형태로 변환해서 보냄 
        print(row)
        time.sleep(1)
        # 한번 메시지를 보내고 1초 쉬는 것을 반복 
        


