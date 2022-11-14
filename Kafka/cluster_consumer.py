from kafka import KafkaConsumer 

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
# consumer가 통신할 brokers 입력 

consumer = KafkaConsumer("first-cluster-topic", bootstrap_servers = brokers)
# 사용할 topic 명과 연결할 broker list 넣어줌 

for mes in consumer:
    print(mes)
# consumer가 message를 받을 때 마다 print 진행 

