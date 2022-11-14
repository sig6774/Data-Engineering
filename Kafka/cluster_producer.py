from kafka import KafkaProducer

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
# 사용할 broker의 포트번호 입력 
# producer가 통신할 brokers 입력 

topicName = "first-cluster-topic"

producer = KafkaProducer(bootstrap_servers = brokers)
producer.send(topicName, b"Hello Kafka Cluster")
# message 전송 
producer.flush()
# 버퍼 클리어 
