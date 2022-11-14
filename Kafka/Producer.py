from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])
# producer를 인스턴스화 해서 만들 수 있음 
# 보낼 서버를 지정해주면 됨 

producer.send('firsttopic', b"hello world from python")
# 생성한 Producer 인스턴스를 통해 특정 topic에 메시지를 보낼 수 있음 
producer.flush()