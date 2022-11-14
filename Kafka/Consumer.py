from kafka import KafkaConsumer

consumer = KafkaConsumer('firsttopic', bootstrap_servers = ['localhost:9092'])
# list로 서버가 들어감 

for mes in consumer:
    print(mes)
# consumer로 메시지가 도착할 때마다 loop가 돌게 됨 