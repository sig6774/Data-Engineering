from kafka import KafkaConsumer, KafkaProducer
import json 
PAYMENT_TOPIC = "payments"
FRAUD_TOPIC = "fraud_payments"
LEGIT_TOPIC = "legit_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]


consumer = KafkaConsumer(PAYMENT_TOPIC, bootstrap_servers = brokers)
# consumer 생성 

producer = KafkaProducer(bootstrap_servers = brokers)

# 데이터가 이상한지 확인하는 함수 
def is_suspicious(transactions):
    if transactions["PAYMENT_TYPE"] == "BITCOIN":
        return True
    else:
        return False

for mes in consumer:
    msg = json.loads(mes.value.decode())
    # producer로 받은 메시지 encoding 
    # {'DATE': '11/14/2022', 'TIME': '22:18:03', 'CARD': 'VISA', 'AMOUNT': 92, 'TO': 'friend'}
    # print(msg["PAYMENT_TYPE"])
    # print(is_suspicious(msg))

    topic = FRAUD_TOPIC if is_suspicious(msg) else LEGIT_TOPIC
    producer.send(topic, json.dumps(msg).encode("utf-8"))
    # msg의 payment_type가 bitcoin이면 Fraud_topic으로 저장되고 아니면 legit_topic으로 저장 
    # producer로 데이터를 보낼 때 python object로 보내면 안되기 때문에 json으로 변환해서 보냄 
    print(topic, is_suspicious(msg), msg["PAYMENT_TYPE"])