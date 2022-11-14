from kafka import KafkaConsumer
import json 
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

LEGIT_TOPIC = "legit_payments"
consumer = KafkaConsumer(LEGIT_TOPIC, bootstrap_servers = brokers)
# 비정상 데이터로 탐지된 데이터를 출력하는 consumer 

    # new_data = {
    #     "DATE" : d, 
    #     "TIME" : t, 
    #     "PAYMENT_TYPE" : payment_type,
    #     "AMOUNT" : amount,
    #     "TO" : to
    # }
for mes in consumer:
    msg = json.loads(mes.value.decode())
    to = msg["TO"]
    amount = msg["AMOUNT"]

    if msg["PAYMENT_TYPE"] == "VISA":
        print(f"[VISA] payment to : {to} - {amount}")
    elif msg["PAYMENT_TYPE"] == "MASTERCARD":
        print(f"[MASTERCARD] payment to : {to} - {amount}")
    else:
        print("[ALERT] unable to process payments")