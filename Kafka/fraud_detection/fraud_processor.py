from kafka import KafkaConsumer
import json 
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

FRAUD_TOPIC = "fraud_payments"
consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers = brokers)
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
    if msg["TO"] == "stranger":
        print(f"[ALERT] fraud detected payment to : {to} - {amount}")
        # 비정상 거래로 탐지된 데이터 중 stranger인 사람이 있으면 alert을 보냄 
    else:
        print(f"[PROCESSING BITCOIN] payment to: {to} - {amount}")