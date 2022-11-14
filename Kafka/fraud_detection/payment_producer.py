from kafka import KafkaProducer
import datetime
import pytz
import time 
import random 
import json 

TOPIC_NAME = "payments"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producer = KafkaProducer(bootstrap_servers = brokers)
# producer 생성 완료 

# time 데이터 생성 함수 
def get_time_date():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))
    # 현재 내가 있는 곳의 time으로 변경 

    d = kst_now.strftime("%m/%d/%Y")
    t = kst_now.strftime("%H:%M:%S")

    return d, t 

# print(get_time_date())

# payment 데이터 생성 함수 
def generate_payment_data():

    payment_type = random.choice(["VISA", "MASTERCARD", "BITCOIN"])

    amount = random.randint(0, 100)
    # random 값 

    to = random.choice(["me", "mom", "dad", "friend", "stranger"])
    return payment_type, amount, to

while True:
    d, t = get_time_date()

    payment_type, amount, to = generate_payment_data()

    new_data = {
        "DATE" : d, 
        "TIME" : t, 
        "PAYMENT_TYPE" : payment_type,
        "AMOUNT" : amount,
        "TO" : to
    }
    # dict 형태로 가상의 데이터를 보냄 

    print(new_data)
    producer.send(TOPIC_NAME, json.dumps(new_data).encode("utf-8"))
    # python object를 그대로 보낼 수 없기 때문에 json으로 변환하고 utf-8로 인코딩 진행 

    time.sleep(1)
    # 1초씩 데이터를 보내기 위해 사용 
