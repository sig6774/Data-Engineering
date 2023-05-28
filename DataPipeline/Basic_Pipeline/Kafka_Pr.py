# 다른 Server에서 진행 

from kafka import KafkaProducer
from json import dumps
import time
import json
import pandas as pd

server = "server_ip"


df = pd.read_csv(".//test6.csv")[:200]

producer = KafkaProducer(
        bootstrap_servers = [server],
        value_serializer = lambda x: json.dumps(x).encode("utf-8")
        )
# print(df.head())
for i in range(len(df)):
    message = {
            "passenger_count" : df["passenger_count"][i],
            "trip_distance" : df["trip_distance"][i],
            "tip_amount" : df["tip_amount"][i],
            "congestion_surcharge" : df["congestion_surcharge"][i],
            "improvement_surcharge" : df["improvement_surcharge"][i]
            }
    # print(df["passenger_count"][i])
    producer.send("ml", value=message)
producer.flush()