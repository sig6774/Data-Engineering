import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pandas import json_normalize
from kafka import KafkaConsumer
from json import loads
import xgboost
import numpy as np
import pandas as pd 
from sklearn.model_selection import train_test_split

'''
1. 데이터 로드 
2. 로드된 데이터로 학습 
3. kafka에서 보낸 데이터로 예측값 도출 후 저장 
'''

default_arg = {
    "start_date" : datetime(2021, 1, 1)

}


filename = './xgb_model.model'
server = "server_ip"
topic_name = "topic_name"

def train_xg():
    df = pd.read_csv("./total_1-5.csv")

    X = df[["passenger_count", "trip_distance", "tip_amount", "congestion_surcharge", "improvement_surcharge"]]
    y = df["total_amount"]

    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.3)

    xgb_model = xgboost.XGBRegressor(n_estimators=50, learning_rate=0.08, gamma=0, subsample=0.75,
                           colsample_bytree=1, max_depth=7)

    xgb_model.fit(X_train,y_train)

    xgb_model.save_model(filename)


def find_predict_values():

    filename = './xgb_model.model'

    new_xgb_model = xgboost.XGBRegressor() # 모델 초기화
    new_xgb_model.load_model(filename)

    consumer = KafkaConsumer(
        topic_name, 
        bootstrap_servers = server, 
        auto_offset_reset="earliest",
        enable_auto_commit = True,
        value_deserializer = lambda x : loads(x.decode('utf-8')),
        consumer_timeout_ms = 10000

    )

    data_list= [] 

    for idx, mes in enumerate(consumer):
        pred_data = {} 
        data = mes.value
        p_count = float(data['passenger_count'])
        t_dist = float(data['trip_distance'])
        t_amount = float(data["tip_amount"])
        c_surcharge = float(data["congestion_surcharge"])
        i_surcharge = float(data["improvement_surcharge"])
        li = [p_count, t_dist, t_amount, c_surcharge, i_surcharge]
        print(li)
        Xnew = np.array(li).reshape((1,-1))
        
        # offset = mes.offset


        pred = new_xgb_model.predict(Xnew)
        
        pred_data = {'idx' : idx, "prediction_value" : pred}
        
        data_list.append(pred_data)

    pred_csv = pd.DataFrame(data_list)
    pred_csv.to_csv("./pred_results.csv", encoding="utf-8", index=False)

with DAG(

    dag_id = "ml-pipeline",
    schedule_interval = "@daily",
    default_args = default_arg,
    catchup = False

) as dag:
		
    train_model = PythonOperator(
        task_id = "train_ml",
        python_callable = train_xg
    )

    ext_model = PythonOperator(
        task_id = "pred",
        python_callable=find_predict_values
    )

		# 의존성 부여 
    train_model >> ext_model