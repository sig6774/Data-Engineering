from airflow import DAG 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date' : datetime(2021,1,1)
}

# 아까 작성했던 preprocess, tune Hyper param, Train 기능을 가진 파이썬 파일을  여기서 Dag로 한번에 진행 

# dag 생성 
with DAG(dag_id='taxi-price-pipeline',
        schedule_interval="@daily",
        default_args=default_args,
        tags=["spark"],
        catchup=False) as dag:

    # preprocessing 
    preprocess = SparkSubmitOperator(
        application="/Users/sig6774/Desktop/Data_Engineering/data-engineering-main/02-airflow/preprocess_moon.py",
        # preprocess 파일의 위치를 지정 
        task_id = "preprocess",
        conn_id="spark_local"
    )
    

    # TuneHyper Param 
    tune_hyperparam = SparkSubmitOperator(
        application="/Users/sig6774/Desktop/Data_Engineering/data-engineering-main/02-airflow/tune_hyperparam_moon.py",
        # tune_hyperparam_moon 파일의 위치를 지정 
        task_id = "tune_hyperparam",
        conn_id="spark_local"
    )

    # Train Model
    train = SparkSubmitOperator(
        application="/Users/sig6774/Desktop/Data_Engineering/data-engineering-main/02-airflow/train_model_moon.py",
        # train_model_moon 파일의 위치를 지정 
        task_id = "train",
        conn_id="spark_local"
    )    

    # 각 기능을 하는 파일을 만들어주고 그것을 airflow에서 확인하기 위해 위의 과정을 진행하는 듯 

    # 의존성 주입 
    preprocess >> tune_hyperparam >> train
    
    # 해당 기능을 각각 만들어 놓고 그 기능들이 잘 돌아가는지 확인하는 것이 airflow를 사용하는 목적이네
