from datetime import datetime
from airflow import DAG 
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'start_date' : datetime(2021,1,1)
}

with DAG(dag_id='spark-example',
         schedule_interval='@daily',
         default_args=default_args,
        # 기초 설정 

         tags=['spark'],
                  # 태그 이름 

         catchup=False) as dag:         

    # sql_jobs = SparkSqlOperator(sql="SELECT * FROM foobar", master="local", task_id="sql_job")
    # airflow에서는 무거운 작업을 돌리면 안되기 때문에 위의 형식으로 하는 것을 지양해야함 

    # spark submit operator 
    # /Users/sig6774/Desktop/Data_Engineering/data-engineering-main/01-spark/count_trips_sql.py 내용을 가져옴 
    # 귀찮아서 해당 폴더에 같은 내용의 파일을 생성 
    submit_job = SparkSubmitOperator(
        application="/Users/sig6774/airflow/dags/count_trips_sql_moon.py",
        # spark를 활용하여 sql 작업에 대한 내용이 있는 경로 지정 
        task_id="submit_job",
        conn_id="spark_local"
        # connection id를 만들어줌 (airflow에서 만듬 )
    )
    # airflow tasks test spark-example submit_job 2021-01-01
    # 해당 내용으로 테스트 진행 
    # show로 데이터가 보여지기만 하기 때문에 따로 airflow에서 sucess가 되지 않음 
    # 잘 실행이 되는 것을 확인했으니 dag에서 실행해도 볼 수 있음 



