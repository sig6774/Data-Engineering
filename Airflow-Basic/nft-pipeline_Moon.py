import json
from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from pandas import json_normalize
# json을 pandas로 바꿔줌 
from airflow.operators.bash import BashOperator

default_arg = {
    "start_date" : datetime(2021, 1, 1)

}

# 함수 생성 
def _processing_nft(ti):
    assets =  ti.xcom_pull(task_ids=['extract_nft'])
    # extract_nft -> process_nft 로 데이터를 넘겨주기 위해서 어디서 데이터를 가지고 오는지 알려주고 xcom_pull()함수를 통해서 진행할 수 있음 

    if not len(assets):
        raise ValueError("assets is empty")
    # nft 데이터는 assets의 첫번쨰에 있음 
    nft = assets[0]['assets'][0]


    processed_nft = json_normalize({
        "token_id" : nft['token_id'], 
        "name" : nft['name'], 
        "image_url" : nft['image_url']
    })
    # 판다스의 json_normalize()를 통해 pandas 형태로 변환 

    processed_nft.to_csv("/tmp/processed_nft.csv", index=None, header=False)


# skeleton 생성 
with DAG(dag_id="nft-pipeline",
        # dag 이름 
         schedule_interval = "@daily",
         default_args = default_arg,
         tags = ['nft'],
         
         # 태그 지정 
         catchup=False
         # catchup으로 backfill을 설정할 수 있음 
         # 1월 2일에 파이프라인을 끄고 1월 4일에 다시 실행한다면 
            # False로 되어 있을 때에는 1월 4일부터 다시 시작
            # True로 되어 있을 때에는 1월 2일부터 1월 4일까지 모두 catchup을 진행 
            # 즉 이전에 
         ) as dag:

# dag에 대한 여러 설정 
# skeleton을 작성하면 airflow ui에 반영됨

# task 생성  
    creating_table = SqliteOperator(
        task_id = "creating_table",
        sqlite_conn_id = "db_sqlite",
        # connection id 생성 
        # airflow ui에서 생성 가능 admin 탭 -> connections -> +를 눌러서 생성 
        # 생성 방법 : host에 airflow.db가 있는 경로를 넣어줘야함 (/Users/sig6774/airflow/airflow.db), 나머지는 공란 

        
        sql = '''
        CREATE TABLE IF NOT EXISTS nfts(
            token_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            image_url TEXT NOT NULL
        )
        
        '''
    )

    # sensor api 
    # opensea api의 connection을 만들고 해당 api가 존재하는지 확인하는 task 
    is_api_available = HttpSensor(

        # task id 
        task_id = "is_api_available",
        # http_conn_id = "opensea_api",
        # ui에서 connection id 생성 
        # opensea_api가 에러가 나기 때문에 다른 방법으로 진행 
        http_conn_id = "githubcontent_api",

        # endpoint = "api/v1/assets?collection=doodles-official&limit=1"
        # 위의 task와 다르게 host를 다르게 지정해줘야함 
        # opensea api를 사용할 예정이므로 host에 https://api.opensea.io/ 작성 
        endpoint = "keon/data-engineering/main/02-airflow/nftresponse.json"
    )
    # api가 존재하는지 확인 

    # task 생성 
    # api가 존재하는 것은 확인했으니 해당 api로 데이터 불러오기 
    extract_nft = SimpleHttpOperator(
        task_id = "extract_nft", 
        http_conn_id="githubcontent_api", 
        endpoint = "keon/data-engineering/main/02-airflow/nftresponse.json",
        method="GET",

        response_filter=lambda res : json.loads(res.text),
        # endpoint로 부터 받은 데이터를 어떻게 처리할 것인지 알려줌 
        # json.load()는 json으로 되어 있는 데이터를 파이썬에서 사용할 수 있게 해주는 것 
        log_response=True  
        # airflow tasks test nft-pipeline extract_nft 2021-01-01
    )

    # task 생성 
    # extract_nft를 통해 받아온 데이터를 process_nft에서 저장하는 순서 
    # 위의 순서로 데이터를 넘기기 위해서 xcom을 사용해야함 
    # 데이터 가공을 하기 위해 python operators 사용 
    process_nft = PythonOperator(
        task_id = "process_nft", 
        python_callable=_processing_nft, 
    )
    # extract_nft와 process_nft의 task를 processing_nft라는 함수를 통해 데이터를 주고 받을 수 있게 됨 
    # cat /tmp/processed_nft.csv 으로 데이터가 실제로 저장되었는지를 확인할 수 있음 

    # task 생성 
    # 데이터 저장 (bash operator )
    store_nft = BashOperator(
        task_id = "store_nft", 
        # bashOperator는 커맨드 라인에서 입력하는 것처럼 할 수 있음 
        bash_command='echo -e ".separator ","\n.import /tmp/processed_nft.csv nfts" | sqlite3 /Users/sig6774/airflow/airflow.db'
        # bash_command='echo -e ".separator ","\n.import /tmp/processed_user.csv users" | sqlite3 /Users/keon/airflow/airflow.db'

        # 커맨드에서 적는 것을 그대로 작성 
        # /tmp/processed_nft.csv 파일에 있는 값을 .을 기준으로 분리하고 그것을 airflow db에 저장 
    )

    # task 생성 
    # task간 의존성 부여 
    # table이 있어야 store를 할 수 있고 이런 의존성이 있어야 에러가 발생하지 않음 
    # 의존성을 메기기 위해서는 >> 로 의존성 부여 가능 

    creating_table >> is_api_available >> extract_nft >> process_nft >> store_nft

    # dag에 가서 실행을 시키게 되면 해당 내용을 순서대로 진행함 



