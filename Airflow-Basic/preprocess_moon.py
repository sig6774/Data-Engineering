from email import header
from pyspark.sql import SparkSession

MAX_MEMORY = "5g"

def patch_pyspark_accumulators():
    from inspect import getsource
    import pyspark.accumulators as pa
    exec(getsource(pa._start_update_server).replace("localhost", "127.0.0.1"), pa.__dict__)

patch_pyspark_accumulators()

spark = SparkSession.builder.appName("taxi-fare-prediction")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()
# 세션 설정 

t_file = "/Users/sig6774/Desktop/Data_Engineering/data-engineering-main/02-airflow/data/trips/*"
# 파일 경로 지정 
df = spark.read.csv(f"file:///{t_file}", inferSchema=True, header=True)
df.createOrReplaceTempView("trips")
# sql을 사용하기 위해 df를 trips라는 이름으로 사용(임시)

query = """

SELECT 
    passenger_count, 
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id, 
    trip_distance, 
    HOUR(tpep_pickup_datetime) as pickup_time,
    DATE_FORMAT(TO_DATE(tpep_pickup_datetime), "EEEE") as day_of_week, 
    total_amount

FROM 
    trips 

WHERE 
    total_amount < 5000
    AND total_amount > 0 
    AND trip_distance > 0 
    AND trip_distance < 500 
    AND passenger_count < 4 
    AND TO_DATE(tpep_pickup_datetime) >= '2021-01-01'
    AND TO_DATE(tpep_pickup_datetime) < '2021-08-01'
"""
# 온전한 데이터만 가져오기 위한 정제 과정 

date_df = spark.sql(query)
# 해당 쿼리를 실행 

train_df, test_df = date_df.randomSplit([0.8, 0.2], seed=410)
# 실행한 내용을 바탕으로 데이터 분리 

data_dir = "/Users/sig6774/Desktop/Data_Engineering/data-engineering-main/02-airflow/data/"
train_df.write.format("parquet").mode("overwrite").save(f"{data_dir}/train/")
test_df.write.format("parquet").mode("overwrite").save(f"{data_dir}/test/")
# 데이터 파이프라인이 여러번 돌 것임으로 overwrite로 계속 갱신되도록 진행 