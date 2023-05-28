import pandas as pd
import xgboost
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import explained_variance_score

# 데이터 불러오기 
# train data 
d1 = pd.read_parquet("./yellow_tripdata_2022-01.parquet")
d2 = pd.read_parquet("./yellow_tripdata_2022-02.parquet")
d3 = pd.read_parquet("./yellow_tripdata_2022-03.parquet")
d4 = pd.read_parquet("./yellow_tripdata_2022-04.parquet")
d5 = pd.read_parquet("./yellow_tripdata_2022-05.parquet")

# test data 
test6 = pd.read_parquet("./yellow_tripdata_2022-06.parquet")

total_df = pd.concat([d1,d2,d3,d4,d5])

'''
사용 column
X : passenger_count, trip_distance, tip_amount, congestion_surcharge, improvement_surcharge
y : total_amount
'''

# 사용 컬럼만 추출 
using_col_df = total_df[["passenger_count", "trip_distance", "tip_amount", "congestion_surcharge", "improvement_surcharge", "total_amount"]]
using_col_test = test6[["passenger_count", "trip_distance", "tip_amount", "congestion_surcharge", "improvement_surcharge", "total_amount"]]

# 저장 
using_col_df.to_csv("total_1-5.csv", encoding= "utf-8", index=False)
using_col_test.to_csv("test6.csv", encoding="utf-8", index=False)

# 데이터 불러오기 
df = pd.read_csv("./total_1-5.csv")

X = df[["passenger_count", "trip_distance", "tip_amount", "congestion_surcharge", "improvement_surcharge"]]
y = df["total_amount"]

# 데이터 분리 
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.3)

# 모델 정의
xgb_model = xgboost.XGBRegressor(n_estimators=50, learning_rate=0.08, gamma=0, subsample=0.75,
                           colsample_bytree=1, max_depth=7)

# 학습 
xgb_model.fit(X_train,y_train)

# 학습 모델 저장 
filename = 'xgb_model.model'
xgb_model.save_model(filename)

# 테스트 
import numpy as np
Xnew = [1.0, 1.39, 0.00, 2.5, 0.3]
Xnew = np.array(Xnew).reshape((1,-1))

new_xgb_model = xgboost.XGBRegressor() # 모델 초기화
new_xgb_model.load_model(filename)

new_xgb_model.predict(Xnew)