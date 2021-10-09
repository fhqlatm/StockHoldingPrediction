import pandas as pd
pd.set_option("display.max_row", 100)
pd.set_option("display.max_column", 100)
import numpy as np
import os

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error as mse
from sklearn.model_selection import train_test_split, StratifiedKFold, KFold
from lightgbm import LGBMRegressor


cus = pd.read_csv("open/cus_info.csv")
iem = pd.read_csv("open/iem_info.csv")
hist = pd.read_csv("open/stk_bnc_hist.csv")
train = pd.read_csv("open/stk_hld_train.csv")
test = pd.read_csv("open/stk_hld_test.csv")

train2=train[['iem_cd','hold_d']]
train3=train[['act_id','hold_d']]

train21=train2.groupby('iem_cd')['hold_d'].agg(**{'iem_mean_hold_d':'mean'}).reset_index()
train22=train2.groupby('iem_cd')['hold_d'].agg(**{'iem_max_hold_d':'max'}).reset_index()
train23=train2.groupby('iem_cd')['hold_d'].agg(**{'iem_median_hold_d':'median'}).reset_index()
train31=train3.groupby('act_id')['hold_d'].agg(**{'act_mean_hold_d':'mean'}).reset_index()
train32=train3.groupby('act_id')['hold_d'].agg(**{'act_max_hold_d':'max'}).reset_index()
train33=train3.groupby('act_id')['hold_d'].agg(**{'act_median_hold_d':'median'}).reset_index()
iem=iem.merge(train21,how='outer',on=['iem_cd'])
iem=iem.merge(train22,how='outer',on=['iem_cd'])
iem=iem.merge(train23,how='outer',on=['iem_cd'])
cus = cus.merge(train31,on=['act_id'])
cus = cus.merge(train32,on=['act_id'])
cus = cus.merge(train33,on=['act_id'])
submission = pd.read_csv("open/sample_submission.csv")

train["hist_d"] = train["hold_d"]*0.45
train.hist_d = np.trunc(train["hist_d"])

train.head(3)

train_data = pd.merge(train, cus, how = "left", on = ["act_id"])
train_data = pd.merge(train_data, iem, how = "left", on = ["iem_cd"])

test_data = pd.merge(test, cus, how = "left", on = ["act_id"])
test_data = pd.merge(test_data, iem, how = "left", on = ["iem_cd"])
train_data.head(3)
# train_data에서 Y값을 추출한 후 hold_d column을 지워주겠습니다.

train_label = train_data["hold_d"]
train_data.drop(["hold_d"], axis = 1, inplace = True)

hist["stk_p"] = hist["tot_aet_amt"] / hist["bnc_qty"]
hist = hist.fillna(0)

train_data = pd.merge(train_data, hist, how = "left", on = ["act_id", "iem_cd"])
train_data = train_data[(train_data["byn_dt"] == train_data["bse_dt"])]
train_data.reset_index(drop = True, inplace = True)

test_data = pd.merge(test_data, hist, how = "left", on = ["act_id", "iem_cd"])
test_data = test_data[(test_data["byn_dt"] == test_data["bse_dt"])]
test_data.reset_index(drop = True, inplace = True)

train_data = train_data.drop(["act_id", "iem_cd", "byn_dt", "bse_dt","stk_par_pr","tot_aet_amt","stk_p","bnc_qty"], axis = 1)
test_data = test_data.drop(["act_id", "iem_cd", "byn_dt", "submit_id", "hold_d", "bse_dt","stk_par_pr","tot_aet_amt","stk_p","bnc_qty"], axis = 1)

L_encoder = LabelEncoder()
L_encoder.fit(iem["iem_krl_nm"])
train_data["iem_krl_nm"] = L_encoder.transform(train_data["iem_krl_nm"])
test_data["iem_krl_nm"] = L_encoder.transform(test_data["iem_krl_nm"])

train_data.head(3)
train_data.reset_index(drop = True, inplace=True)
train_label.reset_index(drop = True, inplace=True)


models = []

folds = KFold(n_splits=10)
for train_idx, val_idx in folds.split(train_data):
    
    train_x = train_data.iloc[train_idx, :]
    train_y = train_label[train_idx]
    val_x = train_data.iloc[val_idx, :]
    val_y = train_label[val_idx]
    
    model = LGBMRegressor(objective= "regression",
                          max_depth= 5,
                          n_estimators= 2000,
                          learning_rate= 0.01,
                          num_leaves = 31)
    
    model.fit(train_x, train_y,
              eval_set=[(val_x, val_y)],
              eval_metric=["rmse"],
              early_stopping_rounds=300,
              verbose=500)
    
    models.append(model)

    
result = []
for i in models:
    result.append(i.predict(test_data))
predict = np.mean(result, axis = 0)
predict

submission["hold_d"] = np.round(predict)

submission.loc[submission.hold_d-test.hist_d>146,'hold_d']=test.hist_d+146
submission.to_csv("dacon_baseline477.csv", index = False)
