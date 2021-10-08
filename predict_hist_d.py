import pandas as pd
import numpy as np
import os

def predict_hist_d(train_data):
    
    train_data=train_data[train_data.hold_d>=14] # 상위 25%
    train_data["hist_d"] = train_data["hold_d"] * 0.5
    train_data.loc[train_data.hold_d > 90,'hist_d']= train_data.hist_d * 1.1 # 3개월 이상 장투에 관성

    train_data.hist_d = np.trunc(train_data["hist_d"])

    return train_data
