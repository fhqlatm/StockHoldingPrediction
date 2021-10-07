import pandas as pd
import numpy as np
import os

def predict_hist_d(train_data):

    train_data["hist_d"] = train_data["hold_d"] * 0.6
    train_data.loc[train_data.hold_d > 14,'hist_d']= train_data.hist_d * 1.1

    train_data.hist_d = np.trunc(train_data["hist_d"])

    return train_data
