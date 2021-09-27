import pandas as pd
import numpy as np
import os

def predict_hist_d(train_data):

    # Exception for IVS_ICN_CD code 99.
    train_data[(train_data["ivs_icn_cd"] == 99)] = 0

    # Exception for IVS_ICN_CD code 09.
    train_data[(train_data["ivs_icn_cd"] == 9)] = 6

    train_data["hist_d"] = (train_data["hold_d"] * 0.5) + (train_data["hold_d"] * (0.1 - train_data["ivs_icn_cd"] * 0.01))
    train_data.hist_d = np.trunc(train_data["hist_d"])

    return train_data
