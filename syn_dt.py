import pandas as pd
from datetime import datetime, timedelta
import os
datetodate=[]
date_date = pd.date_range('2016-01-01','2021-10-13',freq='D')
date_date=date_date[date_date.weekday<=4]


date_date=date_date.strftime('%Y%m%d')

df = pd.DataFrame(date_date,columns=['Date'],index=range(0,len(date_date)))

holiday=['20160101', '20160208','20160209','20160210','20160301','20160413','20160505','20160506','20160606','20160815','20160914','20160915','20160916','20161003','20161230']\
         +['20170127','20170130','20170301','20170501','20170503','20170505','20170509','20170606','20170815','20171002','20171003','20171004','20171005','20171006','20171009','20171225']\
         +['20180101','20180215','20180216','20180301','20180501','20180507','20180522','20180606','20180613','20180815','20180924','20180925','20180926','20181003','20181009','20181225']\
         +['20190101','20190204','20190205','20190206','20190301','20190501','20190506','20190606','20190815','20190912','20190913','20191003','20191009','20191225','20191231']\
         +['20200101','20200124','20200127','20200415','20200430','20200501','20200505','20200817','20200930','20201001','20201002','20201009','20201225','20201231']\
         +['20210101','20210211','20210212','20210301','20210301','20210505','20210519']

df=df[df.isin(holiday)==False]
df=df.dropna()
df=df.reset_index(drop=True)

cus= pd.read_csv("open/cus_info.csv")
iem = pd.read_csv("open/iem_info.csv")
hist = pd.read_csv("open/stk_bnc_hist.csv")
train = pd.read_csv("open/stk_hld_train.csv")
test = pd.read_csv("open/stk_hld_test.csv")

train['byn_dt']=train['byn_dt'].astype('str')

syn_dtt=pd.DataFrame(columns=['syn_dt'])

for i in range(len(train)):
    j=df.index[df['Date'].str.contains(train.byn_dt[i])].tolist()
    valuee=df.Date[j[0]+train.hold_d[i]-1]
    syn_dtt = syn_dtt.append(pd.DataFrame([[valuee]], columns=['syn_dt']), ignore_index=True)
    print(i)
train['syn_dt']=syn_dtt['syn_dt']
train.to_csv("syn_dt.csv", index = False)
#train['syn_dt'].
