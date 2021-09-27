from Investar import Analyzer
import pandas as pd
import os
import datetime

cus= pd.read_csv("open/cus_info.csv")
iem = pd.read_csv("open/iem_info.csv")
hist = pd.read_csv("open/stk_bnc_hist.csv")
train = pd.read_csv("open/stk_hld_train.csv")
test = pd.read_csv("open/stk_hld_test.csv")
#etf = pd.read_csv("open/etf.csv", encoding = 'cp949')

#etf=etf[['단축코드','한글종목명']]
#etf['한글종목명']=1
#etf=etf.rename(columns={'단축코드':'code','한글종목명':'company'})

is_real=cus['sex_dit_cd']==1
cus=cus[is_real]
#종목에 대해서도 마찬가지로 해볼 수 있을 것.

cus=cus.reset_index(drop=True)




re=pd.merge(train,cus,on='act_id',how ='outer')
re=re.dropna()
re=re.reset_index(drop=True)
#re.hold_d.describe()  마지막에 이걸 입력해주면 mean값이 평균값이다.
