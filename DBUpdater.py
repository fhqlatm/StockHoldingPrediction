import pymysql
import pandas as pd
from datetime import datetime
from threading import Timer
from bs4 import BeautifulSoup
from urllib.request import urlopen
import json
import calendar
import requests



class DBUpdater:
    def __init__(self):
        """생성자: DB연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root', password='dldydrbs1', db='investarr',charset='utf8')

        with self.conn.cursor() as curs:
            sql = """
                create table if not exists company_info (
                    code varchar(20),
                    company varchar(40),
                    last_update date,
                    primary key (code)
                )
            """
            curs.execute(sql)
            sql = """
                create table if not exists daily_price (
                    code varchar(20),
                    date date,
                    open bigint(20),
                    high bigint(20),
                    low bigint(20),
                    close bigint(20),
                    diff bigint(20),
                    volume bigint(20),
                    primary key (code, date)
                )
            """
            curs.execute(sql)
        self.conn.commit()

        self.codes=dict()
        self.update_comp_info()

    def __del__(self):
        """소멸자: DB연결 해제"""
        self.conn.close()
    def read_krx_code(self, company200_pages):
        """KRX로부터 상장기업명 DF로 반환"""
        
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code', '회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx
        
        

       
    def update_comp_info(self):
        """종목코드를 company_info에 업데이트 , 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)

        # codes 딕셔너리에 krx 정보 저장
        for idx in range(len(df)):
            self.codes[df['CODE'].values[idx]] = df['company'].values[idx]

        # 마지막 업데이트 날짜, 오늘 날짜 조회
        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code(100) # 시가총액 구하기 원하는 페이지 수
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} REPLACE INTO company_info VALUES ({code}, {company}, {today})")
                self.conn.commit()
                print('-------------FINISH UPDATE KRX-------------- ')
    def read_naver(self, code, company, pages_to_fetch):
        """네이버에서 주식 시세 읽어서 DF로 반환"""
        try:
            url = f'https://finance.naver.com/item/sise_day.nhn?code={code}'

            # 종목 당 마지막 페이지 숫자 구하기
            with requests.get(url, headers={'User-agent': 'Mozilla/5.0'}) as doc:
                if doc is None:
                    return None
                html = BeautifulSoup(doc.text, "lxml")
                pgrr = html.find('td', class_='pgRR')
                if pgrr is None:
                    return None
                s = str(pgrr.a['href']).split('=')
                last_page = s[-1]

            df = pd.DataFrame()
            pages = min(int(last_page), pages_to_fetch) # 최대 pages_to_fetch를 넘지 않도록 한다.

            # 데이터 스크레이핑
            for page in range(1, int(pages) + 1):
                page_url = '{}&page={}'.format(url, page)
                response_page = requests.get(page_url, headers={'User-agent': 'Mozilla/5.0'}).text
                df = df.append(pd.read_html(response_page)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page, pages), end="\r")

            # df 가공
            df = df.rename(columns={'날짜':'date', '종가':'close', '전일비':'diff', '시가':'open', '고가':'high', '저가':'low', '거래량':'volume'})
            df['date'] = df['date'].replace('.', '-')
              # n/a 제거
            df = df.reset_index(drop=True)  # 인덱스 리셋
            df=df.dropna()            
            df['diff'] = df['close']-df['close'].shift(-1)
            df['diffr']=df['diff']/df['close'].shift(-1)*100
            df= df.iloc[:-1,:]
            df[['close','open','high','low','volume','diffr']]=df[['close','open','high','low','volume','diffr']].astype(int)
            
        except Exception as e:
            print('Exception occured : ', str(e))
            return None

        return df
    def replace_into_db(self, df, num, code, company):
        """네이버에서 가져온 주식 시세 -> DB업데이트"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', '{r.date}', {r.open}, {r.high}, {r.low}, {r.close}, {r.diff}, {r.volume},{r.diffr})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num+1, company, code, len(df)))
    def update_daily_price(self, pages_to_fetch):
        """한 종목씩 주식시세 스크레이핑-> DB업데이트"""
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])
    
    def execute_daily(self):
        """실행 즉시 or 매일 오후 5시에 테이블 업데이트"""
        self.update_comp_info()

        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch':1}
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)

        # 업데이트 시간 구하기
        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]

        # 12월 31일 경우
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year+1, month=1, day=1, hour=17, minute=0, second=0)
        # 9월 30일 경우
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month+1, day=1, hour=17, minute=0, second=0)
        # 5월 24일 경우
        else:
            tmnext = tmnow.replace(day=tmnow.day+1, hour=17, minute=0, second=0)

        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds

        t = Timer(secs, self.execute_daily())
        print("Waiting for next update ({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()
    
if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
