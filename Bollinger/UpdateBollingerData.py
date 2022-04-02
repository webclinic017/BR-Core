import numpy as np
import pymysql
import pandas as pd
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from GetOptimalPortfolio import GetPriceModule
from GetStockAndUpateDB import UpdateDBModule
import json
from datetime import timedelta, datetime

class GetBollingerDataModule:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', user='root', password='apple10g', db='INVESTAR', charset='utf8')

        with self.conn.cursor() as curs :
            sql = """
            CREATE TABLE IF NOT EXISTS bollinger_info (
                code VARCHAR(20),
                date DATE,
                ma20 FLOAT,
                stddev FLOAT,
                upper BIGINT(20),
                lower BIGINT(20),
                pb FLOAT,
                bandwitdh FLOAT,
                mfi10 FLOAT,
                PRIMARY KEY (code,date)      
            )
            """
            curs.execute(sql) 
        self.conn.commit()
        self.codes = dict()
    
    def __del__(self):
        self.conn.close()

    def replace_bollingerdata_into_DB(self, df, num, code, company):
        print(f"replace_bollingerdata_into_DB START, company :: {company}")
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', '{r.date}', '{r.open}','{r.high}', '{r.low}', '{df.pb}', '{r.diff}', '{r.volume}')"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num+1, company, code, len(df)))

    def init_bollingerdata_into_DB(self):
        print(f"init_bollingerdata_into_DB START")
        with self.conn.cursor() as curs:
            mk = GetPriceModule.MarketDB()
            krx = UpdateDBModule.GetStockCodeModule().read_krx_code()
            for idx in range(len(krx)):
                code = krx.code.values[idx]
                print("Check JSON file existence")
                try:
                    with open('../config.json', 'r') as in_file:
                        config = json.load(in_file)
                        fetch_pages = 1
                except FileNotFoundError:
                    fetch_pages = -1
                
                df = pd.DataFrame()
                if(fetch_pages != 1):
                    twenty_days_ago = datetime.today() - timedelta(days = 45)
                    start_date = twenty_days_ago.strftime('%Y-%m-%d')
                    print(f"Not first time, get before 40 days: {start_date}")
                    df = mk.get_daily_price(code, start_date, None)
                    print(df)
                else: 
                    print(f"First time, get all day")
                    df = mk.get_daily_price(code, None, None)
                
                df['MA20'] = df['close'].rolling(window=20).mean()
                df['stddev'] = df['close'].rolling(window=20).std()
                df['upper'] = df['MA20'] + (df['stddev'] * 2)
                df['lower'] = df['MA20'] - (df['stddev'] * 2)
                df['PB'] = (df['close'] - df['lower']) / (df['upper'] - df['lower'])
                df['bandwidth'] = (df['upper'] - df['lower']) / df['MA20'] * 100

                df['TP'] = (df['high'] + df['low'] + df['close']) / 3
                df['PMF'] = 0
                df['NMF'] = 0

                df['II'] = (2*df['close']-df['high']-df['low']) / (df['high'] - df['low'])*df['volume']
                df['IIP21'] = df['II'].rolling(window=21).sum() / df['volume'].rolling(window=21).sum()*100

                for i in range(len(df.close)-1):
                    if df.TP.values[i] < df.TP.values[i+1]:
                        df.PMF.values[i+1] = df.TP.values[i+1] * df.volume.values[i+1]
                        df.NMF.values[i+1] = 0
                    else:
                        df.NMF.values[i+1] = df.TP.values[i+1] * df.volume.values[i+1]
                        df.PMF.values[i+1] = 0
                df['MFR'] = df.PMF.rolling(window=10).sum() / df.NMF.rolling(window=10).sum()
                df['MFI10'] = 100 - 100/(1 + df['MFR'])
                df = df[19:]

                df = df.replace([np.inf, -np.inf])
                df = df.dropna()
                with self.conn.cursor() as curs:
                    for r in df.itertuples():
                        sql = f"REPLACE INTO bollinger_info VALUES ('{code}', '{r.date}', '{r.MA20}', '{r.stddev}','{r.upper}', '{r.lower}', '{r.PB}', '{r.bandwidth}', '{r.MFI10}')"
                        curs.execute(sql)
                    self.conn.commit()
                    print('[{}] #{} : {} rows > REPLACE INTO init_bollingerdata_into_DB [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), code, len(df)))

            # print('[{}] #{:04d} ({}) : {} rows > REPLACE INTO bollinger_info [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), code, len(df)))

a = GetBollingerDataModule().init_bollingerdata_into_DB()
