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
                iip21 FLOAT,
                PRIMARY KEY (code,date)      
            )
            """
            curs.execute(sql) 

            sql = """
            CREATE TABLE IF NOT EXISTS signal_bollinger_trend (
                code VARCHAR(20),
                date DATE,
                type VARCHAR(20),
                PRIMARY KEY (code,date)      
            )
            """
            curs.execute(sql) 

            sql = """
            CREATE TABLE IF NOT EXISTS signal_bollinger_reverse (
                code VARCHAR(20),
                date DATE,
                type VARCHAR(20),
                PRIMARY KEY (code,date)      
            )
            """
            curs.execute(sql) 

        self.conn.commit()
        self.codes = dict()
    
    def __del__(self):
        self.conn.close()

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
                        fetch_pages = 1
                except FileNotFoundError:
                    fetch_pages = -1
                
                df = pd.DataFrame()
                if(fetch_pages != 1):
                    twenty_days_ago = datetime.today() - timedelta(days = 45)
                    start_date = twenty_days_ago.strftime('%Y-%m-%d')
                    print(f"Not first time, get before 40 days: {start_date}")
                    df = mk.get_daily_price(code, start_date, None)
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
                        sql = f"REPLACE INTO bollinger_info VALUES ('{code}', '{r.date}', '{r.MA20}', '{r.stddev}','{r.upper}', '{r.lower}', '{r.PB}', '{r.bandwidth}', '{r.MFI10}', '{r.IIP21}')"
                        curs.execute(sql)
                    self.conn.commit()
                    print('[{}] #{} : {} rows > REPLACE INTO init_bollingerdata_into_DB [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), code, len(df)))

                
                for i in range(len(df.close)):
                    if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:
                        signal = "buy"
                        type = "signal_bollinger_trend"
                        with self.conn.cursor() as curs:
                            print(f"@@@@@@@@@@BUY SIGNAL@@@@@@@@ - {code} - {df.date.values[i]} - {type} - {signal}")
                            sql = f"REPLACE INTO {type} VALUES ('{code}', '{df.date.values[i]}', '{signal}')"
                            curs.execute(sql)
                            self.conn.commit()
                    elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:
                        signal = "sell"
                        type = "signal_bollinger_trend"
                        with self.conn.cursor() as curs:
                            print(f"@@@@@@@@@@SELL SIGNAL@@@@@@@@ - {code} - {df.date.values[i]} - {type} - {signal}")
                            sql = f"REPLACE INTO {type} VALUES ('{code}', '{df.date.values[i]}', '{signal}')"
                            curs.execute(sql)
                            self.conn.commit()

                    if df.PB.values[i] < 0.05 and df.IIP21.values[i] > 0:
                        signal = "buy"
                        type = "signal_bollinger_reverse"
                        with self.conn.cursor() as curs:
                            print(f"@@@@@@@@@@BUY SIGNAL@@@@@@@@ - {code} - {df.date.values[i]} - {type} - {signal}")
                            sql = f"REPLACE INTO {type} VALUES ('{code}', '{df.date.values[i]}', '{signal}')"
                            curs.execute(sql)
                            self.conn.commit()
                    elif df.PB.values[i] > 0.95 and df.IIP21.values[i] < 0:
                        signal = "sell"
                        type = "signal_bollinger_reverse"
                        with self.conn.cursor() as curs:
                            print(f"@@@@@@@@@@SELL SIGNAL@@@@@@@@ - {code} - {df.date.values[i]} - {type} - {signal}")
                            sql = f"REPLACE INTO {type} VALUES ('{code}', '{df.date.values[i]}', '{signal}')"
                            curs.execute(sql)
                            self.conn.commit()

a = GetBollingerDataModule().init_bollingerdata_into_DB()
