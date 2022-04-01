import pandas as pd
import pymysql
from datetime import datetime
from datetime import timedelta
import re

class MarketDB:
    def __init__(self):
        self.conn = pymysql.connect(host = 'localhost', user = 'root', password='apple10g', db = 'INVESTAR', charset = 'utf8')
        self.codes ={}
        self.get_comp_info()

    def __del__(self):
        self.conn.close()

    def get_comp_info(self):
        sql = "SELECT * FROM company_info"
        krx = pd.read_sql(sql, self.conn)
        for idx in range(len(krx)):
            self.codes[krx['code'].values[idx]] = krx['company'].values[idx]
    
    def get_daily_price(self, code, start_date = None, end_date = None):
        if(start_date is None):
            three_year_ago = datetime.today() - timedelta(days = 1095)
            start_date = three_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to  '{}'".format(start_date))
        else:
            start_lst = re.split('\D+', start_date)
            if (start_lst[0] ==''):
                start_lst = start_lst[1:]
            start_year = int(start_lst[0])
            start_month = int(start_lst[1])
            start_day = int(start_lst[2])
            if start_year < 1900 or start_year > 2200:
                print("start_year is wrong")
                return
            if start_month < 1 or start_month > 12:
                print("start_month is wrong")
                return
            if start_day <1 or start_day > 31:
                print("start_day is wrong")
                return
            start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"
    
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            print(f"end_date is initialized to {end_date}")
        else :
            end_lst = re.split('\D+', end_date)
            if (end_lst[0] ==''):
                end_lst = end_lst[1:]
            end_year = int(end_lst[0])
            end_month = int(end_lst[1])
            end_day = int(end_lst[2])
            if end_year < 1900 or end_year > 2200:
                print("end_year is wrong")
                return
            if end_month < 1 or end_month > 12:
                print("end_month is wrong")
                return
            if end_day <1 or end_day > 31:
                print("end_day is wrong")
                return
            end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"

        codes_keys = list(self.codes.keys())
        codes_values = list(self.codes.values())
        if code in codes_keys:
            pass
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print("ValueError ::: Code({}) doesn't exist.".format(code))

        sql = f"SELECT * FROM daily_price WHERE code = '{code}' and date >= '{start_date}' and date <= '{end_date}'"
        df = pd.read_sql(sql, self.conn)
        df.index = df['date']
        return df
    