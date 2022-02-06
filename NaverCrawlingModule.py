import requests
from bs4 import BeautifulSoup
import pandas as pd

# Add header in request for blocking from python crawling
HEADER = { 
'User-Agent' : ('Mozilla/5.0 (Windows NT 10.0;Win64; x64)\
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98\
Safari/537.36'), } 

def __find_max_page(res) :
    if res.status_code == 200 :
        soup = BeautifulSoup(res.text, 'html.parser')
        last_link = soup.find('table', { 'class' : 'Nnavi' }).find('td', { 'class' : 'pgRR' }).find(href=True)
        return last_link['href'].split('page=')[1]
    else :
        print(res.status_code, "ERROR in making soup")

def __get_url(code: int, page: int) :
    return f"http://finance.naver.com/item/sise_day.naver?code={code}&page={page}"

def get_stock_data(stock_code, fetch_pages, company) : 
    if fetch_pages == -1 :
        max_page = int(__find_max_page(requests.get(__get_url(stock_code, 1), headers=HEADER)))
    else :
        max_page = fetch_pages

    df = pd.DataFrame()
    print(f"get_stock_data START :: {company}, total page = {max_page}")
    
    for i in range(1, max_page+1, 1) :
        res = requests.get(__get_url(stock_code, i), headers=HEADER)
        df = pd.concat([df, pd.read_html(res.text, header=0)[0]])

    df = df.rename(columns={'날짜':'date', '종가':'close', '전일비':'diff', '시가':'open', '고가':'high', '저가':
    'low', '거래량':'volume'})
    df['date'] = df['date'].replace('.', '-')
    df = df.dropna()
    df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)
    df = df[['date', 'open', 'high', 'low', 'close', 'diff','volume']]

    return df
    
    