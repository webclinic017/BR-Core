import requests
from bs4 import BeautifulSoup

# Add header in request for blocking from python crawling
HEADER = { 
'User-Agent' : ('Mozilla/5.0 (Windows NT 10.0;Win64; x64)\
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98\
Safari/537.36'), } 

indexLabel = []
valueList = []

def __find_max_page(res) :
    if res.status_code == 200 :
        soup = BeautifulSoup(res.text, 'html.parser')
        last_link = soup.find('table', { 'class' : 'Nnavi' }).find('td', { 'class' : 'pgRR' }).find(href=True)
        return last_link['href'].split('page=')[1]
    else :
        print(res.status_code, "ERROR in making soup")

def __get_url(code: int, page: int) :
    return f"http://finance.naver.com/item/sise_day.naver?code={code}&page={page}"

def get_index_label() :
    return indexLabel

def get_stock_data(stock_code) : 
    max_page = int(__find_max_page(requests.get(__get_url(stock_code, 1), headers=HEADER)))
    for i in range(1, max_page+1, 1) :
        res = requests.get(__get_url(stock_code, i), headers=HEADER)
        soup = BeautifulSoup(res.text, 'html.parser')
    
        #For find index
        if(i == 1) :
            index_info = soup.find('table', { 'class' : 'type2' }).find('tr').find_all('th')
            for item in index_info :
                indexLabel.append(item.get_text())

        value_info = soup.find_all('tr', { 'onmouseover': 'mouseOver(this)' })
        for value in value_info :
            index = 0
            value_obj = {}
            for detail in value.find_all('span') :
                value_obj[indexLabel[index]] = detail.get_text()
                index = index+1
            valueList.append(value_obj)
        print(f"Page {i} successfully crawled")
    
    print(valueList)
    return valueList

    