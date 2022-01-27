import requests
from bs4 import BeautifulSoup

SAMSUNGCODE = 112040
maxPage = 1

# Add header in request for blocking from python crawling
HEADER = { 
'User-Agent' : ('Mozilla/5.0 (Windows NT 10.0;Win64; x64)\
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98\
Safari/537.36'), } 

indexLabel = []
valueList = []

def __findMaxPage(res) :
    if res.status_code == 200 :
        soup = BeautifulSoup(res.text, 'html.parser')
        last_Link = soup.find('table', { 'class' : 'Nnavi' }).find('td', { 'class' : 'pgRR' }).find(href=True)
        return last_Link['href'].split('page=')[1]
    else :
        print(res.status_code, "ERROR in making soup")

def __getURL(code: int, page: int) :
    return f"http://finance.naver.com/item/sise_day.naver?code={code}&page={page}"

def getIndexLabel() :
    return indexLabel

def getStockData(stockCode) : 
    maxPage = int(__findMaxPage(requests.get(__getURL(SAMSUNGCODE, 1), headers=HEADER)))
    for i in range(1, maxPage+1, 1) :
        res = requests.get(__getURL(SAMSUNGCODE, i), headers=HEADER)
        soup = BeautifulSoup(res.text, 'html.parser')
    
        #For find index
        if(i == 1) :
            indexInfo = soup.find('table', { 'class' : 'type2' }).find('tr').find_all('th')
            for str in indexInfo :
                indexLabel.append(str.get_text())

        valueInfo = soup.find_all('tr', { 'onmouseover': 'mouseOver(this)' })
        for value in valueInfo :
            index = 0
            valueObj = {}
            for detail in value.find_all('span') :
                valueObj[indexLabel[index]] = detail.get_text()
                index = index+1
            valueList.append(valueObj)
        print(f"Page {i} successfully crawled")
    
    print(valueList)
    return valueList

    