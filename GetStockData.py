import NaverCrawlingModule as ncm
import UpdateDBModule as gsm
import json, calendar
from datetime import datetime
from threading import Timer

def daily_routine():
    stockCodes = stockCodeUpdater.update_comp_info()
    print(f"Length of stockCodes :: {len(stockCodes)}")
    try:
        with open('config.json', 'r') as in_file:
            config = json.load(in_file)
            fetch_pages = config['fetch_pages']
    except FileNotFoundError:
        with open('config.json', 'w') as out_file:
            fetch_pages = -1
            config = {'fetch_pages': 1}
            json.dump(config, out_file)
    
    for idx, code in enumerate(stockCodes):
        df = ncm.get_stock_data(code, fetch_pages, stockCodes[code])
        if df is None:
            continue
        stockCodeUpdater.replace_stockdata_into_DB(df, idx, code, stockCodes[code])

    tmnow = datetime.now()
    lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
    if tmnow.month == 12 and tmnow.day == lastday:
        tmnext = tmnow.replace(year=tmnow.year+1, month = 1, day = 1, hour=17, minute= 0, second= 0)
    elif tmnow.day == lastday:
        tmnext = tmnow.replace(month = tmnow.month+1, day = 1, hour=17, minute= 0, second= 0)
    else : 
        tmnext = tmnow.replace(day = tmnow.day+1, hour=17, minute= 0, second= 0)
    tmdiff = tmnext - tmnow
    secs = tmdiff.seconds

    t = Timer(secs, daily_routine())
    print("Waiting for next update ({}) ... ".format(tmnext.strftime('%Y-%m-%d %H:%M')))
    t.start()

stockCodeUpdater = gsm.GetStockCodeModule()
daily_routine()