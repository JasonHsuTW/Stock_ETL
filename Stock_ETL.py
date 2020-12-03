
#DB    sa////test
import numpy as np
import requests
import pandas as pd
import datetime
import pyodbc
import pymssql
from sqlalchemy import create_engine

#網頁抓資料
import requests
from bs4 import BeautifulSoup
#import requests.packages.urllib3
requests.packages.urllib3.disable_warnings() #爬蟲時可以讓不必要的錯誤發生

#   http://www.twse.com.tw/exchangeReport/STOCK_DAY?date=20180817&stockNo=2330  取一個月的股價與成交量
def get_stock_history(date, stock_no):
    quotes = []
    url = 'http://www.twse.com.tw/exchangeReport/STOCK_DAY?date=%s&stockNo=%s' % ( date, stock_no)
    r = requests.get(url)
    data = r.json()
    return transform(data['data'])  #進行資料格式轉換

def transform_date(date):
        y, m, d = date.split('/')
        return str(int(y)+1911) + '/' + m  + '/' + d  #民國轉西元
    
def transform_data(data):
    data[0] = datetime.datetime.strptime(transform_date(data[0]), '%Y/%m/%d')
    data[1] = int(data[1].replace(',', ''))  #把千進位的逗點去除
    data[2] = int(data[2].replace(',', ''))
    data[3] = float(data[3].replace(',', ''))
    data[4] = float(data[4].replace(',', ''))
    data[5] = float(data[5].replace(',', ''))
    data[6] = float(data[6].replace(',', ''))
    data[7] = float(0.0 if data[7].replace(',', '') == 'X0.00' else data[7].replace(',', ''))  # +/-/X表示漲/跌/不比價
    data[8] = int(data[8].replace(',', ''))
    return data

def transform(data):
    return [transform_data(d) for d in data]

def create_df(date,stock_no):
    s = pd.DataFrame(get_stock_history(date, stock_no))
    s.columns = ['date', 'shares', 'amount', 'open', 'high', 'low', 'close', 'change', 'turnover']
                #"日期","成交股數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","成交筆數" 
    stock = []
    for i in range(len(s)):
        stock.append(stock_no)
    s['stockno'] = pd.Series(stock ,index=s.index)  #新增股票代碼欄，之後所有股票進入資料表才能知道是哪一張股票
    return s
        
#Get all of stock no (Testing)
def UpdateStockNoList():
    url = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    res = requests.get(url, verify = False)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    table = soup.find("table", {"class" : "h4"})
    i = 0
    j = 1
    for row in table.find_all("tr"):
        stockinfo = pd.DataFrame()
        stockinfo = []
        j = 1
        if i > 10 :
           break
        else:
            for col in row.find_all('td'):
                col.attrs = {}
                print("J" )
                print(j)
                print(col.text)
                if j == 1:
                    print(i)
                    str1 = col.text.split('\u3000')[0]
                    #str2 = col.text.split('\u3000')[1]
                    print(str1)
                    stockinfo.append(str1)
                    
                    #stockinfo.append(str2)
                else:
                    print(i)
                    stockinfo.append(col.text.strip().replace('\u3000', ''))
                #stockinfo.append(col.text.replace('\u3000'))
                j = j + 1
                i = i + 1 
                if len(stockinfo) == 1:
                    pass # title 股票, 上市認購(售)權證, ...
                else:
                    print(stockinfo)
            '''
            stockinfo_data = pd.DataFrame([stockinfo])
            stockinfo_data.columns = ['stockno', 'ISIN_Code', 'Listed_Date', 'Stock_Type', 'Industry', 'CFICode','Remark']
            table_name = "Stock_Information"
            conn = create_engine(GetSQLconn())
            stockinfo_data.to_sql(table_name, conn, index=False, if_exists="append")
            '''
    
#SQL Connet 
def GetSQLconn(): 
    server = 'DESKTOP-38EB22K' 
    sqlid = 'sa'
    sqlpw = 'test'
    dbname = 'STOCK_ETL'
    driver = 'SQL+Server+Native+Client+11.0'
    connstring = 'mssql+pyodbc://%s:%s@%s/%s?driver=%s' % (  sqlid , sqlpw ,server  , dbname , driver)
    return connstring



'''
b_update_stocklist = True
if b_update_stocklist == True:
    UpdateStockNoList()
'''



conn = create_engine(GetSQLconn())
#create_engine('mssql+pyodbc://sa:test@DESKTOP-38EB22K/STOCK_ETL?driver=SQL+Server+Native+Client+11.0')
listDji = ['2330']
x = datetime.datetime.now() 
dateformat ='%s%s%s' % ( x.year , x.month , x.day)
for i in range(len(listDji)):
    result = create_df(dateformat, listDji[i])
    #print(result)
table_name = "STOCK_History"
#Import DB
result.to_sql(table_name, conn, index=False, if_exists="append")



"""畫圖測試
#畫圖
# import matplotlib相關套件
import matplotlib.pyplot as plt
# import字型管理套件
from matplotlib.font_manager import FontProperties
# 指定使用字型和大小

#myfont = FontProperties(fname='C:\Windows\Fonts\Calibri\calibri.ttf', size=40)

# 使用月份當做X軸資料

month = [1,2,3,4,5,6,7,8,9,10,11,12]

# 使用台G電的某年每月收盤價當第一條線的資料

stock_tsmcc = [255,246,247.5,227,224,216.5,246,256,262.5,234,225.5,225.5]

# 使用紅海的某年每月收盤價當第二條線的資料

stock_foxconnn = [92.2,88.1,88.5,82.9,85.7,83.2,83.8,80.5,79.2,78.8,71.9,70.8]

# 設定圖片大小為長15、寬10
plt.figure(figsize=(15,10),dpi=100,linewidth = 2)

# 把資料放進來並指定對應的X軸、Y軸的資料，用方形做標記(s-)，並指定線條顏色為紅色，使用label標記線條含意
plt.plot(month,stock_tsmcc,'s-',color = 'r', label="TSMC")

# 把資料放進來並指定對應的X軸、Y軸的資料 用圓形做標記(o-)，並指定線條顏色為綠色、使用label標記線條含意
plt.plot(month,stock_foxconnn,'o-',color = 'g', label="FOXCONN")

# 設定圖片標題，以及指定字型設定，x代表與圖案最左側的距離，y代表與圖片的距離

plt.title("Python 畫折線圖(Line chart)範例")

# 设置刻度字体大小

plt.xticks(fontsize=20)

plt.yticks(fontsize=20)

# 標示x軸(labelpad代表與圖片的距離)

plt.xlabel("month", fontsize=30, labelpad = 15)

# 標示y軸(labelpad代表與圖片的距離)

plt.ylabel("price", fontsize=30, labelpad = 20)

# 顯示出線條標記位置

plt.legend(loc = "best", fontsize=20)

# 畫出圖片

plt.show()
"""