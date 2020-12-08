'''
股市：stock market
股票：stock
漲：go up / rise
跌：go down / fall / decline / depreciate
暴漲：rise suddenly / jump / boom / soar / skyrocket
暴跌：take a nose dive / collapse / slump / drop sharply
牛市：bull market
熊市：bear market
漲停：limit up
跌停：limit down
減資：capital reduction
增資：capital increase
開盤價：opening price
收盤價：closing price
'''
import time
import random
import numpy as np
import requests
import pandas as pd
import datetime
import pyodbc
import pymssql
from twstock import stock
from lxml import etree
from sqlalchemy import create_engine

#網頁抓資料
import requests
from bs4 import BeautifulSoup
#import requests.packages.urllib3
requests.packages.urllib3.disable_warnings() #爬蟲時可以讓不必要的錯誤發生

#SQL Connet 
def GetSQLconn(): 
    server = 'DESKTOP-38EB22K' 
    sqlid = 'sa'
    sqlpw = 'test'
    dbname = 'STOCK_ETL'
    driver = 'SQL+Server+Native+Client+11.0'
    connstring = 'mssql+pyodbc://%s:%s@%s/%s?driver=%s' % (  sqlid , sqlpw ,server  , dbname , driver)
    return connstring

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
    s.columns = ['date', 'shares', 'amount', 'open', 'high', 'low', 'close', 'change', 'turnover','test']
                #"日期","成交股數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","成交筆數" 
    stock = []
    for i in range(len(s)):
        stock.append(stock_no)
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

import twstock



######上證測試###Start###


#url:https://www.twse.com.tw/exchangeReport/TWT84U?response=json&date=20201204&selectType=ALL%s' % ( date
#TWT84U
def get_all_stock_TWT84U_history(date): #上證
    quotes = []
    url = 'https://www.twse.com.tw/exchangeReport/TWT84U?response=json&date=%s&selectType=ALL' % (date)
    r = requests.get(url)
    data = r.json()
    return transform_data_for_TWT84U(data['data'])  #進行資料格式轉換

def transform_TWT84U(data):
    return [transform_data_for_TWT84U(d) for d in data]

def create_df_TWT84U(date):
#refer to data format 
#https://www.twse.com.tw/exchangeReport/TWT84U?response=json&date=%s&selectType=ALL
#"證券代號","證券名稱","漲停價","開盤競價基準","跌停價","開盤競價基準","收盤價","買進揭示價","賣出揭示價","最近成交日","可否零股交易"
#["0050","元大台灣50","126.90","115.40","103.90","115.40","115.40","115.40","115.45","109.12.03","不可"
    s = pd.DataFrame(get_all_stock_TWT84U_history(date))
    s.columns = ['stockno','stockname','limit_up','opening_price','limit_down','opening_price_2','closing_price','last_bidprice','last_askprice','last_txdate','Odd_lot']
                #"證券代號","證券名稱","漲停價","開盤競價基準","跌停價","開盤競價基準","收盤價","買進揭示價","賣出揭示價","最近成交日","可否零股交易"
    s['datecode'] = date
    return s	
def transform_data_for_TWT84U(data):   
    data[0] = data[0]
    data[1] = data[1]
    data[2] = data[2]
    data[3] = data[3]
    data[4] = data[4]
    data[5] = data[5]
    data[6] = data[6]
    data[7] = data[7]
    data[8] = data[8]
    data[9] = data[9]
    data[10] = data[10]    
    data[11] = data[11] 
    return data    

'''
result = create_df_TWT84U('20201204')
#print(result)
conn = create_engine(GetSQLconn())
table_name = "Stock_History_TWT84U_2"
result.to_sql(table_name, conn, index=False, if_exists="append")
print(result)
'''
######上證測試###End###


######類股測試###Start##
#Sector
def get_all_stock_Sector_history(date,typeno): #類股
    quotes = []
    #url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s&type=01' % (date) #typemo = 類股編號
    url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s&type=%s' % (date,typeno) #typemo = 類股編號
    print(url)
    r = requests.get(url)
    data = r.json()
    return transform_data_for_Sector(data['data1'])  #進行資料格式轉換

def transform_Sector(data,typeno):
    return [transform_data_for_Sector(d) for d in data]

def create_df_Sector(date,typeno):
#refer to data format 
#https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=20201204&type=01
#"證券代號","證券名稱"  ,"成交股數"   ,"成交筆數" ,"成交金額"  ,"開盤價" ,"最高價"  ,"最低價"  ,"收盤價"  ,"漲跌(+/-)"      ,"漲跌價差"   ,"最後揭示買價"   ,"最後揭示買量"   ,"最後揭示賣價"  ,"最後揭示賣量"   ,"本益比"
#"1101B"  ,"台泥乙特"   ,"11,084"    ,"13"      ,"592,297"  ,"53.40"  ,"53.60"  ,"53.10"  ,"53.60"   ,"<p> <\u002fp>"  ,"0.00"      ,"53.10"         ,"1"             ,"53.50"        ,"1"            ,""
    url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s&type=%s' % (date,typeno) #typemo = 類股編號
    r = requests.get(url)
    time.sleep(random.randint(2, 10))
    if(r.text != '{"stat":"很抱歉，沒有符合條件的資料!"}'): 
        try:
            s = pd.DataFrame(get_all_stock_Sector_history(date,typeno))                                                          
            s.columns = ['stockno','stockname','shares_qty','tx_qty','tx_price','opening_price','highest_price','lowest_price','closing_price','up_down','up_down_spread','last_bidprice','last_bidvolume','last_askprice','last_askvolume','PE_ratio']  
            s['datecode'] = date
            s['stocktype'] = typeno
        except:
            s=''
    else:
        s=''
    return s



def transform_data_for_Sector(data):   
    data[0] = data[0]

    return data    


#result = create_df_Sector('20201204')

#print(get_all_stock_Sector_history(20201204))
#抓取類股資訊
def Find_Sector(date):
    i = 1 
    for i in range(1,31): #總共30種類股
        time.sleep(2)
        result = create_df_Sector(date,str(i).zfill(2))
        if len(result) != 0 :
            conn = create_engine(GetSQLconn())
            table_name = "Stock_History_Sector"
            result.to_sql(table_name, conn, index=False, if_exists="append")

######類股測試###End##

import datetime
'''
date = datetime.datetime(2019, 8, 16)
if is_workday(date):
  Find_Sector('20201204')
  print("是工作日")
else:
  print("是休息日")

'''
start='2020-11-10'
end='2020-11-30'
 
datestart=datetime.datetime.strptime(start,'%Y-%m-%d')
dateend=datetime.datetime.strptime(end,'%Y-%m-%d')
 
while datestart<dateend:
    print(datestart) 
    weekno = datestart.weekday()
    if weekno < 5:
        Find_Sector(datestart.strftime('%Y%m%d'))
    datestart+=datetime.timedelta(days=1)

        #print(datestart.strftime('%Y%m%d'))


#print(s.strip().lstrip().rstrip(','))
#print(pd.DataFrame(twstock.codes['0050']).replace(' ', '') )

#result = pd.DataFrame(twstock.codes['0050']) 
#print(result.split(' '))

#from twstock import Stock
#from twstock import BestFourPoint
#stock = Stock('2231')
#bfp = BestFourPoint(stock)

#print(bfp.best_four_point_to_buy())    # 判斷是否為四大買點
#print(bfp.best_four_point_to_sell())   # 判斷是否為四大賣點
#print(bfp.best_four_point())           # 綜合判斷
#listDji = ['2330']
#x = datetime.datetime.now() 
#dateformat ='%s%s%s' % ( x.year , x.month , x.day)
#for i in range(len(listDji)):
#    #result = create_df(dateformat, listDji[i])
#    print(result)


#寫進資料庫
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
'''


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