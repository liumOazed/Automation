import requests
import json
import urllib3
import pandas as pd
from datetime import datetime
urllib3.disable_warnings()
import calendar
import warnings
import numpy as np
import mysql.connector as mysql


writer = pd.ExcelWriter('TradeReconALL_APTP702.xlsx', engine='xlsxwriter')

API_URL_BASE = 'https://api.isprimefx.com/api/'
USERNAME = 'APTP702'
PASSWORD = 'stylebutits'



# login and extract the authentication token
response = requests.post(API_URL_BASE + 'login', 
headers={'Content-Type': 'application/json'},
data=json.dumps({'username': USERNAME, 'password': PASSWORD}), verify=False)

token = response.json()['token']
headers = {'X-Token': token}
#print(token)

  
def findDay(date):
    dday = datetime.strptime(date, '%Y-%m-%d').weekday()
    return (calendar.day_name[dday])

'''Getting the february's Value'''
feb = pd.DataFrame()

lis = []

for x in range(26,29):
    params = {'date': '2021-02-'+ str(x), 
    'tradeType':'Spot'}
    # 'fromDttm':'2021-05-'+ str(x)+'T00:01:01.001',
    # 'toDttm': '2021-05-'+str(x)+'T23:59:59.999'}
    
    y = '2021-02-'+str(int(x))
    if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
        continue
    
    response = requests.get(API_URL_BASE + 'trades', headers=headers, params=params, verify=False)
    d = json.dumps(response.json(), indent=4)
    df = pd.read_json(d)
    lis.append(df)
   
for i in range(0, len(lis)):
    feb = pd.concat([feb, lis[i]], ignore_index=True, sort=False)
'''Getting Trade Values from Only for Spot'''

data = pd.DataFrame()
months = {1:32,
          2:29,
          3:32,
          4:31,
          5:32,
          6:31   }

arr = []
mnth = [3,4,5]
for key, value in months.items():
    for i in mnth:
        if i == key:
            for z in range(1,months[key]):
            # print(months[key])
                params = {'date': '2021-'+str(i)+'-'+ str(z), 
                'tradeType':'Spot',
                'category':'FX'}
                # 'fromDttm':'2021-05-'+ str(x)+'T00:01:01.001',
                # 'toDttm': '2021-05-'+str(x)+'T23:59:59.999'}
                
                y = '2021-'+str(i)+'-'+str(z)
                if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
                    continue
                
                response = requests.get(API_URL_BASE + 'trades', headers=headers, params=params, verify=False)
                d = json.dumps(response.json(), indent=4)
                df = pd.read_json(d)
                arr.append(df)
    
for i in range(0, len(arr)):
    data = pd.concat([data, arr[i]], ignore_index=True, sort=False)


lp = pd.concat([ feb, data])
lp.to_excel(writer, sheet_name='ALL-LP-SIDE',index=False)
lp_pi = pd.pivot_table(data, index=['instrument', 'side'],values=['tradedQuantity'],aggfunc=np.sum)
lp_pi = lp_pi.reset_index()
for l in range(0,len(lp_pi.index)+1):
    i = l
lpo = lp_pi.iloc[1:i:2]
lpe = lp_pi.iloc[0:i:2]
lp_m = pd.merge(lpe, lpo, on="instrument")
lp_m['instrument'].replace(to_replace="/", value="", regex=True, inplace=True)
lp_m['tradedQuantity_y'] = lp_m['tradedQuantity_y'].abs()
lp_m.to_excel(writer, sheet_name='LP-PIV', index=False)
'''Taking APTP Values From MT4 Side'''

cnct = mysql.connect(user='Oazed', 
        password='v7H9khMD', host='45.77.239.134', port='3603', database='')

cursor = cnct.cursor()
cursor.execute("""SELECT 
mt4_users.LOGIN,TICKET,mt4_users.GROUP,LEFT(SYMBOL,6),cmd as side,VOLUME/100, open_time as time,OPEN_PRICE as price,"OPEN" as ACTION
FROM reportsrv_foofx.mt4_trades
LEFT join reportsrv_foofx.mt4_users on mt4_trades.login=mt4_users.login
where open_time between '2021-2-26' and '2021-6-1'
and mt4_users.group like 'AZ%'
and mt4_users.group not like '%H%'
and cmd<2
UNION
SELECT 
mt4_users.LOGIN,TICKET,mt4_users.GROUP,LEFT(SYMBOL,6),1-cmd as side,VOLUME/100, close_time as time,CLOSE_PRICE as price,"CLOSE" as ACTION
FROM reportsrv_foofx.mt4_trades
LEFT join reportsrv_foofx.mt4_users on mt4_trades.login=mt4_users.login
where close_time between '2021-2-26' and '2021-6-1'
and mt4_users.group like 'AZ%'
and mt4_users.group not like '%H%'
and cmd<2""")

data = cursor.fetchall()
df = pd.DataFrame(data,columns=['LOGIN', 'TICKET','GROUP','LEFT(SYMBOL,6)','side','VOLUME/100','time','price','ACTION'])
df['buy/sell'] = ["BUY" if x == 0 else "SELL" for x in df['side']]
# df.to_csv('mts_p.csv', index=False)
df= df.rename(columns={'LEFT(SYMBOL,6)': 'instrument', 'buy/sell': 'buyNsell'})

'''Excluding CFDS'''

cfds = ['AUS200', 'GER30.', 'NAS100', 'UK100.', 'US30.i','US500.', 'XTIUSD']
cfd = df[df['instrument'].isin(cfds)].reset_index()
cfd.to_excel(writer, sheet_name='CFDS-MT',index = False)

'''Only FX'''

df= df[df['instrument'].isin(cfds).apply(lambda x: not x)].reset_index()
df = df.drop(['index'], axis = 1)

'''Translating for Gold, Silver & FX'''
def multiplier(x):
    if 'XAGUSD' in x[3]:
        return x[5]* 5000
    if 'XAUUSD' in x[3]:
        return x[5]* 100
    else:
        return x[5] * 100000
df['VOLUME'] = df.apply(multiplier,axis=1)
df[['VOLUME']]= df[['VOLUME']].apply(pd.to_numeric)
df.to_excel(writer, sheet_name='ALL-META-SIDE',index=False)
df_pi = pd.pivot_table(df, index=['instrument', 'buyNsell'],values=['VOLUME'],aggfunc=np.sum)
df_pi = df_pi.reset_index()
for l in range(0,len(df_pi.index)+1):
    i = l
dfo = df_pi.iloc[1:i:2]
dfe = df_pi.iloc[0:i:2]
df_m = pd.merge(dfe, dfo, on="instrument")
df_m[['VOLUME_x', 'VOLUME_y']] = df_m[['VOLUME_x', 'VOLUME_y']].apply(pd.to_numeric)
df_m.to_excel(writer, sheet_name='MT_PIV', index=False)
df_all = pd.merge(lp_m, df_m, how= "left", on="instrument")
df_all.replace(np.nan,0)
'''Checking LP Buy n Sell matching with MT buy n Sell'''
df_all['fill_rate_buy'] = df_all['tradedQuantity_x']- df_all['VOLUME_x']
df_all['fill_rate_sell'] = df_all['tradedQuantity_y'] -  df_all['VOLUME_y']
df_all[['VOLUME_x', 'VOLUME_y', 'fill_rate_buy', 'fill_rate_sell']] = df_all[['VOLUME_x', 'VOLUME_y', 'fill_rate_buy', 'fill_rate_sell']].apply(pd.to_numeric)
df_all.to_excel(writer, sheet_name='ALL-RECON', index=False)
writer.save()