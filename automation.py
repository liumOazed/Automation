import requests
import json
import urllib3
import pandas as pd
from datetime import datetime
urllib3.disable_warnings()
import calendar
import warnings
import numpy as np



API_URL_BASE = 'https://api.isprimefx.com/api/'
USERNAME = 'operations@alphatrade.com.au'
PASSWORD = 'promptshadeimpose'



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
  

'''Getting Trade Values from day 1 to 28 Only for Spot'''

data = pd.DataFrame()

arr = []

for x in range(1,29):
    params = {'date': '2021-04-'+ str(x), 
    'tradeType': 'Spot',
    'category':'FX',
    'fromDttm':'2021-04-01T00:01:01.001',
    'toDttm': '2021-04-28T20:59:59.999'}
    
    y = '2021-04-'+str(int(x))
    if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
        continue
    
    response = requests.get(API_URL_BASE + 'trades', headers=headers, params=params, verify=False)
    d = json.dumps(response.json(), indent=4)
    df = pd.read_json(d)
    arr.append(df)
   
for i in range(0, len(arr)):
    data = pd.concat([data, arr[i]], ignore_index=True, sort=False)
# print(data.tail)

'''Getting Trade Values from day 1 to 28: INDEXSWAP'''

df_i = pd.DataFrame()

lis = []

for x in range(1,29):
    params = {'date': '2021-04-'+ str(x), 
    'tradeType': 'Spot',
    'category':'IndexSwap'}
    
    y = '2021-04-'+str(int(x))
    if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
        continue
    
    response = requests.get(API_URL_BASE + 'trades', headers=headers, params=params, verify=False)
    d = json.dumps(response.json(), indent=4)
    df = pd.read_json(d)
    lis.append(df)
   
for i in range(0, len(lis)):
    df_i = pd.concat([df_i, lis[i]], ignore_index=True, sort=False)


'''Getting eod/Marks values for day 1 to 28'''

df_marks = pd.DataFrame()

marks = []

for x in range(1,29):
    params = {'date': '2021-04-'+ str(x)}
    y = '2021-04-'+str(int(x))

    if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
        continue
    
    response = requests.get(API_URL_BASE + 'marks', headers=headers, params=params, verify=False)
    e = json.dumps(response.json(), indent=4)
    df3 = pd.read_json(e)
    marks.append(df3)

for i in range(0, len(marks)):
    df_marks = pd.concat([df_marks, marks[i]], ignore_index=True, sort=False)
    
#print(df3.tail())

#print(df_i.tail())

merged = pd.concat([data, df_i])
# print(merged.shape)
# print(merged.tail())
df2 = merged[['tradeType', 'category', 'clientReference', 'accountNo', 'tradeDate', 'valueDate', 'tradeDttm', 'instrumentName', 'instrument', 'tradedCurrency', 'tradedQuantity', 'baseQuantity', 'contraQuantity','price', 'side', 'cancelled']]
df2['Base Absolute Amount']= df2['tradedQuantity'].abs()
df2['Contra Absolute Amount']= df2['contraQuantity'].abs()
#df2.to_csv("April_trade.csv", index= False) 
#df2.to_excel("test_trade2.xlsx", index= False) 
df2["Contract Absolute Amount"] = df2['price'] * df2['Base Absolute Amount'] * 10
df2['tradeDate'] =  pd.to_datetime(df2['tradeDate'], format='%Y-%m-%d')
df_marks['date'] =  pd.to_datetime(df_marks['date'], format='%Y-%m-%d')
df_marks = df_marks.rename(columns={'date':'tradeDate', 'price':'eodPrice'})
df2 = df2.rename(columns={'price':'tradedPrice'})
volume = pd.merge(df_marks, df2, how="inner", on=["tradeDate","instrument"])
#print(volume.shape)
#olume.to_excel("test_vol.xlsx", index= False) 
#volume = volume.drop(volume.columns[[ 1,11,18,20]], axis=1)
volume.to_excel("test_vol.xlsx", index= False) 
#print(volume.columns)
volume2 = pd.pivot_table(volume, index=['instrument'],values=['eodPrice'])
volume3 = pd.pivot_table(volume, index=['instrument'],values=['Base Absolute Amount', 'Contract Absolute Amount'],aggfunc=np.sum)
volume_eod = pd.merge(volume2, volume3, how="inner", on=["instrument"])
#volume_eod.head()
#volume_eod.to_excel('eod.xlsx')
def custom_instrument(x):
    if "/" in x:
        vals = x.split("/")
        if vals[0] == "USD" or vals[1] == "USD":
            return 1
        else:
            return 0
    if "." in x:
        return 1
vol_eod_ri = volume_eod.reset_index()
vol_eod_ri["eodValue"] = vol_eod_ri["instrument"].apply(lambda x: custom_instrument(x))
vol_eod_ri['eodValue'] = np.where(vol_eod_ri['eodValue'] == 0, vol_eod_ri['eodPrice'], vol_eod_ri['eodValue'])
vol_eod_ri['IN_USD'] = vol_eod_ri['Base Absolute Amount'] * vol_eod_ri['eodValue']
#Agg_Vol = vol_eod_ri.drop(vol_eod_ri.columns[[ 1,11,18,20]], axis=1)
vol_eod_ri.to_excel('Agg_Volume.xlsx', index=False)