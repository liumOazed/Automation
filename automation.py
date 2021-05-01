import requests
import json
import urllib3
import pandas as pd
from datetime import datetime
urllib3.disable_warnings()
import calendar



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

# End of day marks
# for x in range(1,3):
#     params = {'date': '2021-04-'+ str(x)}
#     response = requests.get(API_URL_BASE + 'trades', headers=headers, params=params, verify=False)
#     #d = json.dumps(response.json(), indent=4)
#     #print(json.dumps(response.json(), indent=4))
#     df = pd.read_json(d)
#     x+=1
# print(df.tail())
    
# dfdata = pd.DataFrame()

  
def findDay(date):
    dday = datetime.strptime(date, '%Y-%m-%d').weekday()
    return (calendar.day_name[dday])
  
# print(findDay(date)) 


p = pd.DataFrame()

arr = []

for x in range(1,29):
    params = {'date': '2021-04-'+ str(x)}
    y = '2021-04-'+str(int(x))
    
    if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
        continue
    
    response = requests.get(API_URL_BASE + 'trades', headers=headers, params=params, verify=False)
    d = json.dumps(response.json(), indent=4)
    df = pd.read_json(d)
    arr.append(df)
    
for i in range(0, len(arr)):




    p = pd.concat([p, arr[i]], ignore_index=True, sort=False)
print(p.tail)


df2 = p[['tradeType', 'clientReference', 'accountNo', 'tradeDate', 'valueDate', 'tradeDttm', 'instrumentName', 'instrument', 'tradedCurrency', 'tradedQuantity', 'baseQuantity', 'contraQuantity','price', 'side', 'cancelled']]

#df2.to_csv("April_trade.csv", index= False) 
df2.to_excel("test.xlsx", index= False) 

