import requests
import json
import urllib3
import pandas as pd
from datetime import datetime
urllib3.disable_warnings()
import calendar
import warnings
import numpy as np

writer = pd.ExcelWriter('monthly_june.xlsx', engine='xlsxwriter')

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

for x in range(1,12):
    params = {'date': '2021-06-'+ str(x), 
    'tradeType':'Spot',
    'category':'FX'}
    
    y = '2021-06-'+str(int(x))
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

for x in range(1,12):
    params = {'date': '2021-06-'+ str(x),
    'tradeType':'Spot', 
    'category':'IndexSwap'}
    
    y = '2021-06-'+str(int(x))
    if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
        continue
    
    response = requests.get(API_URL_BASE + 'trades', headers=headers, params=params, verify=False)
    d = json.dumps(response.json(), indent=4)
    df = pd.read_json(d)
    lis.append(df)
   
for i in range(0, len(lis)):
    df_i = pd.concat([df_i, lis[i]], ignore_index=True, sort=False)


'''Getting eod Price of day 28'''

df_marks = pd.DataFrame()

marks = []

for x in range(11,12):
    params = {'date': '2021-06-'+ str(x)}
    y = '2021-06-'+str(int(x))

    if (findDay(y) == 'Saturday') or (findDay(y) == 'Sunday'):
        continue
    
    response = requests.get(API_URL_BASE + 'marks', headers=headers, params=params, verify=False)
    e = json.dumps(response.json(), indent=4)
    df3 = pd.read_json(e)
    marks.append(df3)

for i in range(0, len(marks)):
    df_marks = pd.concat([df_marks, marks[i]], ignore_index=True, sort=False)
df_marks = df_marks.rename(columns={'price':'eodPrice'})
df_marks.to_excel(writer, sheet_name='Sheet1')
# df_marks.to_csv('eod_may.csv')
'''Merging IndexSwap and Spot'''
merged = pd.concat([data, df_i])

df2 = merged[['tradeType', 'category', 'clientReference', 'accountNo', 'tradeDate', 'valueDate', 'tradeDttm', 'instrumentName', 'instrument', 'tradedCurrency', 'tradedQuantity', 'baseQuantity', 'contraQuantity','price', 'side', 'cancelled']]

'''Taking the absolute value of the Base value and Contract value '''

df2['Base Absolute Amount']= df2['tradedQuantity'].abs()
df2['Contra Absolute Amount']= df2['contraQuantity'].abs()
#df2.to_csv("April_trade.csv", index= False)
instrument_dict = { 'IDX':{
                            'US': 10,
                            'DE':10,
                            'JP':100,
                            'UK':10,
                            'AU':10,
                            'HKD':10},
                    'SPT':{'CO':10}
                  }


def cust_contract(x):
    placeholder = 0
    placebo = 0
    if '.' in x[0]:
        cols = x[0].split('.')
        if cols[0].startswith('SPT'):
            placebo+= x[1] * x[2] * instrument_dict[cols[0]][cols[1]]
            return placebo
        else:
            placeholder += x[1] * (x[2]/ instrument_dict[cols[0]][cols[1]])
            return placeholder
    else:
        return x[3]
df2['Contract'] = df2[['instrument', 'price','Base Absolute Amount', 'Contra Absolute Amount']].apply(cust_contract, axis = 1) 
df2.to_excel(writer, sheet_name= "Sheet2", index= False) 
# df_pi = pd.pivot_table(df2, index=['instrument'],values=['Base Absolute Amount','Contract'],aggfunc=np.sum)
# df_pi = df_pi.reset_index()
# merged_eod = pd.merge(df_marks, df_pi, how="inner", on=["instrument"])
# merged_eod=merged_eod.loc[:, ~merged_eod.columns.str.match('Unnamed')]
'''PriceDIct contains eod values of day 28'''
priceDict={}
for _, row in df_marks.iterrows():
    priceDict[row["instrument"]] = row["eodPrice"]
# print(priceDict)
countryDict = {
    "IDX": {
       (0, "AU"): "AUD",
        (0, "EU", "FR", "DE", "ES"): "EUR",
        (0, "HK"): "HKD",
        (0, "JP"): "JPY",
        (0, "UK"): "GBP",
        (0, "US"): "USD",
    },
    "SPT": {
        "CO": { 
            ("UK", "US"): "USD"
        }
    },
    "IXC": {
        "CO": {
            ("US", "UK"): "USD"
        }
    }
}
'''Here We are calculating eod price. the function will look for LHS/RHS not equal USD
if not then look for LHS/USD values in priceDict and return else return 1/(USD/RHS)
    if LHS == USD or RHS == USD then return 1 and if there are CFDS then return 1'''
def determine_price(x):
    if "/" in x[0]:  
        instrument = x[0].split("/")
        if instrument[0] != "USD" and instrument[1] != "USD":
            if (instrument[0]+"/USD") in priceDict:
                return priceDict[instrument[0]+"/USD"]
            else:
                return 1 / priceDict["USD/"+instrument[0]]
        if instrument[0] == "USD" or instrument[1] == "USD":
            return 1
    if '.' in x[0]:
        instrument = x[0].split(".") 
        price = 0
        if instrument[0] in countryDict:
            prefix = countryDict[instrument[0]]
            for key in prefix:
                if type(key) == tuple:
                    if instrument[1] in key:
                        postfix = prefix[key]
                        if postfix != "USD":
                            i = postfix + "/USD"
                            i_opp = "USD/" + postfix
                            if not i in priceDict:
                                price = price + (1/priceDict[i_opp])
                            else:
                                price = price + priceDict[i]
                        else:
                             price = price + 1
                elif type(key) == str: 
                    if instrument[1] in key:
                        middlefix = prefix[instrument[1]]
                        for m in middlefix:
                            if instrument[2] in m:
                                curr = middlefix[m]
                                if curr == "USD":
                                    price = price + 1
                                else:
                                        pass
        return price
           
  
def math_calc(x):
    placeholder = 0
    if '/' in x[0]:
        cols = x[0].split('/')
        if cols[1] =='USD':
            placeholder+= x[2] * 1
            return placeholder
        else:
            placeholder+= x[1]* x[3]
            return placeholder
    if '.' in x[0]:
        placeholder+= x[2] * x[3]
        return placeholder

accNo = list(df2["accountNo"].unique())
dfs = pd.DataFrame(columns=["accountNo", "FX($M)", "CFD($M)", "Commission(Revenue)", "Commission(Cost)", "NetCommission"])

for code in accNo:
    
    temp = df2[df2["accountNo"].isin([code])]
    
    category = temp["category"].unique()
    
    if "FX" in category and "IndexSwap" in category:
        # leave when there is a case that account no has more than one category
        pass
    
    elif "FX" in category:
        fx = temp[temp["category"] == "FX"]
        fx_grouped = fx.groupby("instrument").agg({
                        "Base Absolute Amount": ["sum"],
                        "Contract": ["sum"]
                    }).reset_index()
        fx_grouped["eodPrice"] = fx_grouped[["instrument"]].apply(determine_price, axis=1)
        fx_grouped["IN_USD"] = fx_grouped[["instrument", "Base Absolute Amount", "Contract", "eodPrice"]].apply(math_calc, axis=1)
        fx_grouped_usd_sum = fx_grouped["IN_USD"].sum()/1000000
        comms_house = fx_grouped_usd_sum * 7 
        if code in ['APT701', 'APT704', 'APT714']:
            comms_client = fx_grouped_usd_sum * 12
        if code in ['APT703', 'APT713']:
            comms_client = fx_grouped_usd_sum * 15
        else:
            comms_client = fx_grouped_usd_sum * 10
        net_comms = comms_client - comms_house
        dfs = dfs.append({"accountNo": code, "FX($M)": fx_grouped_usd_sum, "CFD($M)": np.nan,"Commission(Cost)":comms_house, "Commission(Revenue)":comms_client,  "NetCommission":net_comms}, ignore_index=True)
        
    
    elif "IndexSwap" in category:
        indexSwap = temp[temp["category"] == "IndexSwap"]
        indexSwap_grouped = indexSwap.groupby("instrument").agg({
                        "Base Absolute Amount": ["sum"],
                        "Contract": ["sum"]
                    }).reset_index()
        indexSwap_grouped["eodPrice"] = indexSwap_grouped[["instrument"]].apply(determine_price, axis=1)
        indexSwap_grouped["IN_USD"] = indexSwap_grouped[["instrument", "Base Absolute Amount", "Contract", "eodPrice"]].apply(math_calc, axis=1)
        indexSwap_grouped_usd_sum = indexSwap_grouped["IN_USD"].sum()/1000000
        comms_house = indexSwap_grouped_usd_sum * 8
        if code in ['APT411', 'APT414']:
            comms_client = indexSwap_grouped_usd_sum * 12
        elif code == "APT413":
            comms_client = indexSwap_grouped_usd_sum * 15
        else:
            comms_client = indexSwap_grouped_usd_sum * 10
        net_comms = comms_client - comms_house
        dfs = dfs.append({"accountNo": code, "FX($M)": np.nan, "CFD($M)": indexSwap_grouped_usd_sum, "Commission(Cost)":comms_house, "Commission(Revenue)":comms_client,"NetCommission":net_comms }, ignore_index=True)
dfs = dfs.append(dfs[['FX($M)','CFD($M)', 'Commission(Cost)', 'Commission(Revenue)', 'NetCommission']].sum().rename('Grand Total')).fillna('')
dfs.to_excel(writer, sheet_name= "Sheet3") 
writer.save()
