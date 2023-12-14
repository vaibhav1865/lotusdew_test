from scipy import stats
import pandas as pd
import numpy as np
import json


from websocket import create_connection, WebSocketConnectionClosedException
import json
from datetime import datetime
import ssl
import time
import csv
import numpy
import math
import pandas as pd
from scipy import stats
import threading
pnl = 0
def connect():
    print("Connecting to websocket...")
    ws = create_connection("wss://api.airalgo.com/socket/websocket", sslopt={"cert_reqs": ssl.CERT_NONE})
    conn = {
        "topic" : "api:join",
        "event" : "phx_join",
        "payload" :
            {
                "phone_no" : "9589001042",
            },
        "ref" : ""
        }
    ws.send(json.dumps(conn))
    print(ws.recv())
    print("Connected to websocket")
    return ws

def diff(data):
    diffData = []
    for i in range(len(data)-1):
        if data[i+1] - data[i] != 0:
            diffData.append(data[i+1] - data[i])
        else:
            diffData.append(np.nan)
    return diffData


def strategy1(data):
    newData = data.copy()
    for dataKey in data:
        newLst = diff(newData[dataKey])
        if len(newLst) > 0:
            newData[dataKey] = newLst
        else:
            del newData[dataKey]
    df = pd.DataFrame(newData)
    joint_distribution = df.agg(['mean', 'std']).T
    joint_distribution['z_score'] = stats.zscore(joint_distribution['mean'])
    percentile_threshold = stats.norm.ppf(0.95)
    top_5_percentile_companies = joint_distribution[joint_distribution['z_score'] > percentile_threshold]
    companyName = []
    for i in range(len(top_5_percentile_companies)):
        companyName.append(top_5_percentile_companies.index[i])
    return companyName
def extractPrice(jsonData):
    return jsonData["payload"][2]



def getCurrPrice(ws,symbol):
    ltp_tickers ={
        "topic" : "api:join",
        "event" : "ltp_quote",
        "payload" : [symbol],
        "ref" : ""
    }
    ws.send(json.dumps(ltp_tickers))
    temp = ws.recv()
    temp = json.loads(temp)
    return extractPrice(temp)

   
def sellOrder(ws,symbol):
    global pnl
    price = getCurrPrice(ws,symbol)
    pnl += price
    order = {
        "topic" : "api:join", 
        "event" : "order", 
        "payload" : {
            "phone_no" : "9589001042", 
            "symbol" : symbol, 
            "buy_sell" : "S", 
            "quantity" : 1, 
            "price" : price
            }, 
        "ref" : ""
    }
    ws.send(json.dumps(order))
    print(ws.recv())

def buyOrder(ws,symbol):
    global pnl
    price = getCurrPrice(ws,symbol)
    pnl -= price
    order = {
        "topic" : "api:join", 
        "event" : "order", 
        "payload" : {
            "phone_no" : "9589001042", 
            "symbol" : symbol, 
            "buy_sell" : "B", 
            "quantity" : 1, 
            "price" : price
            }, 
        "ref" : ""
    }
    ws.send(json.dumps(order))
    print(ws.recv())

def executeTrade(ws,topCompany , data):
    for key in topCompany:
        print("Buying " , key , " at ",data[key][-1])
        threading.Thread(target=buyOrder,args=(ws,key )).start()
    print("Waiting for 5 minutes")
    time.sleep(10)
    for key in topCompany:
        print("Selling ",key," at " , data[key][-1])
        threading.Thread(target=sellOrder,args=(ws,key)).start()

def flush(ws):
    while True:
        try:
            print(ws.recv())
        except WebSocketConnectionClosedException:
            print("Connection Closed")
            break
    


def main():
    data = json.load(open("data1.json","r"))
    ws = connect()
    # flush(ws)
    topCompany = strategy1(data)
    executeTrade(ws,topCompany,data)

if __name__ == "__main__":
    main()
    