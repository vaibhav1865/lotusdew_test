


# collect 300 data points for company = ADANIENT


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

fileName = "Nifty50_300final.csv"
def extractPrice(jsonData):
    return jsonData["payload"][2]

def reqData(data , symbol , ws , itr):
    ltp_tickers ={
        "topic" : "api:join",
        "event" : "ltp_quote",
        "payload" : [symbol],
        "ref" : ""
    }
 
    data[symbol] = []
    for i in range(itr):
        print("Requesting data for ",symbol, " at " , i , "th iteration")
        ws.send(json.dumps(ltp_tickers))
        temp = ws.recv()
        temp = json.loads(temp)
        print("Response" , temp)
        data[symbol].append(extractPrice(temp))
        time.sleep(0.1)
    return data

# def setHeaders():
#     csvWriter = csv.writer(open(fileName,"w",newline=""),delimiter=",")
#     csvWriter.writerow(["Company","List_of_prices"])

# def storeData(data):
#     csvWriter = csv.writer(open(fileName,"a",newline=""),delimiter=",")
#     for key in data:
#         csvWriter.writerow([key,data[key]])
#     print("Data stored in ",fileName)


nifty50csv = "ind_nifty50list.csv"
def extractSymbols():
    symbols = []
    with open(nifty50csv,"r") as csvFile:
        csvReader = csv.reader(csvFile)
        next(csvReader)
        for row in csvReader:
            symbols.append(row[2])
    for i in range(len(symbols)):
        symbols[i] = symbols[i].strip()
        # print(symbols[i])
    return symbols
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
    # print(newData)
    df = pd.DataFrame(newData)
    joint_distribution = df.agg(['mean', 'std']).T
    joint_distribution['z_score'] = stats.zscore(joint_distribution['mean'])
    percentile_threshold = stats.norm.ppf(0.95)
    top_5_percentile_companies = joint_distribution[joint_distribution['z_score'] > percentile_threshold]
    # print(top_5_percentile_companies)
    companyName = []
    for i in range(len(top_5_percentile_companies)):
        companyName.append(top_5_percentile_companies.index[i])
    return companyName

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
    price = getCurrPrice(ws,symbol)
    order = {
        "topic" : "api:join", 
        "event" : "order", 
        "payload" : {
            "phone_no" : "9589001042", 
            "symbol" : symbol, 
            "buy_sell" : "S", 
            "quantity" : 10, 
            "price" : price
            }, 
        "ref" : ""
    }
    ws.send(json.dumps(order))
    print(ws.recv())

def buyOrder(ws,symbol):
    price = getCurrPrice(ws,symbol)
    order = {
        "topic" : "api:join", 
        "event" : "order", 
        "payload" : {
            "phone_no" : "9589001042", 
            "symbol" : symbol, 
            "buy_sell" : "B", 
            "quantity" : 10, 
            "price" : price
            }, 
        "ref" : ""
    }
    ws.send(json.dumps(order))
    print(ws.recv())


pnL = 0

def executeTrade(ws,topCompany , data):
    # use threading to execute buy and sell order
    print("Executing Trade...")
   
    for key in topCompany:
        print("Buying " , key , " at ",data[key][-1])
        threading.Thread(target=buyOrder,args=(ws,key )).start()
        pnL += data[key][-1]
        
    time.sleep(300)
    for key in topCompany:
        print("Selling ",key," at ", data[key][-1])
        threading.Thread(target=sellOrder,args=(ws,key)).start()
        pnL -= data[key][-1]
    print("Trade Executed")


def main():
    ws = connect()
    company = extractSymbols()
    data = {}
    for i in range(11):
        print("Requesting data for ",company[i])
        data = reqData(data,company[i],ws,20)
        print("Data Received for ", data[company[i]])

    json.dump(data,open("data1.json","w"))

if __name__ == "__main__":
    main()

    




