from websocket import create_connection, WebSocketConnectionClosedException
import json
from datetime import datetime
import ssl

# Check and Establish connection with websocket
ws = create_connection("wss://api.airalgo.com/socket/websocket", sslopt={"cert_reqs": ssl.CERT_NONE})
# E:\lotusdew\tradeTest.py
# Payload to authenticate with the websocket
conn = {
    "topic" : "api:join",
    "event" : "phx_join",
    "payload" :
        {
            "phone_no" : "9589001042",
        },
    "ref" : ""
    }

# Authenticate with websocket
ws.send(json.dumps(conn))
print(ws.recv())

# Create Payload to subscribe Equity ltp 
# {"topic" : "api:join", "event" : "ltp_quote", "payload" : ["ACC", "ABB", "ADANIENT"], "ref" : ""}

tickers = ["AIAENG", "APLAPOLLO"]
ltp_tickers = {
  "topic" : "api:join",
  "event" : "ltp_quote",
  "payload" : ["AIAENG"],
  "ref" : ""
}
# ws.send(json.dumps(ltp_tickers)) 

ws.send(json.dumps(ltp_tickers))
print(ws.recv())
setUniqRes = set()
while True:
  print(ws.recv())
  order = {
     "topic" : "api:join", 
     "event" : "order", 
     "payload" : {
        "phone_no" : "9589001042", 
        "symbol" : "ACC", 
        "buy_sell" : "B", 
        "quantity" : 1, 
        "price" : 1012.34
        }, 
      "ref" : ""
  }
  ws.send(json.dumps(order))
  temp = ws.recv()
  if temp not in setUniqRes:
    print(temp)
    break
  setUniqRes.add(temp)
  