# async-socket-server
Python3.6, asyncio, aiohttp, SQLAlchemy, sqlite

Create a asynchronous server, sending to websocket actual values for some currency pairs. 
  
1. In the beginning create fixture for all assets (EURUSD, USDJPY, GBPUSD, AUDUSD, USDCAD):
Asset:
  - id;
  - symbol.
 
2. Receive (from https://ratesjson.fxcm.com/DataDisplayer) and store points in the memory.
Point: 
  - id;
  - asset id;
  - timestamp;
  - value.
value = (bid+ask)/2.
 
3. Request of assets {"action":"assets","message":{}}:
  response: {"action":"assets","message":{"assets":[{"id":1,"name":"EURUSD"},{"id":2,"name":"USDJPY"},{"id":3,"nam
e":"GBPUSD"},{"id":4,"name":"AUDUSD"},{"id":5,"name":"USDCAD"}]}}

4. Request of subscribing for some asset {"action":"subscribe","message":{"assetId":1}}:
  response: {"message":{"points":[{"assetName":"EURUSD","time":1455883484,"assetId":1,"value":1.110481
},{"assetName":"EURUSD","time":1455883485,"assetId":1,"value":1.110948},{"assetName":"EU
RUSD","time":1455883486,"assetId":1,"value":1.111122}]},"action":"asset_history"}

Client can be subscribed to only one asset in the moment. New request on subscribing have to finish previous subscribing and start new one with new asset history for some last period.

5. Message for subscribers with new point {"message":{"assetName":"EURUSD","time":1453556718,"assetId":1,"value":1.079755},"action":"point"}

Socket must be placed on port 8080.
