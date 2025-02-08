# import websocket
# import time
# import json
# import requests
# import websockets
# import websockets.connection

# # DB_URL = "http://localhost:8081/post"


# import asyncio
# import httpx

# # async def create_news(data):
# #     endpoint = "http://localhost:8081/post"
# #     async with httpx.AsyncClient() as client:
# #         headers = {'Content-Type': 'application/json'}
# #         response = await client.post(endpoint, headers=headers, data = data)
# #         print(response.status_code)
# #         return response.json()
    



# async def connect():
#     header = {"x-api-key": "vVFfrq5TZipcYV7lEOtKJ2NzH5Xg4HgUdK7CsZopwXi1uJciaYDSt6mcL7z1c7jO"}
    
#     while True:
#         try:
#             async with websockets.connect("wss://wss.phoenixnews.io", extra_headers = header) as websocket:
#                 print("Websocket connection has been opened..")
#                 async for incoming_data in websocket:
#                     try:
#                         message = json.loads(incoming_data)
#                         print(f"{message} --- {type(message)}")
#                         source = message["source"]
#                         print(source)
#                         if source == "Twitter":
#                             row = {}
#                             row["source"] = "Twitter"
#                             row["source_url"] = message["url"]
#                             row["content"] = message["body"]
#                             # row["thumbnail_url"] = message["icon"]
#                             row["profileImage"] = message["icon"]
#                             if "image" in message:
#                                 row["thumbnail_url"] = message["image"]
#                             row["username"] = message["username"]
#                             row["coins"] = []
#                             row["tweetId"] = message["tweetId"]
#                             row["isRetweet"] = message["isRetweet"]
#                             row["isReply"] = message["isReply"]
#                             row["isSelfReply"] = message["isSelfReply"]
#                             row["isQuote"] = message["isQuote"]
#                             if "imageQuote" in message:
#                                 row["imageQuote"] = message["imageQuote"]
#                             if len(message["suggestions"]) > 0:
                                
#                                 for c in message["suggestions"]:
#                                     for x in c["symbols"]:
#                                         if x["exchange"] == "binance-futures":
#                                             row["coins"].append({"name":x['symbol']})
#                             print(f">>> {row}")
#                             data = json.dumps(row, ensure_ascii=False, indent= None, separators=(',', ':'));
#                             # response_data = await create_news(data)
#                             # print(response_data)
#                         if source == "Crypto":
#                             row = {}
#                             row["source"] = "Crypto"
#                             row["source_url"] = message["url"]
#                             if "description" in message:
#                                 row["description"] = message["description"]
#                             row["title"] = message["title"]
#                             row["content"] = message["description"]
#                             # row["time"] = message["time"]
#                             row["source_name"] = message["sourceName"]
#                             row["coins"] = []
#                             if len(message["suggestions"]) > 0:
                                
#                                 for c in message["suggestions"]:
#                                     for x in c["symbols"]:
#                                         if x["exchange"] == "binance-futures":
#                                             row["coins"].append({"name":x['symbol']})
#                             print(f">>> {row}")
#                             data = json.dumps(row)
#                             # response_data = await create_news(data)
#                             # print(response_data)
#                         if source == "Webs":
#                             row = {}
#                             row["source"] = "Webs"
#                             row["source_url"] = message["url"]
#                             row["source_name"] = message["sourceName"]
#                             row["title"] = message["title"]
#                             row["content"] = message["description"]
#                             # row["time"] = message["time"]
#                             row["coins"] = []
#                             if len(message["suggestions"]) > 0:
#                                 for c in message["suggestions"]:
#                                     for x in c["symbols"]:
#                                         if x["exchange"] == "binance-futures":
#                                             row["coins"].append({"name":x['symbol']})
#                             print(f">>> {row}")
#                             data = json.dumps(row)
#                             # response_data = await create_news(data)
#                             # print(response_data)
#                         if source == "Blogs":
#                             row = {}
#                             row["source"] = "Blogs"
#                             row["source_url"] = message["url"]
#                             row["source_name"] = message["sourceName"]
#                             if "description" in message:
#                                 row["description"] = message["description"]
#                             row["title"] = message["title"]
#                             row["content"] = message["description"]
#                             # row["time"] = message["time"]
#                             row["coins"] = []
#                             if len(message["suggestions"]) > 0:
#                                 for c in message["suggestions"]:
#                                     for x in c["symbols"]:
#                                         if x["exchange"] == "binance-futures":
#                                             row["coins"].append({"name":x['symbol']})
#                             print(f">>> {row}")
#                             data = json.dumps(row)
#                             # response_data = await create_news(data)
#                             # print(response_data)
                            
#                     except KeyError:
#                         print(">>>> ### <<<<<")
#                     except json.JSONDecodeError:
#                         print("Invalid JSON data")
                        
#         except websockets.ConnectionClosedError:
#             print("Connection has been closed.Now try to connect again.")
#             await asyncio.sleep(1)
                
# if __name__ == "__main__":
#     asyncio.get_event_loop().run_until_complete(connect())

import asyncio
import json
import websockets
import websockets.connection
import websockets.server

# Global set of connected client websockets (to broadcast news messages)
connected_clients = set()

# -----------------------------------------------------------------------------
# 1. News Fetcher: Connects to the external PhoenixNews WebSocket API.
# -----------------------------------------------------------------------------
async def news_fetcher():
    header = {"x-api-key": "vVFfrq5TZipcYV7lEOtKJ2NzH5Xg4HgUdK7CsZopwXi1uJciaYDSt6mcL7z1c7jO"}
    external_ws_url = "wss://wss.phoenixnews.io"

    while True:
        try:
            async with websockets.connect(external_ws_url, additional_headers=header) as ext_ws:
                print("Connected to external WebSocket API.")
                async for incoming_data in ext_ws:
                    try:
                        # Parse the incoming JSON message.
                        message = json.loads(incoming_data)
                        print("Received from external API:", message)

                        # Process the message based on its "source"
                        # (You can adjust this logic to reformat or filter the data as needed.)
                        source = message.get("source")
                        row = {}
                        if source == "Twitter":
                            row = {
                                "source": "Twitter",
                                "source_url": message.get("url"),
                                "content": message.get("body"),
                                "profileImage": message.get("icon"),
                                "thumbnail_url": message.get("image") if "image" in message else None,
                                "username": message.get("username"),
                                "tweetId": message.get("tweetId"),
                                "isRetweet": message.get("isRetweet"),
                                "isReply": message.get("isReply"),
                                "isSelfReply": message.get("isSelfReply"),
                                "isQuote": message.get("isQuote")
                            }
                            # Process coin suggestions if available.
                            coins = []
                            if message.get("suggestions"):
                                for suggestion in message["suggestions"]:
                                    for symbol in suggestion.get("symbols", []):
                                        if symbol.get("exchange") == "binance-futures":
                                            coins.append({"name": symbol.get("symbol")})
                            row["coins"] = coins

                        elif source in ("Crypto", "Webs", "Blogs"):
                            # Example processing for other sources (adjust as needed)
                            row = {
                                "source": source,
                                "source_url": message.get("url"),
                                "title": message.get("title"),
                                "content": message.get("description", ""),
                                "source_name": message.get("sourceName"),
                                "coins": []
                            }
                            if message.get("suggestions"):
                                for suggestion in message["suggestions"]:
                                    for symbol in suggestion.get("symbols", []):
                                        if symbol.get("exchange") == "binance-futures":
                                            row["coins"].append({"name": symbol.get("symbol")})
                        else:
                            # For any unhandled sources, pass the raw message.
                            row = message

                        # Convert the processed message to JSON.
                        broadcast_data = json.dumps(row)
                        print("Broadcasting data:", broadcast_data)

                        # Broadcast to all connected WebSocket clients.
                        await broadcast_to_clients(broadcast_data)

                    except KeyError as ke:
                        print("Key error while processing message:", ke)
                    except json.JSONDecodeError:
                        print("Invalid JSON data received.")
        except websockets.ConnectionClosedError:
            print("External WebSocket connection closed. Reconnecting in 1 second...")
            await asyncio.sleep(1)
        except Exception as e:
            print("Error in news_fetcher:", e)
            await asyncio.sleep(1)

# -----------------------------------------------------------------------------
# 2. Broadcast Helper: Sends the message to all connected clients.
# -----------------------------------------------------------------------------
async def broadcast_to_clients(message):
    if connected_clients:
        # Using asyncio.wait ensures that messages are sent concurrently.
        await asyncio.wait([client.send(message) for client in connected_clients])

# -----------------------------------------------------------------------------
# 3. WebSocket Server: Handles connections from React Native clients.
# -----------------------------------------------------------------------------
# async def client_handler(websocket, path):
#     print("React Native client connected.")
#     connected_clients.add(websocket)
#     try:
#         # Optionally, you can listen for messages from the client (if needed).
#         async for client_message in websocket:
#             print("Message from client:", client_message)
#     except websockets.ConnectionClosed:
#         print("Client disconnected.")
#     finally:
#         connected_clients.remove(websocket)

# Correct definition of the client handler:
async def client_handler(websocket, path):
    print("React Native client connected on path:", path)
    connected_clients.add(websocket)
    try:
        async for client_message in websocket:
            print("Message from client:", client_message)
    except websockets.ConnectionClosed:
        print("Client disconnected.")
    finally:
        connected_clients.remove(websocket)

# -----------------------------------------------------------------------------
# 4. Main: Starts both the news fetcher and the WebSocket server concurrently.
# -----------------------------------------------------------------------------
async def main():
    # Start the news fetcher (connects to external API).
    news_task = asyncio.create_task(news_fetcher())

    # Start the WebSocket server for React Native clients on port 8000.
    server = await websockets.server.serve(client_handler, "0.0.0.0", 8000)
    print("WebSocket server started at ws://0.0.0.0:8000")
    
    # Run indefinitely.
    await asyncio.Future()  # This future never completes.

if __name__ == "__main__":
    asyncio.run(main())
