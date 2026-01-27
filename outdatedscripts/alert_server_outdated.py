#!/usr/bin/env python3
import asyncio, json
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

CLIENTS = set()

async def stream_handler(websocket):
    client_id = id(websocket)
    CLIENTS.add(websocket)
    logger.info(f"📱 Stream client connected. Total clients: {len(CLIENTS)}")
    
    try:
        async for message in websocket:
            logger.info(f"📱 Received from stream client: {message[:100]}...")
    except (ConnectionClosedOK, ConnectionClosedError) as e:
        logger.info(f"📱 Stream client disconnected. Code: {e.code if hasattr(e, 'code') else 'N/A'}")
    finally:
        CLIENTS.discard(websocket)
        logger.info(f"📱 Stream client removed. Total clients: {len(CLIENTS)}")

async def ingest_handler(websocket):
    logger.info(f"📡 Ingest client connected")
    
    try:
        async for msg in websocket:
            logger.info(f"📡 Received from ingest: {len(msg)} bytes")
            
            try:
                data = json.loads(msg)
                logger.info(f"📡 Parsed JSON data type: {type(data)}")
                
                # fanout to all stream clients
                dead = []
                for ws in list(CLIENTS):
                    try:
                        await ws.send(json.dumps(data))
                        logger.info(f"📡 Broadcasted to stream client {id(ws)}")
                    except Exception as e:
                        logger.error(f"📡 Error broadcasting to client: {e}")
                        dead.append(ws)
                
                for d in dead:
                    CLIENTS.discard(d)
                    logger.info(f"📡 Removed dead client. Total: {len(CLIENTS)}")
                    
            except Exception as e:
                logger.error(f"📡 JSON parse error: {e}")
                continue
                
    except (ConnectionClosedOK, ConnectionClosedError) as e:
        logger.info(f"📡 Ingest client disconnected")
    except Exception as e:
        logger.error(f"📡 Ingest handler error: {e}")

async def main():
    server_stream = await websockets.serve(stream_handler, "0.0.0.0", 6789, ping_interval=20, ping_timeout=20)
    server_ingest = await websockets.serve(ingest_handler, "0.0.0.0", 6790, ping_interval=20, ping_timeout=20)
    print("WS servers up:")
    print("  - stream (browser clients): ws://localhost:6789")
    print("  - ingest (detector sends): ws://localhost:6790")
    logger.info("🚀 WebSocket servers started successfully")
    
    await asyncio.gather(server_stream.wait_closed(), server_ingest.wait_closed())

if __name__ == "__main__":
    asyncio.run(main())