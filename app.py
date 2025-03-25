import asyncio
import json
import logging
import random
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(generate_data())
    yield


app = FastAPI(title="Real-Time Data API", lifespan=lifespan)


# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory data store
data_store = {}


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.subscriptions: dict[WebSocket, list[str]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        self.subscriptions[websocket] = []
        logger.info(f"New connection. Total active: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.subscriptions:
            del self.subscriptions[websocket]
        logger.info(f"Connection closed. Total active: {len(self.active_connections)}")

    def subscribe(self, websocket: WebSocket, topics: list[str]):
        if websocket in self.subscriptions:
            self.subscriptions[websocket].extend(topics)
            # Remove duplicates
            self.subscriptions[websocket] = list(set(self.subscriptions[websocket]))

    def unsubscribe(self, websocket: WebSocket, topics: list[str] | None = None):
        if websocket in self.subscriptions:
            if topics is None:
                # Unsubscribe from all
                self.subscriptions[websocket] = []
            else:
                # Unsubscribe from specific topics
                for topic in topics:
                    if topic in self.subscriptions[websocket]:
                        self.subscriptions[websocket].remove(topic)

    async def broadcast_data(self):
        for websocket in self.active_connections:
            topics = self.subscriptions.get(websocket, [])
            if not topics:
                continue

            data_to_send = {}
            for topic in topics:
                if topic in data_store:
                    data_to_send[topic] = {
                        "value": data_store[topic],
                        "timestamp": time.time(),
                    }

            if data_to_send:
                try:
                    await websocket.send_json(data_to_send)
                except Exception as e:
                    logger.error(f"Error sending data: {e}")
                    await self.disconnect(websocket)


manager = ConnectionManager()


# Data generator
async def generate_data():
    while True:
        # Update stock prices
        for ticker in ["MSFT", "AAPL", "GOOG", "AMZN"]:
            key = f"STOCK:{ticker}"
            current = data_store.get(key, 100)
            # Simulate price movement
            new_price = current * (1 + random.uniform(-0.01, 0.01))
            data_store[key] = round(new_price, 2)

        # Update sensor readings
        for sensor_id in range(1, 5):
            key = f"SENSOR:{sensor_id}"
            data_store[key] = round(random.uniform(20, 30), 2)

        # Broadcast updates to all connected clients
        await manager.broadcast_data()

        # Sleep to prevent high CPU usage
        await asyncio.sleep(1.0)


# API routes
@app.get("/data/{data_type}/{param}")
async def get_data(data_type: str, param: str):
    key = f"{data_type}:{param}"
    if key in data_store:
        return {"value": data_store[key], "timestamp": time.time()}
    return {"error": "Data not found"}


@app.get("/data")
async def get_all_data():
    return data_store


# WebSocket route
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                command = message.get("command")

                if command == "subscribe":
                    topics = message.get("topics", [])
                    manager.subscribe(websocket, topics)
                    await websocket.send_json(
                        {"status": "subscribed", "topics": topics}
                    )

                elif command == "unsubscribe":
                    topics = message.get("topics")
                    manager.unsubscribe(websocket, topics)
                    await websocket.send_json({"status": "unsubscribed"})

                else:
                    await websocket.send_json({"error": "Unknown command"})

            except json.JSONDecodeError:
                await websocket.send_json({"error": "Invalid JSON"})

    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
