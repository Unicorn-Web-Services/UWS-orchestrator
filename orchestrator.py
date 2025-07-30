import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import JSONResponse
import httpx
import websockets as ws_client
from pydantic import BaseModel
from typing import Dict
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

# Get the node authentication token from environment
NODE_AUTH_TOKEN = os.getenv("NODE_AUTH_TOKEN", "default-token")

def get_auth_headers():
    """Returns headers with authentication token for node requests"""
    return {"Authorization": f"Bearer {NODE_AUTH_TOKEN}"}

class ContainerConfig(BaseModel):
    image: str
    name: Optional[str]
    env: Optional[Dict[str, str]] = {}
    cpu: float  # e.g., 0.2
    memory: str  # e.g., "512m"
    ports: Optional[Dict[str, int]] = {}  # e.g., {"5000/tcp": 8080}


app = FastAPI()

nodes = {}

HEALTH_CHECK_INTERVAL = 10

async def health_check_node(node_id: str, url: str):

    normalized_url = url.rstrip('/')
    health_url = f"{normalized_url}/health"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            #print(f"Attempting health check for node {node_id} at {health_url}")
            resp = await client.get(health_url, headers=get_auth_headers())
            #print(f"Health check for node {node_id} at {health_url}: {resp.status_code}")
            return resp.status_code == 200
        except httpx.TimeoutException as e:
            #print(f"Health check for node {node_id} at {health_url} timed out: {e}")
            return False
        except httpx.ConnectError as e:
            #print(f"Health check for node {node_id} at {health_url} connection failed: {e}")
            return False
        except httpx.RequestError as e:
            #print(f"Health check for node {node_id} at {health_url} failed: {e}")
            return False
        except Exception as e:
            #print(f"Health check for node {node_id} at {health_url} unexpected error: {e}")
            return False

async def health_check_loop():
    while True:
        print(f"Starting health check cycle for {len(nodes)} nodes")
        to_remove = []
        for node_id, url in nodes.items():
            healthy = await health_check_node(node_id, url)
            if not healthy:
                print(f"Node {node_id} down, removing")
                to_remove.append(node_id)
        for node_id in to_remove:
            nodes.pop(node_id, None)
            
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

@app.on_event("startup")
async def startup():
    asyncio.create_task(health_check_loop())

@app.post("/register_node/{node_id}")
async def register_node(node_id: str, url: str, request: Request):
    # If the URL contains 0.0.0.0, replace it with the actual client IP
    if "0.0.0.0" in url:
        client_ip = request.client.host

        if client_ip in ["127.0.0.1", "::1"] and "x-forwarded-for" in request.headers:
            client_ip = request.headers["x-forwarded-for"].split(",")[0].strip()
        
        corrected_url = url.replace("0.0.0.0", client_ip)

        nodes[node_id] = corrected_url
        return {"status": "registered", "node_id": node_id, "url": corrected_url, "original_url": url}
    else:
        nodes[node_id] = url
        return {"status": "registered", "node_id": node_id, "url": url}

@app.get("/nodes")
async def list_nodes():
    return nodes

@app.get("/health_check/{node_id}")
async def manual_health_check(node_id: str):
    if node_id not in nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    
    url = nodes[node_id]
    healthy = await health_check_node(node_id, url)
    return {
        "node_id": node_id,
        "url": url,
        "healthy": healthy,
        "timestamp": asyncio.get_event_loop().time()
    }

@app.post("/launch")
async def launch_container(config: ContainerConfig):
    if not nodes:
        raise HTTPException(status_code=503, detail="No available nodes")

    # Select first available node (simple strategy)
    for node_id, node_url in nodes.items():
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{node_url}/launch", 
                    json=config.dict(),
                    headers=get_auth_headers()
                )
                return {
                    "node_id": node_id,
                    "url": node_url,
                    "container": resp.json()
                }
        except Exception as e:
            print(f"Node {node_id} failed to launch container:", e)
            continue

    raise HTTPException(status_code=500, detail="All nodes failed to launch the container")



@app.websocket("/ws/terminal/{node_id}/{container_id}")
async def terminal_proxy(websocket: WebSocket, node_id: str, container_id: str):
    await websocket.accept()

    if node_id not in nodes:
        await websocket.close(code=1008)
        return

    node_url = nodes[node_id]
    
    # Properly convert HTTP URL to WebSocket URL
    if node_url.startswith("http://"):
        node_ws_url = node_url.replace("http://", "ws://")
    elif node_url.startswith("https://"):
        node_ws_url = node_url.replace("https://", "wss://")
    else:
        # Assume http if no protocol specified
        node_ws_url = f"ws://{node_url}"
    
    # Change port from 8000 to 8765 for WebSocket if needed
    if ":8000" in node_ws_url:
        node_ws_url = node_ws_url.replace(":8000", ":8765")
    
    # Add authentication token as query parameter for WebSocket
    full_ws_url = f"{node_ws_url}/ws/terminal/{container_id}"
    
    print(f"Connecting to WebSocket: {full_ws_url}")

    try:
        # Include authorization header for WebSocket connection
        async with ws_client.connect(full_ws_url) as node_ws:

            async def user_to_node():
                try:
                    async for msg in websocket.iter_text():
                        await node_ws.send(msg)
                except:
                    pass

            async def node_to_user():
                try:
                    async for msg in node_ws:
                        await websocket.send_text(msg)
                except:
                    pass

            await asyncio.gather(user_to_node(), node_to_user())

    except Exception as e:
        print("Proxy error:", e)
        await websocket.close(code=1011)