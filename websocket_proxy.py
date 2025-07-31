import json
import asyncio
import structlog
import websockets as ws_client
from fastapi import WebSocket, WebSocketDisconnect

from database import get_db, Node
from metrics import WEBSOCKET_CONNECTIONS

logger = structlog.get_logger()


async def terminal_proxy(websocket: WebSocket, node_id: str, container_id: str):
    """WebSocket proxy for terminal connections with enhanced logging"""
    await websocket.accept()
    WEBSOCKET_CONNECTIONS.inc()

    logger.info(
        "WebSocket connection established", node_id=node_id, container_id=container_id
    )

    try:
        # Get node URL from database
        db = next(get_db())
        node = db.query(Node).filter(Node.node_id == node_id).first()
        db.close()

        if not node:
            await websocket.send_text(json.dumps({"error": "Node not found"}))
            return

        # Connect to node's terminal WebSocket
        node_ws_url = (
            f"ws://{node.url.replace('http://', '')}/ws/terminal/{container_id}"
        )

        async with ws_client.connect(node_ws_url) as node_websocket:
            logger.info(
                "Connected to node terminal", node_id=node_id, container_id=container_id
            )

            # Create tasks for bidirectional communication
            async def user_to_node():
                try:
                    while True:
                        data = await websocket.receive_text()
                        await node_websocket.send(data)
                except Exception as e:
                    logger.error("User to node communication error", error=str(e))

            async def node_to_user():
                try:
                    while True:
                        data = await node_websocket.recv()
                        await websocket.send_text(data)
                except Exception as e:
                    logger.error("Node to user communication error", error=str(e))

            # Run both tasks concurrently
            await asyncio.gather(user_to_node(), node_to_user())

    except WebSocketDisconnect:
        logger.info(
            "WebSocket disconnected", node_id=node_id, container_id=container_id
        )
    except Exception as e:
        logger.error(
            "WebSocket error", error=str(e), node_id=node_id, container_id=container_id
        )
    finally:
        WEBSOCKET_CONNECTIONS.dec()
