import asyncio
import httpx
import structlog
from datetime import datetime
from sqlalchemy.orm import Session

from database import get_db, Node
from auth import get_auth_headers
from metrics import ACTIVE_NODES

logger = structlog.get_logger()

HEALTH_CHECK_INTERVAL = 10


async def health_check_node(node: Node, db: Session):
    """Check if a node is healthy and update database"""
    normalized_url = node.url.rstrip("/")
    health_url = f"{normalized_url}/health"

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(health_url, headers=get_auth_headers())
            is_healthy = resp.status_code == 200

            # Update database
            node.is_healthy = is_healthy
            node.last_health_check = datetime.utcnow()
            db.commit()

            logger.info("Node health check", node_id=node.node_id, healthy=is_healthy)
            return is_healthy
        except Exception as e:
            # Mark as unhealthy
            node.is_healthy = False
            node.last_health_check = datetime.utcnow()
            db.commit()
            logger.error("Node health check failed", node_id=node.node_id, error=str(e))
            return False


async def health_check_loop():
    """Periodic health check for all registered nodes"""
    while True:
        db = next(get_db())
        try:
            nodes = db.query(Node).all()
            logger.info("Starting health check cycle", node_count=len(nodes))

            healthy_count = 0
            for node in nodes:
                healthy = await health_check_node(node, db)
                if healthy:
                    healthy_count += 1

            # Update metrics
            ACTIVE_NODES.set(healthy_count)

            logger.info(
                "Health check cycle complete",
                total_nodes=len(nodes),
                healthy_nodes=healthy_count,
            )
        except Exception as e:
            logger.error("Health check loop error", error=str(e))
        finally:
            db.close()

        await asyncio.sleep(HEALTH_CHECK_INTERVAL)
