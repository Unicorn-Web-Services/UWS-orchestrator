from pydantic import BaseModel
from typing import Dict, Optional


class ContainerConfig(BaseModel):
    """Configuration for launching a container"""
    image: str
    name: Optional[str] = None
    env: Optional[Dict[str, str]] = {}
    cpu: float  # e.g., 0.2
    memory: str  # e.g., "512m"
    ports: Optional[Dict[str, int]] = {}  # e.g., {"5000/tcp": 8080}


class ContainerLaunchRequest(BaseModel):
    """Request model for launching a container"""
    user_id: str
    config: ContainerConfig