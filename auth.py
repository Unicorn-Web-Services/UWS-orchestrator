import os
from dotenv import load_dotenv

load_dotenv()

# Get the node authentication token from environment
NODE_AUTH_TOKEN = os.getenv("NODE_AUTH_TOKEN", "orchestrator-secret-key-2024")


def get_auth_headers():
    """Returns headers with authentication token for node requests"""
    return {"Authorization": f"Bearer {NODE_AUTH_TOKEN}"}