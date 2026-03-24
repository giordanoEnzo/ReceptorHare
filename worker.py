import redis
import json
import time
import requests
import logging
import os
from typing import Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("OpenClawWorker")

# Configuration (In production, use environment variables)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "queue:openclaw_tasks"
ERROR_QUEUE = "queue:openclaw_errors"
API_CALLBACK_URL = os.getenv("API_CALLBACK_URL", "http://localhost:8000/api/v1/assignments/tasks")
MAX_RETRIES = 3

# Placeholder for OpenClaw integration
# Note: You need to install the openclaw package: pip install openclaw
try:
    from openclaw import OpenClawAgent
except ImportError:
    logger.warning("OpenClaw package not found. Using a mock agent for demonstration.")
    class OpenClawAgent:
        def __init__(self, name="HareAgent"):
            self.name = name
        
        def run(self, task_description):
            logger.info(f"Agent {self.name} is processing: {task_description}")
            # Simulate processing time
            time.sleep(2)
            return {"status": "success", "log": f"Task processed by {self.name}"}

def update_task_status(task_id: str, status: str, log: str):
    """
    Sends a PATCH request to the API to update the task status.
    """
    url = f"{API_CALLBACK_URL}/{task_id}"
    payload = {
        "status": status,
        "description": log # Or a dedicated field if exists
    }
    try:
        # Note: You might need an Auth token here depending on your API security
        response = requests.put(url, json=payload)
        response.raise_for_status()
        logger.info(f"Task {task_id} updated to {status}")
    except Exception as e:
        logger.error(f"Failed to update task {task_id}: {e}")

def process_task(task_data_json: str):
    try:
        task_data = json.loads(task_data_json)
        task_id = task_data.get("id")
        title = task_data.get("title")
        description = task_data.get("description")
        
        logger.info(f"Processing task {task_id}: {title}")
        
        # Instantiate OpenClaw Agent
        agent = OpenClawAgent(name="HareAI-Manager")
        
        # In a real scenario, you'd pass more context to the agent
        execution_result = agent.run(f"Title: {title}\nDescription: {description}")
        
        if execution_result.get("status") == "success":
            # Update API with success
            # Mapping status to TaskStatus.FINALIZADA (from app.models.assignment)
            # "Finalizada" is the string used in the Enum
            update_task_status(task_id, "Finalizada", execution_result.get("log"))
        else:
            raise Exception(execution_result.get("error", "Unknown error in OpenClaw"))
            
    except Exception as e:
        logger.error(f"Error processing task: {e}")
        handle_failure(task_data_json, str(e))

def handle_failure(task_data_json: str, error_msg: str):
    """
    Moves the task to the error queue for audit.
    """
    client = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        error_payload = {
            "original_task": json.loads(task_data_json),
            "error": error_msg,
            "failed_at": time.time()
        }
        client.lpush(ERROR_QUEUE, json.dumps(error_payload))
        logger.info("Task moved to error queue.")
    except Exception as e:
        logger.error(f"Critical: Failed to push to error queue: {e}")
    finally:
        client.close()

def run_worker():
    logger.info("Starting Redis Consumer Worker...")
    
    while True:
        client = None
        try:
            client = redis.from_url(REDIS_URL, decode_responses=True)
            logger.info(f"Connected to Redis. Monitoring {QUEUE_NAME}...")
            
            while True:
                # BLPOP blocks until an item is available
                # Result is a tuple: (queue_name, item_value)
                result = client.blpop(QUEUE_NAME, timeout=0)
                if result:
                    _, task_data_json = result
                    process_task(task_data_json)
                    
        except redis.ConnectionError:
            logger.error("Redis connection lost. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Worker encountered an error: {e}")
            time.sleep(5)
        finally:
            if client:
                client.close()

if __name__ == "__main__":
    run_worker()
