from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uvicorn
import logging
from typing import Dict, Any
import asyncio
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Supply Chain Analytics Orchestrator", version="1.0.0")

# Service endpoints for Kubernetes
SERVICES = {
    "producer": os.getenv("PRODUCER_SERVICE_URL", "http://localhost:8001"),
    "consumer": os.getenv("CONSUMER_SERVICE_URL", "http://localhost:8002"), 
    "mongodb": os.getenv("MONGODB_SERVICE_URL", "http://localhost:8003"),
    "etl": os.getenv("ETL_SERVICE_URL", "http://localhost:8004")
}

class PipelineStatus(BaseModel):
    producer_status: str
    consumer_status: str
    mongodb_status: str
    etl_status: str
    overall_status: str

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "supply-chain-orchestrator"}

@app.get("/services/health")
async def check_all_services():
    """Check health of all microservices"""
    results = {}
    
    async with httpx.AsyncClient() as client:
        for service_name, service_url in SERVICES.items():
            try:
                response = await client.get(f"{service_url}/health", timeout=5.0)
                results[service_name] = {
                    "status": "healthy" if response.status_code == 200 else "unhealthy",
                    "response": response.json() if response.status_code == 200 else None
                }
            except Exception as e:
                results[service_name] = {
                    "status": "unreachable",
                    "error": str(e)
                }
    
    return results

@app.post("/pipeline/start")
async def start_pipeline():
    """Start the complete data pipeline"""
    try:
        async with httpx.AsyncClient() as client:
            # 1. Start consumer first
            consumer_response = await client.post(f"{SERVICES['consumer']}/consumer/start")
            if consumer_response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to start consumer")
            
            # 2. Wait a bit for consumer to initialize
            await asyncio.sleep(2)
            
            # 3. Start producer
            producer_response = await client.post(f"{SERVICES['producer']}/stream/start", json={})
            if producer_response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to start producer")
            
            return {
                "status": "started",
                "message": "Pipeline started successfully",
                "consumer": consumer_response.json(),
                "producer": producer_response.json()
            }
    
    except Exception as e:
        logger.error(f"Failed to start pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pipeline/stop")
async def stop_pipeline():
    """Stop the data pipeline"""
    try:
        async with httpx.AsyncClient() as client:
            # Stop consumer
            consumer_response = await client.post(f"{SERVICES['consumer']}/consumer/stop")
            
            return {
                "status": "stopped",
                "message": "Pipeline stopped successfully",
                "consumer": consumer_response.json() if consumer_response.status_code == 200 else None
            }
    
    except Exception as e:
        logger.error(f"Failed to stop pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipeline/status")
async def get_pipeline_status():
    """Get overall pipeline status"""
    try:
        async with httpx.AsyncClient() as client:
            # Get status from all services
            producer_status = await client.get(f"{SERVICES['producer']}/stream/status")
            consumer_status = await client.get(f"{SERVICES['consumer']}/consumer/status")
            mongodb_stats = await client.get(f"{SERVICES['mongodb']}/stats")
            etl_status = await client.get(f"{SERVICES['etl']}/etl/status")
            
            return {
                "producer": producer_status.json() if producer_status.status_code == 200 else None,
                "consumer": consumer_status.json() if consumer_status.status_code == 200 else None,
                "mongodb": mongodb_stats.json() if mongodb_stats.status_code == 200 else None,
                "etl": etl_status.json() if etl_status.status_code == 200 else None
            }
    
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/etl/run")
async def run_etl():
    """Trigger ETL process to move data from MongoDB to BigQuery"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{SERVICES['etl']}/etl/run")
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail="ETL process failed")
            
            return response.json()
    
    except Exception as e:
        logger.error(f"Failed to run ETL: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/recent-events")
async def get_recent_events(limit: int = 10):
    """Get recent events from MongoDB"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{SERVICES['mongodb']}/events/recent?limit={limit}")
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to get recent events")
            
            return response.json()
    
    except Exception as e:
        logger.error(f"Failed to get recent events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/high-risk-events")
async def get_high_risk_events(risk_threshold: float = 1.0, limit: int = 20):
    """Get high-risk events from MongoDB"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{SERVICES['mongodb']}/events/high-risk?risk_threshold={risk_threshold}&limit={limit}"
            )
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to get high-risk events")
            
            return response.json()
    
    except Exception as e:
        logger.error(f"Failed to get high-risk events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/daily-stats")
async def get_daily_stats():
    """Get daily analytics from MongoDB"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{SERVICES['mongodb']}/analytics/daily-stats")
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to get daily stats")
            
            return response.json()
    
    except Exception as e:
        logger.error(f"Failed to get daily stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)