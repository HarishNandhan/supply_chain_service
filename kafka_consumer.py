import os
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uvicorn
import asyncio
import threading

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Supply Chain Consumer API", version="1.0.0")

class ConsumerStatus(BaseModel):
    is_running: bool
    messages_processed: int
    last_message_time: Optional[str]

class SupplyChainConsumer:
    def __init__(self):
        """Initialize Kafka consumer and MongoDB connection"""
        
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET'),
            'group.id': 'supply-chain-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        
        self.topic = os.getenv('KAFKA_TOPIC', 'supply-chain-events')
        self.consumer = Consumer(self.kafka_config)
        
        # MongoDB configuration
        self.mongo_uri = os.getenv('MONGODB_URI')
        self.db_name = os.getenv('MONGODB_DATABASE', 'supply_chain_analytics')
        self.collection_name = os.getenv('MONGODB_COLLECTION', 'shipment_events')
        
        # Initialize MongoDB connection
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.db_name]
        self.collection = self.db[self.collection_name]
        
        # Subscribe to topic
        self.consumer.subscribe([self.topic])
        
        # Status tracking
        self.is_running = False
        self.messages_processed = 0
        self.last_message_time = None
        
        logger.info(f"Consumer initialized and subscribed to topic: {self.topic}")
        logger.info(f"MongoDB connected to database: {self.db_name}, collection: {self.collection_name}")
    
    def process_message(self, message_value):
        """Process and validate incoming message"""
        try:
            # Parse JSON message
            event_data = json.loads(message_value)
            
            # Add processing metadata
            event_data['processed_timestamp'] = datetime.utcnow().isoformat()
            event_data['processing_status'] = 'success'
            
            # Validate required fields
            required_fields = ['event_id', 'timestamp', 'vehicle_data', 'performance_indicators']
            for field in required_fields:
                if field not in event_data:
                    raise ValueError(f"Missing required field: {field}")
            
            return event_data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Message processing error: {str(e)}")
            return None
    
    def store_to_mongodb(self, event_data):
        """Store processed event data to MongoDB"""
        try:
            # Insert document
            result = self.collection.insert_one(event_data)
            logger.info(f"Stored event {event_data['event_id']} to MongoDB with ID: {result.inserted_id}")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB storage error: {str(e)}")
            return False
    
    def consume_messages(self):
        """Main consumer loop"""
        logger.info("Starting message consumption...")
        self.is_running = True
        
        try:
            while self.is_running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Process message
                logger.info(f"Received message: {msg.key().decode('utf-8') if msg.key() else 'No key'}")
                
                # Process and store message
                processed_data = self.process_message(msg.value().decode('utf-8'))
                
                if processed_data:
                    success = self.store_to_mongodb(processed_data)
                    if success:
                        self.messages_processed += 1
                        self.last_message_time = datetime.utcnow().isoformat()
                        logger.info(f"Successfully processed and stored event: {processed_data['event_id']}")
                    else:
                        logger.error(f"Failed to store event: {processed_data['event_id']}")
                else:
                    logger.error("Failed to process message")
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
        finally:
            self.is_running = False
            self.close()
    
    def stop_consuming(self):
        """Stop the consumer loop"""
        self.is_running = False
    
    def close(self):
        """Close consumer and MongoDB connections"""
        self.consumer.close()
        self.mongo_client.close()
        logger.info("Consumer and MongoDB connections closed")

# Global consumer instance
consumer_instance = None
consumer_thread = None

def get_consumer():
    global consumer_instance
    if consumer_instance is None:
        consumer_instance = SupplyChainConsumer()
    return consumer_instance

@app.on_event("startup")
async def startup_event():
    logger.info("Consumer API starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    global consumer_instance, consumer_thread
    if consumer_instance:
        consumer_instance.stop_consuming()
    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=5)
    logger.info("Consumer API shutting down...")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "supply-chain-consumer"}

@app.post("/consumer/start")
async def start_consumer():
    global consumer_thread
    try:
        consumer = get_consumer()
        
        if consumer.is_running:
            return {"status": "already_running", "message": "Consumer is already running"}
        
        consumer_thread = threading.Thread(target=consumer.consume_messages, daemon=True)
        consumer_thread.start()
        
        return {"status": "started", "message": "Consumer started successfully"}
    except Exception as e:
        logger.error(f"Failed to start consumer: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/consumer/stop")
async def stop_consumer():
    try:
        consumer = get_consumer()
        consumer.stop_consuming()
        return {"status": "stopped", "message": "Consumer stopped successfully"}
    except Exception as e:
        logger.error(f"Failed to stop consumer: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/consumer/status")
async def get_consumer_status():
    consumer = get_consumer()
    return ConsumerStatus(
        is_running=consumer.is_running,
        messages_processed=consumer.messages_processed,
        last_message_time=consumer.last_message_time
    )

@app.get("/events/recent")
async def get_recent_events(limit: int = 10):
    try:
        consumer = get_consumer()
        cursor = consumer.collection.find().sort('ingestion_timestamp', -1).limit(limit)
        events = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
            events.append(doc)
        return {"events": events, "count": len(events)}
    except Exception as e:
        logger.error(f"Failed to get recent events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/high-risk")
async def get_high_risk_events(risk_threshold: float = 1.0, limit: int = 20):
    try:
        consumer = get_consumer()
        query = {'performance_indicators.risk_classification': {'$gte': risk_threshold}}
        cursor = consumer.collection.find(query).sort('timestamp', -1).limit(limit)
        events = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            events.append(doc)
        return {"events": events, "count": len(events), "risk_threshold": risk_threshold}
    except Exception as e:
        logger.error(f"Failed to get high-risk events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)