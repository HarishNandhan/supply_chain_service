import os
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        
        try:
            while True:
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
            self.close()
    
    def close(self):
        """Close consumer and MongoDB connections"""
        self.consumer.close()
        self.mongo_client.close()
        logger.info("Consumer and MongoDB connections closed")

if __name__ == "__main__":
    consumer = SupplyChainConsumer()
    consumer.consume_messages()