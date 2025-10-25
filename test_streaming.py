import os
import time
import threading
from dotenv import load_dotenv
from kafka_producer import SupplyChainProducer
from kafka_consumer import SupplyChainConsumer
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_producer():
    """Test the Kafka producer"""
    logger.info("Starting producer test...")
    producer = SupplyChainProducer()
    
    try:
        # Stream first 50 records for testing
        import pandas as pd
        df = pd.read_csv('data/cleaned_supply_chain_data.csv')
        test_df = df.head(50)  # Use first 50 records for testing
        
        logger.info(f"Streaming {len(test_df)} test records...")
        
        for i, (_, row) in enumerate(test_df.iterrows()):
            message = producer.prepare_message(row)
            import json
            producer.producer.produce(
                topic=producer.topic,
                key=message['event_id'],
                value=json.dumps(message),
                callback=producer.delivery_report
            )
            
            if (i + 1) % 10 == 0:
                producer.producer.flush()
                logger.info(f"Produced {i + 1} messages")
                time.sleep(1)  # Small delay for testing
        
        producer.producer.flush()
        logger.info("Producer test completed successfully")
        
    except Exception as e:
        logger.error(f"Producer test failed: {str(e)}")
    finally:
        producer.close()

def test_consumer():
    """Test the Kafka consumer"""
    logger.info("Starting consumer test...")
    consumer = SupplyChainConsumer()
    
    try:
        # Run consumer for a limited time for testing
        start_time = time.time()
        test_duration = 60  # Run for 60 seconds
        
        while time.time() - start_time < test_duration:
            msg = consumer.consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Process message
            processed_data = consumer.process_message(msg.value().decode('utf-8'))
            
            if processed_data:
                success = consumer.store_to_mongodb(processed_data)
                if success:
                    logger.info(f"Test: Successfully processed event {processed_data['event_id']}")
            
        logger.info("Consumer test completed")
        
    except Exception as e:
        logger.error(f"Consumer test failed: {str(e)}")
    finally:
        consumer.close()

def run_streaming_test():
    """Run complete streaming test"""
    logger.info("=== Supply Chain Streaming Test ===")
    
    # Check environment variables
    required_vars = [
        'KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_API_KEY', 'KAFKA_API_SECRET',
        'MONGODB_URI', 'KAFKA_TOPIC'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {missing_vars}")
        logger.error("Please update your .env file with the required credentials")
        return
    
    logger.info("Environment variables validated ✓")
    
    # Start consumer in a separate thread
    consumer_thread = threading.Thread(target=test_consumer, daemon=True)
    consumer_thread.start()
    
    # Wait a bit for consumer to initialize
    time.sleep(5)
    
    # Run producer
    test_producer()
    
    # Wait for consumer to process messages
    logger.info("Waiting for consumer to process messages...")
    time.sleep(10)
    
    logger.info("=== Streaming test completed ===")

if __name__ == "__main__":
    run_streaming_test()