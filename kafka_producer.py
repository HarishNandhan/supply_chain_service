import os
import json
import pandas as pd
import time
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupplyChainProducer:
    def __init__(self):
        """Initialize Kafka producer with Confluent Cloud configuration"""
        self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET'),
            'client.id': 'supply-chain-producer'
        }
        
        self.topic = os.getenv('KAFKA_TOPIC', 'supply-chain-events')
        self.producer = Producer(self.config)
        self.batch_size = int(os.getenv('BATCH_SIZE', 100))
        self.streaming_interval = int(os.getenv('STREAMING_INTERVAL', 1))
        
        logger.info("Kafka producer initialized successfully")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def prepare_message(self, row):
        """Convert DataFrame row to JSON message"""
        message = {
            'event_id': f"evt_{int(time.time() * 1000)}_{hash(str(row.to_dict()))}",
            'timestamp': row['timestamp'],
            'vehicle_data': {
                'gps_latitude': float(row['vehicle_gps_latitude']),
                'gps_longitude': float(row['vehicle_gps_longitude']),
                'eta_variation_hours': float(row['eta_variation_hours'])
            },
            'operational_metrics': {
                'traffic_congestion_level': float(row['traffic_congestion_level']),
                'loading_unloading_time': float(row['loading_unloading_time']),
                'handling_equipment_availability': float(row['handling_equipment_availability']),
                'order_fulfillment_status': float(row['order_fulfillment_status'])
            },
            'external_factors': {
                'weather_condition_severity': float(row['weather_condition_severity']),
                'port_congestion_level': float(row['port_congestion_level']),
                'shipping_costs': float(row['shipping_costs'])
            },
            'performance_indicators': {
                'lead_time_days': float(row['lead_time_days']),
                'disruption_likelihood_score': float(row['disruption_likelihood_score']),
                'delay_probability': float(row['delay_probability']),
                'risk_classification': float(row['risk_classification']),
                'delivery_time_deviation': float(row['delivery_time_deviation'])
            },
            'temporal_features': {
                'hour': int(row['hour']),
                'day': int(row['day']),
                'month': int(row['month']),
                'weekday': int(row['weekday'])
            },
            'ingestion_timestamp': datetime.utcnow().isoformat()
        }
        return message
    
    def stream_data(self, csv_file_path='data/cleaned_supply_chain_data.csv'):
        """Stream supply chain data to Kafka topic"""
        try:
            # Load the dataset
            df = pd.read_csv(csv_file_path)
            logger.info(f"Loaded {len(df)} records from {csv_file_path}")
            
            # Stream data in batches
            for i in range(0, len(df), self.batch_size):
                batch = df.iloc[i:i + self.batch_size]
                
                for _, row in batch.iterrows():
                    message = self.prepare_message(row)
                    
                    # Produce message to Kafka
                    self.producer.produce(
                        topic=self.topic,
                        key=message['event_id'],
                        value=json.dumps(message),
                        callback=self.delivery_report
                    )
                
                # Flush messages and wait
                self.producer.flush()
                logger.info(f"Streamed batch {i//self.batch_size + 1}, records {i+1}-{min(i+self.batch_size, len(df))}")
                
                # Wait before next batch
                time.sleep(self.streaming_interval)
            
            logger.info("Data streaming completed successfully")
            
        except Exception as e:
            logger.error(f"Error streaming data: {str(e)}")
            raise
    
    def close(self):
        """Close the producer"""
        self.producer.flush()
        logger.info("Producer closed")

if __name__ == "__main__":
    producer = SupplyChainProducer()
    try:
        producer.stream_data()
    finally:
        producer.close()