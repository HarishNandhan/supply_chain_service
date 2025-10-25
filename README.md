# Supply Chain Analytics - Kafka Streaming Setup

## Overview
This module implements real-time streaming of supply chain data using Confluent Kafka and MongoDB storage.

## Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Update the `.env` file with your credentials:

```env
# Confluent Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=your_confluent_bootstrap_servers
KAFKA_API_KEY=your_confluent_api_key
KAFKA_API_SECRET=your_confluent_api_secret
KAFKA_TOPIC=supply-chain-events

# MongoDB Configuration
MONGODB_URI=your_mongodb_connection_string
MONGODB_DATABASE=supply_chain_analytics
MONGODB_COLLECTION=shipment_events
```

### 3. Create Kafka Topic
Create a topic named `supply-chain-events` in your Confluent Cloud console.

## Usage

### Test MongoDB Connection
```bash
python mongodb_utils.py
```

### Run Producer (Stream Data)
```bash
python kafka_producer.py
```

### Run Consumer (Store to MongoDB)
```bash
python kafka_consumer.py
```

### Run Complete Test
```bash
python test_streaming.py
```

## Architecture

### Data Flow
1. **Producer** reads CSV data and streams to Kafka topic
2. **Consumer** processes messages and stores to MongoDB
3. **MongoDB** stores structured events with indexes for fast queries

### Message Structure
```json
{
  "event_id": "evt_1234567890_hash",
  "timestamp": "2021-01-01 00:00:00",
  "vehicle_data": {
    "gps_latitude": 0.34,
    "gps_longitude": 0.75,
    "eta_variation_hours": 0.93
  },
  "operational_metrics": {
    "traffic_congestion_level": 0.27,
    "loading_unloading_time": 1.71,
    "handling_equipment_availability": 0.55,
    "order_fulfillment_status": 0.46
  },
  "external_factors": {
    "weather_condition_severity": -0.39,
    "port_congestion_level": -0.83,
    "shipping_costs": -0.01
  },
  "performance_indicators": {
    "lead_time_days": -0.69,
    "disruption_likelihood_score": -1.07,
    "delay_probability": 0.57,
    "risk_classification": 2.14,
    "delivery_time_deviation": 0.95
  },
  "temporal_features": {
    "hour": -1,
    "day": -1,
    "month": -1,
    "weekday": 0
  },
  "ingestion_timestamp": "2024-01-01T12:00:00.000Z"
}
```

## Security Features
- Environment variables for sensitive credentials
- SASL_SSL security protocol for Kafka
- MongoDB connection with authentication
- Proper error handling and logging

## Next Steps
Once streaming is working:
1. Add Redis caching layer
2. Implement real-time analytics
3. Build prediction models
4. Create API endpoints
5. Add Streamlit dashboard