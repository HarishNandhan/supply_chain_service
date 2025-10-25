import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBManager:
    def __init__(self):
        """Initialize MongoDB connection and setup"""
        self.mongo_uri = os.getenv('MONGODB_URI')
        self.db_name = os.getenv('MONGODB_DATABASE', 'supply_chain_analytics')
        self.collection_name = os.getenv('MONGODB_COLLECTION', 'shipment_events')
        
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        
        logger.info(f"Connected to MongoDB: {self.db_name}.{self.collection_name}")
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        try:
            # Create indexes on commonly queried fields
            indexes = [
                ('event_id', 1),
                ('timestamp', 1),
                ('ingestion_timestamp', 1),
                ('performance_indicators.risk_classification', 1),
                ('performance_indicators.delay_probability', 1),
                ('temporal_features.hour', 1),
                ('temporal_features.day', 1),
                ('temporal_features.month', 1)
            ]
            
            for field, direction in indexes:
                self.collection.create_index([(field, direction)])
                logger.info(f"Created index on {field}")
            
            # Create compound index for time-based queries
            self.collection.create_index([
                ('timestamp', 1),
                ('performance_indicators.risk_classification', 1)
            ])
            logger.info("Created compound index on timestamp and risk_classification")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
    
    def get_collection_stats(self):
        """Get collection statistics"""
        try:
            stats = self.db.command("collStats", self.collection_name)
            return {
                'document_count': stats.get('count', 0),
                'storage_size': stats.get('storageSize', 0),
                'avg_document_size': stats.get('avgObjSize', 0),
                'total_indexes': stats.get('nindexes', 0)
            }
        except Exception as e:
            logger.error(f"Error getting collection stats: {str(e)}")
            return None
    
    def query_recent_events(self, limit=10):
        """Query recent events from the collection"""
        try:
            cursor = self.collection.find().sort('ingestion_timestamp', -1).limit(limit)
            events = list(cursor)
            logger.info(f"Retrieved {len(events)} recent events")
            return events
        except Exception as e:
            logger.error(f"Error querying recent events: {str(e)}")
            return []
    
    def query_high_risk_events(self, risk_threshold=1.0, limit=20):
        """Query high-risk events"""
        try:
            query = {
                'performance_indicators.risk_classification': {'$gte': risk_threshold}
            }
            cursor = self.collection.find(query).sort('timestamp', -1).limit(limit)
            events = list(cursor)
            logger.info(f"Retrieved {len(events)} high-risk events")
            return events
        except Exception as e:
            logger.error(f"Error querying high-risk events: {str(e)}")
            return []
    
    def aggregate_daily_stats(self):
        """Aggregate daily statistics"""
        try:
            pipeline = [
                {
                    '$group': {
                        '_id': {
                            'year': '$temporal_features.year',
                            'month': '$temporal_features.month',
                            'day': '$temporal_features.day'
                        },
                        'total_events': {'$sum': 1},
                        'avg_delay_probability': {'$avg': '$performance_indicators.delay_probability'},
                        'avg_risk_score': {'$avg': '$performance_indicators.risk_classification'},
                        'high_risk_count': {
                            '$sum': {
                                '$cond': [
                                    {'$gte': ['$performance_indicators.risk_classification', 1.0]},
                                    1, 0
                                ]
                            }
                        }
                    }
                },
                {'$sort': {'_id.year': -1, '_id.month': -1, '_id.day': -1}},
                {'$limit': 30}
            ]
            
            results = list(self.collection.aggregate(pipeline))
            logger.info(f"Generated daily stats for {len(results)} days")
            return results
        except Exception as e:
            logger.error(f"Error aggregating daily stats: {str(e)}")
            return []
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        logger.info("MongoDB connection closed")

def test_mongodb_connection():
    """Test MongoDB connection and operations"""
    logger.info("=== Testing MongoDB Connection ===")
    
    try:
        mongo_manager = MongoDBManager()
        
        # Test connection
        mongo_manager.client.admin.command('ping')
        logger.info("MongoDB connection successful ✓")
        
        # Create indexes
        mongo_manager.create_indexes()
        logger.info("Indexes created ✓")
        
        # Get collection stats
        stats = mongo_manager.get_collection_stats()
        if stats:
            logger.info(f"Collection stats: {stats}")
        
        # Test queries (will work after data is inserted)
        recent_events = mongo_manager.query_recent_events(limit=5)
        logger.info(f"Recent events query returned {len(recent_events)} results")
        
        mongo_manager.close()
        logger.info("MongoDB test completed successfully ✓")
        
    except Exception as e:
        logger.error(f"MongoDB test failed: {str(e)}")

if __name__ == "__main__":
    test_mongodb_connection()