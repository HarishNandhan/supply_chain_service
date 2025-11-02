import os
import re
from dotenv import load_dotenv
from euriai import EuriaiClient
from google.cloud import bigquery
import pandas as pd

# Load environment variables
load_dotenv(dotenv_path="../configs/.env")

class LLMConnector:
    def __init__(self):
        """Initialize the EURI AI client"""
        import logging
        logger = logging.getLogger(__name__)
        
        self.api_key = os.getenv("EURI_API_KEY")
        self.model_name = os.getenv("EURI_MODEL_NAME", "gpt-4.1-nano")
        
        logger.info(f"Initializing LLM with model: {self.model_name}")
        
        if not self.api_key:
            logger.error("EURI_API_KEY not found in environment variables")
            raise ValueError("EURI_API_KEY not found in environment variables")
        
        try:
            self.client = EuriaiClient(
                api_key=self.api_key,
                model=self.model_name
            )
            logger.info("EURI AI client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize EURI AI client: {str(e)}")
            self.client = None
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
        self.project_id = os.getenv("BIGQUERY_PROJECT")
        self.dataset = os.getenv("BIGQUERY_DATASET")
        self.test_table = os.getenv("BQ_TEST_TBL", "test_table")
        self.model_name_bq = os.getenv("BQ_MODEL", "eta_delay_dnn")
    
    def extract_shipment_id(self, user_query):
        """Extract shipment ID from user query using LLM or regex fallback"""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Extracting shipment ID from: {user_query}")
        
        # Try LLM first if available
        if self.client:
            prompt = f"""
            Extract the shipment ID from the following user query. The shipment ID could be in various formats like alphanumeric strings, numbers, or mixed characters.
            
            User query: "{user_query}"
            
            Please respond with ONLY the shipment ID if found, or "NOT_FOUND" if no shipment ID is present.
            
            Examples:
            - "Where is my shipment 68f807725b30835d5d60808?" -> 68f807725b30835d5d60808
            - "Status of ABC123" -> ABC123
            - "How are you?" -> NOT_FOUND
            """
            
            try:
                response = self.client.generate_completion(
                    prompt=prompt,
                    temperature=0.1,
                    max_tokens=50
                )
                
                shipment_id = response.strip()
                logger.info(f"LLM extracted ID: {shipment_id}")
                return shipment_id if shipment_id != "NOT_FOUND" else None
                
            except Exception as e:
                logger.error(f"LLM extraction failed: {str(e)}")
        
        # Fallback to regex extraction
        logger.info("Using regex fallback for ID extraction")
        return self._extract_id_regex(user_query)
    
    def _extract_id_regex(self, user_query):
        """Fallback method to extract ID using regex"""
        # Look for alphanumeric strings that could be shipment IDs
        patterns = [
            r'\b[a-fA-F0-9]{24}\b',  # MongoDB ObjectId pattern
            r'\b[A-Za-z0-9]{10,}\b',  # General alphanumeric ID
            r'\b\d{6,}\b'  # Numeric ID
        ]
        
        for pattern in patterns:
            match = re.search(pattern, user_query)
            if match:
                return match.group()
        
        return None
    
    def generate_response(self, shipment_id, prediction_data):
        """Generate natural language response about shipment status"""
        import logging
        logger = logging.getLogger(__name__)
        
        delay_hours = prediction_data.get('predicted_delay_hours', 0)
        logger.info(f"Generating response for delay: {delay_hours} hours")
        
        # Determine status based on delay - ML model predicts in hours
        if delay_hours > 0.5:  # More than 30 minutes late
            status = f"delayed by {delay_hours:.2f} hours"
            emoji = "🔴"
        elif delay_hours < -0.5:  # More than 30 minutes early
            status = f"arriving {abs(delay_hours):.2f} hours early"
            emoji = "🟢"
        else:  # Within 30 minutes of scheduled time
            status = f"on time (predicted delay: {delay_hours:.3f} hours)"
            emoji = "🟡"
        
        logger.info(f"Determined status: {status}")
        
        # Create detailed response with debug info
        debug_info = f"""
        **🔍 ML Prediction Details:**
        - Shipment ID: {shipment_id}
        - Raw ML prediction: {delay_hours:.6f} hours
        - Status determination: {status}
        - Threshold logic: >0.5h = delayed, <-0.5h = early, else on time
        - Model output type: {type(delay_hours)}
        """
        
        # Create a good fallback response first
        if delay_hours > 0.5:
            fallback_response = f"""
            {emoji} **Shipment Alert: {shipment_id}**
            
            Your shipment is currently **delayed by {delay_hours:.2f} hours**.
            
            Our AI model predicts this delay based on current traffic conditions, weather, and operational factors. We'll continue monitoring and update you if anything changes.
            """
        elif delay_hours < -0.5:
            fallback_response = f"""
            {emoji} **Great News: {shipment_id}**
            
            Your shipment is **arriving {abs(delay_hours):.2f} hours early**!
            
            Better than expected conditions have allowed for faster delivery. Your package should arrive ahead of schedule.
            """
        else:
            fallback_response = f"""
            {emoji} **Shipment Status: {shipment_id}**
            
            Your shipment is **on time** (ML predicts {delay_hours:.3f} hours variation).
            
            The AI model shows minimal deviation from scheduled delivery time.
            """
        
        # Try LLM if available
        if self.client:
            prompt = f"""
            Generate a friendly and informative response about a shipment status.
            
            Shipment ID: {shipment_id}
            Predicted delay: {delay_hours:.2f} hours
            Status: {status}
            
            Create a natural response that includes:
            1. The shipment ID
            2. The current status (delayed/on time/early)
            3. A brief explanation
            
            Keep it concise and customer-friendly.
            """
            
            try:
                response = self.client.generate_completion(
                    prompt=prompt,
                    temperature=0.3,
                    max_tokens=150
                )
                
                # Add debug info to response
                full_response = f"{response.strip()}\n\n{debug_info}"
                logger.info(f"Generated LLM response successfully")
                return full_response
                
            except Exception as e:
                logger.error(f"LLM generation failed: {str(e)}")
        
        # Use fallback response with debug info
        fallback = f"{fallback_response}\n\n{debug_info}"
        if not self.client:
            fallback += "\n\n**Note:** Using built-in response (LLM not available)."
        else:
            fallback += "\n\n**Note:** LLM response generation failed, using fallback."
        
        return fallback

def get_shipment_data(shipment_id):
    """Get shipment data from BigQuery shipment_metrics table"""
    load_dotenv(dotenv_path="../configs/.env")
    
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    project_id = os.getenv("BIGQUERY_PROJECT")
    dataset = os.getenv("BIGQUERY_DATASET")
    
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset}.shipment_metrics`
    WHERE _id = '{shipment_id}'
    LIMIT 1
    """
    
    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            return None
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Error fetching shipment data: {str(e)}")
        return None

def predict_shipment_delay(shipment_id):
    """Main function to predict shipment delay and generate response"""
    import logging
    logger = logging.getLogger(__name__)
    
    load_dotenv(dotenv_path="../configs/.env")
    
    logger.info(f"Starting prediction for shipment ID: {shipment_id}")
    
    # Get shipment data
    shipment_data = get_shipment_data(shipment_id)
    if not shipment_data:
        logger.warning(f"No shipment data found for ID: {shipment_id}")
        return f"Sorry, I couldn't find shipment {shipment_id} in our system. Please check the shipment ID and try again."
    
    logger.info(f"Found shipment data with {len(shipment_data)} fields")
    
    # Initialize BigQuery client for ML prediction
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    project_id = os.getenv("BIGQUERY_PROJECT")
    dataset = os.getenv("BIGQUERY_DATASET")
    model_name = os.getenv("BQ_MODEL", "eta_delay_dnn")
    
    logger.info(f"Using ML model: {project_id}.{dataset}.{model_name}")
    
    # Feature columns for the ML model (same as in test_predict.py)
    feature_select = """
      gps_latitude, gps_longitude,
      traffic_congestion_level, loading_unloading_time,
      handling_equipment_availability, order_fulfillment_status,
      weather_condition_severity, port_congestion_level, shipping_costs,
      lead_time_days, disruption_likelihood_score,
      hour_of_day, day_of_week, month_of_year, iso_week,
      is_weekend, is_rush_hour,
      sin_hour, cos_hour, sin_month, cos_month,
      cong_x_loading, traffic_x_weather, load_x_equipment, port_x_traffic,
      IFNULL(avg_delay_region4_hour, 0.0) AS avg_delay_region4_hour,
      IFNULL(avg_delay_region5_hour, 0.0) AS avg_delay_region5_hour,
      region4, region5,
      traffic_bucket, loading_time_bucket, handling_availability_bucket,
      weather_bucket, port_congestion_bucket, lead_time_bucket,
      risk_classification
    """
    
    # ML prediction query
    prediction_query = f"""
    WITH shipment_data AS (
      SELECT {feature_select}
      FROM `{project_id}.{dataset}.shipment_metrics`
      WHERE _id = '{shipment_id}'
      LIMIT 1
    )
    SELECT
      predicted_label_delay_hours AS predicted_delay_hours
    FROM ML.PREDICT(
      MODEL `{project_id}.{dataset}.{model_name}`,
      (SELECT * FROM shipment_data)
    )
    """
    
    logger.info("Executing ML prediction query...")
    
    try:
        # Get ML prediction
        logger.info(f"Executing prediction query: {prediction_query}")
        prediction_df = client.query(prediction_query).to_dataframe()
        
        if prediction_df.empty:
            logger.error("ML prediction returned empty results")
            return f"Sorry, I couldn't generate a prediction for shipment {shipment_id}. The ML model might not be available."
        
        # Log the full prediction result for debugging
        logger.info(f"Prediction DataFrame: {prediction_df.to_dict()}")
        
        predicted_delay = prediction_df.iloc[0]['predicted_delay_hours']
        logger.info(f"Raw ML prediction result: {predicted_delay} (type: {type(predicted_delay)})")
        
        # Check if prediction is valid
        if predicted_delay is None or pd.isna(predicted_delay):
            logger.warning("Prediction returned None/NaN value")
            predicted_delay = 0.0
        
        # Convert to float to ensure it's a number
        try:
            predicted_delay = float(predicted_delay)
            logger.info(f"Converted prediction to float: {predicted_delay}")
        except (ValueError, TypeError) as e:
            logger.error(f"Could not convert prediction to float: {e}")
            predicted_delay = 0.0
        
        # Initialize LLM connector and generate response
        llm_connector = LLMConnector()
        response = llm_connector.generate_response(
            shipment_id, 
            {'predicted_delay_hours': predicted_delay}
        )
        
        logger.info(f"Generated response for shipment {shipment_id}")
        return response
        
    except Exception as e:
        logger.error(f"Error in prediction for {shipment_id}: {str(e)}")
        logger.error(f"Query that failed: {prediction_query}")
        return f"Sorry, I encountered an error while analyzing shipment {shipment_id}. Error: {str(e)}"

# Test function
def test_llm_connection():
    """Test the EURI AI connection"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Testing LLM connection...")
        llm_connector = LLMConnector()
        
        if not llm_connector.client:
            logger.error("LLM client not initialized")
            return False
        
        response = llm_connector.client.generate_completion(
            prompt="Write a short greeting for a supply chain chatbot.",
            temperature=0.7,
            max_tokens=100
        )
        logger.info("LLM Connection Test Successful!")
        logger.info(f"Response: {response}")
        print("LLM Connection Test Successful!")
        print(f"Response: {response}")
        return True
    except Exception as e:
        logger.error(f"LLM Connection Test Failed: {str(e)}")
        print(f"LLM Connection Test Failed: {str(e)}")
        return False

def test_euri_api_directly():
    """Test EURI API directly with minimal setup"""
    import logging
    from dotenv import load_dotenv
    
    logger = logging.getLogger(__name__)
    load_dotenv(dotenv_path="../configs/.env")
    
    api_key = os.getenv("EURI_API_KEY")
    model_name = os.getenv("EURI_MODEL_NAME", "gpt-4.1-nano")
    
    logger.info(f"Testing EURI API directly with key: {api_key[:10]}...")
    
    try:
        client = EuriaiClient(api_key=api_key, model=model_name)
        response = client.generate_completion(
            prompt="Say hello",
            temperature=0.1,
            max_tokens=20
        )
        logger.info(f"Direct EURI test successful: {response}")
        return True, response
    except Exception as e:
        logger.error(f"Direct EURI test failed: {str(e)}")
        return False, str(e)

def test_ml_model_predictions():
    """Test ML model with multiple shipments to check for variation"""
    import logging
    from dotenv import load_dotenv
    
    logger = logging.getLogger(__name__)
    load_dotenv(dotenv_path="../configs/.env")
    
    try:
        client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
        project_id = os.getenv("BIGQUERY_PROJECT")
        dataset = os.getenv("BIGQUERY_DATASET")
        model_name = os.getenv("BQ_MODEL", "eta_delay_dnn")
        
        # Get multiple sample shipments
        sample_query = f"""
        SELECT _id, label_delay_hours
        FROM `{project_id}.{dataset}.shipment_metrics`
        WHERE _id IS NOT NULL AND label_delay_hours IS NOT NULL
        ORDER BY RAND()
        LIMIT 5
        """
        
        samples_df = client.query(sample_query).to_dataframe()
        
        if samples_df.empty:
            return False, "No sample data found"
        
        results = []
        
        for _, row in samples_df.iterrows():
            shipment_id = row['_id']
            actual_delay = row['label_delay_hours']
            
            # Test ML prediction for this shipment
            feature_select = """
              gps_latitude, gps_longitude,
              traffic_congestion_level, loading_unloading_time,
              handling_equipment_availability, order_fulfillment_status,
              weather_condition_severity, port_congestion_level, shipping_costs,
              lead_time_days, disruption_likelihood_score,
              hour_of_day, day_of_week, month_of_year, iso_week,
              is_weekend, is_rush_hour,
              sin_hour, cos_hour, sin_month, cos_month,
              cong_x_loading, traffic_x_weather, load_x_equipment, port_x_traffic,
              IFNULL(avg_delay_region4_hour, 0.0) AS avg_delay_region4_hour,
              IFNULL(avg_delay_region5_hour, 0.0) AS avg_delay_region5_hour,
              region4, region5,
              traffic_bucket, loading_time_bucket, handling_availability_bucket,
              weather_bucket, port_congestion_bucket, lead_time_bucket,
              risk_classification
            """
            
            prediction_query = f"""
            WITH shipment_data AS (
              SELECT {feature_select}
              FROM `{project_id}.{dataset}.shipment_metrics`
              WHERE _id = '{shipment_id}'
              LIMIT 1
            )
            SELECT
              predicted_label_delay_hours AS predicted_delay_hours
            FROM ML.PREDICT(
              MODEL `{project_id}.{dataset}.{model_name}`,
              (SELECT * FROM shipment_data)
            )
            """
            
            try:
                pred_df = client.query(prediction_query).to_dataframe()
                if not pred_df.empty:
                    predicted_delay = pred_df.iloc[0]['predicted_delay_hours']
                    results.append({
                        'shipment_id': shipment_id,
                        'actual_delay': actual_delay,
                        'predicted_delay': predicted_delay,
                        'difference': abs(actual_delay - predicted_delay)
                    })
                else:
                    results.append({
                        'shipment_id': shipment_id,
                        'actual_delay': actual_delay,
                        'predicted_delay': 'NO_PREDICTION',
                        'difference': 'N/A'
                    })
            except Exception as e:
                results.append({
                    'shipment_id': shipment_id,
                    'actual_delay': actual_delay,
                    'predicted_delay': f'ERROR: {str(e)}',
                    'difference': 'N/A'
                })
        
        return True, results
        
    except Exception as e:
        logger.error(f"ML model test failed: {str(e)}")
        return False, str(e)

if __name__ == "__main__":
    # Test the connection
    test_llm_connection()
    
    # Test shipment prediction
    test_id = "68f807725b30835d5d60808"  # Replace with actual ID from your test_table
    result = predict_shipment_delay(test_id)
    print(f"\nTest Prediction Result: {result}")