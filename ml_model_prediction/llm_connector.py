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
    
    def generate_response(self, shipment_id, prediction_data, shipment_features=None):
        """Generate natural language response about shipment status with feature analysis"""
        import logging
        logger = logging.getLogger(__name__)
        
        delay_hours = prediction_data.get('predicted_delay_hours', 0)
        logger.info(f"Generating response for delay: {delay_hours} hours")
        
        # Determine status based on delay - ML model predicts in hours
        if delay_hours > 0.5:  # More than 30 minutes late
            status = "delayed"
            status_detail = f"delayed by {delay_hours:.2f} hours"
            emoji = "🔴"
        elif delay_hours < -0.5:  # More than 30 minutes early
            status = "early"
            status_detail = f"arriving {abs(delay_hours):.2f} hours early"
            emoji = "🟢"
        else:  # Within 30 minutes of scheduled time
            status = "on time"
            status_detail = f"on time (predicted variation: {delay_hours:.3f} hours)"
            emoji = "🟡"
        
        logger.info(f"Determined status: {status}")
        
        # Analyze contributing features only if shipment is delayed
        contributing_factors = []
        factors_text = ""
        if status == "delayed" and shipment_features:
            contributing_factors = analyze_feature_importance(shipment_features)
            if contributing_factors:
                factors_list = [f"- {factor[2]} (value: {factor[1]:.3f})" for factor in contributing_factors]
                factors_text = f"""
                
                Top contributing factors for this delay:
                {chr(10).join(factors_list)}
                """
        
        # Try LLM if available
        if self.client:
            prompt = f"""
            You are an AI assistant for a supply chain tracking system. Generate a professional and informative response about a shipment's delivery status.
            
            Shipment Details:
            - Shipment ID: {shipment_id}
            - ML Model Prediction: {delay_hours:.3f} hours
            - Status: {status} ({status_detail})
            
            {factors_text}
            
            Instructions:
            1. Start with a clear status statement about whether the shipment is delayed, on time, or early
            2. Provide the specific time prediction
            3. If the shipment is DELAYED and there are contributing factors, explain the top 3 factors that caused this delay in simple business terms
            4. If the shipment is ON TIME or EARLY, do NOT mention contributing factors - just provide a positive message
            5. Keep the tone professional but friendly
            6. End with a brief reassurance or next steps
            
            Format the response to be customer-friendly and informative.
            """
            
            try:
                response = self.client.generate_completion(
                    prompt=prompt,
                    temperature=0.3,
                    max_tokens=250
                )
                
                logger.info(f"Generated LLM response successfully")
                return response.strip()
                
            except Exception as e:
                logger.error(f"LLM generation failed: {str(e)}")
        
        # Fallback response if LLM is not available
        fallback_response = f"""
        {emoji} **Shipment Update: {shipment_id}**
        
        Your shipment is **{status_detail}**.
        """
        
        # Only show contributing factors for delayed shipments
        if status == "delayed" and contributing_factors:
            fallback_response += f"""
            
        **Key factors causing this delay:**
        """
            for i, (_, value, description) in enumerate(contributing_factors, 1):
                fallback_response += f"\n{i}. {description}"
        
        if status == "delayed":
            fallback_response += f"""
            
        Our AI model analyzed multiple operational factors to identify the cause of this delay. We'll continue monitoring your shipment and update you of any changes.
        """
        else:
            fallback_response += f"""
            
        Our AI model shows your shipment is progressing smoothly. We'll continue monitoring and keep you updated.
        """
        
        return fallback_response

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

def analyze_feature_importance(shipment_data):
    """Analyze which features are most likely contributing to the prediction"""
    # Define feature categories and their business meanings
    feature_meanings = {
        'traffic_congestion_level': 'Traffic Congestion Level',
        'weather_condition_severity': 'Weather Conditions',
        'port_congestion_level': 'Port Congestion',
        'loading_unloading_time': 'Loading/Unloading Time',
        'handling_equipment_availability': 'Equipment Availability',
        'lead_time_days': 'Lead Time',
        'disruption_likelihood_score': 'Disruption Risk Score',
        'is_rush_hour': 'Rush Hour Traffic',
        'is_weekend': 'Weekend Operations',
        'shipping_costs': 'Shipping Costs',
        'order_fulfillment_status': 'Order Fulfillment Status'
    }
    
    # Analyze feature values and identify potential contributors
    contributing_factors = []
    
    # High traffic congestion
    if shipment_data.get('traffic_congestion_level', 0) > 0.7:
        contributing_factors.append(('traffic_congestion_level', shipment_data['traffic_congestion_level'], 'High traffic congestion'))
    
    # Severe weather
    if abs(shipment_data.get('weather_condition_severity', 0)) > 0.5:
        contributing_factors.append(('weather_condition_severity', shipment_data['weather_condition_severity'], 'Severe weather conditions'))
    
    # Port congestion
    if shipment_data.get('port_congestion_level', 0) > 0.6:
        contributing_factors.append(('port_congestion_level', shipment_data['port_congestion_level'], 'High port congestion'))
    
    # Long loading time
    if shipment_data.get('loading_unloading_time', 0) > 2.0:
        contributing_factors.append(('loading_unloading_time', shipment_data['loading_unloading_time'], 'Extended loading/unloading time'))
    
    # Poor equipment availability
    if shipment_data.get('handling_equipment_availability', 1) < 0.4:
        contributing_factors.append(('handling_equipment_availability', shipment_data['handling_equipment_availability'], 'Limited equipment availability'))
    
    # High disruption risk
    if shipment_data.get('disruption_likelihood_score', 0) > 0.5:
        contributing_factors.append(('disruption_likelihood_score', shipment_data['disruption_likelihood_score'], 'High disruption risk'))
    
    # Rush hour impact
    if shipment_data.get('is_rush_hour', 0) == 1:
        contributing_factors.append(('is_rush_hour', 1, 'Rush hour traffic impact'))
    
    # Long lead time
    if shipment_data.get('lead_time_days', 0) > 3:
        contributing_factors.append(('lead_time_days', shipment_data['lead_time_days'], 'Extended lead time'))
    
    # Sort by impact (higher values generally mean more impact)
    contributing_factors.sort(key=lambda x: abs(x[1]) if isinstance(x[1], (int, float)) else 0, reverse=True)
    
    return contributing_factors[:3]  # Return top 3 factors

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
    
    # All columns except the excluded ones (matches your CREATE MODEL query)
    # Your model uses: * EXCEPT(`timestamp`, _id, event_id, label_delay_hours_raw)
    feature_select = """
      label_delay_hours_capped,
      label_delay_hours,
      is_delayed,
      hour_of_day, day_of_week, month_of_year, iso_week,
      is_weekend, is_rush_hour,
      sin_hour, cos_hour, sin_month, cos_month,
      gps_latitude, gps_longitude,
      region4, region5,
      traffic_congestion_level, loading_unloading_time,
      handling_equipment_availability, order_fulfillment_status,
      weather_condition_severity, port_congestion_level, shipping_costs,
      lead_time_days, disruption_likelihood_score,
      cong_x_loading, traffic_x_weather, load_x_equipment, port_x_traffic,
      traffic_x_disruption, leadtime_x_port, weather_x_leadtime,
      traffic_bucket, loading_time_bucket, handling_availability_bucket,
      weather_bucket, port_congestion_bucket, lead_time_bucket,
      risk_classification,
      avg_delay_region4_hour, avg_delay_region4_day, avg_delay_region4_week,
      is_severe_delay
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
      predicted_label_delay_hours_capped AS predicted_delay_hours
    FROM ML.PREDICT(
      MODEL `{project_id}.{dataset}.{model_name}`,
      (SELECT * EXCEPT(label_delay_hours_capped) FROM shipment_data)
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
        
        # Use original prediction (no amplification needed with new model)
        logger.info(f"Using original ML prediction: {predicted_delay}")
        
        # Initialize LLM connector and generate response with feature analysis
        llm_connector = LLMConnector()
        response = llm_connector.generate_response(
            shipment_id, 
            {'predicted_delay_hours': predicted_delay},
            shipment_features=shipment_data
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
    try:
        llm_connector = LLMConnector()
        
        if not llm_connector.client:
            print("❌ LLM client not initialized")
            return False
        
        response = llm_connector.client.generate_completion(
            prompt="Write a short greeting for a supply chain chatbot.",
            temperature=0.7,
            max_tokens=100
        )
        print("✅ LLM Connection Test Successful!")
        print(f"Response: {response}")
        return True
    except Exception as e:
        print(f"❌ LLM Connection Test Failed: {str(e)}")
        return False

def test_enhanced_prediction():
    """Test the enhanced prediction with feature analysis"""
    try:
        # Sample shipment data for testing
        sample_data = {
            'traffic_congestion_level': 0.8,
            'weather_condition_severity': 0.6,
            'port_congestion_level': 0.4,
            'loading_unloading_time': 2.5,
            'handling_equipment_availability': 0.3,
            'is_rush_hour': 1,
            'lead_time_days': 4
        }
        
        llm_connector = LLMConnector()
        
        # Test 1: Delayed shipment (should show factors)
        print("Test 1 - Delayed Shipment:")
        response1 = llm_connector.generate_response(
            "DELAYED123",
            {'predicted_delay_hours': 1.5},
            shipment_features=sample_data
        )
        print(response1)
        
        print("\n" + "-" * 40)
        
        # Test 2: On-time shipment (should NOT show factors)
        print("Test 2 - On-Time Shipment:")
        response2 = llm_connector.generate_response(
            "ONTIME456",
            {'predicted_delay_hours': 0.1},
            shipment_features=sample_data
        )
        print(response2)
        
        print("\n" + "-" * 40)
        
        # Test 3: Early shipment (should NOT show factors)
        print("Test 3 - Early Shipment:")
        response3 = llm_connector.generate_response(
            "EARLY789",
            {'predicted_delay_hours': -0.8},
            shipment_features=sample_data
        )
        print(response3)
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced Prediction Test Failed: {str(e)}")
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
    print("🧪 Testing LLM Generation...")
    print("=" * 50)
    
    # Test 1: Basic LLM connection
    print("1. Testing basic LLM connection:")
    test_llm_connection()
    
    print("\n" + "=" * 50)
    
    # Test 2: Enhanced prediction with features
    print("2. Testing enhanced prediction with feature analysis:")
    test_enhanced_prediction()