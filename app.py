import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
import os
from dotenv import load_dotenv
from ml_model_prediction.llm_connector import LLMConnector, predict_shipment_delay
import re
import logging
import sys

# Load environment variables
load_dotenv(dotenv_path="configs/.env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('dashboard.log')
    ]
)
logger = logging.getLogger(__name__)

# Configure page
st.set_page_config(
    page_title="Supply Chain Analytics Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize BigQuery client
@st.cache_resource
def init_bigquery_client():
    return bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))

# Load aggregated metrics from shipment_metrics table
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_shipment_metrics():
    client = init_bigquery_client()
    
    try:
        # Calculate metrics directly from shipment_metrics table
        query = f"""
        SELECT
            COUNT(*) as total_shipments,
            AVG(label_delay_hours_capped) as avg_eta_variation_hours,
            AVG(lead_time_days) as avg_lead_time_days,
            AVG(disruption_likelihood_score) as avg_delay_probability,
            STDDEV(label_delay_hours_capped) as std_eta_variation,
            MIN(label_delay_hours_capped) as min_delay,
            MAX(label_delay_hours_capped) as max_delay,
            -- Risk classification counts (assuming string values)
            COUNTIF(risk_classification = '1') as high_risk_count,
            COUNTIF(risk_classification = '2') as medium_risk_count,
            COUNTIF(risk_classification = '3') as low_risk_count,
            -- Additional metrics
            AVG(traffic_congestion_level) as avg_traffic_congestion,
            AVG(weather_condition_severity) as avg_weather_severity,
            AVG(port_congestion_level) as avg_port_congestion,
            -- Time-based metrics
            COUNTIF(is_weekend = 1) as weekend_shipments,
            COUNTIF(is_rush_hour = 1) as rush_hour_shipments
        FROM `{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.shipment_metrics`
        WHERE label_delay_hours_capped IS NOT NULL
        """
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error loading metrics data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_detailed_shipments():
    client = init_bigquery_client()
    query = f"""
    SELECT 
        _id,
        event_id,
        timestamp,
        gps_latitude,
        gps_longitude,
        label_delay_hours_capped as eta_variation_hours,
        lead_time_days,
        disruption_likelihood_score as delay_probability,
        risk_classification,
        traffic_congestion_level,
        weather_condition_severity,
        port_congestion_level,
        loading_unloading_time,
        handling_equipment_availability,
        order_fulfillment_status,
        shipping_costs,
        hour_of_day,
        day_of_week,
        month_of_year,
        is_weekend,
        is_rush_hour,
        traffic_bucket,
        weather_bucket,
        port_congestion_bucket,
        region4,
        region5
    FROM `{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.shipment_metrics`
    WHERE label_delay_hours_capped IS NOT NULL
    ORDER BY timestamp DESC
    LIMIT 1000
    """
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error loading detailed data: {str(e)}")
        return pd.DataFrame()

def main():
    st.title("📦 Supply Chain Analytics Dashboard")
    
    # Add debug info in sidebar
    with st.sidebar:
        st.header("🔧 Debug Info")
        st.write(f"**Project:** {os.getenv('BIGQUERY_PROJECT')}")
        st.write(f"**Dataset:** {os.getenv('BIGQUERY_DATASET')}")
        
        if st.button("🧪 Test Data Connection"):
            with st.spinner("Testing connection..."):
                try:
                    client = init_bigquery_client()
                    query = f"""
                    SELECT 
                        COUNT(*) as count,
                        COUNT(DISTINCT risk_classification) as risk_levels,
                        MIN(label_delay_hours_capped) as min_delay,
                        MAX(label_delay_hours_capped) as max_delay
                    FROM `{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.shipment_metrics`
                    """
                    result = client.query(query).to_dataframe()
                    row = result.iloc[0]
                    st.success(f"✅ Found {row['count']} records in shipment_metrics")
                    st.info(f"📊 Risk levels: {row['risk_levels']}, Delay range: {row['min_delay']:.2f}h to {row['max_delay']:.2f}h")
                except Exception as e:
                    st.error(f"❌ Connection failed: {str(e)}")
    
    # Create tabs for better organization
    tab1, tab2 = st.tabs(["💬 AI Shipment Assistant", "📊 Analytics Dashboard"])
    
    with tab1:
        st.header("� AI Shipcment Assistant")
        
        # Welcome message
        if "messages" not in st.session_state:
            st.session_state.messages = []
            # Add welcome message
            welcome_msg = """
            👋 **Welcome to the AI Shipment Assistant!**
            
            I can help you track your shipments and predict delivery times using our advanced ML models.
            
            Just ask me about any shipment by providing the shipment ID!
            """
            st.session_state.messages.append({"role": "assistant", "content": welcome_msg})
        
        # Chat container with fixed height
        chat_container = st.container()
        
        with chat_container:
            # Display chat messages
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])
        
        # Chat input at the bottom
        if prompt := st.chat_input("💬 Ask about your shipment (e.g., 'Where is my shipment 68f807725b30835d5d60808?')"):
            # Add user message to chat history
            st.session_state.messages.append({"role": "user", "content": prompt})
            
            # Process the query
            with st.chat_message("assistant"):
                with st.spinner("🔍 Analyzing your shipment..."):
                    try:
                        # Initialize LLM connector
                        llm_connector = LLMConnector()
                        
                        # Extract shipment ID from user query
                        shipment_id = llm_connector.extract_shipment_id(prompt)
                        
                        if shipment_id:
                            st.info(f"🔍 Found shipment ID: {shipment_id}")
                            
                            # Add debug logging
                            with st.expander("🔧 Debug Logs", expanded=False):
                                debug_container = st.empty()
                                
                                # Log the process
                                debug_logs = []
                                debug_logs.append(f"✅ Extracted shipment ID: {shipment_id}")
                                debug_container.text("\n".join(debug_logs))
                                
                                # Get prediction with detailed logging
                                try:
                                    debug_logs.append("🔍 Fetching shipment data from BigQuery...")
                                    debug_container.text("\n".join(debug_logs))
                                    
                                    # Import the function to get shipment data directly
                                    from ml_model_prediction.llm_connector import get_shipment_data
                                    shipment_data = get_shipment_data(shipment_id)
                                    
                                    if shipment_data:
                                        debug_logs.append(f"✅ Found shipment data: {len(shipment_data)} fields")
                                        debug_logs.append(f"📊 Sample data: {dict(list(shipment_data.items())[:3])}")
                                        debug_container.text("\n".join(debug_logs))
                                        
                                        debug_logs.append("🤖 Running ML prediction...")
                                        debug_container.text("\n".join(debug_logs))
                                        
                                        response = predict_shipment_delay(shipment_id)
                                        debug_logs.append(f"✅ Prediction completed")
                                        debug_container.text("\n".join(debug_logs))
                                    else:
                                        debug_logs.append(f"❌ No shipment data found for ID: {shipment_id}")
                                        debug_container.text("\n".join(debug_logs))
                                        response = f"Shipment {shipment_id} not found in database."
                                        
                                except Exception as debug_e:
                                    debug_logs.append(f"❌ Debug error: {str(debug_e)}")
                                    debug_container.text("\n".join(debug_logs))
                                    response = predict_shipment_delay(shipment_id)
                            
                            st.markdown(response)
                            
                            # Add assistant response to chat history
                            st.session_state.messages.append({"role": "assistant", "content": response})
                        else:
                            error_msg = """
                            ❌ **Shipment ID not found**
                            
                            I couldn't find a shipment ID in your message. Please provide a valid shipment ID.
                            
                            **Examples:**
                            - "Where is my shipment 68f807725b30835d5d60808?"
                            - "Status of ABC123"
                            - "Track shipment 12345"
                            """
                            st.markdown(error_msg)
                            st.session_state.messages.append({"role": "assistant", "content": error_msg})
                    
                    except Exception as e:
                        error_msg = f"""
                        ⚠️ **Processing Error**
                        
                        Sorry, I encountered an error while processing your request:
                        
                        `{str(e)}`
                        
                        Please try again or contact support if the issue persists.
                        """
                        st.markdown(error_msg)
                        st.session_state.messages.append({"role": "assistant", "content": error_msg})
            
            # Rerun to show the new messages
            st.rerun()
        
        # Sidebar with examples and controls
        st.markdown("---")
        
        # Example queries in an expander
        with st.expander("💡 Example Queries", expanded=True):
            st.markdown("""
            **Try asking:**
            - "Where is my shipment 68f807725b30835d5d60808?"
            - "What's the status of shipment ABC123?"
            - "Is my shipment 12345 delayed?"
            - "When will shipment XYZ789 arrive?"
            - "Track order 987654321"
            - "Delivery time for shipment DEF456"
            """)
        
        # Chat controls
        col_clear, col_info, col_debug = st.columns(3)
        with col_clear:
            if st.button("🗑️ Clear Chat", use_container_width=True):
                st.session_state.messages = []
                st.rerun()
        
        with col_info:
            if st.button("ℹ️ How it works", use_container_width=True):
                info_msg = """
                **🤖 How the AI Assistant Works:**
                
                1. **Extract ID**: I use AI to find shipment IDs in your message
                2. **Fetch Data**: I look up your shipment in our database
                3. **ML Prediction**: I use our trained model to predict delays
                4. **Smart Response**: I generate a natural language response
                
                **🎯 Powered by:**
                - EURI AI for natural language processing
                - BigQuery ML for delay predictions
                - Real-time shipment data
                """
                st.session_state.messages.append({"role": "assistant", "content": info_msg})
                st.rerun()
        
        with col_debug:
            if st.button("🔧 Test Sample", use_container_width=True):
                # Test with a sample shipment ID
                test_msg = """
                **🧪 Testing with sample data...**
                
                Let me test the system with a sample shipment ID from your database.
                """
                st.session_state.messages.append({"role": "assistant", "content": test_msg})
                
                # Get a sample ID from the database
                try:
                    client = init_bigquery_client()
                    query = f"""
                    SELECT _id, label_delay_hours_capped
                    FROM `{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.shipment_metrics`
                    WHERE _id IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """
                    result = client.query(query).to_dataframe()
                    if not result.empty:
                        sample_id = result.iloc[0]['_id']
                        actual_delay = result.iloc[0]['label_delay_hours_capped']
                        
                        test_response = f"""
                        **📊 Sample Test Results:**
                        - Sample ID: {sample_id}
                        - Actual delay in data: {actual_delay:.3f} hours
                        
                        Now testing prediction...
                        """
                        st.session_state.messages.append({"role": "assistant", "content": test_response})
                        
                        # Test the prediction
                        prediction_result = predict_shipment_delay(sample_id)
                        st.session_state.messages.append({"role": "assistant", "content": prediction_result})
                        
                        # Also show prediction statistics
                        try:
                            stats_query = f"""
                            SELECT 
                                COUNT(*) as total_predictions,
                                AVG(label_delay_hours_capped) as avg_actual_delay,
                                STDDEV(label_delay_hours_capped) as std_actual_delay,
                                MIN(label_delay_hours_capped) as min_delay,
                                MAX(label_delay_hours_capped) as max_delay,
                                COUNTIF(label_delay_hours_capped > 0.1) as delayed_count,
                                COUNTIF(label_delay_hours_capped < -0.1) as early_count,
                                COUNTIF(label_delay_hours_capped BETWEEN -0.1 AND 0.1) as ontime_count
                            FROM `{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.shipment_metrics`
                            WHERE label_delay_hours_capped IS NOT NULL
                            """
                            stats_result = client.query(stats_query).to_dataframe()
                            if not stats_result.empty:
                                stats_row = stats_result.iloc[0]
                                stats_msg = f"""
                                **📊 Dataset Statistics:**
                                - Total shipments: {int(stats_row['total_predictions'])}
                                - Average delay: {stats_row['avg_actual_delay']:.3f} hours ({stats_row['avg_actual_delay']*60:.1f} min)
                                - Delay range: {stats_row['min_delay']:.2f}h to {stats_row['max_delay']:.2f}h
                                - Delayed (>6min): {int(stats_row['delayed_count'])} ({stats_row['delayed_count']/stats_row['total_predictions']*100:.1f}%)
                                - On time (±6min): {int(stats_row['ontime_count'])} ({stats_row['ontime_count']/stats_row['total_predictions']*100:.1f}%)
                                - Early (>6min): {int(stats_row['early_count'])} ({stats_row['early_count']/stats_row['total_predictions']*100:.1f}%)
                                """
                                st.session_state.messages.append({"role": "assistant", "content": stats_msg})
                        except Exception as stats_e:
                            st.session_state.messages.append({"role": "assistant", "content": f"📊 Stats error: {str(stats_e)}"})
                    else:
                        st.session_state.messages.append({"role": "assistant", "content": "❌ No sample data found in database"})
                except Exception as e:
                    st.session_state.messages.append({"role": "assistant", "content": f"❌ Test failed: {str(e)}"})
                
                st.rerun()
        
        # Debug section
        with st.expander("🔍 System Debug Info", expanded=False):
            st.write("**Environment Variables:**")
            env_vars = {
                'BIGQUERY_PROJECT': os.getenv('BIGQUERY_PROJECT'),
                'BIGQUERY_DATASET': os.getenv('BIGQUERY_DATASET'),
                'BQ_MODEL': os.getenv('BQ_MODEL'),
                'EURI_API_KEY': f"{os.getenv('EURI_API_KEY')[:10]}..." if os.getenv('EURI_API_KEY') else 'Not Set',
                'EURI_MODEL_NAME': os.getenv('EURI_MODEL_NAME')
            }
            st.json(env_vars)
            
            # Test connections
            col_test1, col_test2, col_test3 = st.columns(3)
            
            with col_test1:
                if st.button("🧪 Test EURI AI", use_container_width=True):
                    with st.spinner("Testing EURI AI connection..."):
                        try:
                            from ml_model_prediction.llm_connector import test_euri_api_directly
                            success, result = test_euri_api_directly()
                            if success:
                                st.success(f"✅ EURI AI working: {result}")
                            else:
                                st.error(f"❌ EURI AI failed: {result}")
                        except Exception as e:
                            st.error(f"❌ Test error: {str(e)}")
            
            with col_test2:
                if st.button("🧪 Test BigQuery", use_container_width=True):
                    with st.spinner("Testing BigQuery connection..."):
                        try:
                            client = init_bigquery_client()
                            query = f"""
                            SELECT COUNT(*) as count
                            FROM `{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.shipment_metrics`
                            LIMIT 1
                            """
                            result = client.query(query).to_dataframe()
                            st.success(f"✅ BigQuery working: {result.iloc[0]['count']} records")
                        except Exception as e:
                            st.error(f"❌ BigQuery failed: {str(e)}")
            
            with col_test3:
                if st.button("🤖 Test ML Model", use_container_width=True):
                    with st.spinner("Testing ML model predictions..."):
                        try:
                            from ml_model_prediction.llm_connector import test_ml_model_predictions
                            success, results = test_ml_model_predictions()
                            if success:
                                st.success("✅ ML Model test completed")
                                
                                # Display results in a table
                                import pandas as pd
                                results_df = pd.DataFrame(results)
                                st.dataframe(results_df, use_container_width=True)
                                
                                # Check for variation
                                predictions = [r['predicted_delay'] for r in results if isinstance(r['predicted_delay'], (int, float))]
                                if len(set(predictions)) == 1:
                                    st.warning("⚠️ All predictions are identical - ML model may not be working properly")
                                else:
                                    st.info(f"✅ Found {len(set(predictions))} different prediction values")
                            else:
                                st.error(f"❌ ML Model test failed: {results}")
                        except Exception as e:
                            st.error(f"❌ Test error: {str(e)}")
            
            st.write("**Recent Log File (last 20 lines):**")
            try:
                with open('dashboard.log', 'r') as f:
                    lines = f.readlines()
                    recent_logs = ''.join(lines[-20:]) if lines else "No logs yet"
                    st.text(recent_logs)
            except FileNotFoundError:
                st.text("Log file not created yet")
    
    with tab2:
        st.header("📊 Analytics Dashboard")
        
        # Load metrics data
        metrics_df = load_shipment_metrics()
        detailed_df = load_detailed_shipments()
        
        if not metrics_df.empty:
            # Display key metrics
            st.subheader("📊 Key Performance Metrics")
            
            # Create metrics cards
            if len(metrics_df) > 0:
                row = metrics_df.iloc[0]
                
                # First row of metrics
                metric_cols1 = st.columns(4)
                with metric_cols1[0]:
                    st.metric("Total Shipments", f"{int(row['total_shipments']):,}")
                with metric_cols1[1]:
                    st.metric("Avg ETA Variation", f"{row['avg_eta_variation_hours']:.2f}h")
                with metric_cols1[2]:
                    st.metric("Avg Lead Time", f"{row['avg_lead_time_days']:.1f} days")
                with metric_cols1[3]:
                    st.metric("Avg Disruption Score", f"{row['avg_delay_probability']:.3f}")
                
                # Second row of metrics
                metric_cols2 = st.columns(4)
                with metric_cols2[0]:
                    st.metric("Delay Range", f"{row['min_delay']:.1f}h to {row['max_delay']:.1f}h")
                with metric_cols2[1]:
                    st.metric("Avg Traffic Level", f"{row['avg_traffic_congestion']:.2f}")
                with metric_cols2[2]:
                    st.metric("Avg Weather Severity", f"{row['avg_weather_severity']:.2f}")
                with metric_cols2[3]:
                    st.metric("Avg Port Congestion", f"{row['avg_port_congestion']:.2f}")
                

                
                # Operational Metrics
                st.subheader("⏰ Operational Insights")
                op_cols = st.columns(2)
                with op_cols[0]:
                    weekend_pct = (row['weekend_shipments'] / row['total_shipments']) * 100
                    st.metric("Weekend Shipments", f"{int(row['weekend_shipments'])}", f"{weekend_pct:.1f}%")
                with op_cols[1]:
                    rush_pct = (row['rush_hour_shipments'] / row['total_shipments']) * 100
                    st.metric("Rush Hour Shipments", f"{int(row['rush_hour_shipments'])}", f"{rush_pct:.1f}%")
                
                # Delay Distribution
                st.subheader("📊 Delay Distribution")
                delay_stats_cols = st.columns(3)
                with delay_stats_cols[0]:
                    st.metric("Min Delay", f"{row['min_delay']:.2f}h")
                with delay_stats_cols[1]:
                    st.metric("Max Delay", f"{row['max_delay']:.2f}h")
                with delay_stats_cols[2]:
                    st.metric("Std Deviation", f"{row['std_eta_variation']:.2f}h")
        
        # Detailed analytics if we have detailed data
        if not detailed_df.empty:
            st.subheader("📈 Detailed Analytics")
            
            # Data validation and cleaning
            st.write(f"📊 Analyzing {len(detailed_df)} shipment records")
            
            # Show data preview in expander
            with st.expander("🔍 Data Preview"):
                st.write("**Sample Records:**")
                st.dataframe(detailed_df.head(3), use_container_width=True)
                st.write("**Data Types:**")
                st.write(detailed_df.dtypes.to_dict())
            
            # ETA Variation Distribution
            fig_eta = px.histogram(
                detailed_df, 
                x='eta_variation_hours',
                nbins=30,
                title="ETA Variation Distribution (Hours)",
                labels={'eta_variation_hours': 'ETA Variation (Hours)', 'count': 'Frequency'},
                color_discrete_sequence=['#4ECDC4']
            )
            fig_eta.update_layout(showlegend=False)
            st.plotly_chart(fig_eta, use_container_width=True)
            
            # Traffic vs Weather Impact Analysis
            st.subheader("🌦️ Environmental Impact Analysis")
            
            # Create a copy of the dataframe and fix size values (must be positive)
            plot_df = detailed_df.copy()
            plot_df['size_value'] = abs(plot_df['port_congestion_level']) + 0.1  # Ensure positive values
            
            fig_env = px.scatter(
                plot_df,
                x='traffic_congestion_level',
                y='weather_condition_severity',
                color='eta_variation_hours',
                size='size_value',
                title="Traffic vs Weather Impact on Delays",
                labels={
                    'traffic_congestion_level': 'Traffic Congestion Level',
                    'weather_condition_severity': 'Weather Severity',
                    'eta_variation_hours': 'ETA Variation (Hours)'
                },
                color_continuous_scale='RdYlBu_r'
            )
            st.plotly_chart(fig_env, use_container_width=True)
            
            # Operational Efficiency Analysis
            st.subheader("⚙️ Operational Efficiency")
            
            # Fix size values for operational plot
            plot_df['size_ops'] = abs(plot_df['eta_variation_hours']) + 0.1  # Ensure positive values
            
            fig_ops = px.scatter(
                plot_df,
                x='loading_unloading_time',
                y='handling_equipment_availability',
                color='risk_classification',
                size='size_ops',
                title="Loading Time vs Equipment Availability",
                labels={
                    'loading_unloading_time': 'Loading/Unloading Time',
                    'handling_equipment_availability': 'Equipment Availability',
                    'risk_classification': 'Risk Level'
                }
            )
            st.plotly_chart(fig_ops, use_container_width=True)
            
            # Categorical Analysis
            st.subheader("📊 Categorical Breakdowns")
            
            # Traffic bucket analysis
            if 'traffic_bucket' in detailed_df.columns:
                traffic_analysis = detailed_df.groupby('traffic_bucket')['eta_variation_hours'].agg(['mean', 'count']).reset_index()
                fig_traffic = px.bar(
                    traffic_analysis,
                    x='traffic_bucket',
                    y='mean',
                    title="Average Delay by Traffic Level",
                    labels={'mean': 'Average ETA Variation (Hours)', 'traffic_bucket': 'Traffic Level'},
                    color='mean',
                    color_continuous_scale='Reds'
                )
                st.plotly_chart(fig_traffic, use_container_width=True)
            
            # Time-based patterns
            if 'hour_of_day' in detailed_df.columns:
                st.subheader("⏰ Time-based Patterns")
                hourly_analysis = detailed_df.groupby('hour_of_day')['eta_variation_hours'].agg(['mean', 'count']).reset_index()
                fig_hourly = px.line(
                    hourly_analysis,
                    x='hour_of_day',
                    y='mean',
                    title="Average Delay by Hour of Day",
                    labels={'mean': 'Average ETA Variation (Hours)', 'hour_of_day': 'Hour of Day'},
                    markers=True
                )
                fig_hourly.update_traces(line_color='#FF6B6B')
                st.plotly_chart(fig_hourly, use_container_width=True)
            
            # Regional Analysis
            if 'region4' in detailed_df.columns:
                st.subheader("🗺️ Regional Performance")
                regional_analysis = detailed_df.groupby('region4')['eta_variation_hours'].agg(['mean', 'count']).reset_index()
                regional_analysis = regional_analysis.sort_values('mean', ascending=False).head(10)
                fig_regional = px.bar(
                    regional_analysis,
                    x='region4',
                    y='mean',
                    title="Top 10 Regions by Average Delay",
                    labels={'mean': 'Average ETA Variation (Hours)', 'region4': 'Region'},
                    color='mean',
                    color_continuous_scale='Viridis'
                )
                fig_regional.update_xaxes(tickangle=45)
                st.plotly_chart(fig_regional, use_container_width=True)
            
            # Correlation Analysis
            st.subheader("🔗 Factor Correlation Analysis")
            numeric_cols = [
                'eta_variation_hours', 'traffic_congestion_level', 'weather_condition_severity',
                'port_congestion_level', 'loading_unloading_time', 'handling_equipment_availability',
                'order_fulfillment_status', 'shipping_costs', 'lead_time_days', 'delay_probability'
            ]
            
            # Filter columns that exist in the dataframe
            available_cols = [col for col in numeric_cols if col in detailed_df.columns]
            
            if len(available_cols) > 3:
                correlation_matrix = detailed_df[available_cols].corr()
                
                fig_corr = px.imshow(
                    correlation_matrix,
                    title="Correlation Matrix of Key Factors",
                    color_continuous_scale='RdBu',
                    aspect='auto'
                )
                fig_corr.update_layout(height=500)
                st.plotly_chart(fig_corr, use_container_width=True)
                
                # Top correlations with ETA variation
                if 'eta_variation_hours' in available_cols:
                    eta_corr = correlation_matrix['eta_variation_hours'].abs().sort_values(ascending=False)[1:6]  # Exclude self-correlation
                    st.write("**Top 5 factors correlated with ETA variation:**")
                    for factor, corr_value in eta_corr.items():
                        st.write(f"• {factor}: {corr_value:.3f}")
            
            # Summary Statistics Table
            st.subheader("📋 Summary Statistics")
            if len(available_cols) > 0:
                summary_stats = detailed_df[available_cols].describe().round(3)
                st.dataframe(summary_stats, use_container_width=True)
            

            



if __name__ == "__main__":
    main()