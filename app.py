"""
Supply Chain Analytics Dashboard - Streamlit App

Configuration:
- Set environment variables or update defaults below
- Requires Google Cloud credentials for BigQuery access
- Requires Euri AI API key for chat functionality
"""

from google.cloud import bigquery
import pandas as pd
import altair as alt
import streamlit as st
import os
import requests
import json
import hashlib
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv('configs/.env')

# ===========================
# USER DATABASE MANAGEMENT
# ===========================
USER_DB_FILE = "data/users.json"

def load_users():
    """Load users from JSON file"""
    if os.path.exists(USER_DB_FILE):
        try:
            with open(USER_DB_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading user database: {e}")
            return {}
    return {}

def save_users(users_dict):
    """Save users to JSON file"""
    try:
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(USER_DB_FILE), exist_ok=True)
        with open(USER_DB_FILE, 'w') as f:
            json.dump(users_dict, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving user database: {e}")
        return False

def hash_password(password):
    """Hash password for secure storage"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed_password):
    """Verify password against hash"""
    return hash_password(password) == hashed_password

# ===========================
# BIGQUERY CONFIGURATION
# ===========================
BQ_PROJECT = os.getenv("BIGQUERY_PROJECT", "savvy-equator-476206-r2")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "supply_chain_analytics")
BQ_TABLE_RAW = os.getenv("BIGQUERY_TABLE_RAW", "shipments_raw")
BQ_MODEL_NAME = os.getenv("BQ_MODEL", "delay_regressor_v6")
BQ_TEST_TABLE = os.getenv("BQ_TEST_TBL", "test_table")

# ===========================
# EURI AI CONFIGURATION
# ===========================
EURI_API_KEY = os.getenv("EURI_API_KEY")
EURI_MODEL_NAME = os.getenv("EURI_MODEL_NAME", "gpt-4.1-nano")
# Correct Euri AI endpoint
EURI_API_URL = "https://api.euron.one/api/v1/euri/chat/completions"

# ===========================
# AIRFLOW CONFIGURATION
# ===========================
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "supply_chain_pipeline")

bq_client = bigquery.Client(project=BQ_PROJECT) 

# ===========================
# MODERN KPI CARD FUNCTION
# ===========================
def kpi_card(title, value, icon="", color="#4F46E5"):
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, rgba(79, 70, 229, 0.05) 0%, rgba(59, 130, 246, 0.05) 100%);
        padding: 24px 20px;
        border-radius: 16px;
        border: 2px solid rgba(79, 70, 229, 0.2);
        text-align: center;
        width: 100%;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    ">
        <div style="
            font-size: 48px;
            margin-bottom: 8px;
        ">
            {icon}
        </div>
        <div style="
            font-size: 14px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 8px;
        ">
            {title}
        </div>
        <div style="
            font-size: 36px;
            font-weight: 800;
            background: linear-gradient(135deg, {color} 0%, #3B82F6 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        ">
            {value}
        </div>
    </div>
    """, unsafe_allow_html=True)


def get_client_logs():
    """Fetch client query logs from BigQuery"""
    try:
        query = f"""
            SELECT
                timestamp,
                client_username,
                order_id,
                prediction_status,
                predicted_delay_hours,
                ai_response,
                query_type
            FROM `{BQ_PROJECT}.{BQ_DATASET}.client_query_logs`
            ORDER BY timestamp DESC
            LIMIT 100
        """
        df = bq_client.query(query).to_dataframe()
        return df
    except Exception as e:
        # Table might not exist yet
        return pd.DataFrame()


def get_all_shipments():
    """Fetch all shipments from BigQuery - loads from test_table (accumulated data)"""
    # Load from test_table which accumulates all processed records
    try:
        test_query = f"""
            SELECT
                event_id,
                timestamp,
                gps_latitude,
                gps_longitude,
                disruption_likelihood_score,
                label_delay_hours as delay_probability,
                CAST(risk_classification AS FLOAT64) as risk_classification,
                weather_condition_severity,
                traffic_congestion_level,
                port_congestion_level,
                lead_time_days
            FROM `{BQ_PROJECT}.{BQ_DATASET}.test_table`
            ORDER BY timestamp DESC
        """
        df = bq_client.query(test_query).to_dataframe()
        
        if not df.empty:
            # Ensure numeric columns are float type
            numeric_cols = ['gps_latitude', 'gps_longitude', 'disruption_likelihood_score', 
                          'delay_probability', 'risk_classification', 'weather_condition_severity',
                          'traffic_congestion_level', 'port_congestion_level', 'lead_time_days']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
    except Exception as e:
        st.warning(f"Could not load from test_table, falling back to raw data: {str(e)[:100]}")
    
    # Fallback to shipments_raw if test_table is not available
    raw_query = f"""
        SELECT
            event_id,
            timestamp,
            vehicle_data_gps_latitude AS gps_latitude,
            vehicle_data_gps_longitude AS gps_longitude,
            performance_indicators_disruption_likelihood_score AS disruption_likelihood_score,
            performance_indicators_delay_probability AS delay_probability,
            performance_indicators_risk_classification AS risk_classification,
            external_factors_weather_condition_severity AS weather_condition_severity,
            operational_metrics_traffic_congestion_level AS traffic_congestion_level,
            external_factors_port_congestion_level AS port_congestion_level,
            performance_indicators_lead_time_days AS lead_time_days
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}`
        ORDER BY timestamp DESC
    """
    df = bq_client.query(raw_query).to_dataframe()
    return df




st.set_page_config(page_title="Landing Page", layout="wide")

# ===========================
# SESSION / USERS
# ===========================
if "users" not in st.session_state:
    # Load users from persistent storage
    loaded_users = load_users()
    
    # If no users exist, create default admin and client
    if not loaded_users:
        default_users = {
            "admin": {
                "password": hash_password("admin123"),
                "role": "admin"
            },
            "client": {
                "password": hash_password("client123"),
                "role": "client"
            }
        }
        save_users(default_users)
        st.session_state["users"] = default_users
    else:
        st.session_state["users"] = loaded_users

st.session_state.setdefault("authenticated", False)
st.session_state.setdefault("current_role", None)
st.session_state.setdefault("current_user", None)
st.session_state.setdefault("current_view", "landing")


# ===========================
# GLOBAL CSS - Modern, Professional UI
# ===========================
def inject_css():
    st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* Root Variables for Theme Compatibility */
    :root {
        --primary-color: #4F46E5;
        --primary-hover: #4338CA;
        --success-color: #10B981;
        --warning-color: #F59E0B;
        --danger-color: #EF4444;
        --info-color: #3B82F6;
        --bg-primary: #FFFFFF;
        --bg-secondary: #F9FAFB;
        --bg-card: #FFFFFF;
        --text-primary: #111827;
        --text-secondary: #6B7280;
        --border-color: #E5E7EB;
        --shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
        --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
    }

    /* Dark Mode Support */
    @media (prefers-color-scheme: dark) {
        :root {
            --bg-primary: #0F172A;
            --bg-secondary: #1E293B;
            --bg-card: #1E293B;
            --text-primary: #F1F5F9;
            --text-secondary: #94A3B8;
            --border-color: #334155;
        }
    }

    /* Global Styles */
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    .stApp {
        background: linear-gradient(135deg, var(--bg-primary) 0%, var(--bg-secondary) 100%);
    }

    /* Hide Streamlit Branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Page Title Styling */
    .page-title {
        font-size: 56px;
        font-weight: 800;
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--info-color) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 12px;
        letter-spacing: -0.02em;
    }

    .page-subtitle {
        font-size: 20px;
        color: var(--text-secondary);
        margin-bottom: 48px;
        font-weight: 400;
    }

    /* Card Styling */
    .card-center {
        display: flex;
        justify-content: center;
        gap: 40px;
        margin-top: 48px;
        flex-wrap: wrap;
    }

    .card-box {
        width: 380px;
        min-height: 280px;
        background: var(--bg-card);
        border-radius: 20px;
        padding: 36px 28px;
        border: 2px solid var(--border-color);
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        align-items: center;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: var(--shadow);
    }

    .card-box:hover {
        transform: translateY(-8px);
        box-shadow: var(--shadow-lg);
        border-color: var(--primary-color);
    }

    .card-title {
        font-size: 32px;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 24px;
    }

    .btn-row {
        display: flex;
        gap: 16px;
        width: 100%;
    }

    .card-btn {
        flex: 1;
        padding: 12px 24px;
        background: var(--primary-color);
        border-radius: 10px;
        color: white !important;
        border: none;
        text-decoration: none !important;
        font-size: 15px;
        font-weight: 600;
        transition: all 0.2s ease;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(79, 70, 229, 0.3);
    }

    .card-btn:hover {
        background: var(--primary-hover);
        transform: translateY(-2px);
        box-shadow: 0 6px 12px -2px rgba(79, 70, 229, 0.4);
    }

    /* Button Styling */
    .stButton > button {
        background: var(--primary-color);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 12px 28px;
        font-weight: 600;
        font-size: 15px;
        transition: all 0.2s ease;
        box-shadow: 0 4px 6px -1px rgba(79, 70, 229, 0.3);
    }

    .stButton > button:hover {
        background: var(--primary-hover);
        transform: translateY(-2px);
        box-shadow: 0 6px 12px -2px rgba(79, 70, 229, 0.4);
    }

    /* Input Styling */
    .stTextInput > div > div > input {
        border-radius: 10px;
        border: 2px solid var(--border-color);
        padding: 12px 16px;
        font-size: 15px;
        transition: all 0.2s ease;
        background: var(--bg-card);
        color: var(--text-primary);
    }

    .stTextInput > div > div > input:focus {
        border-color: var(--primary-color);
        box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.1);
    }

    /* Metric Cards */
    [data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 700;
        color: var(--text-primary);
    }

    [data-testid="stMetricLabel"] {
        font-size: 14px;
        font-weight: 500;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Dataframe Styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: var(--shadow);
    }

    /* Expander Styling */
    .streamlit-expanderHeader {
        background: var(--bg-card);
        border-radius: 10px;
        border: 2px solid var(--border-color);
        font-weight: 600;
        color: var(--text-primary);
    }

    /* Progress Bar */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, var(--primary-color) 0%, var(--info-color) 100%);
        border-radius: 10px;
    }

    /* Success/Error/Info Messages */
    .stSuccess, .stError, .stWarning, .stInfo {
        border-radius: 10px;
        padding: 16px;
        font-weight: 500;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 12px 24px;
        font-weight: 600;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: var(--bg-card);
        border-right: 2px solid var(--border-color);
    }

    /* Charts */
    .stPlotlyChart, .stVegaLiteChart {
        border-radius: 12px;
        box-shadow: var(--shadow);
        padding: 16px;
        background: var(--bg-card);
    }

    </style>
    """, unsafe_allow_html=True)


# ===========================
# PAGE 1: LANDING
# ===========================
def show_landing():
    # Centered title + subtitle
    st.markdown("""
    <div style='width:100%; text-align:center; margin-top:80px; margin-bottom:60px;'>
        <div class="page-title">🚀 Supply Chain Analytics</div>
        <div class="page-subtitle">Real-time shipment tracking</div>
    </div>
    """, unsafe_allow_html=True)

    # Cards
    cards_html = """
    <div class="card-center">

    <div class="card-box">
        <div style="font-size:64px; margin-bottom:16px;">👤</div>
        <div class="card-title">Client Portal</div>
        <div style="font-size:14px; color:var(--text-secondary); margin-bottom:24px;">
            Track your shipments and get AI-powered delivery predictions
        </div>
        <div class="btn-row">
            <a class="card-btn" href="?view=client_login">Login</a>
            <a class="card-btn" href="?view=client_register">Register</a>
        </div>
    </div>

    <div class="card-box">
        <div style="font-size:64px; margin-bottom:16px;">⚙️</div>
        <div class="card-title">Admin Portal</div>
        <div style="font-size:14px; color:var(--text-secondary); margin-bottom:24px;">
            Manage operations, view analytics, and monitor client activity
        </div>
        <div class="btn-row">
            <a class="card-btn" href="?view=admin_login">Login</a>
            <a class="card-btn" href="?view=admin_register">Register</a>
        </div>
    </div>

    </div>
    """
    st.markdown(cards_html, unsafe_allow_html=True)

    # Footer
    st.markdown("""
    <div style='text-align:center; color:var(--text-secondary); font-size:14px; margin-top:80px; padding:20px;'>
        <div style='margin-bottom:8px;'>
            <strong>Supply Chain Analytics Platform</strong>
        </div>
        <div>
            Powered by BigQuery ML • Airflow • MongoDB • Euri AI
        </div>
        <div style='margin-top:16px; font-size:12px;'>
            © 2025 All Rights Reserved
        </div>
    </div>
    """, unsafe_allow_html=True)


# ===========================
# PAGE 2: ADMIN LOGIN
# ===========================
def show_admin_login():
    st.markdown(
        "<h2 style='text-align:center; margin-top:40px;'>Admin Login</h2>",
        unsafe_allow_html=True,
    )

    with st.form("admin_login_form"):
        username = st.text_input("Username", value="admin")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

    if submit:
        users = st.session_state["users"]
        if username in users:
            user_data = users[username]
            # Check if password matches and user is admin
            if verify_password(password, user_data["password"]) and user_data["role"] == "admin":
                st.session_state["authenticated"] = True
                st.session_state["current_role"] = "admin"
                st.session_state["current_user"] = username
                st.session_state["current_view"] = "admin_dashboard"

                # Clear query params and rerun
                st.query_params.clear()
                st.success("Login successful! Redirecting to Admin Dashboard...")
                st.rerun()
            else:
                st.error("Invalid admin credentials. Try again.")
        else:
            st.error("Invalid admin credentials. Try again.")

    if st.button("⬅ Back to landing"):
        st.query_params.clear()
        st.session_state["current_view"] = "landing"
        st.rerun()


def show_admin_register():
    st.markdown("<h2 style='text-align:center; margin-top:40px;'>Admin Registration</h2>",
                unsafe_allow_html=True)
    
    st.warning("⚠️ Admin registration requires an admin access code.")

    with st.form("admin_register_form"):
        username = st.text_input("Create Username", placeholder="Choose a unique username")
        password = st.text_input("Create Password", type="password", placeholder="Min 6 characters")
        confirm = st.text_input("Confirm Password", type="password", placeholder="Re-enter password")
        access_code = st.text_input("Admin Access Code", type="password", placeholder="Enter admin code")
        submit = st.form_submit_button("Register")

    if submit:
        # Simple access code check (you can change this)
        ADMIN_ACCESS_CODE = "ADMIN2025"
        
        if not username or not password or not access_code:
            st.error("All fields are required.")
        elif access_code != ADMIN_ACCESS_CODE:
            st.error("Invalid admin access code.")
        elif len(password) < 6:
            st.error("Password must be at least 6 characters long.")
        elif password != confirm:
            st.error("Password and Confirm Password do not match.")
        elif username in st.session_state["users"]:
            st.error("Username already exists. Try a different one.")
        else:
            # Save new admin with hashed password
            new_user = {
                "password": hash_password(password),
                "role": "admin"
            }
            st.session_state["users"][username] = new_user
            
            # Save to persistent storage
            if save_users(st.session_state["users"]):
                st.success("🎉 Admin registration successful! Logging you in...")
                
                # Auto-login the user after registration
                st.session_state["authenticated"] = True
                st.session_state["current_role"] = "admin"
                st.session_state["current_user"] = username
                st.session_state["current_view"] = "admin_dashboard"
                st.query_params.clear()
                st.rerun()
            else:
                st.error("Failed to save user. Please try again.")

    if st.button("⬅ Back to landing"):
        st.session_state["current_view"] = "landing"
        st.query_params.clear()
        st.rerun()


def show_client_login():
    st.markdown("<h2 style='text-align:center; margin-top:40px;'>Client Login</h2>", 
                unsafe_allow_html=True)

    with st.form("client_login_form"):
        username = st.text_input("Username", placeholder="Enter your username")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        submit = st.form_submit_button("Login")

    if submit:
        users = st.session_state["users"]

        if username in users:
            user_data = users[username]
            # Check if password matches and user is client
            if verify_password(password, user_data["password"]) and user_data["role"] == "client":
                st.session_state["authenticated"] = True
                st.session_state["current_role"] = "client"
                st.session_state["current_user"] = username
                st.session_state["current_view"] = "client_dashboard"
                st.query_params.clear()
                st.success("Login successful! Redirecting...")
                st.rerun()
            else:
                st.error("Invalid credentials. Please check your username and password.")
        else:
            st.error("Invalid credentials. Please check your username and password.")
    
    if st.button("⬅ Back to landing"):
        st.session_state["current_view"] = "landing"
        st.query_params.clear()
        st.rerun()

    


def show_client_register():
    st.markdown("<h2 style='text-align:center; margin-top:40px;'>Client Registration</h2>",
                unsafe_allow_html=True)

    with st.form("client_register_form"):
        username = st.text_input("Create Username", placeholder="Choose a unique username")
        password = st.text_input("Create Password", type="password", placeholder="Min 6 characters")
        confirm = st.text_input("Confirm Password", type="password", placeholder="Re-enter password")
        submit = st.form_submit_button("Register")

    if submit:
        if not username or not password:
            st.error("All fields are required.")
        elif len(password) < 6:
            st.error("Password must be at least 6 characters long.")
        elif password != confirm:
            st.error("Password and Confirm Password do not match.")
        elif username in st.session_state["users"]:
            st.error("Username already exists. Try a different one.")
        else:
            # Save new client with hashed password
            new_user = {
                "password": hash_password(password),
                "role": "client"
            }
            st.session_state["users"][username] = new_user
            
            # Save to persistent storage
            if save_users(st.session_state["users"]):
                st.success("🎉 Registration successful! Logging you in...")
                
                # Auto-login the user after registration
                st.session_state["authenticated"] = True
                st.session_state["current_role"] = "client"
                st.session_state["current_user"] = username
                st.session_state["current_view"] = "client_dashboard"
                st.query_params.clear()
                st.rerun()
            else:
                st.error("Failed to save user. Please try again.")

    if st.button("⬅ Back to landing"):
        st.session_state["current_view"] = "landing"
        st.query_params.clear()
        st.rerun()

        


def get_order_data(record_id: str):
    """Get order data from test_table for a specific _id"""
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TEST_TABLE}`
        WHERE _id = @record_id
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("record_id", "STRING", record_id)
        ]
    )
    
    df = bq_client.query(query, job_config=job_config).to_dataframe()
    return df


def predict_order_status(record_id: str):
    """
    Predict delivery status for a specific record using BigQuery ML model.
    Queries test_table by _id and runs ML prediction.
    """
    
    # Feature selection - must include label_delay_hours as the model expects it
    # The model was trained with: * EXCEPT(timestamp, _id, event_id, label_delay_hours_raw)
    # So it includes label_delay_hours and is_delayed as features
    feature_select = """
        label_delay_hours,
        is_delayed,
        hour_of_day,
        day_of_week,
        month_of_year,
        iso_week,
        is_weekend,
        is_rush_hour,
        sin_hour,
        cos_hour,
        sin_month,
        cos_month,
        gps_latitude,
        gps_longitude,
        region4,
        region5,
        traffic_congestion_level,
        loading_unloading_time,
        handling_equipment_availability,
        order_fulfillment_status,
        weather_condition_severity,
        port_congestion_level,
        shipping_costs,
        lead_time_days,
        disruption_likelihood_score,
        cong_x_loading,
        traffic_x_weather,
        load_x_equipment,
        port_x_traffic,
        traffic_x_disruption,
        leadtime_x_port,
        weather_x_leadtime,
        traffic_bucket,
        loading_time_bucket,
        handling_availability_bucket,
        weather_bucket,
        port_congestion_bucket,
        lead_time_bucket,
        risk_classification,
        avg_delay_region4_hour,
        avg_delay_region4_day,
        avg_delay_region4_week,
        is_severe_delay
    """
    
    # Query to get prediction for specific record
    query = f"""
        WITH shipment_data AS (
            SELECT {feature_select}
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TEST_TABLE}`
            WHERE _id = @record_id
            LIMIT 1
        )
        SELECT
            predicted_label_delay_hours_capped AS predicted_delay_hours,
            CASE
                WHEN predicted_label_delay_hours_capped > 0.5 THEN 'DELAYED'
                WHEN predicted_label_delay_hours_capped < -0.5 THEN 'EARLY'
                ELSE 'ON_TIME'
            END AS prediction_status
        FROM ML.PREDICT(
            MODEL `{BQ_PROJECT}.{BQ_DATASET}.{BQ_MODEL_NAME}`,
            (SELECT * FROM shipment_data)
        )
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("record_id", "STRING", record_id)
        ]
    )
    
    try:
        query_job = bq_client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        # Log the full error for debugging
        st.error(f"BigQuery Error: {str(e)}")
        if hasattr(e, 'errors'):
            for error in e.errors:
                st.error(f"Error details: {error}")
        # Also print the query for debugging
        st.code(query, language="sql")
        return pd.DataFrame()


def generate_ai_response(order_data, prediction_data):
    """Generate AI chat response using Euri AI based on order and prediction data"""
    
    if not EURI_API_KEY:
        return "⚠️ Euri AI is not configured. Please set EURI_API_KEY in your environment."
    
    # Extract key information
    record_id = order_data['_id'].iloc[0] if '_id' in order_data.columns else 'N/A'
    event_id = order_data['event_id'].iloc[0] if 'event_id' in order_data.columns else 'N/A'
    predicted_delay = prediction_data['predicted_delay_hours'].iloc[0]
    prediction_status = prediction_data['prediction_status'].iloc[0]
    
    # Get relevant features from order data
    traffic_level = float(order_data.get('traffic_congestion_level', [0])[0]) if 'traffic_congestion_level' in order_data.columns else 0
    weather_severity = float(order_data.get('weather_condition_severity', [0])[0]) if 'weather_condition_severity' in order_data.columns else 0
    port_congestion = float(order_data.get('port_congestion_level', [0])[0]) if 'port_congestion_level' in order_data.columns else 0
    lead_time = float(order_data.get('lead_time_days', [0])[0]) if 'lead_time_days' in order_data.columns else 0
    disruption_score = float(order_data.get('disruption_likelihood_score', [0])[0]) if 'disruption_likelihood_score' in order_data.columns else 0
    risk_class = order_data.get('risk_classification', ['N/A'])[0] if 'risk_classification' in order_data.columns else 'N/A'
    
    # Create customer-friendly prompt based on status
    if prediction_status == "ON_TIME":
        prompt = f"""You are a friendly supply chain customer service assistant. A customer is checking their order status.

Order Details:
- Order ID: {record_id}
- Status: ON TIME ✅
- Expected delivery: As scheduled

Write a warm, enthusiastic message (100-120 words) that: [Don't generate in an email template]
1. Makes their day special by confirming on-time delivery
2. Shows excitement about meeting their expectations
3. Thanks them for their patience and trust
4. Keeps a positive, upbeat tone

DO NOT mention technical factors like traffic or weather. Focus on making them happy!"""

    elif prediction_status == "EARLY":
        prompt = f"""You are a friendly supply chain customer service assistant. A customer is checking their order status.

Order Details:
- Order ID: {record_id}
- Status: ARRIVING EARLY 🚀
- Early by: {abs(predicted_delay):.1f} hours

Write an exciting, proud message (100-120 words) that: [Don't generate in an email template]
1. Shows off that we're delivering sooner than expected
2. Makes them feel special and valued
3. Expresses pride in exceeding expectations
4. Keeps an enthusiastic, celebratory tone

DO NOT mention technical factors. Focus on the great news and making their day!"""

    else:  # DELAYED
        prompt = f"""You are a compassionate supply chain customer service assistant. A customer is checking their order status.

Order Details:
- Order ID: {record_id}
- Status: DELAYED ⚠️
- Delay: {predicted_delay:.1f} hours
- Main factors: Traffic ({traffic_level:.1f}), Weather ({weather_severity:.1f}), Port Congestion ({port_congestion:.1f})

Write a sincere, apologetic message (120-150 words) that: [Don't generate in an email template]
1. Starts with a genuine apology
2. Explains the main factors causing the delay (mention the highest scoring factors)
3. Takes responsibility and shows empathy
4. Apologizes again and promises better service next time
5. Maintains a professional, caring tone

Be honest about the challenges but reassuring about our commitment to improve."""

    try:
        headers = {
            "Authorization": f"Bearer {EURI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": EURI_MODEL_NAME,
            "messages": [
                {"role": "system", "content": "You are a helpful supply chain logistics assistant."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "max_tokens": 300
        }
        
        response = requests.post(EURI_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        ai_message = result['choices'][0]['message']['content']
        return ai_message
        
    except Exception as e:
        # Use fallback response
        st.info("💡 Using intelligent fallback response")
        return generate_fallback_response(prediction_status, predicted_delay, traffic_level, weather_severity, port_congestion, risk_class)


def extract_record_id(user_query):
    """Extract record ID from natural language query using LLM"""
    
    if not EURI_API_KEY:
        # Fallback: use regex to find ID-like patterns
        import re
        # Look for MongoDB ObjectId pattern (24 hex characters) or similar IDs
        patterns = [
            r'[0-9a-f]{24}',  # MongoDB ObjectId
            r'[0-9a-f]{12,}',  # Shorter hex IDs
            r'evt_\w+',  # Event IDs
        ]
        
        for pattern in patterns:
            match = re.search(pattern, user_query, re.IGNORECASE)
            if match:
                return match.group(0)
        return None
    
    try:
        headers = {
            "Authorization": f"Bearer {EURI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        prompt = f"""Extract the order/record ID from this customer query. Return ONLY the ID, nothing else.

Customer query: "{user_query}"

If there's an ID (like a MongoDB ObjectId, event ID, or tracking number), return it exactly as written.
If no ID is found, return "NONE"."""

        payload = {
            "model": EURI_MODEL_NAME,
            "messages": [
                {"role": "system", "content": "You extract IDs from text. Return only the ID or NONE."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 100
        }
        
        response = requests.post(EURI_API_URL, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        
        result = response.json()
        extracted_id = result['choices'][0]['message']['content'].strip()
        
        if extracted_id.upper() == "NONE":
            return None
        
        return extracted_id
        
    except Exception:
        # Fallback to regex
        import re
        patterns = [
            r'[0-9a-f]{24}',
            r'[0-9a-f]{12,}',
            r'evt_\w+',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, user_query, re.IGNORECASE)
            if match:
                return match.group(0)
        return None


def trigger_airflow_dag():
    """Trigger Airflow DAG and wait for completion"""
    import time
    from requests.auth import HTTPBasicAuth
    
    try:
        # Trigger DAG
        trigger_url = f"{AIRFLOW_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"
        auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        
        payload = {
            "conf": {},
            "dag_run_id": f"manual_streamlit_{int(time.time())}"
        }
        
        response = requests.post(trigger_url, json=payload, auth=auth, timeout=10)
        
        if response.status_code not in [200, 201]:
            return False, f"Failed to trigger DAG: {response.text}"
        
        dag_run_id = response.json()["dag_run_id"]
        
        # Poll for completion
        status_url = f"{AIRFLOW_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns/{dag_run_id}"
        max_wait = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            status_response = requests.get(status_url, auth=auth, timeout=10)
            
            if status_response.status_code == 200:
                state = status_response.json()["state"]
                
                if state == "success":
                    return True, "Pipeline completed successfully"
                elif state == "failed":
                    return False, "Pipeline failed"
                elif state in ["running", "queued"]:
                    time.sleep(5)  # Wait 5 seconds before next check
                    continue
            
            time.sleep(5)
        
        return False, "Pipeline timeout"
        
    except Exception as e:
        return False, f"Error: {str(e)}"


def get_latest_prediction():
    """Get the latest prediction from test_table_airflow - using exact Airflow approach"""
    try:
        # Get all columns from test_table_airflow
        schema_query = f"""
        SELECT column_name
        FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'test_table_airflow'
        """
        
        columns_df = bq_client.query(schema_query).to_dataframe()
        all_columns = columns_df['column_name'].tolist()
        
        # Exclude columns for prediction (same as Airflow)
        columns_to_exclude_for_prediction = ['timestamp', '_id', 'event_id', 'label_delay_hours_raw', 'label_delay_hours_capped']
        
        # Get features that match the training data
        feature_columns = [col for col in all_columns if col not in columns_to_exclude_for_prediction]
        feature_list = ", ".join(feature_columns)
        
        # Use exact same query structure as Airflow, but only for latest record
        query = f"""
        WITH predictions AS (
          SELECT
            row_num,
            predicted_label_delay_hours_capped AS predicted_delay_hours,
            CASE
              WHEN predicted_label_delay_hours_capped > 0.5 THEN 'DELAYED'
              WHEN predicted_label_delay_hours_capped < -0.5 THEN 'EARLY'
              ELSE 'ON_TIME'
            END AS prediction_status
          FROM ML.PREDICT(
            MODEL `{BQ_PROJECT}.{BQ_DATASET}.{BQ_MODEL_NAME}`,
            (
              SELECT ROW_NUMBER() OVER() as row_num, {feature_list}
              FROM `{BQ_PROJECT}.{BQ_DATASET}.test_table_airflow`
              ORDER BY timestamp DESC
              LIMIT 1
            )
          )
        ),
        original_data AS (
          SELECT 
            ROW_NUMBER() OVER() as row_num,
            _id,
            event_id,
            timestamp
          FROM `{BQ_PROJECT}.{BQ_DATASET}.test_table_airflow`
          ORDER BY timestamp DESC
          LIMIT 1
        )
        SELECT
          o._id,
          o.event_id,
          o.timestamp,
          p.predicted_delay_hours,
          p.prediction_status
        FROM predictions p
        JOIN original_data o ON p.row_num = o.row_num
        """
        
        df = bq_client.query(query).to_dataframe()
        return df
        
    except Exception as e:
        st.error(f"Error fetching prediction: {str(e)}")
        return pd.DataFrame()


def log_client_query(username, order_id, prediction_status, predicted_delay, ai_response):
    """Log client query to BigQuery for admin tracking"""
    try:
        log_data = {
            'timestamp': pd.Timestamp.now(),
            'client_username': username,
            'order_id': order_id,
            'prediction_status': prediction_status,
            'predicted_delay_hours': predicted_delay,
            'ai_response': ai_response[:500],  # Truncate long responses
            'query_type': 'order_status_check'
        }
        
        df = pd.DataFrame([log_data])
        
        # Insert into BigQuery logs table
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.client_query_logs"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
            create_disposition="CREATE_IF_NEEDED",
        )
        
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
    except Exception as e:
        # Don't fail the main flow if logging fails
        pass


def generate_fallback_response(status, delay, traffic, weather, port, risk):
    """Generate a fallback response when AI API is unavailable"""
    
    if status == "DELAYED":
        message = f"We sincerely apologize for the delay in your shipment. Your order is expected to be delayed by approximately {delay:.1f} hours. "
        
        factors = []
        if traffic > 0.7:
            factors.append("high traffic congestion")
        if weather > 0.7:
            factors.append("severe weather conditions")
        if port > 0.7:
            factors.append("port congestion")
        
        if factors:
            message += f"This delay is primarily due to {', '.join(factors)}. "
        
        message += "We take full responsibility and are working hard to ensure this doesn't happen again. We truly appreciate your patience and promise to serve you better next time."
        
    elif status == "EARLY":
        message = f"🎉 Fantastic news! Your shipment is arriving EARLY by approximately {abs(delay):.1f} hours! "
        message += "We're thrilled to exceed your expectations and deliver sooner than promised. Thank you for choosing us - we're proud to serve you!"
        
    else:
        message = f"✅ Great news! Your shipment is right on schedule and will arrive exactly as planned. "
        message += "We're delighted to meet your expectations and ensure a smooth delivery experience. Thank you for your trust in us!"
    
    return message

def show_client_dashboard():c
    st.markdown("<h1 style='text-align:center;'>Client Portal</h1>", unsafe_allow_html=True)
    st.markdown("### 📦 Track Your Shipment")

    # Natural language input
    user_query = st.text_input(
        "Ask about your order", 
        placeholder="e.g., 'Where is my order 673e6b6e3c245?' or just enter the order ID"
    )

    if st.button("🔍 Check Delivery Status", use_container_width=True):
        if not user_query.strip():
            st.error("Please enter your order ID or ask about your order")
            return
        
        # Extract record ID from natural language
        with st.spinner("🔍 Understanding your query..."):
            record_id = extract_record_id(user_query)
        
        if not record_id:
            st.error("❌ Could not find an order ID in your query. Please include your order ID.")
            st.info("💡 Example: 'Where is my order 673e6b6e3c245?' or just '673e6b6e3c245'")
            return
        
        st.success(f"Found Order ID: {record_id}")
        
        # Step 1: Get order data from test_table by _id
        with st.spinner("📊 Fetching record data from BigQuery..."):
            order_data = get_order_data(record_id)
        
        if order_data.empty:
            st.error("❌ No record found for this ID. Please check and try again.")
            st.info("💡 Tip: Make sure the record exists in the test_table.")
            return
        
        # Step 2: Run ML prediction on the order data
        with st.spinner(" Running ML prediction..."):
            prediction_result = predict_order_status(record_id)
        
        if prediction_result is None or prediction_result.empty:
            st.error("❌ Unable to generate prediction for this order.")
            return
        
        prediction_status = prediction_result["prediction_status"].iloc[0]
        predicted_delay = prediction_result["predicted_delay_hours"].iloc[0]

        # Determine color and icon based on prediction status
        if prediction_status == "DELAYED":
            color = "#CC3333"
            icon = "⚠️"
        elif prediction_status == "ON_TIME":
            color = "#33CC66"
            icon = "✅"
        elif prediction_status == "EARLY":
            color = "#3399FF"
            icon = "🚀"
        else:
            color = "#FFA500"
            icon = "⏳"

        # Display prediction status
        status_html = f"<div style='background:{color};padding:20px;border-radius:12px;text-align:center;font-size:24px;font-weight:700;color:white;margin-bottom:20px;'>{icon} PREDICTED STATUS: {prediction_status}</div>"
        st.markdown(status_html, unsafe_allow_html=True)

        # Display metrics
        st.markdown("### 📊 Prediction Details")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Record ID", record_id)
        with col2:
            st.metric("Predicted Delay", f"{predicted_delay:.2f} hrs")
        with col3:
            st.metric("Status", prediction_status)

        st.markdown("---")

        # Step 3: Generate AI chat response
        st.markdown("###  Supply chain AI Assistant Analysis")
        with st.spinner("Generating AI insights..."):
            ai_response = generate_ai_response(order_data, prediction_result)
        
        # Display AI response in a chat-like interface
        ai_html = f"<div style='background:#1A1A1A;padding:20px;border-radius:12px;border-left:4px solid #3399FF;margin-bottom:20px;'><div style='font-size:14px;color:#3399FF;margin-bottom:10px;'>🤖 Euri AI Assistant</div><div style='font-size:16px;line-height:1.6;color:rgba(255,255,255,0.9);'>{ai_response}</div></div>"
        st.markdown(ai_html, unsafe_allow_html=True)

        # Log this query for admin tracking
        current_user = st.session_state.get("current_user", "anonymous")
        log_client_query(current_user, record_id, prediction_status, predicted_delay, ai_response)

        # Optional: Show raw data in expander
        with st.expander("🔍 View Raw Order Data"):
            st.dataframe(order_data, use_container_width=True)


# ===========================
# HIGHLIGHT RISK COLOR FUNCTION
# ===========================
def highlight_risk(val):
    if val > 0.5:
        color = "#8e1c1c"  # red
    elif val > 0:
        color = "#8e6f1c"  # yellow
    else:
        color = "#1c8e39"  # green
    return f"background-color:{color}; color:white;"

def show_admin_dashboard():

    # =========================
# HANDLE VIEW SWITCH FIRST
# =========================
    params = st.query_params
    if "switch" in params:
        st.session_state.dashboard_view = params["switch"]
        st.query_params.clear()


    st.markdown(
        "<h1 style='text-align:center; margin-bottom:40px;'>Admin Dashboard</h1>",
        unsafe_allow_html=True,
    )

    # Load data (force refresh if coming from schedule view)
    with st.spinner("Loading live shipment data from BigQuery..."):
        df = get_all_shipments()
    
    # Show data freshness indicator
    st.caption(f"📊 Showing {len(df)} total shipments | Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # =========================
    # KPI CARDS - Show prediction status counts
    # =========================
    st.markdown("### 📊 Shipment Status Overview")

    # Calculate status counts based on delay_probability
    # DELAYED: > 0.5, ON_TIME: -0.5 to 0.5, EARLY: < -0.5
    delayed_count = len(df[df["delay_probability"] > 0.5])
    on_time_count = len(df[(df["delay_probability"] >= -0.5) & (df["delay_probability"] <= 0.5)])
    early_count = len(df[df["delay_probability"] < -0.5])
    total_count = len(df)

    colA, colB, colC, colD = st.columns(4)

    with colA:
        kpi_card("Total Shipments", total_count, "📦")

    with colB:
        kpi_card(" Delayed", delayed_count, "⚠️")

    with colC:
        kpi_card(" On Time", on_time_count, "✅")
    
    with colD:
        kpi_card(" Early", early_count, "🚀")

    st.markdown("---")

   # =========================
# VIEW SWITCH BUTTONS (PIXEL PERFECT CENTER)
# =========================

# Initialize
    if "dashboard_view" not in st.session_state:
        st.session_state.dashboard_view = None

# Reduce extra gap above buttons
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

# Create centered row
    left, center, right = st.columns([1, 4, 1])

    with center:
        btn1, btn2, btn3, btn4 = st.columns([1, 1, 1, 1])

        with btn1:
            if st.button("📋 View Data", use_container_width=True):
                st.session_state.dashboard_view = "data"
                st.rerun()

        with btn2:
            if st.button("📊 Analytics", use_container_width=True):
                st.session_state.dashboard_view = "analytics"
                st.rerun()
        
        with btn3:
            if st.button("📝 Client Logs", use_container_width=True):
                st.session_state.dashboard_view = "logs"
                st.rerun()
        
        with btn4:
            if st.button("🚀 Schedule Order", use_container_width=True, type="primary"):
                st.session_state.dashboard_view = "schedule"
                st.rerun()

# Small divider line
    st.markdown("<div style='margin-bottom: 15px;'></div>", unsafe_allow_html=True)
    st.markdown("---")



    # =========================
    # RENDER VIEW BASED ON BUTTON
    # =========================

    if st.session_state.dashboard_view == "data":
        st.markdown("### 📋 Real-Time Shipment Feed")
        styled_df = df.style.applymap(highlight_risk, subset=["risk_classification"])
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    elif st.session_state.dashboard_view == "analytics":
        st.markdown("### 📊 Analytics Dashboard")
        st.markdown("Categorical analysis of shipment factors")

        # Prepare data - load categorical columns from test_table
        try:
            analytics_query = f"""
                SELECT
                    traffic_bucket,
                    loading_time_bucket,
                    handling_availability_bucket,
                    weather_bucket,
                    port_congestion_bucket,
                    lead_time_bucket,
                    label_delay_hours as delay_hours,
                    CASE
                        WHEN label_delay_hours > 0.5 THEN 'DELAYED'
                        WHEN label_delay_hours < -0.5 THEN 'EARLY'
                        ELSE 'ON_TIME'
                    END as status
                FROM `{BQ_PROJECT}.{BQ_DATASET}.test_table`
            """
            analytics_df = bq_client.query(analytics_query).to_dataframe()
        except:
            st.error("Could not load analytics data")
            return

        # Chart 1: Traffic Bucket vs Delay Status
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🚦 Traffic Impact on Delays")
            traffic_status = pd.crosstab(analytics_df['traffic_bucket'], analytics_df['status'])
            st.bar_chart(traffic_status)
        
        with col2:
            st.markdown("#### 🌤️ Weather Impact on Delays")
            weather_status = pd.crosstab(analytics_df['weather_bucket'], analytics_df['status'])
            st.bar_chart(weather_status)

        st.markdown("---")

        # Chart 2: Port Congestion and Loading Time
        col3, col4 = st.columns(2)
        
        with col3:
            st.markdown("#### 🚢 Port Congestion Distribution")
            port_counts = analytics_df['port_congestion_bucket'].value_counts()
            st.bar_chart(port_counts)
        
        with col4:
            st.markdown("#### ⏱️ Loading Time Distribution")
            loading_counts = analytics_df['loading_time_bucket'].value_counts()
            st.bar_chart(loading_counts)

        st.markdown("---")

        # Chart 3: Lead Time vs Average Delay
        st.markdown("#### 📅 Lead Time Impact on Delays")
        lead_time_delay = analytics_df.groupby('lead_time_bucket')['delay_hours'].mean().sort_index()
        st.bar_chart(lead_time_delay)

        st.markdown("---")

        # Summary Table
        st.markdown("#### 📋 Category Distribution Summary")
        summary_data = {
            'Category': ['Traffic', 'Weather', 'Port Congestion', 'Loading Time', 'Handling Availability', 'Lead Time'],
            'Most Common': [
                analytics_df['traffic_bucket'].mode()[0] if not analytics_df['traffic_bucket'].mode().empty else 'N/A',
                analytics_df['weather_bucket'].mode()[0] if not analytics_df['weather_bucket'].mode().empty else 'N/A',
                analytics_df['port_congestion_bucket'].mode()[0] if not analytics_df['port_congestion_bucket'].mode().empty else 'N/A',
                analytics_df['loading_time_bucket'].mode()[0] if not analytics_df['loading_time_bucket'].mode().empty else 'N/A',
                analytics_df['handling_availability_bucket'].mode()[0] if not analytics_df['handling_availability_bucket'].mode().empty else 'N/A',
                analytics_df['lead_time_bucket'].mode()[0] if not analytics_df['lead_time_bucket'].mode().empty else 'N/A'
            ],
            'Unique Values': [
                analytics_df['traffic_bucket'].nunique(),
                analytics_df['weather_bucket'].nunique(),
                analytics_df['port_congestion_bucket'].nunique(),
                analytics_df['loading_time_bucket'].nunique(),
                analytics_df['handling_availability_bucket'].nunique(),
                analytics_df['lead_time_bucket'].nunique()
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    elif st.session_state.dashboard_view == "logs":
        st.markdown("### 📝 Client Activity Logs")
        st.markdown("Track all client queries and responses in real-time")
        
        # Load client logs
        with st.spinner("Loading client activity logs..."):
            logs_df = get_client_logs()
        
        if logs_df.empty:
            st.info("📭 No client activity logged yet. Logs will appear here when clients check their orders.")
        else:
            st.success(f"📊 Showing {len(logs_df)} recent client queries")
            
            # Display logs in an expandable format
            for idx, row in logs_df.iterrows():
                timestamp = row['timestamp']
                username = row['client_username']
                order_id = row['order_id']
                status = row['prediction_status']
                delay = row['predicted_delay_hours']
                
                # Color code by status
                if status == "DELAYED":
                    status_color = "#CC3333"
                    status_icon = "⚠️"
                elif status == "ON_TIME":
                    status_color = "#33CC66"
                    status_icon = "✅"
                else:
                    status_color = "#3399FF"
                    status_icon = "🚀"
                
                with st.expander(f"{status_icon} {username} | {order_id[:12]}... | {timestamp.strftime('%Y-%m-%d %H:%M:%S')}"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Client", username)
                    with col2:
                        st.metric("Status", status)
                    with col3:
                        st.metric("Delay", f"{delay:.2f} hrs")
                    
                    st.markdown("**Full Order ID:**")
                    st.code(order_id, language="text")
    
    elif st.session_state.dashboard_view == "schedule":
        st.markdown("### 🚀 Schedule New Order")
        st.markdown("This will trigger the Airflow pipeline to process a new order from Google Sheets.")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            if st.button("▶️ Start Pipeline", use_container_width=True, type="primary"):
                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Step 1: Trigger DAG
                status_text.text("⏳ Step 1/5: Triggering Airflow pipeline...")
                progress_bar.progress(20)
                
                success, message = trigger_airflow_dag()
                
                if success:
                    # Step 2-4: Pipeline running (handled by Airflow)
                    status_text.text("⏳ Step 2/5: Extracting from Google Sheets...")
                    progress_bar.progress(40)
                    
                    status_text.text("⏳ Step 3/5: Loading to MongoDB & BigQuery...")
                    progress_bar.progress(60)
                    
                    status_text.text("⏳ Step 4/5: Running transformations...")
                    progress_bar.progress(80)
                    
                    # Step 5: Fetch results
                    status_text.text("⏳ Step 5/5: Generating ML predictions...")
                    progress_bar.progress(100)
                    
                    # Get latest prediction
                    result_df = get_latest_prediction()
                    
                    if not result_df.empty:
                        status_text.text(" Pipeline completed successfully!")
                        
                        # Display results
                        st.success(" New Order Processed Successfully!")
                        
                        record_id = result_df['_id'].iloc[0] if '_id' in result_df.columns else 'N/A'
                        event_id = result_df['event_id'].iloc[0] if 'event_id' in result_df.columns else 'N/A'
                        predicted_delay = result_df['predicted_delay_hours'].iloc[0]
                        prediction_status = result_df['prediction_status'].iloc[0]
                        
                        # Status card
                        if prediction_status == "DELAYED":
                            color = "#CC3333"
                            icon = "⚠️"
                        elif prediction_status == "ON_TIME":
                            color = "#33CC66"
                            icon = "✅"
                        else:
                            color = "#3399FF"
                            icon = "🚀"
                        
                        st.markdown(f"""
                        <div style="
                            background:{color};
                            padding:20px;
                            border-radius:12px;
                            text-align:center;
                            font-size:24px;
                            font-weight:700;
                            color:white;
                            margin:20px 0;">
                            {icon} STATUS: {prediction_status}
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Details
                        st.markdown("### 📋 Order Details")
                        
                        # Full Order ID in a code block
                        st.markdown("**Order ID:**")
                        st.code(record_id, language="text")
                        
                        # Metrics in columns
                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.metric("Predicted Delay", f"{predicted_delay:.2f} hrs")
                        with col_b:
                            st.metric("Status", prediction_status)
                        
                        st.info("💡 The new order has been added to the database.")
                        
                        # Add buttons to view updated data
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            if st.button("📊 View Updated Data", use_container_width=True):
                                st.session_state.dashboard_view = "data"
                                st.rerun()
                        with col_btn2:
                            if st.button("📈 View Analytics", use_container_width=True):
                                st.session_state.dashboard_view = "analytics"
                                st.rerun()
                        
                    else:
                        st.error("❌ Could not fetch prediction results")
                else:
                    progress_bar.progress(0)
                    status_text.text("")
                    st.error(f"❌ Pipeline failed: {message}")
                    st.info("💡 Make sure Airflow is running at http://localhost:8080")
    
    else:
        st.markdown("### Select a view to begin")
        st.info("Click **View Data**, **Analytics**, **Client Logs**, or **Schedule Order** to continue.")

# ===========================
# ROUTER
# ===========================
def main():
    inject_css()

    # Read ?view=... from URL
    params = st.query_params
    view_param = params.get("view", "landing")
    if isinstance(view_param, list):
        view_param = view_param[0]

    # If logged in as admin and view not forced → go to dashboard
    if st.session_state["authenticated"] and st.session_state["current_role"] == "admin":
        if st.session_state.get("current_view") == "admin_dashboard":
            show_admin_dashboard()
            return
    if st.session_state["authenticated"] and st.session_state["current_role"] == "client":
        if st.session_state.get("current_view") == "client_dashboard":
            show_client_dashboard()
            return


    # Route based on view parameter
    if view_param == "admin_login":
        st.session_state["current_view"] = "admin_login"
        show_admin_login()
    elif view_param == "admin_register":
        st.session_state["current_view"] = "admin_register"
        show_admin_register()
    elif view_param == "client_login":
        st.session_state["current_view"] = "client_login"
        show_client_login()
    elif view_param == "client_register":
        st.session_state["current_view"] = "client_register"
        show_client_register()
    else:
        # default: landing
        st.session_state["current_view"] = "landing"
        show_landing()


if __name__ == "__main__":
    main()