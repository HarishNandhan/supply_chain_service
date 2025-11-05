# test_predict.py
# Purpose: Fetch 1 row from test_table, select only model features, call BQML model, print prediction.

from google.cloud import bigquery
import os

# ---- CONFIG (edit if needed) ----
PROJECT_ID = os.getenv("BQ_PROJECT", "supply-chain-project-476405")
DATASET    = os.getenv("BQ_DATASET", "supply_chain")
MODEL_NAME = os.getenv("BQ_MODEL",   "delay_regressor_v6")  # the new Boosted Tree model
SOURCE_TABLE = "shipment_metrics"  # Use the full shipment_metrics table
# ---------------------------------

client = bigquery.Client(project=PROJECT_ID)

model_fq = f"`{PROJECT_ID}.{DATASET}.{MODEL_NAME}`"
source_tbl = f"`{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`"

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

# We’ll also fetch ID fields for display (not required for prediction).
id_fields = "_id, event_id, timestamp"

query = f"""
WITH all_shipments AS (
  SELECT {id_fields}, {feature_select}
  FROM {source_tbl}
  WHERE label_delay_hours_capped IS NOT NULL
)
SELECT
  o._id,
  o.event_id,
  o.timestamp,
  p.predicted_label_delay_hours_capped AS predicted_delay_hours
FROM ML.PREDICT(
  MODEL {model_fq},
  (SELECT * EXCEPT(_id, event_id, timestamp) FROM all_shipments)
) AS p
JOIN all_shipments AS o ON TRUE  -- Since we can't join on _id after EXCEPT, use TRUE and rely on order
ORDER BY o.timestamp DESC
"""

def determine_status(delay_hours):
    """Determine shipment status based on delay - same logic as in app.py"""
    if delay_hours > 0.5:  # More than 30 minutes late
        return "DELAYED", "🔴"
    elif delay_hours < -0.5:  # More than 30 minutes early
        return "EARLY", "🟢"
    else:  # Within 30 minutes of scheduled time
        return "ON TIME", "🟡"

def main():
    print(f"[info] Project: {PROJECT_ID}  Dataset: {DATASET}")
    print(f"[info] Model:   {MODEL_NAME}  Source Table: {SOURCE_TABLE}")
    print(f"[info] Running ML predictions on ALL records in shipment_metrics table...")
    
    # First, get total count
    count_query = f"SELECT COUNT(*) as total FROM {source_tbl} WHERE label_delay_hours_capped IS NOT NULL"
    count_result = client.query(count_query).to_dataframe()
    total_records = count_result.iloc[0]['total']
    
    print(f"[info] Found {total_records:,} records with valid delay data")
    
    if total_records == 0:
        print("[warn] No records found with valid label_delay_hours_capped. Check your data.")
        return
    
    # Limit to 1000 records to avoid BigQuery resource limits
    sample_size = min(1000, total_records)
    print(f"[info] Processing sample of {sample_size:,} records to avoid BigQuery resource limits...")
    print(f"[info] (BigQuery ML.PREDICT on {total_records:,} records exceeds CPU limits)")
    
    # Create a more efficient query with sampling
    sample_query = f"""
    WITH sample_data AS (
      SELECT {id_fields}, {feature_select}
      FROM {source_tbl}
      WHERE label_delay_hours_capped IS NOT NULL
      ORDER BY RAND()
      LIMIT {sample_size}
    )
    SELECT
      o._id,
      o.event_id,
      o.timestamp,
      p.predicted_label_delay_hours_capped AS predicted_delay_hours
    FROM ML.PREDICT(
      MODEL {model_fq},
      (SELECT * EXCEPT(_id, event_id, timestamp) FROM sample_data)
    ) AS p
    JOIN sample_data AS o ON TRUE
    ORDER BY o.timestamp DESC
    """
    
    print("[info] Executing ML.PREDICT query on sample...")
    job = client.query(sample_query)
    df = job.to_dataframe()
    
    if df.empty:
        print("[warn] No predictions returned. Check if ML model exists and is accessible.")
        return

    print(f"\n=== ML Model Predictions for {len(df):,} Records (Sample from {total_records:,} total) ===")
    
    # Add status determination
    statuses = []
    predictions = df['predicted_delay_hours'].values
    
    # Determine status from original ML predictions (no amplification needed)
    for delay_hours in predictions:
        status, _ = determine_status(delay_hours)
        statuses.append(status)
    
    # Show sample of predictions (first 10 and last 10)
    print(f"\n=== Sample Predictions (First 10 Records) ===")
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        delay_hours = row['predicted_delay_hours']
        status, emoji = determine_status(delay_hours)
        print(f"Record {i+1:2d}: ID={row['_id'][:12]}... | Predicted: {delay_hours:.6f}h | Status: {emoji} {status}")
    
    if len(df) > 10:
        print(f"\n=== Sample Predictions (Last 10 Records) ===")
        for i in range(max(0, len(df)-10), len(df)):
            row = df.iloc[i]
            delay_hours = row['predicted_delay_hours']
            status, emoji = determine_status(delay_hours)
            print(f"Record {i+1:2d}: ID={row['_id'][:12]}... | Predicted: {delay_hours:.6f}h | Status: {emoji} {status}")
    
    # Statistical analysis - showing both original and amplified
    print(f"\n=== COMPLETE DATASET STATISTICAL ANALYSIS ===")
    print(f"Total records processed: {len(predictions):,}")
    print(f"\n--- ML PREDICTION STATISTICS ---")
    print(f"Unique prediction values: {len(set(predictions)):,}")
    print(f"Min prediction: {min(predictions):.6f} hours")
    print(f"Max prediction: {max(predictions):.6f} hours")
    print(f"Average prediction: {sum(predictions)/len(predictions):.6f} hours")
    print(f"Standard deviation: {(sum((x - sum(predictions)/len(predictions))**2 for x in predictions) / len(predictions))**0.5:.6f}")
    
    # MAIN RESULT: Status distribution for sample dataset
    print(f"\n=== 🎯 STATUS DISTRIBUTION (SAMPLE: {len(predictions):,} of {total_records:,} RECORDS) ===")
    status_counts = {}
    for status in statuses:
        status_counts[status] = status_counts.get(status, 0) + 1
    
    sample_records = len(statuses)
    
    # Sort by status for consistent display
    status_order = ["DELAYED", "ON TIME", "EARLY"]
    for status in status_order:
        if status in status_counts:
            count = status_counts[status]
            percentage = (count / sample_records) * 100
            emoji = "🔴" if status == "DELAYED" else "🟢" if status == "EARLY" else "🟡"
            print(f"  {emoji} {status:8}: {count:,} records ({percentage:.2f}%)")
    
    # Detailed threshold analysis using original predictions
    print(f"\n=== THRESHOLD BREAKDOWN ===")
    print(f"Status Logic: >0.5h = DELAYED, <-0.5h = EARLY, else ON TIME")
    
    delayed_count = sum(1 for p in predictions if p > 0.5)
    early_count = sum(1 for p in predictions if p < -0.5)
    ontime_count = sum(1 for p in predictions if -0.5 <= p <= 0.5)
    
    print(f"� DELAIYED shipments (>0.5h):  {delayed_count:,} ({delayed_count/total_records*100:.2f}%)")
    print(f"�  ON TIME shipments (±0.5h):  {ontime_count:,} ({ontime_count/total_records*100:.2f}%)")
    print(f"🟢 EARLY shipments (<-0.5h):   {early_count:,} ({early_count/total_records*100:.2f}%)")
    
    # Model performance assessment
    print(f"\n=== MODEL PERFORMANCE ASSESSMENT ===")
    if len(set(predictions)) == 1:
        print(f"❌ CRITICAL: All predictions are identical ({predictions[0]:.6f} hours)")
        print("   The ML model is NOT working properly - all inputs produce same output")
    elif len(set(predictions)) < 10:
        print(f"⚠️  WARNING: Very limited variation ({len(set(predictions))} unique values)")
        print("   The ML model may have limited predictive power")
    else:
        print(f"✅ GOOD: Model shows reasonable variation ({len(set(predictions)):,} unique values)")
    
    if delayed_count == 0 and early_count == 0:
        print("⚠️  All predictions fall within 'ON TIME' range (±30 minutes)")
        print("   Consider: 1) Adjusting thresholds, 2) Retraining model, 3) Checking features")
    
    # Show most common prediction values
    print(f"\n=== TOP 10 MOST COMMON PREDICTIONS ===")
    from collections import Counter
    pred_counter = Counter(predictions)
    most_common = pred_counter.most_common(10)
    
    for i, (pred, count) in enumerate(most_common, 1):
        percentage = (count / len(predictions)) * 100
        status, emoji = determine_status(pred)
        print(f"{i:2d}. {pred:.6f} hours ({emoji} {status}): {count:,} records ({percentage:.2f}%)")
    
    print("=" * 80)

if __name__ == "__main__":
    main()
