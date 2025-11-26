{{ config(materialized='view') }}

-- ============================================================
-- Enhanced Feature View for ETA Delay Prediction
-- Adds region × day rolling averages, capped/smoothed labels,
-- and extra cross-feature interactions for ML training
-- ============================================================

WITH raw AS (
  SELECT
    _id,
    event_id,

    CASE
      WHEN REGEXP_CONTAINS(timestamp, r'T') THEN TIMESTAMP(timestamp)
      ELSE SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp)
    END AS ts,

    -- Delay labels
    SAFE_CAST(eta_variation_hours AS FLOAT64) AS label_delay_hours_raw,

    CASE
      WHEN SAFE_CAST(eta_variation_hours AS FLOAT64) > 5 THEN 5
      WHEN SAFE_CAST(eta_variation_hours AS FLOAT64) < -1 THEN -1
      ELSE SAFE_CAST(eta_variation_hours AS FLOAT64)
    END AS label_delay_hours_capped,

    SAFE_CAST(ROUND(
      CASE
        WHEN SAFE_CAST(eta_variation_hours AS FLOAT64) > 5 THEN 5
        WHEN SAFE_CAST(eta_variation_hours AS FLOAT64) < -1 THEN -1
        ELSE SAFE_CAST(eta_variation_hours AS FLOAT64)
      END, 1) AS FLOAT64) AS label_delay_hours_smoothed,

    SAFE_CAST(gps_latitude AS FLOAT64)  AS gps_latitude,
    SAFE_CAST(gps_longitude AS FLOAT64) AS gps_longitude,
    SAFE_CAST(traffic_congestion_level AS FLOAT64) AS traffic_congestion_level,
    SAFE_CAST(loading_unloading_time AS FLOAT64) AS loading_unloading_time,
    SAFE_CAST(handling_equipment_availability AS FLOAT64) AS handling_equipment_availability,
    SAFE_CAST(order_fulfillment_status AS FLOAT64) AS order_fulfillment_status,
    SAFE_CAST(weather_condition_severity AS FLOAT64) AS weather_condition_severity,
    SAFE_CAST(port_congestion_level AS FLOAT64) AS port_congestion_level,
    SAFE_CAST(shipping_costs AS FLOAT64) AS shipping_costs,
    SAFE_CAST(lead_time_days AS FLOAT64) AS lead_time_days,
    SAFE_CAST(disruption_likelihood_score AS FLOAT64) AS disruption_likelihood_score,
    CAST(risk_classification AS STRING) AS risk_classification
  FROM {{ ref('stg_shipments') }}
  WHERE eta_variation_hours IS NOT NULL
),

geo AS (
  SELECT
    r.*,
    CASE
      WHEN gps_latitude IS NULL OR gps_longitude IS NULL THEN NULL
      ELSE ST_GeoHash(ST_GeogPoint(gps_longitude, gps_latitude), 4)
    END AS region4,
    CASE
      WHEN gps_latitude IS NULL OR gps_longitude IS NULL THEN NULL
      ELSE ST_GeoHash(ST_GeogPoint(gps_longitude, gps_latitude), 5)
    END AS region5
  FROM raw r
),

fe AS (
  SELECT
    g.*,
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM ts) AS day_of_week,
    EXTRACT(MONTH FROM ts) AS month_of_year,
    EXTRACT(ISOWEEK FROM ts) AS iso_week,

    CASE WHEN EXTRACT(DAYOFWEEK FROM ts) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN EXTRACT(HOUR FROM ts) BETWEEN 7 AND 10
           OR EXTRACT(HOUR FROM ts) BETWEEN 16 AND 19 THEN 1 ELSE 0 END AS is_rush_hour,

    SIN(2 * 3.141592653589793 * EXTRACT(HOUR FROM ts) / 24.0)  AS sin_hour,
    COS(2 * 3.141592653589793 * EXTRACT(HOUR FROM ts) / 24.0)  AS cos_hour,
    SIN(2 * 3.141592653589793 * EXTRACT(MONTH FROM ts) / 12.0) AS sin_month,
    COS(2 * 3.141592653589793 * EXTRACT(MONTH FROM ts) / 12.0) AS cos_month,

    -- Interaction features
    traffic_congestion_level * loading_unloading_time AS cong_x_loading,
    traffic_congestion_level * weather_condition_severity AS traffic_x_weather,
    loading_unloading_time * handling_equipment_availability AS load_x_equipment,
    port_congestion_level * traffic_congestion_level AS port_x_traffic,

    -- New engineered features
    traffic_congestion_level * disruption_likelihood_score AS traffic_x_disruption,
    lead_time_days * port_congestion_level AS leadtime_x_port,
    weather_condition_severity * lead_time_days AS weather_x_leadtime,

    -- Route distance proxy
    ST_DISTANCE(ST_GeogPoint(gps_longitude, gps_latitude),
                ST_GeogPoint(gps_longitude + 0.01, gps_latitude + 0.01)) / 1000 AS approx_distance_km,

    -- Categorical buckets
    CASE WHEN traffic_congestion_level < 0.3 THEN 'low'
         WHEN traffic_congestion_level < 0.7 THEN 'medium'
         ELSE 'high' END AS traffic_bucket,

    CASE WHEN loading_unloading_time < 1.0 THEN 'short'
         WHEN loading_unloading_time <= 2.0 THEN 'normal'
         ELSE 'long' END AS loading_time_bucket,

    CASE WHEN handling_equipment_availability < 0.4 THEN 'poor'
         WHEN handling_equipment_availability < 0.7 THEN 'ok'
         ELSE 'good' END AS handling_availability_bucket,

    CASE WHEN weather_condition_severity = 0 THEN 'clear'
         WHEN weather_condition_severity < 0.3 THEN 'mild'
         ELSE 'severe' END AS weather_bucket,

    CASE WHEN port_congestion_level < 0.3 THEN 'low'
         WHEN port_congestion_level < 0.7 THEN 'medium'
         ELSE 'high' END AS port_congestion_bucket,

    CASE WHEN lead_time_days = 0 THEN 'same_day'
         WHEN lead_time_days <= 2 THEN 'short'
         ELSE 'long' END AS lead_time_bucket
  FROM geo g
),

hist AS (
  SELECT
    fe.*,
    AVG(label_delay_hours_smoothed) OVER (
  PARTITION BY region4, hour_of_day
  ORDER BY ts
  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
) AS avg_delay_region4_hour,

AVG(label_delay_hours_smoothed) OVER (
  PARTITION BY region4, day_of_week
  ORDER BY ts
  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
) AS avg_delay_region4_day,

-- 7-day moving average delay for region (captures weekly congestion trends)
AVG(label_delay_hours_smoothed) OVER (
  PARTITION BY region4
  ORDER BY ts
  ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
) AS avg_delay_region4_week,


-- Binary indicator for severe delays (> 2 hours late)
CASE
  WHEN label_delay_hours_capped > 2 THEN 1
  ELSE 0
END AS is_severe_delay

  FROM fe
)

SELECT
  _id,
  event_id,
  ts AS timestamp,
  COALESCE(label_delay_hours_raw, 0) AS label_delay_hours_raw,
  COALESCE(label_delay_hours_capped, 0) AS label_delay_hours_capped,
  COALESCE(label_delay_hours_smoothed, 0) AS label_delay_hours,
  CASE
    WHEN label_delay_hours_smoothed > 1 THEN 1
    ELSE 0
  END AS is_delayed,
  COALESCE(hour_of_day, 0) AS hour_of_day,
  COALESCE(day_of_week, 0) AS day_of_week,
  COALESCE(month_of_year, 0) AS month_of_year,
  COALESCE(iso_week, 0) AS iso_week,
  COALESCE(is_weekend, 0) AS is_weekend,
  COALESCE(is_rush_hour, 0) AS is_rush_hour,
  COALESCE(sin_hour, 0) AS sin_hour,
  COALESCE(cos_hour, 0) AS cos_hour,
  COALESCE(sin_month, 0) AS sin_month,
  COALESCE(cos_month, 0) AS cos_month,
  COALESCE(gps_latitude, 0) AS gps_latitude,
  COALESCE(gps_longitude, 0) AS gps_longitude,
  COALESCE(region4, 'unknown') AS region4,
  COALESCE(region5, 'unknown') AS region5,
  COALESCE(traffic_congestion_level, 0) AS traffic_congestion_level,
  COALESCE(loading_unloading_time, 0) AS loading_unloading_time,
  COALESCE(handling_equipment_availability, 1) AS handling_equipment_availability,
  COALESCE(order_fulfillment_status, 0) AS order_fulfillment_status,
  COALESCE(weather_condition_severity, 0) AS weather_condition_severity,
  COALESCE(port_congestion_level, 0) AS port_congestion_level,
  COALESCE(shipping_costs, 0) AS shipping_costs,
  COALESCE(lead_time_days, 0) AS lead_time_days,
  COALESCE(disruption_likelihood_score, 0) AS disruption_likelihood_score,
  COALESCE(cong_x_loading, 0) AS cong_x_loading,
  COALESCE(traffic_x_weather, 0) AS traffic_x_weather,
  COALESCE(load_x_equipment, 0) AS load_x_equipment,
  COALESCE(port_x_traffic, 0) AS port_x_traffic,
  COALESCE(traffic_x_disruption, 0) AS traffic_x_disruption,
  COALESCE(leadtime_x_port, 0) AS leadtime_x_port,
  COALESCE(weather_x_leadtime, 0) AS weather_x_leadtime,
  COALESCE(traffic_bucket, 'unknown') AS traffic_bucket,
  COALESCE(loading_time_bucket, 'unknown') AS loading_time_bucket,
  COALESCE(handling_availability_bucket, 'unknown') AS handling_availability_bucket,
  COALESCE(weather_bucket, 'unknown') AS weather_bucket,
  COALESCE(port_congestion_bucket, 'unknown') AS port_congestion_bucket,
  COALESCE(lead_time_bucket, 'unknown') AS lead_time_bucket,
  COALESCE(risk_classification, 'unknown') AS risk_classification,
  COALESCE(avg_delay_region4_hour, 0) AS avg_delay_region4_hour,
COALESCE(avg_delay_region4_day, 0)  AS avg_delay_region4_day,
COALESCE(avg_delay_region4_week, 0) AS avg_delay_region4_week,
COALESCE(is_severe_delay, 0) AS is_severe_delay
FROM hist