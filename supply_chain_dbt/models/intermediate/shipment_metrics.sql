{{ config(materialized='view') }}

-- ============================================================
-- Feature view for ETA delay prediction (regression target)
-- - Target: label_delay_hours (eta_variation_hours in hours)
-- - Engineered time, location, interaction, and history features
-- - BigQuery dialect
-- ============================================================

with raw as (
  select
    _id,
    event_id,

    -- Robust timestamp parsing (ISO or 'YYYY-MM-DD HH:MM:SS')
    case
      when regexp_contains(timestamp, r'T') then timestamp(timestamp)
      else safe.parse_timestamp('%Y-%m-%d %H:%M:%S', timestamp)
    end as ts,

    -- === Target (no leakage fields) ===
    safe_cast(eta_variation_hours as float64) as label_delay_hours,

    -- === Core numeric signals (safe-cast to avoid failures) ===
    safe_cast(gps_latitude  as float64) as gps_latitude,
    safe_cast(gps_longitude as float64) as gps_longitude,

    safe_cast(traffic_congestion_level         as float64) as traffic_congestion_level,
    safe_cast(loading_unloading_time           as float64) as loading_unloading_time,
    safe_cast(handling_equipment_availability  as float64) as handling_equipment_availability,
    safe_cast(order_fulfillment_status         as float64) as order_fulfillment_status,
    safe_cast(weather_condition_severity       as float64) as weather_condition_severity,
    safe_cast(port_congestion_level            as float64) as port_congestion_level,
    safe_cast(shipping_costs                   as float64) as shipping_costs,
    safe_cast(lead_time_days                   as float64) as lead_time_days,
    safe_cast(disruption_likelihood_score      as float64) as disruption_likelihood_score,

    -- Categorical as-is (BQML one-hot encodes strings)
    cast(risk_classification as string) as risk_classification

    -- NOTE: Intentionally excluding:
    --   delay_probability, delivery_time_deviation  (likely leakage)
  from {{ ref('stg_shipments') }}
  where eta_variation_hours is not null
),

-- Location enrichment: GeoHash zones from lat/lon
geo as (
  select
    r.*,
    case
      when gps_latitude is null or gps_longitude is null then null
      else st_geohash(st_geogpoint(gps_longitude, gps_latitude), 4)  -- ~20km cells
    end as region4,
    case
      when gps_latitude is null or gps_longitude is null then null
      else st_geohash(st_geogpoint(gps_longitude, gps_latitude), 5)  -- ~5km cells
    end as region5
  from raw r
),

-- Base engineered features (time, cyclic, interactions, buckets)
fe as (
  select
    g.*,

    -- ==== Time-derived features ====
    extract(hour      from ts) as hour_of_day,
    extract(dayofweek from ts) as day_of_week,       -- 1=Sun … 7=Sat
    extract(month     from ts) as month_of_year,
    extract(isoweek   from ts) as iso_week,

    -- Human-friendly flags
    case when extract(dayofweek from ts) in (1,7) then 1 else 0 end as is_weekend,
    case when extract(hour from ts) between 7 and 10
           or  extract(hour from ts) between 16 and 19 then 1 else 0 end as is_rush_hour,

    -- Cyclic encodings (help tree & linear models)
    -- hour: 24h cycle
    -- BigQuery: no pi(); use constant
    sin(2 * 3.141592653589793 * extract(hour from ts) / 24.0)  as sin_hour,
    cos(2 * 3.141592653589793 * extract(hour from ts) / 24.0)  as cos_hour,
    sin(2 * 3.141592653589793 * extract(month from ts) / 12.0) as sin_month,
    cos(2 * 3.141592653589793 * extract(month from ts) / 12.0) as cos_month,

    -- ==== Interactions (nonlinear drivers) ====
    traffic_congestion_level * loading_unloading_time            as cong_x_loading,
    traffic_congestion_level * weather_condition_severity        as traffic_x_weather,
    loading_unloading_time * handling_equipment_availability     as load_x_equipment,
    port_congestion_level  * traffic_congestion_level            as port_x_traffic,

    -- ==== Business buckets (categorical, interpretable) ====
    case
      when traffic_congestion_level is null then 'unknown'
      when traffic_congestion_level < 0.3 then 'low'
      when traffic_congestion_level < 0.7 then 'medium'
      else 'high'
    end as traffic_bucket,

    case
      when loading_unloading_time is null then 'unknown'
      when loading_unloading_time < 1.0 then 'short'
      when loading_unloading_time <= 2.0 then 'normal'
      else 'long'
    end as loading_time_bucket,

    case
      when handling_equipment_availability is null then 'unknown'
      when handling_equipment_availability < 0.4 then 'poor'
      when handling_equipment_availability < 0.7 then 'ok'
      else 'good'
    end as handling_availability_bucket,

    case
      when weather_condition_severity is null then 'unknown'
      when weather_condition_severity = 0 then 'clear'
      when weather_condition_severity < 0.3 then 'mild'
      else 'severe'
    end as weather_bucket,

    case
      when port_congestion_level is null then 'unknown'
      when port_congestion_level < 0.3 then 'low'
      when port_congestion_level < 0.7 then 'medium'
      else 'high'
    end as port_congestion_bucket,

    case
      when lead_time_days is null then 'unknown'
      when lead_time_days = 0 then 'same_day'
      when lead_time_days <= 2 then 'short'
      else 'long'
    end as lead_time_bucket

  from geo g
),

-- History-based signal: rolling average delay by region × hour (exclude current row to avoid leakage)
hist as (
  select
    fe.*,
    -- Uses ROWS frame to exclude current row (… AND 1 PRECEDING)
    avg(label_delay_hours) over (
      partition by region4, hour_of_day
      order by ts
      rows between unbounded preceding and 1 preceding
    ) as avg_delay_region4_hour,

    avg(label_delay_hours) over (
      partition by region5, hour_of_day
      order by ts
      rows between unbounded preceding and 1 preceding
    ) as avg_delay_region5_hour
  from fe
)

-- Final projection: target + all features we want available to BQML
select
  _id,
  event_id,
  ts as timestamp,

  -- === TARGET ===
  label_delay_hours,

  -- === Time features ===
  hour_of_day, day_of_week, month_of_year, iso_week,
  is_weekend, is_rush_hour,
  sin_hour, cos_hour, sin_month, cos_month,

  -- === Location features ===
  gps_latitude, gps_longitude,
  region4, region5,

  -- === Core numeric ===
  traffic_congestion_level,
  loading_unloading_time,
  handling_equipment_availability,
  order_fulfillment_status,
  weather_condition_severity,
  port_congestion_level,
  shipping_costs,
  lead_time_days,
  disruption_likelihood_score,

  -- === Interactions ===
  cong_x_loading, traffic_x_weather, load_x_equipment, port_x_traffic,

  -- === Buckets (categorical, BQML one-hot encodes strings) ===
  traffic_bucket, loading_time_bucket, handling_availability_bucket,
  weather_bucket, port_congestion_bucket, lead_time_bucket,
  risk_classification,

  -- === History (strong signal; may be null for earliest rows) ===
  avg_delay_region4_hour,
  avg_delay_region5_hour

from hist
