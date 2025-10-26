-- models/staging/stg_shipments.sql

with source as (

    select *
    from `savvy-equator-476206-r2.supply_chain.shipments_raw`

)

select
    _id,
    event_id,
    timestamp,
    ingestion_timestamp,
    processed_timestamp,
    processing_status,
    vehicle_data_gps_latitude as gps_latitude,
    vehicle_data_gps_longitude as gps_longitude,
    vehicle_data_eta_variation_hours as eta_variation_hours,
    operational_metrics_traffic_congestion_level as traffic_congestion_level,
    operational_metrics_loading_unloading_time as loading_unloading_time,
    operational_metrics_handling_equipment_availability as handling_equipment_availability,
    operational_metrics_order_fulfillment_status as order_fulfillment_status,
    external_factors_weather_condition_severity as weather_condition_severity,
    external_factors_port_congestion_level as port_congestion_level,
    external_factors_shipping_costs as shipping_costs,
    performance_indicators_lead_time_days as lead_time_days,
    performance_indicators_disruption_likelihood_score as disruption_likelihood_score,
    performance_indicators_delay_probability as delay_probability,
    performance_indicators_risk_classification as risk_classification,
    performance_indicators_delivery_time_deviation as delivery_time_deviation,
    temporal_features_hour as hour,
    temporal_features_day as day,
    temporal_features_month as month,
    temporal_features_weekday as weekday
from source
