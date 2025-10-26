-- Added risk categories, which are helpful for features in ML.
with base as (
    select * 
    from {{ ref('stg_shipments') }}
)

select
    _id,
    event_id,
    timestamp,
    gps_latitude,
    gps_longitude,
    eta_variation_hours,
    traffic_congestion_level,
    loading_unloading_time,
    handling_equipment_availability,
    order_fulfillment_status,
    weather_condition_severity,
    port_congestion_level,
    shipping_costs,
    lead_time_days,
    disruption_likelihood_score,
    delay_probability,
    risk_classification,
    delivery_time_deviation,
    hour,
    day,
    month,
    weekday,
    case 
        when lead_time_days > 2 then 'High'
        when lead_time_days between 1 and 2 then 'Medium'
        else 'Low'
    end as lead_time_risk,
    case 
        when disruption_likelihood_score > 1 then 'High'
        when disruption_likelihood_score between 0 and 1 then 'Medium'
        else 'Low'
    end as disruption_risk
from base
