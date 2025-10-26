-- models/marts/shipment_performance_metrics.sql
-- for dashboards or summary tables.
with metrics as (

    select
        count(*) as total_shipments,
        avg(eta_variation_hours) as avg_eta_variation_hours,
        avg(lead_time_days) as avg_lead_time_days,
        avg(delay_probability) as avg_delay_probability,
        sum(case when risk_classification = 1 then 1 else 0 end) as high_risk_count,
        sum(case when risk_classification = 2 then 1 else 0 end) as medium_risk_count,
        sum(case when risk_classification = 3 then 1 else 0 end) as low_risk_count
    from {{ ref('shipment_metrics') }}

)

select * from metrics
