-- Real-time sales summary for streaming data
-- Updates every dbt run with latest streaming data

{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id', 'sale_date', 'sale_hour'],
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

SELECT 
    store_id,
    dept_id,
    DATE(event_time) as sale_date,
    HOUR(event_time) as sale_hour,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_transaction_value,
    SUM(CASE WHEN transaction_type = 'SALE' THEN 1 ELSE 0 END) as sale_count,
    SUM(CASE WHEN transaction_type = 'RETURN' THEN 1 ELSE 0 END) as return_count,
    SUM(CASE WHEN transaction_type = 'EXCHANGE' THEN 1 ELSE 0 END) as exchange_count,
    SUM(CASE WHEN promotion_applied = 1 THEN 1 ELSE 0 END) as promotional_transactions,
    MAX(event_time) as latest_transaction_time,
    CURRENT_TIMESTAMP as last_updated
FROM {{ source('retail_analytics', 'stream_sales_events') }}
{% if is_incremental() %}
  -- Only process new events since last run
  WHERE event_time > (SELECT COALESCE(MAX(latest_transaction_time), '1900-01-01') FROM {{ this }})
{% else %}
  -- Full refresh: process last 24 hours
  WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
{% endif %}
GROUP BY 
    store_id, 
    dept_id, 
    DATE(event_time), 
    HOUR(event_time)
ORDER BY 
    sale_date DESC, 
    sale_hour DESC, 
    total_sales DESC