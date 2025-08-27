{#
    Test that incremental models meet performance requirements
#}

{{ config(severity='warn') }}

WITH performance_metrics AS (
    SELECT 
        'rt_sales_summary' as model_name,
        COUNT(*) as record_count,
        MAX(last_updated) as latest_update,
        TIMESTAMPDIFF(MINUTE, MAX(last_updated), NOW()) as minutes_since_update
    FROM {{ ref('rt_sales_summary') }}
    
    UNION ALL
    
    SELECT 
        'rt_customer_analytics' as model_name,
        COUNT(*) as record_count,
        MAX(analysis_timestamp) as latest_update,
        TIMESTAMPDIFF(MINUTE, MAX(analysis_timestamp), NOW()) as minutes_since_update
    FROM {{ ref('rt_customer_analytics') }}
    
    UNION ALL
    
    SELECT 
        'rt_inventory_alerts' as model_name,
        COUNT(*) as record_count,
        MAX(alert_generated_at) as latest_update,
        TIMESTAMPDIFF(MINUTE, MAX(alert_generated_at), NOW()) as minutes_since_update
    FROM {{ ref('rt_inventory_alerts') }}
)

-- Return models that haven't been updated in more than 15 minutes
SELECT 
    model_name,
    record_count,
    latest_update,
    minutes_since_update,
    'Data freshness violation' as performance_issue
FROM performance_metrics
WHERE minutes_since_update > 15

UNION ALL

-- Return models with unexpectedly low record counts
SELECT 
    model_name,
    record_count,
    latest_update,
    minutes_since_update,
    'Low record count' as performance_issue
FROM performance_metrics
WHERE record_count < 10  -- Adjust threshold based on expected volumes