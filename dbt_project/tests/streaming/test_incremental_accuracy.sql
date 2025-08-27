{#
    Test that incremental models produce accurate results
    This test compares a sample of incremental results with expected full refresh results
#}

{{ config(severity='warn') }}

WITH incremental_sample AS (
    -- Sample from incremental rt_sales_summary
    SELECT 
        store_id,
        dept_id,
        sale_date,
        sale_hour,
        transaction_count,
        total_sales,
        avg_transaction_value
    FROM {{ ref('rt_sales_summary') }}
    WHERE last_updated >= CURRENT_DATE - INTERVAL 1 DAY
    ORDER BY total_sales DESC
    LIMIT 100
),

full_refresh_sample AS (
    -- Calculate same metrics with full refresh logic
    SELECT 
        store_id,
        dept_id,
        DATE(event_time) as sale_date,
        HOUR(event_time) as sale_hour,
        COUNT(*) as transaction_count,
        SUM(total_amount) as total_sales,
        AVG(total_amount) as avg_transaction_value
    FROM {{ source('retail_analytics', 'stream_sales_events') }}
    WHERE DATE(event_time) >= CURRENT_DATE - INTERVAL 1 DAY
    GROUP BY store_id, dept_id, DATE(event_time), HOUR(event_time)
),

accuracy_check AS (
    SELECT 
        i.store_id,
        i.dept_id,
        i.sale_date,
        i.sale_hour,
        i.total_sales as incremental_sales,
        f.total_sales as full_refresh_sales,
        ABS(i.total_sales - f.total_sales) as sales_diff,
        CASE 
            WHEN f.total_sales = 0 THEN 0
            ELSE ABS(i.total_sales - f.total_sales) / f.total_sales * 100
        END as sales_diff_percent
    FROM incremental_sample i
    JOIN full_refresh_sample f 
        ON i.store_id = f.store_id 
        AND i.dept_id = f.dept_id
        AND i.sale_date = f.sale_date 
        AND i.sale_hour = f.sale_hour
    WHERE ABS(i.total_sales - f.total_sales) / NULLIF(f.total_sales, 0) > 0.01  -- 1% tolerance
)

-- Return records where incremental results differ from full refresh by more than 1%
SELECT *
FROM accuracy_check