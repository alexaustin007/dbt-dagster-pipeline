{#
    Utility macros for incremental streaming models
#}

{% macro get_incremental_watermark(relation, timestamp_column) %}
    {#
        Get the maximum timestamp from an existing table for incremental processing
        
        Args:
            relation: The table to get the watermark from
            timestamp_column: The column to use for watermarking
            
        Returns:
            SQL expression for the watermark timestamp
    #}
    (
        SELECT COALESCE(MAX({{ timestamp_column }}), '1900-01-01 00:00:00') 
        FROM {{ relation }}
    )
{% endmacro %}

{% macro should_full_refresh_model(model_name) %}
    {#
        Determine if a model should be full refreshed based on various conditions
        
        Args:
            model_name: Name of the model to check
            
        Returns:
            Boolean indicating if full refresh is needed
    #}
    {% set ns = namespace(should_refresh=false) %}
    
    {# Check if it's Sunday (weekly full refresh) #}
    {% if (modules.datetime.datetime.now().weekday() == 6) %}
        {% set ns.should_refresh = true %}
    {% endif %}
    
    {# Check environment variable override #}
    {% if env_var('DBT_FULL_REFRESH', 'false') == 'true' %}
        {% set ns.should_refresh = true %}
    {% endif %}
    
    {{ return(ns.should_refresh) }}
{% endmacro %}

{% macro log_incremental_stats(model_name, operation_type) %}
    {#
        Log statistics about incremental model performance
        
        Args:
            model_name: Name of the model
            operation_type: Type of operation (incremental, full_refresh)
    #}
    {% if execute %}
        {% set stats_query %}
            SELECT 
                COUNT(*) as row_count,
                NOW() as execution_time,
                '{{ model_name }}' as model_name,
                '{{ operation_type }}' as operation_type
            FROM {{ this }}
        {% endset %}
        
        {{ log("Model stats: " ~ stats_query, info=true) }}
    {% endif %}
{% endmacro %}

{% macro validate_incremental_data(source_table, target_table, sample_size=1000) %}
    {#
        Validate that incremental processing matches expected results
        
        Args:
            source_table: Source table to validate against
            target_table: Target incremental table
            sample_size: Number of records to sample for validation
    #}
    {% set validation_query %}
        WITH source_sample AS (
            SELECT * FROM {{ source_table }}
            ORDER BY RAND()
            LIMIT {{ sample_size }}
        ),
        target_sample AS (
            SELECT * FROM {{ target_table }}
            WHERE exists (SELECT 1 FROM source_sample WHERE source_sample.id = {{ target_table }}.id)
        )
        SELECT 
            COUNT(*) as matched_records,
            {{ sample_size }} as expected_records,
            ROUND(COUNT(*) / {{ sample_size }} * 100, 2) as match_percentage
        FROM target_sample
    {% endset %}
    
    {{ return(validation_query) }}
{% endmacro %}