"""
Dagster assets for complete streaming pipeline orchestration
Manages the entire flow: Kafka → Spark → MySQL → dbt transformations
"""
import subprocess
import time
import pandas as pd
from dagster import asset, AssetMaterialization, MetadataValue, DagsterEventType
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
import os
import signal
import psutil
import datetime
from typing import Dict, Any

# dbt project path
DBT_PROJECT_PATH = Path("/Users/alexaustinchettiar/Downloads/retail_data_pipeline_full/dbt_project")

def should_full_refresh(context) -> bool:
    """
    Determine if full refresh is needed based on business rules
    """
    current_time = datetime.datetime.now()
    
    # Full refresh weekly on Sundays
    if current_time.weekday() == 6:
        context.log.info("Triggering weekly full refresh (Sunday)")
        return True
    
    # Full refresh if data quality issues detected
    if detect_data_quality_issues(context):
        context.log.warning("Data quality issues detected, triggering full refresh")
        return True
        
    # Full refresh if it's been more than 7 days since last full refresh
    if days_since_last_full_refresh() > 7:
        context.log.info("More than 7 days since last full refresh")
        return True
        
    return False

def detect_data_quality_issues(context) -> bool:
    """
    Check for data quality issues that require full refresh
    """
    try:
        import mysql.connector
        
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='retail_analytics',
            user='root',
            password='Alex@12345'
        )
        
        cursor = connection.cursor()
        
        # Check for data gaps or anomalies
        cursor.execute("""
            SELECT COUNT(*) as recent_records
            FROM stream_sales_events 
            WHERE event_time >= NOW() - INTERVAL 10 MINUTE
        """)
        
        recent_count = cursor.fetchone()[0]
        
        # If no recent data, might indicate issues
        if recent_count == 0:
            context.log.warning("No recent streaming data found")
            return True
            
        return False
        
    except Exception as e:
        context.log.error(f"Error checking data quality: {e}")
        return True
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def days_since_last_full_refresh() -> int:
    """
    Calculate days since last full refresh
    """
    # This would check a metadata table or log
    # For now, return 0 to avoid triggering
    return 0

@asset(group_name="streaming_transformations")
def run_streaming_dbt(context) -> dict:
    """
    Run dbt streaming models with smart incremental/full refresh logic
    """
    try:
        import subprocess
        import os
        
        # Determine refresh strategy
        needs_full_refresh = should_full_refresh(context)
        
        # Set environment variables for dbt
        env = os.environ.copy()
        env.update({
            "DB_USER": "root",
            "DB_PASSWORD": "Alex@12345", 
            "DB_HOST": "127.0.0.1",
            "DB_NAME": "retail_analytics"
        })
        
        # Build dbt command
        if needs_full_refresh:
            context.log.info("Running FULL REFRESH for streaming models")
            dbt_command = [
                "dbt", "run", "--select", "streaming", "--full-refresh",
                "--project-dir", str(DBT_PROJECT_PATH),
                "--profiles-dir", str(DBT_PROJECT_PATH)
            ]
        else:
            context.log.info("Running INCREMENTAL refresh for streaming models")
            dbt_command = [
                "dbt", "run", "--select", "streaming",
                "--project-dir", str(DBT_PROJECT_PATH),
                "--profiles-dir", str(DBT_PROJECT_PATH)
            ]
        
        # Execute with timing
        start_time = time.time()
        result = subprocess.run(
            dbt_command,
            capture_output=True, 
            text=True, 
            env=env,
            cwd=str(DBT_PROJECT_PATH)
        )
        processing_time = time.time() - start_time
        
        if result.returncode == 0:
            context.log.info(f"dbt streaming models completed in {processing_time:.2f}s")
            
            # Parse dbt output for model counts
            models_processed = result.stdout.count("OK created") + result.stdout.count("OK inserted")
            
            return {
                "status": "success",
                "refresh_type": "full" if needs_full_refresh else "incremental",
                "processing_time_seconds": round(processing_time, 2),
                "models_processed": models_processed,
                "stdout": result.stdout[:1000]  # Truncate for display
            }
        else:
            context.log.error(f"dbt failed: {result.stderr}")
            return {
                "status": "failed",
                "refresh_type": "full" if needs_full_refresh else "incremental",
                "processing_time_seconds": round(processing_time, 2),
                "stderr": result.stderr
            }
            
    except Exception as e:
        context.log.error(f"Error running dbt: {e}")
        return {"status": "error", "message": str(e)}


@asset(group_name="streaming_infrastructure")
def kafka_producer_status(context) -> dict:
    """
    Check if Kafka producer is running and generating events
    """
    try:
        # Check if producer process is running
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'store_aware_producer.py' in cmdline:
                    context.log.info(f"Kafka producer running with PID: {proc.info['pid']}")
                    return {
                        "status": "running",
                        "pid": proc.info['pid'],
                        "process_name": proc.info['name']
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        context.log.warning("Kafka producer not found running")
        return {"status": "not_running", "pid": None}
        
    except Exception as e:
        context.log.error(f"Error checking producer status: {e}")
        return {"status": "error", "message": str(e)}


@asset(group_name="streaming_infrastructure")
def spark_streaming_status(context) -> dict:
    """
    Check if Spark streaming job is running and processing data
    """
    try:
        # Check if spark streaming process is running
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'enhanced_spark_stream.py' in cmdline:
                    context.log.info(f"Spark streaming running with PID: {proc.info['pid']}")
                    return {
                        "status": "running",
                        "pid": proc.info['pid'],
                        "process_name": proc.info['name']
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        context.log.warning("Spark streaming job not found running")
        return {"status": "not_running", "pid": None}
        
    except Exception as e:
        context.log.error(f"Error checking spark status: {e}")
        return {"status": "error", "message": str(e)}


@asset(group_name="streaming_data", deps=[kafka_producer_status, spark_streaming_status])
def streaming_data_validation(context) -> dict:
    """
    Validate that streaming data is flowing into MySQL
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        # Connect to MySQL
        connection = mysql.connector.connect(
            host='localhost',
            database='retail_analytics',
            user='root',
            password='Alex@12345'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Check recent data (last 5 minutes)
            query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN event_time >= NOW() - INTERVAL 5 MINUTE THEN 1 END) as recent_records,
                MAX(event_time) as latest_record,
                MIN(event_time) as earliest_record,
                COUNT(DISTINCT store_id) as unique_stores,
                COUNT(DISTINCT dept_id) as unique_departments
            FROM stream_sales_events
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                total_records, recent_records, latest_record, earliest_record, unique_stores, unique_departments = result
                
                validation_result = {
                    "total_records": total_records,
                    "recent_records": recent_records,
                    "latest_record": str(latest_record) if latest_record else None,
                    "earliest_record": str(earliest_record) if earliest_record else None,
                    "unique_stores": unique_stores,
                    "unique_departments": unique_departments,
                    "data_freshness": "fresh" if recent_records > 0 else "stale",
                    "validation_status": "healthy" if recent_records > 0 else "warning"
                }
                
                context.log.info(f"Streaming data validation: {validation_result}")
                return validation_result
            
    except Error as e:
        context.log.error(f"MySQL connection error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    
    return {"status": "error", "message": "Failed to validate streaming data"}


@asset(group_name="streaming_monitoring", deps=[run_streaming_dbt])
def streaming_performance_monitor(context, run_streaming_dbt) -> dict:
    """
    Monitor incremental streaming pipeline performance and data quality
    """
    try:
        import mysql.connector
        
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='retail_analytics',
            user='root',
            password='Alex@12345'
        )
        
        cursor = connection.cursor()
        
        # Get performance metrics
        metrics = {
            "dbt_processing_time": run_streaming_dbt.get("processing_time_seconds", 0),
            "dbt_refresh_type": run_streaming_dbt.get("refresh_type", "unknown"),
            "dbt_status": run_streaming_dbt.get("status", "unknown")
        }
        
        # Check data freshness
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                MAX(last_updated) as latest_update,
                TIMESTAMPDIFF(MINUTE, MAX(last_updated), NOW()) as minutes_since_update
            FROM rt_sales_summary
        """)
        
        freshness_data = cursor.fetchone()
        if freshness_data:
            metrics.update({
                "rt_sales_summary_records": freshness_data[0],
                "latest_update": str(freshness_data[1]),
                "data_freshness_minutes": freshness_data[2] or 0
            })
        
        # Check incremental performance thresholds
        performance_alerts = []
        
        if metrics["dbt_processing_time"] > 5.0:
            performance_alerts.append(f"High processing time: {metrics['dbt_processing_time']}s")
            
        if metrics["data_freshness_minutes"] > 10:
            performance_alerts.append(f"Stale data: {metrics['data_freshness_minutes']} minutes old")
        
        metrics["performance_alerts"] = performance_alerts
        metrics["performance_score"] = "good" if not performance_alerts else "warning"
        
        # Log alerts
        if performance_alerts:
            context.log.warning(f"Performance alerts: {performance_alerts}")
        else:
            context.log.info("All performance metrics within normal ranges")
        
        return metrics
        
    except Exception as e:
        context.log.error(f"Error monitoring performance: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


@asset(group_name="streaming_analytics", deps=[streaming_data_validation, run_streaming_dbt])
def streaming_analytics_summary(context) -> dict:
    """
    Generate business metrics from streaming data
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host='localhost',
            database='retail_analytics',
            user='root',
            password='Alex@12345'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Get hourly sales summary
            hourly_query = """
            SELECT 
                HOUR(event_time) as hour_of_day,
                COUNT(*) as transactions,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction_value
            FROM stream_sales_events 
            WHERE DATE(event_time) = CURDATE()
            GROUP BY HOUR(event_time)
            ORDER BY hour_of_day DESC
            LIMIT 5
            """
            
            cursor.execute(hourly_query)
            hourly_results = cursor.fetchall()
            
            # Get top performing stores
            store_query = """
            SELECT 
                store_id,
                COUNT(*) as transactions,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction_value
            FROM stream_sales_events 
            WHERE event_time >= NOW() - INTERVAL 1 HOUR
            GROUP BY store_id
            ORDER BY total_revenue DESC
            LIMIT 5
            """
            
            cursor.execute(store_query)
            store_results = cursor.fetchall()
            
            analytics = {
                "hourly_performance": [
                    {
                        "hour": row[0],
                        "transactions": row[1],
                        "revenue": float(row[2]),
                        "avg_value": float(row[3])
                    } for row in hourly_results
                ],
                "top_stores_last_hour": [
                    {
                        "store_id": row[0],
                        "transactions": row[1],
                        "revenue": float(row[2]),
                        "avg_value": float(row[3])
                    } for row in store_results
                ],
                "analysis_timestamp": pd.Timestamp.now().isoformat()
            }
            
            context.log.info(f"Generated streaming analytics: {len(hourly_results)} hourly records, {len(store_results)} top stores")
            return analytics
            
    except Error as e:
        context.log.error(f"Error generating analytics: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    
    return {"status": "error", "message": "Failed to generate analytics"}


# Job to run the complete streaming pipeline
from dagster import job

@job
def streaming_pipeline_job():
    """
    Complete streaming pipeline orchestration
    """
    # Check infrastructure
    producer_status = kafka_producer_status()
    spark_status = spark_streaming_status()
    
    # Validate data flow
    data_validation = streaming_data_validation()
    
    # Run dbt transformations
    dbt_run = run_streaming_dbt()
    
    # Generate analytics
    analytics = streaming_analytics_summary()