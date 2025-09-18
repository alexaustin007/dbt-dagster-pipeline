
# Retail Data Pipeline

A simple data pipeline that processes retail sales data using Python, MySQL, dbt, and Dagster.

## What This Does

This project takes CSV files with sales data and transforms them into clean, analyzed data ready for business insights. It handles both batch processing (files) and real-time streaming data.

## Quick Setup

### 1. Prerequisites
- Python 3.9 or higher
- MySQL 8.0 running locally
- Java 8+ (only needed for streaming features)

### 2. Installation
```bash
# Navigate to project folder
cd /Users/alexaustinchettiar/Downloads/retail_data_pipeline_full

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create database
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS retail_analytics;"
```

### 3. Run the Pipeline

**Option A: Using Dagster (Recommended)**
```bash
# Start Dagster web interface
dagster dev -f dagster_project/repository.py

# Open http://localhost:3000 in your browser
# Click "Materialize All" for the data loading assets
# Then run dbt transformations
cd dbt_project
dbt run
```

**Option B: Manual Steps**
```bash
# Load data from CSV files
python ingestion/from_file.py --db_user root --db_password YOUR_PASSWORD --db_name retail_analytics

# Transform data
cd dbt_project
dbt run

# Check results
mysql -u root -p retail_analytics -e "SELECT * FROM fct_sales_summary LIMIT 10;"
```

## What's Inside

```
├── data/raw/              # Put your CSV files here
├── ingestion/             # Scripts to load data into MySQL
├── dbt_project/           # Data transformations
├── dagster_project/       # Pipeline orchestration
├── streaming/             # Real-time data processing (optional)
└── notebooks/             # Data analysis
```

## Data Flow

1. **Raw Data**: Place CSV files in `data/raw/`
2. **Load**: Scripts move CSV data into MySQL raw tables
3. **Transform**: dbt cleans and joins the data
4. **Analyze**: Final tables ready for reporting

## Key Commands

```bash
# Start the pipeline orchestrator
dagster dev -f dagster_project/repository.py

# Run data transformations
cd dbt_project && dbt run

# Test data quality
cd dbt_project && dbt test

# Load new data files
python ingestion/from_file.py --db_user root --db_password YOUR_PASSWORD
```

## Streaming Demo (Optional)

If you want to try real-time data processing:

```bash
# Terminal 1: Start Kafka
brew services start kafka

# Terminal 2: Generate fake sales events
python streaming/kafka_producer.py

# Terminal 3: Process streaming data
spark-submit streaming/spark_stream_job.py
```

## Configuration

- Database settings: `.env` file
- dbt connection: `dbt_project/profiles.yml`
- Pipeline settings: `dagster_project/dagster.yaml`

## Getting Help

- Check the Dagster UI at http://localhost:3000 for pipeline status
- Look at `CLAUDE.md` for detailed technical documentation
- All commands should be run from the project root directory with the virtual environment activated
