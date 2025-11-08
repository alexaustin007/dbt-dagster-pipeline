# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # Java (required for PySpark)
    openjdk-17-jdk-headless \
    # MySQL client for CLI access
    default-mysql-client \
    # Build tools for some Python packages
    gcc \
    g++ \
    make \
    # Network tools for debugging
    curl \
    wget \
    netcat-traditional \
    # Clean up
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy requirements file first (for better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy entire project
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data/raw /app/.dagster_home

# Expose Dagster port
EXPOSE 3000

# Set default command (can be overridden)
CMD ["tail", "-f", "/dev/null"]