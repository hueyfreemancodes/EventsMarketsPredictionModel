FROM python:3.9-slim

WORKDIR /app

# Install system dependencies (basic build tools)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY config/ /app/config/

# Copy necessary data files
COPY polymarket_nba_markets_100639.json /app/

# Set Environment Variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV COLLECTION_DURATION_SECONDS=-1

# Run the collector
CMD ["python", "scripts/run_targeted_collector.py"]
