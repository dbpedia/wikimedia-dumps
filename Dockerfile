FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY wiki_dumps_crawler.py .
COPY wikimedia_publish.py .
COPY crawl_publish.sh .

# Make script executable
RUN chmod +x crawl_publish.sh

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs


# Set up environment
ENV PYTHONUNBUFFERED=1
ENV DATABUS_API_KEY=""

# Health check - check if the process is running instead of file existence
HEALTHCHECK --interval=5m --timeout=30s --start-period=30s --retries=3 \
    CMD pgrep -f "python|crawl_publish" > /dev/null || exit 1

# Default command
CMD ["./crawl_publish.sh"]
