#!/bin/bash

# Wikimedia Dumps Crawler and Publisher
# This script crawls wikimedia dumps and publishes them to DBpedia Databus
set -e  

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
CRAWLED_URLS_FILE="${SCRIPT_DIR}/crawled_urls.txt"
LOG_FILE="${LOG_DIR}/crawl_publish_$(date +%Y%m%d_%H%M%S).log"

# Create logs directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

# Function to check if required environment variables are set
check_env_vars() {
    if [ -z "${DATABUS_API_KEY}" ]; then
        log "ERROR: DATABUS_API_KEY environment variable is not set"
        exit 1
    fi
    log "Environment variables check passed"
}

# Function to check if Python and required packages are available
check_dependencies() {
    log "Checking dependencies..."
    
    if ! command -v python3 &> /dev/null; then
        log "ERROR: python3 is not installed"
        exit 1
    fi
    
    # Check if required Python packages are installed
    python3 -c "import asyncio, aiohttp, json, os, time" 2>/dev/null || {
        log "ERROR: Required Python packages are missing. Installing..."
        pip3 install aiohttp beautifulsoup4 || {
            log "ERROR: Failed to install required packages"
            exit 1
        }
    }
    
    log "Dependencies check passed"
}

# Function to run the crawler
run_crawler() {
    log "Starting wikimedia dumps crawler..."
    
    cd "${SCRIPT_DIR}"
    
    if [ -f "${CRAWLED_URLS_FILE}" ]; then
        log "Backing up existing crawled_urls.txt"
        cp "${CRAWLED_URLS_FILE}" "${CRAWLED_URLS_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
    fi
    
    python3 wiki_dumps_crawler.py 2>&1 | tee -a "${LOG_FILE}"
    
    if [ $? -eq 0 ] && [ -f "${CRAWLED_URLS_FILE}" ]; then
        CRAWLED_COUNT=$(wc -l < "${CRAWLED_URLS_FILE}")
        log "Crawler completed successfully. Found ${CRAWLED_COUNT} URLs"
    else
        log "ERROR: Crawler failed or no URLs found"
        exit 1
    fi
}

# Function to run the publisher
run_publisher() {
    log "Starting wikimedia dumps publisher..."
    
    cd "${SCRIPT_DIR}"
    
    if [ ! -f "${CRAWLED_URLS_FILE}" ]; then
        log "ERROR: crawled_urls.txt not found. Run crawler first."
        exit 1
    fi
    
    URLS_COUNT=$(wc -l < "${CRAWLED_URLS_FILE}")
    log "Publishing ${URLS_COUNT} URLs to DBpedia Databus..."
    
    python3 wikimedia_publish.py 2>&1 | tee -a "${LOG_FILE}"
    
    if [ $? -eq 0 ]; then
        log "Publisher completed successfully"
    else
        log "ERROR: Publisher failed"
        exit 1
    fi
}

# Function to cleanup old log files (keep last 10)
cleanup_logs() {
    log "Cleaning up old log files..."
    cd "${LOG_DIR}"
    ls -t crawl_publish_*.log 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true
    log "Log cleanup completed"
}

# Function to generate summary report
generate_report() {
    log "Generating summary report..."
    
    cat << EOF | tee -a "${LOG_FILE}"

========================================
CRAWL AND PUBLISH SUMMARY REPORT
========================================
Date: $(date)
Script: $0
Working Directory: ${SCRIPT_DIR}
Log File: ${LOG_FILE}

URLs Crawled: $([ -f "${CRAWLED_URLS_FILE}" ] && wc -l < "${CRAWLED_URLS_FILE}" || echo "0")
Log Size: $([ -f "${LOG_FILE}" ] && du -h "${LOG_FILE}" | cut -f1 || echo "0")

Last 10 lines of execution:
$(tail -n 10 "${LOG_FILE}" 2>/dev/null || echo "No log entries found")

========================================
EOF
}

# Function to handle script interruption
cleanup_on_exit() {
    log "Script interrupted. Performing cleanup..."
    generate_report
    exit 1
}

# Set up signal handlers
trap cleanup_on_exit INT TERM

# Main execution function
main() {
    log "========================================="
    log "Starting Wikimedia Dumps Crawl & Publish"
    log "========================================="
    log "Script started from: ${SCRIPT_DIR}"
    log "Log file: ${LOG_FILE}"
    
    # Step 1: Check environment and dependencies
    check_env_vars
    check_dependencies
    
    # Step 2: Run crawler
    run_crawler
    
    # Step 3: Run publisher
    run_publisher
    
    # Step 4: Cleanup and reporting
    cleanup_logs
    generate_report
    
    log "========================================="
    log "Wikimedia Dumps Crawl & Publish Completed Successfully!"
    log "========================================="
}

# Help function
show_help() {
    cat << EOF
Wikimedia Dumps Crawler and Publisher

Usage: $0 [OPTIONS]

OPTIONS:
    --help, -h          Show this help message
    --crawler-only      Run only the crawler
    --publisher-only    Run only the publisher
    --dry-run          Run in dry-run mode (crawler only, no publishing)

ENVIRONMENT VARIABLES:
    DATABUS_API_KEY     Required. Your DBpedia Databus API key

EXAMPLES:
    # Full crawl and publish
    export DATABUS_API_KEY="your-api-key"
    $0

    # Crawler only
    $0 --crawler-only

    # Publisher only (requires existing crawled_urls.txt)
    $0 --publisher-only

    # Docker usage
    docker run -e DATABUS_API_KEY="your-key" -v /path/to/data:/app/data your-image

CRON USAGE:
    # Run daily at 2 AM
    0 2 * * * cd /path/to/wikimedia-dumps && DATABUS_API_KEY="your-key" ./crawl_publish.sh >> /var/log/wikimedia-dumps.log 2>&1

FILES:
    crawled_urls.txt        Output of crawler, input for publisher
    logs/                   Directory containing execution logs
    wiki_dumps_crawler.py   Python script for crawling
    wikimedia_publish.py    Python script for publishing

EOF
}

# Parse command line arguments
case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    --crawler-only)
        log "Running crawler only..."
        check_env_vars
        check_dependencies
        run_crawler
        generate_report
        log "Crawler-only execution completed"
        ;;
    --publisher-only)
        log "Running publisher only..."
        check_env_vars
        check_dependencies
        run_publisher
        generate_report
        log "Publisher-only execution completed"
        ;;
    --dry-run)
        log "Running in dry-run mode (crawler only)..."
        check_dependencies
        run_crawler
        log "Dry-run completed. Found $(wc -l < "${CRAWLED_URLS_FILE}") URLs"
        log "To publish, run: $0 --publisher-only"
        ;;
    "")
        # No arguments, run full process
        main
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac