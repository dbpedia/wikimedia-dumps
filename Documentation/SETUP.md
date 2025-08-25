# Wikimedia Dumps Crawler & Publisher Setup Guide

## Quick Start

### 1. Local Setup
```bash
# Clone the repository
git clone <your-repo-url>
cd wikimedia-dumps

# Set up environment
cp .env.example .env

# Edit .env and add your DATABUS_API_KEY

# Install dependencies
pip install -r requirements.txt

# Make script executable
chmod +x crawl_publish.sh

# Run full process
export DATABUS_API_KEY="your-api-key"
./crawl_publish.sh
```

### 2. Docker Setup
```bash
# Build and run with docker-compose
cp .env.example .env
# Edit .env and add your DATABUS_API_KEY

docker-compose up --build
```

### 3. Cron Job Setup

#### Local Cron
```bash
# Edit crontab
crontab -e

# Add this line for daily execution at 2 AM
0 2 * * * cd /path/to/wikimedia-dumps && DATABUS_API_KEY="your-key" ./crawl_publish.sh >> /var/log/wikimedia-dumps.log 2>&1
```

#### Docker Cron
```bash
# Edit docker-compose.yml and uncomment the cron command section
# Then run:
docker-compose up -d

```


## File Structure
```
wikimedia-dumps/
├── crawl_publish.sh          # Main script
├── wiki_dumps_crawler.py     # Crawler script
├── wikimedia_publish.py      # Publisher script
├── requirements.txt          # Python dependencies
├── Dockerfile               # Docker image definition
├── docker-compose.yml       # Docker compose configuration
├── .env.example            # Environment variables template
├── crawled_urls.txt        # Output: crawled URLs (generated)
├── data/                   # Volume mount for persistent data
└── logs/                   # Execution logs
    └── crawl_publish_YYYYMMDD_HHMMSS.log
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABUS_API_KEY` | Yes | Your DBpedia Databus API key |
| `PYTHONUNBUFFERED` | No | Set to 1 for real-time logging |

## Monitoring & Logs

### Log Files
- Logs are stored in `logs/` directory
- Each run creates a timestamped log file
- Old logs are automatically cleaned (keeps last 10)

### Log Monitoring with Docker
```bash
# View logs in real-time
docker-compose logs -f wikimedia-crawler

# With log viewer (if monitoring profile enabled)
docker-compose --profile monitoring up -d
# Visit http://localhost:8080
```

## Cron Schedule Examples

```bash
# Daily at 2 AM
0 2 * * * /path/to/script

# Weekly on Sunday at 3 AM
0 3 * * 0 /path/to/script

# Monthly on 1st at 4 AM
0 4 1 * * /path/to/script

# Every 6 hours
0 */6 * * * /path/to/script
```

## Troubleshooting

### Common Issues

1. **Missing API Key**
   ```bash
   ERROR: DATABUS_API_KEY environment variable is not set
   ```
   Solution: Set the environment variable or add it to .env file

2. **Permission Denied**
   ```bash
   Permission denied: ./crawl_publish.sh
   ```
   Solution: `chmod +x crawl_publish.sh`

3. **Python Dependencies Missing**
   ```bash
   ModuleNotFoundError: No module named 'aiohttp'
   ```
   Solution: `pip install -r requirements.txt`

4. **Docker Build Fails**
   Solution: Ensure Docker is running and try `docker-compose build --no-cache`


## Performance Tuning

### For Large Scale Operations
1. Adjust rate limiting in `wikimedia_publish.py`
2. Modify batch sizes in the publisher
3. Increase Docker resource limits

### Resource Requirements
- **Memory**: 1-2 GB recommended
- **CPU**: 2+ cores for parallel processing
- **Storage**: 100MB+ for logs and temporary data
- **Network**: Stable internet connection

## Security Notes

- Store API keys securely (use .env file, not in code)
- Run with non-root user in Docker
- Regularly update dependencies
- Monitor logs for suspicious activity

## Support

For issues and questions:
1. Check the logs for detailed error messages
2. Verify environment variables are set correctly
3. Ensure all dependencies are installed
4. Check network connectivity to Wikimedia and Databus endpoints


