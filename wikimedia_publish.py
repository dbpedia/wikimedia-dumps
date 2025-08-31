import asyncio
import aiohttp
import json
import os
from datetime import datetime
import time
from dotenv import load_dotenv
load_dotenv()

class RateLimiter:
    def __init__(self, max_calls_per_second=10):  # Reduced from 50 to 10
        self.max_calls_per_second = max_calls_per_second
        self.calls = []
    
    async def acquire(self):
        now = time.time()
        # Remove calls older than 1 second
        self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
        
        if len(self.calls) >= self.max_calls_per_second:
            # Wait until we can make another call
            sleep_time = 1.0 - (now - self.calls[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            # Remove the oldest call after sleeping
            self.calls.pop(0)
        
        self.calls.append(now)

# Global rate limiter
rate_limiter = RateLimiter(10)  # Reduced from 50 to 10

# Databus configuration
DATABUS_USER = "tech1priyanshu"  # Configurable user - can be changed anytime

# Job filtering configuration
# To only publish specific jobs, add them to ALLOWED_JOBS list
# To publish all jobs except some, leave ALLOWED_JOBS empty and add jobs to BLOCKED_JOBS
# Examples:
#   ALLOWED_JOBS = ["articlesdumprecombine"]  # Only publish articlesdumprecombine
#   ALLOWED_JOBS = []  # Publish all jobs
#   BLOCKED_JOBS = ["xmlstubsdump", "metacurrentdump"]  # Skip these specific jobs
ALLOWED_JOBS = ["articlesdumprecombine"]  # Only publish this job
BLOCKED_JOBS = []  # Additional jobs to block (applied after ALLOWED_JOBS)

def should_process_job(job_name):
    """
    Determine if a job should be processed based on filtering configuration.
    
    Args:
        job_name (str): Name of the job to check
        
    Returns:
        bool: True if job should be processed, False if it should be skipped
    """
    # If ALLOWED_JOBS is not empty, only process jobs in the allowed list
    if ALLOWED_JOBS and job_name not in ALLOWED_JOBS:
        return False
    
    # If job is in BLOCKED_JOBS, skip it
    if job_name in BLOCKED_JOBS:
        return False
    
    return True

async def check_if_data_exists(session, wiki_name, job_name, version_info, api_key):
    """Check if data already exists on Databus using SPARQL endpoint"""
    
    # Use new URL structure for articlesdumprecombine, old structure for others
    if job_name == "articlesdumprecombine":
        databus_id = f"https://databus.dbpedia.org/{DATABUS_USER}/{wiki_name}/articlesdumprecombine/{version_info}"
    else:
        # Keep old format for other jobs (though they'll be filtered out)
        databus_id = f"https://databus.dbpedia.org/{DATABUS_USER}/wikimedia/{wiki_name}-{job_name}/{version_info}"
    
    try:
        await rate_limiter.acquire()
        
        # SPARQL query to check if the version exists - using correct namespace
        sparql_query = f"""
        PREFIX databus: <https://dataid.dbpedia.org/databus#>
        
        ASK {{
            <{databus_id}> a databus:Version .
        }}
        """
        
        sparql_url = "https://databus.dbpedia.org/sparql"
        
        # Prepare form data for SPARQL endpoint
        data = {
            'query': sparql_query.strip(),
            'format': 'json'
        }
        
        headers = {
            'Accept': 'application/sparql-results+json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        print(f"  Checking existence via SPARQL: {databus_id}")
        
        async with session.post(sparql_url, data=data, headers=headers, timeout=15) as response:
            if response.status == 200:
                result = await response.json()
                # ASK query returns boolean in 'boolean' field
                exists = result.get('boolean', False)
                if exists:
                    print(f"  ✓ Data already exists in Databus: {databus_id}")
                    return True
                else:
                    print(f"  ✗ Data not found in Databus: {databus_id}")
                    return False
            else:
                print(f"  SPARQL query failed (Status: {response.status}), proceeding...")
                return False
                
    except Exception as e:
        print(f"  Error checking existence via SPARQL: {e}, proceeding...")
        return False

async def fetch_and_process_dump_status(session, dump_status_url, api_key):
    """Fetch dump status JSON from URL and process all jobs to make API calls"""
    
    try:
        await rate_limiter.acquire()
        
        print(f"Fetching dump status from: {dump_status_url}")
        
        async with session.get(dump_status_url, timeout=30) as response:
            response.raise_for_status()
            json_data = await response.json()
            print(f"Successfully fetched dump status data")
        
        # Process all jobs
        return await process_all_jobs(session, json_data, api_key, dump_status_url)
        
    except aiohttp.ClientError as e:
        print(f"Failed to fetch dump status: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def extract_wiki_info(filename):
    """Extract wiki name and date from filename"""
    parts = filename.split('-')
    if len(parts) >= 2:
        wiki_name = parts[0]  # aawiki
        date = parts[1]       # 20250601
        return wiki_name, date
    return None, None

def get_content_variant(filename):
    """Get content variant based on filename patterns"""
    if 'stub-meta-history' in filename:
        return 'history'
    elif 'stub-meta-current' in filename:
        return 'current'
    elif 'stub-articles' in filename:
        return 'articles'
    elif 'pages-meta-history' in filename:
        return 'history'
    elif 'pages-meta-current' in filename:
        return 'current'
    elif 'pages-articles' in filename:
        return 'articles'
    elif 'pages-logging' in filename:
        return 'logging'
    elif 'abstract' in filename:
        return 'abstract'
    elif 'all-titles' in filename:
        return 'titles'
    elif 'multistream' in filename:
        return 'multistream'
    elif 'category' in filename:
        return 'category'
    elif 'external' in filename:
        return 'external'
    elif 'image' in filename:
        return 'image'
    elif 'pagelinks' in filename:
        return 'pagelinks'
    elif 'redirect' in filename:
        return 'redirect'
    elif 'template' in filename:
        return 'template'
    elif 'langlinks' in filename:
        return 'langlinks'
    elif 'iwlinks' in filename:
        return 'iwlinks'
    elif 'page_props' in filename:
        return 'page_props'
    elif 'protected_titles' in filename:
        return 'protected_titles'
    elif 'page_restrictions' in filename:
        return 'page_restrictions'
    elif 'user_groups' in filename:
        return 'user_groups'
    elif 'user_former_groups' in filename:
        return 'user_former_groups'
    elif 'change_tag' in filename:
        return 'change_tag'
    elif 'geo_tags' in filename:
        return 'geo_tags'
    elif 'site_stats' in filename:
        return 'site_stats'
    elif 'babel' in filename:
        return 'babel'
    elif 'flagged' in filename:
        return 'flagged'
    elif 'wbc_entity_usage' in filename:
        return 'wbc_entity_usage'
    elif 'linktarget' in filename:
        return 'linktarget'
    elif 'sites' in filename:
        return 'sites'
    elif 'namespaces' in filename:
        return 'namespaces'
    else:
        # Use filename without extension as fallback
        return filename.split('.')[0].split('-')[-1]

def get_file_extension_and_compression(filename):
    """Extract file extension and compression from filename"""
    if filename.endswith('.xml.gz'):
        return 'xml', 'gz'
    elif filename.endswith('.xml.bz2'):
        return 'xml', 'bz2'
    elif filename.endswith('.xml.7z'):
        return 'xml', '7z'
    elif filename.endswith('.sql.gz'):
        return 'sql', 'gz'
    elif filename.endswith('.json.gz'):
        return 'json', 'gz'
    elif filename.endswith('.txt.bz2'):
        return 'txt', 'bz2'
    elif filename.endswith('.gz'):
        return 'txt', 'gz'
    else:
        if '.' in filename:
            ext = filename.split('.')[-1]
            return ext, 'none'
        return 'unknown', 'none'

def create_api_payload(job_name, job_data, wiki_name, version_info, base_download_url="https://dumps.wikimedia.org"):
    """Create API payload for a specific job"""
    
    try:
        # For articlesdumprecombine, version_info is JSON version (like "0.8")
        # For other jobs, version_info is date (like "20250620")
        if job_name == "articlesdumprecombine":
            formatted_version = version_info  # Use JSON version as-is
        else:
            # Format date for other jobs
            formatted_version = f"{version_info[:4]}-{version_info[4:6]}-{version_info[6:8]}"
    except:
        formatted_version = version_info
    
    files = job_data.get('files', {})
    
    # Group files by content variant to create separate distributions
    file_groups = {}
    for filename, file_info in files.items():
        content_variant = get_content_variant(filename)
        if content_variant not in file_groups:
            file_groups[content_variant] = []
        file_groups[content_variant].append((filename, file_info))
    
    # Create separate payloads for each content variant group
    payloads = []
    
    for content_variant, file_list in file_groups.items():
        distributions = []
        
        for filename, file_info in file_list:
            file_ext, compression = get_file_extension_and_compression(filename)
            
            distribution = {
                "@type": "Part",
                "formatExtension": file_ext,
                "compression": compression,
                # Use hardcoded SHA256 hash  (64 characters)
                "sha256sum": "6b148c103921f48a2bfa290bd1c7d86730d1a551fce63425a4dc3aa3d63c390f",
                "dcat:byteSize": file_info.get('size', 0),
                "downloadURL": base_download_url + file_info.get('url', '')
            }
            distributions.append(distribution)
        
        # Create unique identifier for each content variant
        unique_job_name = f"{job_name}-{content_variant}" if len(file_groups) > 1 else job_name
        
        # Use new URL structure for articlesdumprecombine, old structure for others
        if job_name == "articlesdumprecombine":
            databus_id = f"https://databus.dbpedia.org/{DATABUS_USER}/{wiki_name}/articlesdumprecombine/{version_info}"
            title = f"{wiki_name} articlesdumprecombine dump version {version_info}"
            description = f"Wikimedia articlesdumprecombine dump of {wiki_name} for version {version_info}."
        else:
            databus_id = f"https://databus.dbpedia.org/{DATABUS_USER}/wikimedia/{wiki_name}-{unique_job_name}/{version_info}"
            title = f"{wiki_name} {unique_job_name} dump {formatted_version}"
            description = f"Wikimedia {unique_job_name} dump of {wiki_name} for {formatted_version}."
        
        payload = {
            "@context": "https://databus.dbpedia.org/res/context.jsonld",
            "@graph": [{
                "@type": "Version",
                "@id": databus_id,
                "title": title,
                "description": description,
                "license": "https://creativecommons.org/licenses/by-sa/4.0/",
                "distribution": distributions
            }]
        }
        
        payloads.append((unique_job_name, payload))
    
    return payloads

async def make_api_request(session, payload, api_key):
    """Make the API request to DBpedia Databus"""
    
    await rate_limiter.acquire()
    
    url = "https://databus.dbpedia.org/api/publish?fetch-file-properties=false"
    
    headers = {
        'accept': 'application/json',
        'X-API-KEY': api_key,
        'Content-Type': 'application/ld+json'
    }
    
    try:
        # Convert payload to JSON string manually (like curl -d)
        json_data = json.dumps(payload)
        
        # Add debugging
        print(f"Making request to: {url}")
        print(f"Headers: {headers}")
        print(f"Payload size: {len(json_data)} characters")
        
        async with session.post(url, headers=headers, data=json_data, timeout=60) as response:
            response_text = await response.text()
            print(f"Response status: {response.status}")
            if response.status != 200:
                print(f"Response headers: {dict(response.headers)}")
                print(f"Response body: {response_text}")
            return response.status, response_text
            
    except asyncio.TimeoutError as e:
        error_msg = f"Request timeout: {e}"
        print(error_msg)
        return None, error_msg
    except aiohttp.ClientConnectionError as e:
        error_msg = f"Connection error: {e}"
        print(error_msg)
        return None, error_msg
    except aiohttp.ClientResponseError as e:
        error_msg = f"Response error: {e}"
        print(error_msg)
        return None, error_msg
    except aiohttp.ClientError as e:
        error_msg = f"Client error: {e}"
        print(error_msg)
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {type(e).__name__}: {e}"
        print(error_msg)
        return None, error_msg

async def process_single_job(session, job_name, job_data, wiki_name, date, json_version, api_key, semaphore):
    """Process a single job asynchronously with semaphore control"""
    
    async with semaphore:  # Limit concurrent jobs
        print(f"\n--- Processing Job: {job_name} ---")
        
        # Check if job should be processed based on filtering configuration
        if not should_process_job(job_name):
            print(f"  Skipping {job_name} - Job filtered out (not in allowed list or in blocked list)")
            return 'skipped', 'job_filtered'
        
        # Skip jobs that are not done
        if job_data.get('status') != 'done':
            print(f"  Skipping {job_name} - Status: {job_data.get('status')}")
            return 'skipped', 'status_not_done'
        
    # Use JSON version for articlesdumprecombine, date for others
    version_info = json_version if job_name == "articlesdumprecombine" else date
    
    # Create API payload first to get the actual unique job names
    payload_results = create_api_payload(job_name, job_data, wiki_name, version_info)
    
    if not payload_results:
        print(f"Failed to create payload for {job_name}")
        return 'failed', 'payload_creation_failed'
    
    # Process each payload (multiple for jobs with different content variants)
    total_successful = 0
    total_failed = 0
    total_skipped = 0
    
    for unique_job_name, payload in payload_results:
        # Check if this specific unique job already exists
        try:
            if await check_if_data_exists(session, wiki_name, unique_job_name, version_info, api_key):
                print(f"  Skipping {unique_job_name} - Data already exists")
                total_skipped += 1
                continue
        except Exception as e:
            print(f"  Warning: Could not check existence for {unique_job_name}: {e}")
            # Continue anyway
        
        files_count = len(payload['@graph'][0]['distribution'])
        print(f"Created payload for {unique_job_name} with {files_count} files")
        
        # Debug: Print sample payload for first job
        print("Sample payload:")
        print(json.dumps(payload, indent=2)[:500] + "...")
        
        # Make API request
        status_code, response_text = await make_api_request(session, payload, api_key)
        
        if status_code:
            if status_code == 200:
                print(f"✓ Successfully published {unique_job_name}")
                total_successful += 1
            elif status_code == 409:
                print(f"  {unique_job_name} already exists (409 Conflict)")
                total_skipped += 1
            elif status_code == 400:
                print(f"✗ Bad request for {unique_job_name} - Status: {status_code}")
                print(f"Response: {response_text}")
                total_failed += 1
            elif status_code == 401:
                print(f"✗ Authentication failed for {unique_job_name} - Check API key")
                total_failed += 1
            elif status_code == 403:
                print(f"✗ Forbidden for {unique_job_name} - Check permissions")
                total_failed += 1
            else:
                print(f"✗ Failed to publish {unique_job_name} - Status: {status_code}")
                print(f"Response: {response_text[:300]}...")
                total_failed += 1
        else:
            print(f"✗ Failed to make request for {unique_job_name}: {response_text}")
            total_failed += 1
    
    # Return based on overall success
    if total_skipped > 0 and total_successful == 0 and total_failed == 0:
        return 'skipped', 'already_exists'
    elif total_successful > 0 and total_failed == 0:
        return 'success', 'published'
    elif total_successful > 0 and total_failed > 0:
        return 'partial', 'partially_published'
    else:
        return 'failed', 'request_failed'

async def process_all_jobs(session, json_data, api_key, dump_status_url):
    """Process all jobs and make API calls concurrently"""
    
    jobs = json_data.get("jobs", {})
    
    # Extract JSON version
    json_version = json_data.get("version", "unknown")
    print(f"Extracted JSON version: {json_version}")
    
    # Extract wiki info from the first job's first file
    wiki_name = None
    date = None
    
    for job_name, job_data in jobs.items():
        files = job_data.get('files', {})
        if files:
            first_filename = list(files.keys())[0]
            wiki_name, date = extract_wiki_info(first_filename)
            break
    
    if not wiki_name or not date:
        print("Could not extract wiki name or date from filenames")
        return False
    
    print(f"Processing dumps for {wiki_name} on {date}")
    print(f"Using JSON version: {json_version} for articlesdumprecombine")
    print(f"Found {len(jobs)} jobs to process")
    
    # Show filter configuration
    if ALLOWED_JOBS:
        print(f"Job filter: Only processing jobs: {ALLOWED_JOBS}")
    if BLOCKED_JOBS:
        print(f"Job filter: Blocking jobs: {BLOCKED_JOBS}")
    if not ALLOWED_JOBS and not BLOCKED_JOBS:
        print("Job filter: Processing all jobs (no filters applied)")
    
    # Count jobs that will actually be processed
    jobs_to_process = [job_name for job_name in jobs.keys() if should_process_job(job_name)]
    filtered_jobs = [job_name for job_name in jobs.keys() if not should_process_job(job_name)]
    
    print(f"Jobs to process: {len(jobs_to_process)}")
    if filtered_jobs:
        print(f"Jobs filtered out: {len(filtered_jobs)} - {filtered_jobs}")
    
    # Create semaphore to limit concurrent jobs
    semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent jobs per wiki
    
    # Create tasks for all jobs
    tasks = []
    for job_name, job_data in jobs.items():
        task = process_single_job(session, job_name, job_data, wiki_name, date, json_version, api_key, semaphore)
        tasks.append((job_name, task))
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
    
    # Process results
    successful_jobs = 0
    failed_jobs = 0
    skipped_jobs = 0
    partial_jobs = 0
    
    for i, (job_name, result) in enumerate(zip([name for name, _ in tasks], results)):
        if isinstance(result, Exception):
            print(f"Exception in {job_name}: {result}")
            failed_jobs += 1
        else:
            status, reason = result
            if status == 'success':
                successful_jobs += 1
            elif status == 'failed':
                failed_jobs += 1
            elif status == 'skipped':
                skipped_jobs += 1
            elif status == 'partial':
                partial_jobs += 1
    
    print(f"\n--- Summary ---")
    print(f"Successful: {successful_jobs}")
    print(f"Partial: {partial_jobs}")
    print(f"Failed: {failed_jobs}")
    print(f"Skipped: {skipped_jobs}")
    print(f"Total: {successful_jobs + partial_jobs + failed_jobs + skipped_jobs}")
    
    return (successful_jobs + partial_jobs) > 0

async def process_multiple_wikis(urls_file, api_key):
    """Process multiple wiki dump URLs from a file asynchronously"""
    
    try:
        with open(urls_file, 'r') as f:
            base_urls = [url.strip() for url in f.readlines() if url.strip()]
        
        dump_status_urls = [url.rstrip("/") + "/dumpstatus.json" for url in base_urls]
        
        print(f"Processing {len(dump_status_urls)} wiki dumps")
        
        # Create aiohttp session with connection limits
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)  # Reduced limits
        timeout = aiohttp.ClientTimeout(total=120)  # Increased timeout
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Process wikis in batches to reduce load
            batch_size = 10  # Process 10 wikis at a time
            successful_wikis = 0
            failed_wikis = 0
            
            for i in range(0, len(dump_status_urls), batch_size):
                batch_urls = dump_status_urls[i:i+batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(dump_status_urls) + batch_size - 1) // batch_size
                
                print(f"\n{'='*60}")
                print(f"Processing batch {batch_num}/{total_batches} ({len(batch_urls)} wikis)")
                print(f"{'='*60}")
                
                # Create tasks for current batch
                tasks = []
                for j, url in enumerate(batch_urls, 1):
                    print(f"Queuing {i + j}/{len(dump_status_urls)}: {url}")
                    task = fetch_and_process_dump_status(session, url, api_key)
                    tasks.append((url, task))
                
                print(f"\nProcessing batch {batch_num} with {len(tasks)} wikis...")
                
                # Execute batch tasks concurrently
                results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
                
                # Process batch results
                batch_successful = 0
                batch_failed = 0
                
                for j, (url, result) in enumerate(zip([url for url, _ in tasks], results)):
                    if isinstance(result, Exception):
                        print(f"Exception processing {url}: {result}")
                        batch_failed += 1
                    elif result:
                        batch_successful += 1
                    else:
                        batch_failed += 1
                
                successful_wikis += batch_successful
                failed_wikis += batch_failed
                
                print(f"\nBatch {batch_num} Summary:")
                print(f"  Successful: {batch_successful}")
                print(f"  Failed: {batch_failed}")
                print(f"  Total: {batch_successful + batch_failed}")
                
                # Add delay between batches to avoid overwhelming the server
                if i + batch_size < len(dump_status_urls):
                    print(f"Waiting 10 seconds before next batch...")
                    await asyncio.sleep(10)
            
            print(f"\n{'='*60}")
            print(f"FINAL SUMMARY")
            print(f"{'='*60}")
            print(f"Wikis processed successfully: {successful_wikis}")
            print(f"Wikis failed: {failed_wikis}")
            print(f"Total wikis: {successful_wikis + failed_wikis}")
                
    except FileNotFoundError:
        print(f"URLs file not found: {urls_file}")
    except Exception as e:
        print(f"Error processing URLs file: {e}")

async def main():
    # Configuration
    API_KEY = os.getenv("DATABUS_API_KEY")
    
    if not API_KEY:
        print("Please set DATABUS_API_KEY environment variable")
        return
    
    print(f"Using API key: {API_KEY[:10]}...{API_KEY[-4:] if len(API_KEY) > 14 else API_KEY}")
    
    urls_file = "crawled_urls.txt"
    await process_multiple_wikis(urls_file, API_KEY)

if __name__ == "__main__":
    asyncio.run(main())