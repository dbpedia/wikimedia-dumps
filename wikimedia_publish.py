import asyncio
import aiohttp
import json
import os
from datetime import datetime
import time

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

async def check_if_data_exists(session, wiki_name, job_name, date, api_key):
    """Check if data already exists on Databus"""
    
    databus_id = f"https://databus.dbpedia.org/tech0priyanshu/wikimedia/{wiki_name}-{job_name}/{date}"
    
    try:
        await rate_limiter.acquire()
        
        check_url = f"https://databus.dbpedia.org/tech0priyanshu/wikimedia/{wiki_name}-{job_name}/{date}"
        
        headers = {
            'accept': 'application/json',
            'X-API-KEY': api_key
        }
        
        async with session.get(check_url, headers=headers, timeout=10) as response:
            if response.status == 200:
                print(f"Data already exists: {databus_id}")
                return True
            elif response.status == 404:
                return False
            else:
                print(f"Could not verify existence (Status: {response.status}), proceeding...")
                return False
                
    except Exception as e:
        print(f"Error checking existence: {e}, proceeding...")
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

def create_api_payload(job_name, job_data, wiki_name, date, base_download_url="https://dumps.wikimedia.org"):
    """Create API payload for a specific job"""
    
    try:
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
    except:
        formatted_date = date
    
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
        
        payload = {
            "@context": "https://databus.dbpedia.org/res/context.jsonld",
            "@graph": [{
                "@type": "Version",
                "@id": f"https://databus.dbpedia.org/tech0priyanshu/wikimedia/{wiki_name}-{unique_job_name}/{date}",
                "title": f"{wiki_name} {unique_job_name} dump {formatted_date}",
                "description": f"Wikimedia {unique_job_name} dump of {wiki_name} for {formatted_date}.",
                "license": "http://creativecommons.org/licenses/by/4.0/",
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

async def process_single_job(session, job_name, job_data, wiki_name, date, api_key, semaphore):
    """Process a single job asynchronously with semaphore control"""
    
    async with semaphore:  # Limit concurrent jobs
        print(f"\n--- Processing Job: {job_name} ---")
        
        # Skip jobs that are not done
        if job_data.get('status') != 'done':
            print(f"  Skipping {job_name} - Status: {job_data.get('status')}")
            return 'skipped', 'status_not_done'
        
        # Check if data already exists
        try:
            if await check_if_data_exists(session, wiki_name, job_name, date, api_key):
                print(f"  Skipping {job_name} - Data already exists")
                return 'skipped', 'already_exists'
        except Exception as e:
            print(f"  Warning: Could not check existence for {job_name}: {e}")
            # Continue anyway
        
    # Create API payload
    payload_results = create_api_payload(job_name, job_data, wiki_name, date)
    
    if not payload_results:
        print(f"Failed to create payload for {job_name}")
        return 'failed', 'payload_creation_failed'
    
    # Process each payload (multiple for jobs with different content variants)
    total_successful = 0
    total_failed = 0
    
    for unique_job_name, payload in payload_results:
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
                # Don't count as failure
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
    if total_successful > 0 and total_failed == 0:
        return 'success', 'published'
    elif total_successful > 0 and total_failed > 0:
        return 'partial', 'partially_published'
    else:
        return 'failed', 'request_failed'

async def process_all_jobs(session, json_data, api_key, dump_status_url):
    """Process all jobs and make API calls concurrently"""
    
    jobs = json_data.get("jobs", {})
    
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
    print(f"Found {len(jobs)} jobs to process")
    
    # Create semaphore to limit concurrent jobs
    semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent jobs per wiki
    
    # Create tasks for all jobs
    tasks = []
    for job_name, job_data in jobs.items():
        task = process_single_job(session, job_name, job_data, wiki_name, date, api_key, semaphore)
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