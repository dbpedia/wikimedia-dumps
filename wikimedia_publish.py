import asyncio
import aiohttp
import json
import os
import time
import hashlib
from dotenv import load_dotenv
load_dotenv()

# ---------------- RATE LIMITER ---------------- #
class RateLimiter:
    def __init__(self, max_calls_per_second=10):
        self.max_calls_per_second = max_calls_per_second
        self.calls = []

    async def acquire(self):
        now = time.time()
        self.calls = [t for t in self.calls if now - t < 1.0]

        if len(self.calls) >= self.max_calls_per_second:
            sleep_time = 1.0 - (now - self.calls[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            self.calls.pop(0)

        self.calls.append(now)

rate_limiter = RateLimiter(10)

# ---------------- CONFIG ---------------- #
DATABUS_USER = "tech1priyanshu"
ALLOWED_JOBS = ["articlesdumprecombine"]
BLOCKED_JOBS = []

# ---------------- HELPERS ---------------- #
def should_process_job(job_name):
    if ALLOWED_JOBS and job_name not in ALLOWED_JOBS:
        return False
    if job_name in BLOCKED_JOBS:
        return False
    return True

def extract_wiki_info(filename):
    parts = filename.split('-')
    if len(parts) >= 2:
        return parts[0], parts[1]
    return None, None

def get_content_variant(filename):
    keywords = [
        "history", "current", "articles", "logging", "abstract", "titles",
        "multistream", "category", "external", "image", "pagelinks",
        "redirect", "template", "langlinks", "iwlinks", "page_props",
        "protected_titles", "page_restrictions", "user_groups",
        "user_former_groups", "change_tag", "geo_tags", "site_stats",
        "babel", "flagged", "wbc_entity_usage", "linktarget", "sites", "namespaces"
    ]
    for k in keywords:
        if k in filename:
            return k
    return filename.split('.')[-2]

def get_file_extension_and_compression(filename):
    if filename.endswith(".xml.gz"):
        return "xml", "gz"
    if filename.endswith(".xml.bz2"):
        return "xml", "bz2"
    if filename.endswith(".sql.gz"):
        return "sql", "gz"
    if filename.endswith(".json.gz"):
        return "json", "gz"
    if "." in filename:
        return filename.split(".")[-1], "none"
    return "unknown", "none"

# ---------------- SHA256 ---------------- #
async def compute_sha256(session, url):
    sha256 = hashlib.sha256()
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            async for chunk in response.content.iter_chunked(1024 * 1024):
                sha256.update(chunk)
    except Exception as e:
        # For testing, if download fails, return dummy hash
        return "TEST_HASH_1234567890"
    return sha256.hexdigest()

# ---------------- PAYLOAD ---------------- #
async def create_api_payload(session, job_name, job_data, wiki_name, version_info,
                             base_download_url="https://dumps.wikimedia.org"):

    files = job_data.get("files", {})
    file_groups = {}

    for filename, info in files.items():
        variant = get_content_variant(filename)
        file_groups.setdefault(variant, []).append((filename, info))

    payloads = []

    for variant, file_list in file_groups.items():
        distributions = []

        for filename, info in file_list:
            ext, compression = get_file_extension_and_compression(filename)
            download_url = base_download_url + info.get("url", "")
            sha256sum = await compute_sha256(None, download_url)
            distributions.append({
                "@type": "Part",
                "formatExtension": ext,
                "compression": compression,
                "sha256sum": sha256sum,
                "dcat:byteSize": info.get("size", 0),
                "downloadURL": download_url
            })

        databus_id = f"https://databus.dbpedia.org/{DATABUS_USER}/{wiki_name}/{job_name}/{version_info}"
        title = f"{wiki_name} {job_name} dump {version_info}"
        description = f"Wikimedia {job_name} dump for {wiki_name}."

        payloads.append((
            job_name,
            {
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
        ))

    return payloads

# ---------------- API CALL ---------------- #
async def make_api_request(session, payload, api_key):
    await rate_limiter.acquire()
    # For test mode, just print payload instead of calling real API
    print("\n=== TEST PAYLOAD ===")
    print(json.dumps(payload, indent=2))
    return 200, "TEST_OK"

# ---------------- JOB PROCESSING ---------------- #
async def process_single_job(session, job_name, job_data, wiki_name,
                             date, json_version, api_key, semaphore):
    async with semaphore:
        if not should_process_job(job_name):
            return "skipped", "filtered"
        if job_data.get("status") != "done":
            return "skipped", "not_done"

    version_info = json_version if job_name == "articlesdumprecombine" else date
    payloads = await create_api_payload(session, job_name, job_data, wiki_name, version_info)

    for _, payload in payloads:
        status, response = await make_api_request(session, payload, api_key)
        if status != 200:
            return "failed", response

    return "success", "published"

# ---------------- ENTRY ---------------- #
async def main(test_mode=True):
    api_key = os.getenv("DATABUS_API_KEY") or "TESTKEY"

    if test_mode:
        # read a local JSON for testing
        with open("test_dumpstatus.json") as f:
            data = json.load(f)
        jobs = data.get("jobs", {})
        json_version = data.get("version", "unknown")
        wiki, date = None, None
        for j in jobs.values():
            if j.get("files"):
                wiki, date = extract_wiki_info(next(iter(j["files"])))
                break
        semaphore = asyncio.Semaphore(3)
        for job_name, job_data in jobs.items():
            await process_single_job(None, job_name, job_data, wiki, date, json_version, api_key, semaphore)
        print("\n✅ Test run finished successfully!")
        return

    # real mode
    with open("crawled_urls.txt") as f:
        urls = [u.strip() + "/dumpstatus.json" for u in f if u.strip()]

    async with aiohttp.ClientSession() as session:
        for url in urls:
            async with session.get(url) as resp:
                data = await resp.json()
            jobs = data.get("jobs", {})
            json_version = data.get("version", "unknown")
            wiki, date = None, None
            for j in jobs.values():
                if j.get("files"):
                    wiki, date = extract_wiki_info(next(iter(j["files"])))
                    break
            semaphore = asyncio.Semaphore(3)
            for job_name, job_data in jobs.items():
                await process_single_job(session, job_name, job_data, wiki, date, json_version, api_key, semaphore)

if __name__ == "__main__":
    asyncio.run(main(test_mode=True))
