import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

base_url = "https://dumps.wikimedia.org/"
base_domain = urlparse(base_url).netloc

visited_urls = set()
successful_urls = set()
urls_to_visit = asyncio.Queue()

# File extensions to skip
skip_extensions = [".bz2", ".xml", ".sql", ".gz", ".zip", ".tar", ".7z",".html"]

def should_skip(url):
    return any(url.lower().endswith(ext) for ext in skip_extensions)

async def fetch_and_parse(session, url):
    try:
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                print(f"Failed {url} - Status {response.status}")
                return []

            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type:
                print(f"Non-HTML content skipped: {url}")
                return []

            successful_urls.add(url)

            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            links = []

            for tag in soup.find_all("a", href=True):
                href = tag["href"]
                if href.startswith("#") or href.startswith(".."):
                    continue

                next_url = urljoin(url, href)
                parsed = urlparse(next_url)

                if parsed.netloc != base_domain or should_skip(next_url):
                    continue

                if next_url not in visited_urls:
                    links.append(next_url)

            return links

    except Exception as e:
        print(f"Error crawling {url}: {e}")
        return []

async def worker(session, worker_id):
    while True:
        url = await urls_to_visit.get()
        if url in visited_urls:
            urls_to_visit.task_done()
            continue

        print(f"Worker {worker_id} crawling: {url}")
        visited_urls.add(url)
        new_links = await fetch_and_parse(session, url)

        for link in new_links:
            if link not in visited_urls:
                await urls_to_visit.put(link)

        urls_to_visit.task_done()

async def main():
    await urls_to_visit.put(base_url)

    async with aiohttp.ClientSession() as session:
        tasks = []
        num_workers = 40
        for i in range(num_workers):
            task = asyncio.create_task(worker(session, i))
            tasks.append(task)

        await urls_to_visit.join()

        for task in tasks:
            task.cancel()

        # Save only successfully fetched HTML URLs
        with open("crawled_urls.txt", "w", encoding="utf-8") as f:
            for url in sorted(successful_urls):
                f.write(url + "\n")

if __name__ == "__main__":
    asyncio.run(main())
