import os
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup

# Constants
MAX_CONCURRENT_REQUESTS = 200
PROGRESS_FILE = 'progress.json'
FILE_URL_ERROR = 'url_error.txt'
TIMEOUT_DURATION = 15  # Increased timeout duration for fetching URLs

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# === Utility Functions ===
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as file:
            return json.load(file)
    return {'completed_files': [], 'last_processed_record': {}}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as file:
        json.dump(progress, file, ensure_ascii=False, indent=4)

def load_json_file(file_path):
    """Attempt to load JSON data from a file and handle non-standard JSON structures."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read().strip()
        try:
            return json.loads(content)  # Try to parse JSON normally first
        except json.JSONDecodeError:
            data = []
            lines = content.splitlines()
            for line in lines:
                try:
                    if line.strip():
                        data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in file {file_path} on line {line}: {e}")
            return data

def save_json_file(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

# === Crawling Functions ===
async def fetch_url(session, url):
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        async with session.get(url, timeout=TIMEOUT_DURATION) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"Failed to fetch {url} with status: {response.status}")
    except asyncio.TimeoutError:
        print(f"TimeoutError when connecting to {url}")
        return None
    except aiohttp.ClientError as e:
        print(f"Error connecting to {url}: {e}")
        return None

async def extract_product_name(content):
    if content:
        soup = BeautifulSoup(content, "html.parser")
        methods = [
            lambda: soup.find('span', class_='base').text.strip(),
            lambda: soup.find("div", class_="product-info-desc").find("h1").text.strip()
        ]
        for method in methods:
            try:
                return method()
            except (AttributeError, TypeError):
                continue
    return None

# === Processing Functions ===
async def process_record(record, session, index):
    if not isinstance(record, dict):
        print(f"Error: Record at index {index} is not a dictionary.")
        return

    url = record.get('current_url', '').strip()
    if url:
        async with semaphore:
            content = await fetch_url(session, url)
            if content:
                product_name = await extract_product_name(content)
                if product_name:
                    record['product_name'] = product_name
                else:
                    print(f"Product name not found for URL: {url}")
            else:
                with open(FILE_URL_ERROR, 'a', encoding='utf-8') as error_file:
                    error_file.write(f"{url}\n")
    else:
        print(f"Record {index} has no valid URL.")

async def process_json_file(file_path, file_name, start_index, session):
    data = load_json_file(file_path)
    tasks = [process_record(record, session, i) for i, record in enumerate(data[start_index:], start=start_index)]
    await asyncio.gather(*tasks)
    save_json_file(file_path, data)

async def process_json_files(folder_path):
    progress = load_progress()
    async with aiohttp.ClientSession() as session:
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.json') and file_name not in progress['completed_files']:
                file_path = os.path.join(folder_path, file_name)
                print(f"Processing file: {file_name}")
                start_index = progress.get('last_processed_record', {}).get(file_name, 0)
                await process_json_file(file_path, file_name, start_index, session)
                progress['completed_files'].append(file_name)
                save_progress(progress)
                print(f"Completed file: {file_name}")

# === Main Function ===
if __name__ == "__main__":
    folder_path = '/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/IP_Glamira_data'
    asyncio.run(process_json_files(folder_path))