import os
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count

# Constants
MAX_CONCURRENT_REQUESTS = 200
PROGRESS_FILE = 'progress.json'
FILE_URL_ERROR = 'url_error.txt'
TIMEOUT_DURATION = 15  # Timeout duration for fetching URLs

# Semaphore cho asyncio
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# === Utility Functions ===
def load_progress():
    """Load progress từ file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as file:
            return json.load(file)
    return {'completed_files': [], 'last_processed_record': {}}

def save_progress(progress):
    """Save progress vào file."""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as file:
        json.dump(progress, file, ensure_ascii=False, indent=4)

def load_json_file(file_path):
    """Load dữ liệu JSON từ file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            return json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file {file_path}: {e}")
            return []

def save_json_file(file_path, data):
    """Save dữ liệu JSON trở lại file gốc."""
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

# === Crawling Functions ===
async def fetch_url(session, url):
    """Fetch nội dung HTML từ URL."""
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
    except aiohttp.ClientError as e:
        print(f"Error connecting to {url}: {e}")
    return None

async def extract_product_name(content):
    """Trích xuất tên sản phẩm từ nội dung HTML."""
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

async def process_record(record, session, index):
    """Xử lý một bản ghi JSON."""
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

async def process_json_data(data, file_path, start_index):
    """Xử lý dữ liệu JSON."""
    async with aiohttp.ClientSession() as session:
        tasks = [
            process_record(record, session, i) 
            for i, record in enumerate(data[start_index:], start=start_index)
        ]
        await asyncio.gather(*tasks)
    save_json_file(file_path, data)

def process_file(file_path):
    """Xử lý một file JSON."""
    progress = load_progress()
    data = load_json_file(file_path)

    # Lấy vị trí bắt đầu từ progress
    start_index = progress.get('last_processed_record', {}).get(file_path, 0)

    # Xử lý dữ liệu JSON bằng asyncio
    asyncio.run(process_json_data(data, file_path, start_index))

    # Cập nhật progress
    progress['completed_files'].append(file_path)
    progress['last_processed_record'].pop(file_path, None)
    save_progress(progress)
    print(f"Completed file: {file_path}")

# === Main Function ===
def process_json_files_in_folder(folder_path):
    """Xử lý tất cả các file JSON trong folder với multiprocessing."""
    progress = load_progress()
    completed_files = progress.get('completed_files', [])

    # Lấy danh sách file chưa xử lý
    files_to_process = [
        os.path.join(folder_path, file_name) 
        for file_name in os.listdir(folder_path) 
        if file_name.endswith('.json') and file_name not in completed_files
    ]

    # Dùng multiprocessing để xử lý song song các file
    with Pool(cpu_count()) as pool:
        pool.map(process_file, files_to_process)

if __name__ == "__main__":
    folder_path = '/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/IP_Glamira_data'
    process_json_files_in_folder(folder_path)
    print("All files processed!")
