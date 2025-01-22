import json
import os
from threading import Thread, Lock
from queue import Queue
from tqdm import tqdm
import time

# Đường dẫn file và cấu hình
input_file = '/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/glamira.json'
output_dir = '/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/IP_Glamira_data'
processed_ips_file = '/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/processed_ips.txt'
num_threads = 10
chunk_size = 2000000

# Lock để đồng bộ cập nhật tiến độ
progress_lock = Lock()

# Hàm tải danh sách IP đã xử lý
def load_processed_ips():
    if os.path.exists(processed_ips_file):
        with open(processed_ips_file, 'r') as f:
            return set(line.strip() for line in f)
    return set()

# Hàm lưu IP đã xử lý vào file tạm
def save_processed_ip(ip):
    with open(processed_ips_file, 'a') as f:
        f.write(ip + '\n')

# Hàm xử lý một chunk
def process_chunk(chunk, output_dir, progress_bar, processed_ips):
    grouped_data = {}
    for record in chunk:
        ip = record.get('ip', 'unknown')
        if ip not in grouped_data:
            grouped_data[ip] = []
        grouped_data[ip].append(record)
        with progress_lock:
            progress_bar.update(1)

    for ip, records in grouped_data.items():
        if ip in processed_ips:  # Bỏ qua nếu IP đã xử lý
            continue
        sanitized_ip = ip.replace('.', '_')
        output_file = os.path.join(output_dir, f"{sanitized_ip}.json")
        with open(output_file, 'a') as outfile:
            json.dump(records, outfile)
            outfile.write('\n')
        with progress_lock:
            print(f"Đã xử lý {len(records)} bản ghi cho IP: {ip}")
            save_processed_ip(ip)  # Lưu IP đã xử lý

# Worker xử lý các chunks
def worker(processed_ips, progress_bar, queue):
    while True:
        chunk = queue.get()
        if chunk is None:
            break
        process_chunk(chunk, output_dir, progress_bar, processed_ips)
        queue.task_done()

# Điều phối xử lý song song
def process_in_threads(input_file, output_dir, num_threads):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    processed_ips = load_processed_ips()
    queue = Queue()
    threads = []
    progress_bar = tqdm(total=count_lines(input_file))

    for _ in range(num_threads):
        thread = Thread(target=worker, args=(processed_ips, progress_bar, queue))
        thread.start()
        threads.append(thread)

    chunk = []
    with open(input_file, 'r') as infile:
        for line in infile:
            chunk.append(json.loads(line.strip()))
            if len(chunk) >= chunk_size:
                queue.put(chunk)
                chunk = []
        if chunk:
            queue.put(chunk)

    queue.join()

    for _ in range(num_threads):
        queue.put(None)
    for thread in threads:
        thread.join()

    progress_bar.close()
    print("Hoàn thành xử lý.")

# Đếm số dòng trong file
def count_lines(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

# Chạy chương trình
start_time = time.time()
process_in_threads(input_file, output_dir, num_threads)
end_time = time.time()
print(f"Hoàn thành xử lý với {num_threads} luồng và {chunk_size} dòng mỗi chunk.")
print(f"Tổng thời gian xử lý: {end_time - start_time:.2f} giây.")