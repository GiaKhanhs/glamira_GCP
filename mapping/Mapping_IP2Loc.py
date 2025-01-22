import json
import time
import IP2Location

# Bắt đầu đo thời gian
start_time = time.time()

# Khởi tạo IP2Location
ip2location = IP2Location.IP2Location('/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/data_not_restore/IP-COUNTRY-REGION-CITY.BIN')

# Đường dẫn file JSON lớn
input_file = '/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/mapping/unique_ips.json'
output_file = '/Users/giakhanh/Desktop/Data Engineer/glamira_project/project/mappingmapping_glamira_IP.json'

chunk_size = 100  # Số bản ghi trong mỗi phần (tùy chỉnh theo nhu cầu)

# Xử lý file JSON
with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
    # Đọc toàn bộ dữ liệu input (danh sách JSON)
    ip_list = json.load(infile)  # Parse JSON danh sách

    outfile.write('[')  # Bắt đầu danh sách JSON
    first_record = True  # Cờ để thêm dấu phẩy ngăn cách

    # Duyệt qua từng IP trong danh sách
    for idx, ip_address in enumerate(ip_list, start=1):
        print(f"Đang xử lý {idx}/{len(ip_list)}: {ip_address}...", end='\r')

        try:
            # Lấy thông tin vị trí từ IP2Location
            location = ip2location.get_all(ip_address)
            record = {
                "ip": ip_address,
                "location": {
                    "country": location.country_short,
                    "region": location.region,
                    "city": location.city,
                    "latitude": location.latitude,
                    "longitude": location.longitude
                }
            }

            # Ghi từng record vào file output
            if not first_record:
                outfile.write(',\n')  # Ngăn cách các record JSON
            json.dump(record, outfile, indent=4)
            first_record = False  # Sau bản ghi đầu tiên, bật ngăn cách

        except Exception as e:
            print(f"\nLỗi xử lý IP '{ip_address}': {e}")
            continue

    outfile.write('\n]')  # Kết thúc danh sách JSON

print("\nXử lý hoàn tất!")  # Dòng trống để xóa dòng hiển thị tiến trình

# Đo thời gian hoàn thành
end_time = time.time()
print(f"Xử lý hoàn thành. File đầu ra: {output_file}")
print(f"Thời gian chạy toàn bộ chương trình: {end_time - start_time:.2f} giây")
