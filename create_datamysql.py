import pdfplumber
import csv
import re
import mysql.connector

# Đường dẫn đến file PDF của bạn
pdf_path = 'C:/Users/FPTSHOP/PycharmProjects/testuts/Search_PDF/file/tt.pdf'

# Hàm chuẩn hóa giá trị tiền tệ
def fix_currency(value):
    if isinstance(value, str):
        # Xóa dấu phân cách hàng nghìn và giữ lại phần số
        value = value.replace('.', '').replace(',', '')
        if value.isdigit():
            return value
    return value

# Kết nối đến MySQL
connection = mysql.connector.connect(
    host='localhost',        # Địa chỉ máy chủ MySQL
    user='root',             # Tên người dùng MySQL
    password='',# Mật khẩu MySQL
    database='your_database' # Tên cơ sở dữ liệu MySQL
)

cursor = connection.cursor()

# Tạo bảng nếu chưa tồn tại (nếu cần)
create_table_query = """
CREATE TABLE IF NOT EXISTS your_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    col1 VARCHAR(255),
    col2 VARCHAR(255),
    col3 VARCHAR(255),
    col4 int,
    col5 VARCHAR(255)
)
"""
cursor.execute(create_table_query)

# Mở file PDF và đọc nội dung
with pdfplumber.open(pdf_path) as pdf:
    extracted_data = []
    for page in pdf.pages:
        tables = page.extract_tables()
        if tables:
            for table in tables:
                for row in table:
                    new_row = [fix_currency(cell) if isinstance(cell, str) else cell for cell in row]
                    extracted_data.append(new_row)

# Lưu dữ liệu vào MySQL
insert_query = "INSERT INTO your_table (col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s)"
for row in extracted_data:
    # Giả sử bạn có 5 cột, thay đổi nếu cần
    if len(row) >= 5:  # Đảm bảo mỗi hàng có đủ 5 cột
        cursor.execute(insert_query, tuple(row[:5]))

# Lưu các thay đổi vào MySQL
connection.commit()

# Đóng kết nối
cursor.close()
connection.close()

print("Dữ liệu đã được lưu vào MySQL.")
