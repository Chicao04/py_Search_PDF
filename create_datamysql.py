import pdfplumber
import csv
import re
import mysql.connector
from mysql.connector import Error

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


def create_database(database_name):
    try:
        # Kết nối đến MySQL (kết nối với một cơ sở dữ liệu hiện tại, hoặc sử dụng MySQL server không yêu cầu cơ sở dữ liệu cụ thể)
        connection = mysql.connector.connect(
            host='20.198.254.148',
            port=3306,
            user='root',
            password='dPfLnZqwt8Kg',
        )

        if connection.is_connected():
            print("Kết nối MySQL thành công!")

            # Tạo đối tượng cursor
            cursor = connection.cursor()

            # Tạo cơ sở dữ liệu mới
            create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            cursor.execute(create_database_query)
            print(f"Cơ sở dữ liệu '{database_name}' đã được tạo thành công hoặc đã tồn tại.")

            # Đóng cursor và kết nối
            cursor.close()
            connection.close()
            print("Đã ngắt kết nối MySQL.")

    except Error as e:
        print(f"Lỗi: {e}")


# Tạo cơ sở dữ liệu với tên 'my_new_database'
create_database('Var_phong_bat_database')


# Kết nối đến MySQL
connection = mysql.connector.connect(
    # host='localhost',
    # user='root',
    # password='',
    host='20.198.254.148',
    port=3306,
    user='root',
    password='dPfLnZqwt8Kg',
    database='Var_phong_bat_database'
)

cursor = connection.cursor()

# Tạo bảng nếu chưa tồn tại (nếu cần)
create_table_query = """
CREATE TABLE IF NOT EXISTS table_view (
    number INT PRIMARY KEY,
    day VARCHAR(255),
    content VARCHAR(255),
    money int,
    people VARCHAR(255)
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
insert_query = "INSERT INTO table_view (number, day, content, money, people) VALUES (%s, %s, %s, %s, %s)"
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
