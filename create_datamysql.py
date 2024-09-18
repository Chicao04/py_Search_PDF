
import pdfplumber
import csv
import pandas as pd
import mysql.connector
# from aifc import Error
from mysql.connector import Error
from datetime import datetime

####################
#Ghi dữ liệu vô csv
####################

# Đường dẫn đến file PDF của bạn
pdf_path = 'C:/Users/FPTSHOP/PycharmProjects/testuts/Search_PDF/file/tt.pdf'


def fix_currency(value):
    """Hàm kiểm tra và chuẩn hóa giá trị tiền tệ"""
    # Xóa dấu phân cách hàng nghìn (nếu có) và giữ lại phần số
    if isinstance(value, str):
        # Xóa dấu chấm (.) hoặc dấu phẩy (,) nếu nó dùng làm phân cách hàng nghìn
        value = value.replace('.', '').replace(',', '')

        # Kiểm tra xem giá trị có phải là số không sau khi xử lý
        if value.isdigit():
            return value
    return value


# Mở file PDF và đọc nội dung
with pdfplumber.open(pdf_path) as pdf:
    # Khởi tạo list để chứa dữ liệu từ các trang
    extracted_data = []

    # Lặp qua từng trang trong file PDF
    for page in pdf.pages:
        # Trích xuất bảng dữ liệu từ trang
        tables = page.extract_tables()

        # Nếu tìm thấy bảng, thêm dữ liệu vào danh sách
        if tables:
            for table in tables:
                # Kiểm tra và xử lý từng hàng trong bảng
                for row in table:
                    # Xử lý từng ô trong hàng để chuẩn hóa giá trị tiền tệ
                    new_row = [fix_currency(cell) if isinstance(cell, str) else cell for cell in row]
                    extracted_data.append(new_row)

# Ghi dữ liệu đã trích xuất ra file CSV
csv_output_path = r'C:/Users/FPTSHOP/PycharmProjects/testuts/Search_PDF/file/Var_chong_phongbat.csv'  # Sửa đường dẫn bằng chuỗi thô

with open(csv_output_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Ghi từng hàng dữ liệu vào file CSV
    writer.writerows(extracted_data)



#Ghi dữ liệu vô csv

def create_database(database_name):
    try:
        # Kết nối đến MySQL (kết nối với một cơ sở dữ liệu hiện tại, hoặc sử dụng MySQL server không yêu cầu cơ sở dữ liệu cụ thể)
        connection = mysql.connector.connect(
            host='20.198.254.148',
            port=3306,
            user='root',
            password='dPfLnZqwt8Kg',
            # host='localhost',
            # user='root',
            # password='',
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


# Kết nối tới MySQL
conn = mysql.connector.connect(
    # host='localhost',
    # user='root',
    # password='',
    # host='20.198.254.148',
    # port=3306,
    # user='root',
    # password='dPfLnZqwt8Kg',
    # database='Var_phong_bat_database'
)

# Đọc file CSV không có tiêu đề
df = pd.read_csv('C:/Users/FPTSHOP/PycharmProjects/testuts/Search_PDF/file/Var_chong_phongbat.csv', header=None)

# Đặt tên cột thủ công
df.columns = ['A', 'B', 'C', 'D', 'E']

# Chuyển đổi cột 'B' (Ngày giao dịch) thành định dạng ngày MySQL (YYYY-MM-DD HH:MM:SS)
df['B'] = pd.to_datetime(df['B'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')


# Tạo con trỏ MySQL
cursor = conn.cursor()

# Tạo bảng nếu chưa tồn tại
create_table_query = """
CREATE TABLE IF NOT EXISTS history_transaction (
    id INT PRIMARY KEY,
    day DATETIME,
    content VARCHAR(255),
    money INT,
    sender VARCHAR(255)
)
"""
cursor.execute(create_table_query)

# Lặp qua các hàng trong dataframe và chèn vào MySQL
for index, row in df.iterrows():
    sql = "INSERT INTO history_transaction (id, day, content, money, sender) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql, (row['A'], row['B'], row['C'], row['D'], row['E']))

# Commit các thay đổi
conn.commit()

# Đóng kết nối
cursor.close()
conn.close()

print("Dữ liệu đã được chèn thành công từ CSV vào MySQL.")
