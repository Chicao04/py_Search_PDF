import pdfplumber
import csv
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Đường dẫn đến file PDF của bạn
pdf_path = 'C:/Users/FPTSHOP/PycharmProjects/testuts/Search_PDF/file/1viettin.pdf'

# Hàm kiểm tra và chuẩn hóa giá trị tiền tệ
def fix_currency(value):
    if isinstance(value, str):
        value = value.replace('.', '').replace(',', '')
        if value.isdigit():
            return value
    return value

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

# Ghi dữ liệu đã trích xuất ra file CSV
csv_output_path = r'C:/Users/FPTSHOP/PycharmProjects/testuts/Search_PDF/file/Var_chong_phongbatviettin.csv'

with open(csv_output_path, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerows(extracted_data)

# Mở file PDF và đọc nội dung trang đầu tiên
with pdfplumber.open(pdf_path) as pdf:
    first_page = pdf.pages[0]  # Lấy trang đầu tiên
    first_page_text = first_page.extract_text()  # Trích xuất toàn bộ văn bản từ trang đầu tiên

# Hàm để xác định tên ngân hàng từ nội dung đầu trang
def identify_bank(content):
    if 'Vietinbank' in content:
        return 'Vietinbank'
    elif 'BIDV' in content:
        return 'BIDV'
    elif 'Agribank' in content:
        return 'Agribank'
    elif 'MB' in content:
        return 'MB Bank'
    elif 'Techcombank' in content:
        return 'Techcombank'
    elif 'Sacombank' in content:
        return 'Sacombank'
    elif 'ACB' in content:
        return 'ACB'
    elif 'VPBank' in content:
        return 'VPBank'
    else:
        return 'Không xác định'

# Nhận diện tên ngân hàng từ trang đầu tiên
bank_name = identify_bank(first_page_text)

# Đọc file CSV và xóa 2 dòng đầu
df = pd.read_csv(csv_output_path, header=None)
df = df.iloc[2:]  # Bỏ 2 dòng đầu
df['bank_name'] = bank_name

# Ghi lại vào file CSV sau khi thêm cột 'bank_name'
df.to_csv(csv_output_path, index=False, header=False, encoding='utf-8')

print("Đã thêm cột 'bank_name' vào file CSV.")

#####################################
# Phần tạo cơ sở dữ liệu và bảng SQL
#####################################

def create_database(database_name):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            # host='20.198.254.148',
            # port=3306,
            # user='root',
            # password='dPfLnZqwt8Kg',
        )
        if connection.is_connected():
            print("Kết nối MySQL thành công!")
            cursor = connection.cursor()
            create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            cursor.execute(create_database_query)
            print(f"Cơ sở dữ liệu '{database_name}' đã được tạo thành công hoặc đã tồn tại.")
            cursor.close()
            connection.close()
            print("Đã ngắt kết nối MySQL.")
    except Error as e:
        print(f"Lỗi: {e}")

# Tạo cơ sở dữ liệu với tên 'up_Var_phong_bat_data_viettin'
create_database('update_Var_phong_bat_data_viettin')

# Kết nối tới MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    # host='20.198.254.148',
    # port=3306,
    # user='root',
    # password='dPfLnZqwt8Kg',
    database='update_Var_phong_bat_data_viettin'
)

# Đọc file CSV không có tiêu đề
df = pd.read_csv(csv_output_path, header=None)
df.columns = ['id', 'day', 'content', 'money', 'sender', 'bank_name']

# Chuyển đổi cột 'B' (Ngày giao dịch) thành định dạng ngày MySQL (YYYY-MM-DD HH:MM:SS)
df['B'] = pd.to_datetime(df['B'], format='%d/%m/%Y %H:%M:%S', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

# Tạo con trỏ MySQL
cursor = conn.cursor()

# Tạo bảng nếu chưa tồn tại, thêm cột 'bank_name'
create_table_query = """
CREATE TABLE IF NOT EXISTS history_transaction (
    id INT AUTO_INCREMENT PRIMARY KEY,
    day DATETIME,
    content VARCHAR(255),
    money INT,
    sender VARCHAR(255),
    bank_name VARCHAR(255)
)
"""
cursor.execute(create_table_query)

# Kiểm tra và xử lý các giá trị NaN trước khi chèn vào MySQL
df.dropna(inplace=True)
df.fillna('', inplace=True)

# Lấy dữ liệu dạng mảng từ dataframe để chèn vào MySQL
data_to_insert = df[['id', 'day', 'content', 'money', 'sender',  'bank_name']].values.tolist()


# Chèn dữ liệu hàng loạt vào MySQL (bỏ qua cột 'id' vì nó tự động tăng)
sql = "INSERT INTO history_transaction (id,day, content, money, sender, bank_name) VALUES (%s, %s, %s, %s, %s,%s)"

batch_size = 1000  # Số lượng bản ghi mỗi lần chèn

for i in range(0, len(data_to_insert), batch_size):
    batch_data = data_to_insert[i:i + batch_size]
    cursor.executemany(sql, batch_data)
    conn.commit()




## thêm api tim kiếm thông tin tên người chuyển khoản


## thêm api sắp xêp thêm giá trị tiền lớn và nhỏ




# Đóng kết nối
cursor.close()
conn.close()

print("Dữ liệu đã được chèn thành công từ CSV vào MySQL.")


