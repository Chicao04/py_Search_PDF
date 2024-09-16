import pdfplumber
import csv
import re

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
csv_output_path = r'C:/Users/FPTSHOP/PycharmProjects/testuts/Search_PDF/file/Var_phongbat.csv'  # Sửa đường dẫn bằng chuỗi thô

with open(csv_output_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Ghi từng hàng dữ liệu vào file CSV
    writer.writerows(extracted_data)

print(f"Dữ liệu đã được ghi ra file CSV tại: {csv_output_path}")
