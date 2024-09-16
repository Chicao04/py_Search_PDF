CREATE DATABASE your_database;
use your_database;
select * from your_table;

-- sắp xếp theo tăng dần 
SELECT * FROM your_table ORDER BY col4 ASC;

-- sắp xếp theo giảm dần 
SELECT * FROM your_table ORDER BY col4 desc;


-- tìm người ủng hộ nhiều nhất
SELECT * FROM your_table
ORDER BY col4 DESC
LIMIT 1;

-- tìm người ủng hộ ít nhất 
SELECT * FROM your_table
ORDER BY col4 asc
LIMIT 1;



