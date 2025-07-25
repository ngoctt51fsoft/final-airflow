# Vấn đề với thiết kế warehouse
Với dữ liệu raw từ log_data, có nhiều trường hợp user có nhiều level khác nhau

Ví dụ: User: 80, level: free/paid

Vì level đã có trong bảng fact, giải pháp là bỏ đi trường level trong bảng dim users

Các bước thực hiện:
- Tạo connection 
![Postgres Connection](assets/postgres_connection.png)

- Dùng pgadmin tạo bảng:

![Postgres Connection](assets/tables.png)

- Graph sau khi chạy
![Postgres Connection](assets/baitap.png)