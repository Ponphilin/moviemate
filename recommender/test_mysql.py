import mysql.connector

print("เริ่มเชื่อม MySQL...")

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",        # ถ้ามีรหัส ใส่ตรงนี้
    database="moviemate"
)

print("เชื่อมต่อสำเร็จ 🎉")

conn.close()
