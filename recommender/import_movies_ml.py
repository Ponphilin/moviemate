import pandas as pd
import mysql.connector

# เชื่อม MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="moviemate"
)
cursor = conn.cursor()

# อ่านไฟล์ MovieLens (u.item)
movies = pd.read_csv(
    "data/ml-100k/u.item",
    sep="|",
    encoding="latin-1",
    header=None,
    usecols=[0, 1]
)

# ใส่ข้อมูลเข้า table movies_ml
sql = "INSERT INTO movies_ml (movie_id, title) VALUES (%s, %s)"
cursor.executemany(sql, movies.values.tolist())
conn.commit()

print("Imported movies:", len(movies))

cursor.close()
conn.close()
