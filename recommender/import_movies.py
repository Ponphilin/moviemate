import pandas as pd
import mysql.connector

# =========================
# เชื่อม MySQL
# =========================
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="moviemate"
)
cursor = conn.cursor()

# =========================
# อ่านไฟล์ u.item (แก้ path ตรงนี้)
# =========================
movies = pd.read_csv(
    "data/ml-100k/u.item",
    sep="|",
    encoding="latin-1",
    header=None
)

# =========================
# ตั้งชื่อ column ตาม MovieLens
# =========================
movies.columns = [
    "movie_id", "title", "release_date", "video_release",
    "imdb_url",
    "unknown", "Action", "Adventure", "Animation", "Children",
    "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
    "FilmNoir", "Horror", "Musical", "Mystery", "Romance",
    "SciFi", "Thriller", "War", "Western"
]

# =========================
# map genre (เลือกแนวหลัก)
# =========================
def map_genre(row):
    if row.Action == 1: return "action"
    if row.Comedy == 1: return "comedy"
    if row.Drama == 1: return "drama"
    if row.Romance == 1: return "romance"
    if row.Horror == 1: return "horror"
    return "other"

# =========================
# insert ข้อมูลลง DB
# =========================
for _, row in movies.iterrows():
    year = 1990
    if isinstance(row.release_date, str) and len(row.release_date) >= 4:
        year = int(row.release_date[-4:])

    cursor.execute(
        """
        INSERT IGNORE INTO movies
        (id, title, genre, synopsis, actors, director, year)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(row.movie_id),
            row.title,
            map_genre(row),
            "No synopsis available",
            "Unknown",
            "Unknown",
            year
        )
    )

conn.commit()
print("Import movies สำเร็จ 🎬")

conn.close()
