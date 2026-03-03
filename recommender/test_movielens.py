import pandas as pd

print("กำลังอ่านไฟล์ MovieLens...")

ratings = pd.read_csv(
    "ml-100k/u.data",
    sep="\t",
    names=["user_id", "movie_id", "rating", "timestamp"]
)

print("อ่านไฟล์สำเร็จ 🎉")
print(ratings.head())
print("จำนวนแถวทั้งหมด:", len(ratings))
