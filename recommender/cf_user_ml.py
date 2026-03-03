import mysql.connector
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity


def recommend_for_user_ml(target_user_id, top_n=10):
    # 🔹 เชื่อม MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="moviemate"
    )

    # 🔹 ดึง ratings จาก MovieLens
    df = pd.read_sql(
        "SELECT user_id, movie_id, rating FROM ratings_ml",
        conn
    )

    # 🔹 สร้าง user-movie matrix
    user_movie_matrix = df.pivot_table(
        index="user_id",
        columns="movie_id",
        values="rating"
    ).fillna(0)

    # 🔹 คำนวณ similarity ระหว่าง users
    user_similarity = cosine_similarity(user_movie_matrix)
    user_similarity_df = pd.DataFrame(
        user_similarity,
        index=user_movie_matrix.index,
        columns=user_movie_matrix.index
    )

    # 🔹 หาผู้ใช้ที่คล้ายกัน (ไม่รวมตัวเอง)
    similar_users = (
        user_similarity_df[target_user_id]
        .sort_values(ascending=False)
        .drop(target_user_id)
        .head(20)
    )

    # 🔹 หนังที่ user เป้าหมายเคยให้คะแนนแล้ว
    rated_movies = user_movie_matrix.loc[target_user_id]
    rated_movies = rated_movies[rated_movies > 0].index.tolist()

    # 🔹 หนังที่ยังไม่เคยดู
    all_movies = user_movie_matrix.columns.tolist()
    unrated_movies = [m for m in all_movies if m not in rated_movies]

    # 🔹 คำนวณคะแนนแนะนำ
    recommendation_scores = {}

    for movie_id in unrated_movies:
        score = 0
        sim_sum = 0

        for other_user, similarity in similar_users.items():
            rating = user_movie_matrix.loc[other_user, movie_id]
            if rating > 0:
                score += similarity * rating
                sim_sum += similarity

        if sim_sum > 0:
            recommendation_scores[movie_id] = score / sim_sum

    # 🔹 เรียงคะแนน
    recommended_movies = sorted(
        recommendation_scores.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]

    # 🔹 ดึงชื่อหนังจาก movies_ml
    movies_df = pd.read_sql(
        "SELECT movie_id, title FROM movies_ml",
        conn
    )

    movie_dict = dict(zip(movies_df.movie_id, movies_df.title))

    # 🔹 จัดผลลัพธ์
    results = []
    for movie_id, score in recommended_movies:
        results.append({
            "movie_id": int(movie_id),
            "title": movie_dict.get(movie_id, "Unknown"),
            "score": round(score, 2)
        })

    conn.close()
    return results
