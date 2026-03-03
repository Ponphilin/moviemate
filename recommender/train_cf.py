import mysql.connector
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

# -----------------------------
# Connect MySQL
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="moviemate"
)

# -----------------------------
# Load ratings from DB
# -----------------------------
query = """
SELECT user_id, movie_id, rating
FROM ratings_ml
"""

df = pd.read_sql(query, conn)

conn.close()

print("Loaded ratings:", df.shape)

# -----------------------------
# User-Item Matrix
# -----------------------------
user_item_matrix = df.pivot_table(
    index="user_id",
    columns="movie_id",
    values="rating"
).fillna(0)

print("Matrix shape:", user_item_matrix.shape)

# -----------------------------
# Similarity
# -----------------------------
user_similarity = cosine_similarity(user_item_matrix)

user_similarity_df = pd.DataFrame(
    user_similarity,
    index=user_item_matrix.index,
    columns=user_item_matrix.index
)

print("Similarity matrix ready")

# -----------------------------
# Recommend function
# -----------------------------
def recommend_movies(user_id, top_n=5):
    if user_id not in user_item_matrix.index:
        return []

    similar_users = user_similarity_df[user_id].sort_values(ascending=False)[1:11]

    weighted_scores = user_item_matrix.loc[similar_users.index].T.dot(similar_users)
    watched = user_item_matrix.loc[user_id]

    recommendations = weighted_scores[watched == 0].sort_values(ascending=False)

    return recommendations.head(top_n)

# -----------------------------
# TEST
# -----------------------------
test_user = 1
print("Recommend for user", test_user)
print(recommend_movies(test_user))
