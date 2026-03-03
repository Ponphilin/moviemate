from flask import Flask, jsonify
from flask_cors import CORS
import os
import psycopg2
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import json

app = Flask(__name__)
CORS(app)


# =============================
# Database Connection (Postgres)
# =============================
def get_db_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# =============================
# Recommendation API
# =============================
@app.route('/recommend/<int:user_id>')
def recommend(user_id):
    try:
        conn = get_db_connection()

        # ดึงแนวที่เลือก
        user_query = """
            SELECT favorite_genres 
            FROM users 
            WHERE id = %s
        """
        user_data = pd.read_sql(user_query, conn, params=[user_id])

        if user_data.empty or not user_data.iloc[0]['favorite_genres']:
            popular = pd.read_sql("""
                SELECT id, title, title_en, genre, poster_url, rating, release_date
                FROM movies
                WHERE rating >= 4.0 AND poster_url IS NOT NULL
                ORDER BY rating DESC
                LIMIT 12
            """, conn)

            conn.close()

            movies = popular.to_dict('records')
            for m in movies:
                m['reason'] = 'ภาพยนตร์ยอดนิยม'
                m['cf_used'] = False

            return jsonify({
                "status": "success",
                "data": movies,
                "cf_enabled": False,
                "message": "ยังไม่มีแนวที่เลือก → แนะนำหนังยอดนิยม"
            })

        favorite_genres_str = user_data.iloc[0]['favorite_genres']

        if favorite_genres_str.startswith('['):
            favorite_genres = json.loads(favorite_genres_str)
        else:
            favorite_genres = [g.strip() for g in favorite_genres_str.split(',')]

        # =============================
        # ดึงพฤติกรรม
        # =============================
        ratings_data = pd.read_sql(
            "SELECT user_id, movie_id, rating as score FROM ratings", conn
        )

        favorites_data = pd.read_sql(
            "SELECT user_id, movie_id, 4.0 as score FROM favorites", conn
        )

        views_data = pd.read_sql(
            "SELECT user_id, movie_id, 3.0 as score FROM view_history", conn
        )

        all_behaviors = pd.concat(
            [ratings_data, favorites_data, views_data],
            ignore_index=True
        )

        if all_behaviors.empty:
            genre_condition = " OR ".join(
                [f"genre ILIKE '%{g}%'" for g in favorite_genres]
            )

            genre_movies = pd.read_sql(f"""
                SELECT id, title, title_en, genre, poster_url, rating, release_date
                FROM movies
                WHERE ({genre_condition})
                AND rating >= 3.5
                AND poster_url IS NOT NULL
                ORDER BY rating DESC
                LIMIT 12
            """, conn)

            conn.close()

            movies = genre_movies.to_dict('records')
            for m in movies:
                m['reason'] = f'แนวที่คุณชอบ: {", ".join(favorite_genres)}'
                m['cf_used'] = False

            return jsonify({
                "status": "success",
                "data": movies,
                "cf_enabled": False,
                "message": f"แนะนำจากแนว: {', '.join(favorite_genres)}"
            })

        df = all_behaviors.groupby(
            ['user_id', 'movie_id']
        )['score'].max().reset_index()

        df.columns = ['user_id', 'movie_id', 'rating']

        if user_id not in df['user_id'].values:
            return jsonify({
                "status": "error",
                "message": "User ยังไม่มีพฤติกรรม"
            }), 404

        # =============================
        # CF Matrix
        # =============================
        user_movie_matrix = df.pivot_table(
            index='user_id',
            columns='movie_id',
            values='rating'
        ).fillna(0)

        user_similarity = cosine_similarity(user_movie_matrix)

        user_similarity_df = pd.DataFrame(
            user_similarity,
            index=user_movie_matrix.index,
            columns=user_movie_matrix.index
        )

        similar_users = (
            user_similarity_df[user_id]
            .sort_values(ascending=False)
            .drop(user_id)
            .head(10)
        )

        rated_movies = user_movie_matrix.loc[user_id]
        rated_movies = rated_movies[rated_movies > 0].index.tolist()

        all_movies = user_movie_matrix.columns.tolist()
        unrated_movies = [m for m in all_movies if m not in rated_movies]

        genre_condition = " OR ".join(
            [f"genre ILIKE '%{g}%'" for g in favorite_genres]
        )

        genre_movies_df = pd.read_sql(f"""
            SELECT id, title, title_en, genre, poster_url, rating, release_date
            FROM movies
            WHERE ({genre_condition})
            AND poster_url IS NOT NULL
        """, conn)

        genre_movie_ids = set(genre_movies_df['id'].tolist())

        recommendations = []

        for movie_id in unrated_movies:

            if movie_id not in genre_movie_ids:
                continue

            score = 0
            sim_sum = 0

            for other_user_id, similarity in similar_users.items():
                rating = user_movie_matrix.loc[other_user_id, movie_id]

                if rating > 0:
                    score += similarity * rating
                    sim_sum += similarity

            if sim_sum > 0:
                cf_score = score / sim_sum

                if cf_score >= 3.5:
                    recommendations.append({
                        'movie_id': int(movie_id),
                        'cf_score': float(cf_score)
                    })

        recommendations = sorted(
            recommendations,
            key=lambda x: x['cf_score'],
            reverse=True
        )[:12]

        if not recommendations:
            conn.close()
            return jsonify({
                "status": "error",
                "message": "ไม่พบคำแนะนำ"
            }), 404

        movie_ids = [r['movie_id'] for r in recommendations]
        movie_dict = genre_movies_df.set_index('id').to_dict('index')

        final_recommendations = []

        for rec in recommendations:
            movie_info = movie_dict.get(rec['movie_id'], {})

            final_recommendations.append({
                'id': rec['movie_id'],
                'title': movie_info.get('title'),
                'title_en': movie_info.get('title_en'),
                'genre': movie_info.get('genre'),
                'poster_url': movie_info.get('poster_url'),
                'rating': movie_info.get('rating'),
                'release_date': movie_info.get('release_date'),
                'cf_score': rec['cf_score'],
                'reason': "แนะนำจากผู้ใช้ที่คล้ายคุณ",
                'cf_used': True
            })

        conn.close()

        return jsonify({
            "status": "success",
            "data": final_recommendations,
            "cf_enabled": True
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/health')
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
