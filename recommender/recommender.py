import mysql.connector
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity


def recommend_movies_hybrid(user_id, limit=12):
    """
    Hybrid Recommender System
    1. ใช้ MovieLens CF เป็นฐาน
    2. ผสมกับ ratings จริงของ user
    3. แนะนำตามแนวที่ user สนใจ
    """
    
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="moviemate"
    )
    
    recommendations = []
    
    # ==========================================
    # Strategy 1: Collaborative Filtering (MovieLens)
    # ==========================================
    try:
        cf_recommendations = get_cf_recommendations(conn, user_id, limit=8)
        recommendations.extend(cf_recommendations)
    except Exception as e:
        print(f"CF Error: {e}")
    
    # ==========================================
    # Strategy 2: Content-Based (User's favorites)
    # ==========================================
    try:
        content_recommendations = get_content_based_recommendations(conn, user_id, limit=4)
        recommendations.extend(content_recommendations)
    except Exception as e:
        print(f"Content-based Error: {e}")
    
    # ==========================================
    # Remove duplicates & limit
    # ==========================================
    seen_ids = set()
    unique_recommendations = []
    
    for rec in recommendations:
        if rec['id'] not in seen_ids:
            seen_ids.add(rec['id'])
            unique_recommendations.append(rec)
            
            if len(unique_recommendations) >= limit:
                break
    
    # ==========================================
    # Fallback: Popular movies
    # ==========================================
    if len(unique_recommendations) < limit:
        popular = get_popular_movies(conn, limit - len(unique_recommendations), exclude=seen_ids)
        unique_recommendations.extend(popular)
    
    conn.close()
    return unique_recommendations[:limit]


def get_cf_recommendations(conn, user_id, limit=8):
    """
    Collaborative Filtering using MovieLens data
    """
    
    # โหลด ratings จาก MovieLens
    df = pd.read_sql("SELECT user_id, movie_id, rating FROM ratings_ml", conn)
    
    if df.empty:
        return []
    
    # สร้าง user-movie matrix
    matrix = df.pivot_table(
        index="user_id",
        columns="movie_id",
        values="rating"
    ).fillna(0)
    
    # ถ้า user ไม่มีใน MovieLens ใช้ user แรก
    if user_id not in matrix.index:
        # เลือก random user จาก MovieLens
        ml_user_id = matrix.index[0]
    else:
        ml_user_id = user_id
    
    # คำนวณ similarity
    similarity = cosine_similarity(matrix)
    sim_df = pd.DataFrame(
        similarity,
        index=matrix.index,
        columns=matrix.index
    )
    
    # หา users ที่คล้ายกัน (top 20)
    similar_users = sim_df[ml_user_id].sort_values(ascending=False)[1:21]
    
    # หนังที่ user เคยดูแล้ว
    watched = matrix.loc[ml_user_id]
    watched = watched[watched > 0].index.tolist()
    
    # หนังที่ยังไม่ดู
    candidates = [m for m in matrix.columns if m not in watched]
    
    # คำนวณคะแนนแนะนำ
    scores = {}
    for movie_id in candidates:
        score = 0
        sim_sum = 0
        
        for other_user, sim in similar_users.items():
            r = matrix.loc[other_user, movie_id]
            if r > 0:
                score += sim * r
                sim_sum += sim
        
        if sim_sum > 0:
            scores[movie_id] = score / sim_sum
    
    if not scores:
        return []
    
    # Top N movies
    top_movies = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:limit]
    ml_movie_ids = [m[0] for m in top_movies]
    
    # Map MovieLens → TMDB → movies
    if not ml_movie_ids:
        return []
    
    placeholders = ','.join(['%s'] * len(ml_movie_ids))
    
    query = f"""
        SELECT m.id, m.title, m.genre, m.poster_url, m.rating, map.ml_movie_id
        FROM movielens_tmdb_map map
        INNER JOIN movies m ON map.movie_id = m.id
        WHERE map.ml_movie_id IN ({placeholders})
        AND m.id NOT IN (
            SELECT movie_id FROM ratings WHERE user_id = %s
        )
    """
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, ml_movie_ids + [user_id])
    movies_data = cursor.fetchall()
    cursor.close()
    
    # จัดเรียงตาม score
    ml_scores = dict(top_movies)
    result = []
    
    for movie in movies_data:
        ml_id = movie['ml_movie_id']
        score = ml_scores.get(ml_id, 0)
        
        result.append({
            'id': movie['id'],
            'title': movie['title'],
            'genre': movie['genre'],
            'poster_url': movie['poster_url'],
            'rating': float(movie['rating']) if movie['rating'] else 0,
            'score': round(score, 2),
            'reason': 'Users like you loved this (ML)'
        })
    
    return result


def get_content_based_recommendations(conn, user_id, limit=4):
    """
    Content-based: แนะนำตามแนวที่ user ชอบ
    """
    
    # หาแนวที่ user ให้คะแนนสูง
    query = """
        SELECT m.genre, AVG(r.rating) as avg_rating, COUNT(*) as count
        FROM ratings r
        INNER JOIN movies m ON r.movie_id = m.id
        WHERE r.user_id = %s
        AND r.rating >= 4
        GROUP BY m.genre
        HAVING count >= 2
        ORDER BY avg_rating DESC, count DESC
        LIMIT 3
    """
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, (user_id,))
    liked_genres = cursor.fetchall()
    cursor.close()
    
    if not liked_genres:
        # ถ้าไม่มี ใช้แนวที่เลือกตอนสมัคร
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT favorite_genres FROM users WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        cursor.close()
        
        if user_data and user_data['favorite_genres']:
            genres = user_data['favorite_genres'].split(',')
            liked_genres = [{'genre': g.strip()} for g in genres]
    
    if not liked_genres:
        return []
    
    # หาหนังในแนวที่ชอบ
    genre_conditions = []
    for g in liked_genres:
        genre = g['genre']
        genre_conditions.append(f"(genre LIKE '%{genre}%' OR genre = '{genre}')")
    
    query = f"""
        SELECT id, title, genre, poster_url, rating
        FROM movies
        WHERE ({' OR '.join(genre_conditions)})
        AND id NOT IN (SELECT movie_id FROM ratings WHERE user_id = %s)
        AND (release_date IS NULL OR release_date <= CURDATE())
        ORDER BY rating DESC
        LIMIT %s
    """
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, (user_id, limit))
    movies = cursor.fetchall()
    cursor.close()
    
    result = []
    for movie in movies:
        result.append({
            'id': movie['id'],
            'title': movie['title'],
            'genre': movie['genre'],
            'poster_url': movie['poster_url'],
            'rating': float(movie['rating']) if movie['rating'] else 0,
            'score': float(movie['rating']) if movie['rating'] else 3.5,
            'reason': 'Based on your favorite genres'
        })
    
    return result


def get_popular_movies(conn, limit=4, exclude=None):
    """
    Fallback: หนังยอดนิยม
    """
    if exclude is None:
        exclude = set()
    
    if exclude:
        exclude_ids = ','.join(map(str, exclude))
        exclude_clause = f"AND id NOT IN ({exclude_ids})"
    else:
        exclude_clause = ""
    
    query = f"""
        SELECT id, title, genre, poster_url, rating
        FROM movies
        WHERE (release_date IS NULL OR release_date <= CURDATE())
        {exclude_clause}
        ORDER BY rating DESC
        LIMIT %s
    """
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, (limit,))
    movies = cursor.fetchall()
    cursor.close()
    
    result = []
    for movie in movies:
        result.append({
            'id': movie['id'],
            'title': movie['title'],
            'genre': movie['genre'],
            'poster_url': movie['poster_url'],
            'rating': float(movie['rating']) if movie['rating'] else 0,
            'score': float(movie['rating']) if movie['rating'] else 3.5,
            'reason': 'Popular movies'
        })
    
    return result