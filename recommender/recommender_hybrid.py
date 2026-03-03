import mysql.connector
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import random
import json


def recommend_movies_hybrid(user_id, limit=12):
    """
    Strict Genre-Only Recommender:
    - แนะนำเฉพาะแนวที่เลือก 100%
    - ไม่ผสม popular เลย
    - ถ้าเลือก 1 แนว → แนะนำ 12 เรื่องจากแนวนั้น
    - ถ้าเลือก 2 แนว → แนะนำแนวละ 6 เรื่อง
    """
    
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="moviemate"
    )
    
    behavior_genres = get_user_behavior_genres(conn, user_id)
    
    cursor = conn.cursor()
    cursor.execute("SELECT movie_id FROM ratings WHERE user_id = %s", (user_id,))
    rated_movie_ids = {row[0] for row in cursor.fetchall()}
    cursor.close()
    
    print(f"\n{'='*60}")
    print(f"🎯 User {user_id} Strict Genre-Only Recommendation")
    print(f"{'='*60}")
    print(f"🚫 Rated movies: {len(rated_movie_ids)}")
    print(f"📝 Initial genres: {behavior_genres['initial_genres']}")
    print(f"🎓 Learned genres: {behavior_genres['learned_genres']}")
    
    recommendations = []
    
    # ===============================================
    # ถ้ายังไม่มีพฤติกรรม → ใช้แนวที่เลือกตอนสมัคร 100%
    # ===============================================
    if not behavior_genres['learned_genres']:
        if behavior_genres['initial_genres']:
            print(f"\n🎯 NEW USER: Recommend ONLY from initial genres")
            
            genre_recommendations = get_strict_genre_recommendations(
                conn, user_id, rated_movie_ids, behavior_genres['initial_genres'], 
                limit=limit
            )
            recommendations.extend(genre_recommendations)
            print(f"✅ Genre-only recommendations: {len(genre_recommendations)} movies")
        else:
            # ไม่มีแนวเลย → ให้ popular (กรณีพิเศษ)
            print(f"\n⚠️  No genres selected - using popular movies")
            popular = get_popular_movies(conn, limit, exclude=rated_movie_ids)
            recommendations.extend(popular)
    
    # ===============================================
    # ถ้ามีพฤติกรรมแล้ว → ผสมแนวเดิม + แนวที่เรียนรู้
    # ===============================================
    else:
        print(f"\n🎓 ACTIVE USER: Mixing initial + learned genres")
        
        # แนวที่เรียนรู้ 60%
        learned_recommendations = get_strict_genre_recommendations(
            conn, user_id, rated_movie_ids, behavior_genres['learned_genres'], 
            limit=int(limit * 0.6)
        )
        recommendations.extend(learned_recommendations)
        print(f"✅ Learned genres: {len(learned_recommendations)} movies")
        
        # แนวเดิม 40%
        if behavior_genres['initial_genres']:
            initial_recommendations = get_strict_genre_recommendations(
                conn, user_id, rated_movie_ids, behavior_genres['initial_genres'], 
                limit=limit - len(recommendations)
            )
            recommendations.extend(initial_recommendations)
            print(f"✅ Initial genres: {len(initial_recommendations)} movies")
    
    # ===============================================
    # Remove duplicates
    # ===============================================
    unique_recommendations = []
    seen_final = set()
    
    for rec in recommendations:
        if rec['id'] not in seen_final and rec['id'] not in rated_movie_ids:
            seen_final.add(rec['id'])
            unique_recommendations.append(rec)
    
    conn.close()
    
    print(f"\n📊 Total recommendations: {len(unique_recommendations)}")
    print(f"🎯 100% from selected genres - NO popular movies!")
    print(f"{'='*60}\n")
    
    return unique_recommendations[:limit]


def get_user_behavior_genres(conn, user_id):
    """วิเคราะห์พฤติกรรม"""
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT favorite_genres FROM users WHERE id = %s", (user_id,))
    user_data = cursor.fetchone()
    
    initial_genres = []
    if user_data and user_data['favorite_genres']:
        fav_str = user_data['favorite_genres'].strip()
        
        if fav_str.startswith('[') or fav_str.startswith('{'):
            try:
                fav_list = json.loads(fav_str)
                initial_genres = fav_list if isinstance(fav_list, list) else []
            except:
                pass
        else:
            initial_genres = [g.strip() for g in fav_str.split(',') if g.strip()]
    
    genre_weights = {}
    
    # จากการให้คะแนนสูง
    cursor.execute("""
        SELECT m.genre, COUNT(*) as count
        FROM ratings r
        INNER JOIN movies m ON r.movie_id = m.id
        WHERE r.user_id = %s AND r.rating >= 4
        GROUP BY m.genre
    """, (user_id,))
    
    for row in cursor.fetchall():
        genre_str = row['genre']
        if genre_str:
            genres = [g.strip() for g in genre_str.split(',')]
            for genre in genres:
                if genre not in genre_weights:
                    genre_weights[genre] = 0
                genre_weights[genre] += row['count'] * 5
    
    # จากรายการโปรด
    cursor.execute("""
        SELECT m.genre, COUNT(*) as count
        FROM favorites f
        INNER JOIN movies m ON f.movie_id = m.id
        WHERE f.user_id = %s
        GROUP BY m.genre
    """, (user_id,))
    
    for row in cursor.fetchall():
        genre_str = row['genre']
        if genre_str:
            genres = [g.strip() for g in genre_str.split(',')]
            for genre in genres:
                if genre not in genre_weights:
                    genre_weights[genre] = 0
                genre_weights[genre] += row['count'] * 4
    
    # จากการคลิกดู
    cursor.execute("""
        SELECT m.genre, COUNT(*) as count
        FROM view_history v
        INNER JOIN movies m ON v.movie_id = m.id
        WHERE v.user_id = %s
        GROUP BY m.genre
    """, (user_id,))
    
    for row in cursor.fetchall():
        genre_str = row['genre']
        if genre_str:
            genres = [g.strip() for g in genre_str.split(',')]
            for genre in genres:
                if genre not in genre_weights:
                    genre_weights[genre] = 0
                genre_weights[genre] += row['count'] * 1
    
    cursor.close()
    
    sorted_genres = sorted(genre_weights.items(), key=lambda x: x[1], reverse=True)
    learned_genres = [genre for genre, weight in sorted_genres[:5]]
    
    return {
        'initial_genres': initial_genres,
        'learned_genres': learned_genres,
        'genre_weights': genre_weights
    }


def get_strict_genre_recommendations(conn, user_id, rated_movie_ids, genres_list, limit=12):
    """
    แนะนำเฉพาะจากแนวที่กำหนด - ห้ามผสมอย่างอื่นเลย!
    
    ตัวอย่าง:
    - เลือก romance (1 แนว) → แนะนำ romance 12 เรื่อง
    - เลือก romance + action (2 แนว) → แนะนำ romance 6 + action 6
    - เลือก romance + action + comedy (3 แนว) → แนะนำแนวละ 4 เรื่อง
    """
    
    if not genres_list:
        return []
    
    num_genres = len(genres_list)
    print(f"\n🎯 Strict recommendation for: {', '.join(genres_list)}")
    print(f"📊 {num_genres} genre(s) selected → {limit // num_genres}+ movies per genre")
    
    # แบ่งจำนวนเท่าๆ กัน
    base_per_genre = limit // num_genres
    extra_slots = limit % num_genres
    
    result = []
    genre_movies_count = {}
    
    for i, genre in enumerate(genres_list):
        # คำนวณจำนวนที่แต่ละแนวจะได้
        current_limit = base_per_genre
        if i < extra_slots:
            current_limit += 1
        
        if rated_movie_ids:
            excluded_ids = ','.join(map(str, rated_movie_ids))
            exclude_clause = f"AND id NOT IN ({excluded_ids})"
        else:
            exclude_clause = ""
        
        # ดึงหนังเฉพาะแนวนั้นๆ
        query = f"""
            SELECT id, title, genre, poster_url, rating, synopsis
            FROM movies
            WHERE (
                genre = '{genre}' 
                OR genre LIKE '{genre},%' 
                OR genre LIKE '%,{genre},%' 
                OR genre LIKE '%,{genre}'
            )
            {exclude_clause}
            AND poster_url IS NOT NULL
            AND (release_date IS NULL OR release_date <= DATE_ADD(CURDATE(), INTERVAL 90 DAY))
            ORDER BY 
                CASE 
                    WHEN genre = '{genre}' THEN 1
                    WHEN genre LIKE '{genre},%' THEN 2
                    ELSE 3
                END,
                rating DESC,
                RAND()
            LIMIT {current_limit * 3}
        """
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        movies = cursor.fetchall()
        cursor.close()
        
        # ถ้าได้เยอะ ให้ random เพื่อความหลากหลาย
        if len(movies) > current_limit:
            # เอาหนัง rating สูงก่อน 70% แล้ว random ที่เหลือ 30%
            top_movies = movies[:int(current_limit * 0.7)]
            random_movies = random.sample(movies[int(current_limit * 0.7):], 
                                         current_limit - len(top_movies)) if len(movies) > len(top_movies) else []
            movies = top_movies + random_movies
        
        genre_movies_count[genre] = len(movies)
        
        for movie in movies:
            if movie['id'] in rated_movie_ids:
                continue
            
            # ไม่ซ้ำ
            if any(m['id'] == movie['id'] for m in result):
                continue
                
            result.append({
                'id': movie['id'],
                'title': movie['title'],
                'genre': movie['genre'],
                'poster_url': movie['poster_url'],
                'rating': float(movie['rating']) if movie['rating'] else 0,
                'score': 5.0,
                'reason': f"Matches your interest: {genre}" if num_genres == 1 
                         else f"From your selected genres"
            })
    
    # แสดงสถิติ
    print(f"\n📊 Distribution:")
    for genre, count in genre_movies_count.items():
        percentage = (count / len(result) * 100) if result else 0
        print(f"   - {genre}: {count} movies ({percentage:.1f}%)")
    
    return result[:limit]


def get_popular_movies(conn, limit=12, exclude=None):
    """
    Popular movies - ใช้เฉพาะกรณีที่ไม่มีแนวเลือก
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
        WHERE (release_date IS NULL OR release_date <= DATE_ADD(CURDATE(), INTERVAL 90 DAY))
        AND poster_url IS NOT NULL
        {exclude_clause}
        ORDER BY rating DESC
        LIMIT {limit}
    """
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    movies = cursor.fetchall()
    cursor.close()
    
    result = []
    for movie in movies:
        if movie['id'] in exclude:
            continue
            
        result.append({
            'id': movie['id'],
            'title': movie['title'],
            'genre': movie['genre'],
            'poster_url': movie['poster_url'],
            'rating': float(movie['rating']) if movie['rating'] else 0,
            'score': float(movie['rating']) if movie['rating'] else 3.5,
            'reason': 'Highly rated movie'
        })
    
    return result