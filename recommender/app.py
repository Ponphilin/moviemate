from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

app = Flask(__name__)
CORS(app)


def get_db_connection():
    return mysql.connector.connect(
        host="sql300.byetcluster.com",
        user="if0_41292544",
        password="smyBNrV05tPc",
        database="if0_41292544_moviemate"
    )


@app.route('/recommend/<int:user_id>')
def recommend(user_id):
    """
    Hybrid CF + Content-Based:
    - ใช้ CF หา User คล้ายกัน
    - แต่กรองเฉพาะหนังที่ตรงกับแนวที่เลือกตอนสมัคร
    """
    try:
        conn = get_db_connection()
        
        # ดึงแนวที่ User เลือกตอนสมัคร
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT favorite_genres FROM users WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        
        if not user_data or not user_data['favorite_genres']:
            # ไม่มีแนวที่เลือก → ให้หนังยอดนิยม
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
        
        # แปลงแนวที่เลือก
        favorite_genres_str = user_data['favorite_genres']
        if favorite_genres_str.startswith('['):
            import json
            favorite_genres = json.loads(favorite_genres_str)
        else:
            favorite_genres = [g.strip() for g in favorite_genres_str.split(',')]
        
        print(f"\n{'='*60}")
        print(f"🎯 User {user_id} เลือกแนว: {favorite_genres}")
        print(f"{'='*60}")
        
        # ดึงพฤติกรรมทั้ง 3 แบบ
        cursor = conn.cursor(dictionary=True)
        
        # 1. การให้คะแนน (น้ำหนัก = rating ที่ให้)
        cursor.execute("""
            SELECT user_id, movie_id, rating as score
            FROM ratings
        """)
        ratings_data = cursor.fetchall()
        
        # 2. กดถูกใจ (น้ำหนัก = 4.0)
        cursor.execute("""
            SELECT user_id, movie_id, 4.0 as score
            FROM favorites
        """)
        favorites_data = cursor.fetchall()
        
        # 3. คลิกดู (น้ำหนัก = 3.0)
        cursor.execute("""
            SELECT user_id, movie_id, 3.0 as score
            FROM view_history
        """)
        views_data = cursor.fetchall()
        
        # รวมทั้ง 3 แหล่ง
        all_behaviors = ratings_data + favorites_data + views_data
        
        if not all_behaviors:
            # ยังไม่มีพฤติกรรม → แนะนำจากแนวที่เลือก
            genre_condition = " OR ".join([f"genre LIKE '%{g}%'" for g in favorite_genres])
            
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
                "message": f"แนะนำจากแนวที่เลือก: {', '.join(favorite_genres)}"
            })
        
        # สร้าง DataFrame จากพฤติกรรมทั้งหมด
        df = pd.DataFrame(all_behaviors)
        
        # ถ้ามีหลาย behavior สำหรับหนังเดียวกัน → เอาค่าสูงสุด
        df = df.groupby(['user_id', 'movie_id'])['score'].max().reset_index()
        df.columns = ['user_id', 'movie_id', 'rating']
        
        print(f"\n📊 Behaviors:")
        print(f"   - Ratings: {len(ratings_data)}")
        print(f"   - Favorites: {len(favorites_data)}")
        print(f"   - Views: {len(views_data)}")
        print(f"   - Total (unique): {len(df)}")
        
        if user_id not in df['user_id'].values:
            # ยังไม่มี ratings → แนะนำจากแนวที่เลือก
            genre_condition = " OR ".join([f"genre LIKE '%{g}%'" for g in favorite_genres])
            
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
                "message": f"แนะนำจากแนวที่เลือก: {', '.join(favorite_genres)}"
            })
        
        # สร้าง user-movie matrix
        user_movie_matrix = df.pivot_table(
            index='user_id',
            columns='movie_id',
            values='rating'
        ).fillna(0)
        
        # คำนวณ similarity
        user_similarity = cosine_similarity(user_movie_matrix)
        user_similarity_df = pd.DataFrame(
            user_similarity,
            index=user_movie_matrix.index,
            columns=user_movie_matrix.index
        )
        
        # หา Top 10 users คล้ายกัน
        similar_users = (
            user_similarity_df[user_id]
            .sort_values(ascending=False)
            .drop(user_id)
            .head(10)
        )
        
        print(f"\n👥 Top 5 Similar Users:")
        for i, (uid, sim) in enumerate(similar_users.head(5).items(), 1):
            print(f"   {i}. User {uid}: {sim:.3f}")
        
        # หนังที่เคยดูแล้ว
        rated_movies = user_movie_matrix.loc[user_id]
        rated_movies = rated_movies[rated_movies > 0].index.tolist()
        
        # หนังที่ยังไม่เคยดู
        all_movies = user_movie_matrix.columns.tolist()
        unrated_movies = [m for m in all_movies if m not in rated_movies]
        
        # คำนวณคะแนนแนะนำ (เฉพาะหนังที่ตรงแนว)
        recommendations = []
        
        # ดึงหนังทั้งหมดที่ตรงแนว
        genre_condition = " OR ".join([f"genre LIKE '%{g}%'" for g in favorite_genres])
        
        genre_movies_df = pd.read_sql(f"""
            SELECT id, title, title_en, genre, poster_url, rating, release_date
            FROM movies
            WHERE ({genre_condition})
            AND poster_url IS NOT NULL
        """, conn)
        
        genre_movie_ids = set(genre_movies_df['id'].tolist())
        
        print(f"\n🎬 หนังที่ตรงแนว: {len(genre_movie_ids)} เรื่อง")
        
        for movie_id in unrated_movies:
            # เฉพาะหนังที่ตรงแนว
            if movie_id not in genre_movie_ids:
                continue
            
            score = 0
            sim_sum = 0
            contributors = []
            
            for other_user_id, similarity in similar_users.items():
                rating = user_movie_matrix.loc[other_user_id, movie_id]
                
                if rating > 0:
                    score += similarity * rating
                    sim_sum += similarity
                    
                    contributors.append({
                        'user_id': int(other_user_id),
                        'similarity': float(similarity),
                        'rating': float(rating)
                    })
            
            if sim_sum > 0 and len(contributors) >= 2:
                cf_score = score / sim_sum
                
                if cf_score >= 3.5:
                    recommendations.append({
                        'movie_id': int(movie_id),
                        'cf_score': float(cf_score),
                        'contributors': contributors[:3],
                        'num_similar_users_liked': len(contributors)
                    })
        
        print(f"✅ CF Recommendations: {len(recommendations)} เรื่อง (ตรงแนว)")
        
        # เรียงตามคะแนน
        recommendations = sorted(
            recommendations,
            key=lambda x: x['cf_score'],
            reverse=True
        )[:12]
        
        # ดึงข้อมูลหนัง
        if recommendations:
            movie_ids = [r['movie_id'] for r in recommendations]
            movie_dict = genre_movies_df.set_index('id').to_dict('index')
            
            final_recommendations = []
            for rec in recommendations:
                movie_info = movie_dict.get(rec['movie_id'], {})
                
                # สร้างเหตุผล
                contributors = rec['contributors']
                reason_parts = []
                for c in contributors[:2]:
                    reason_parts.append(
                        f"User {c['user_id']} (คล้าย {c['similarity']*100:.0f}%) ให้ {c['rating']:.1f}⭐"
                    )
                
                reason = f"👥 {rec['num_similar_users_liked']} users ที่คล้ายคุณชอบ: " + ", ".join(reason_parts)
                
                final_recommendations.append({
                    'id': rec['movie_id'],
                    'title': movie_info.get('title', 'Unknown'),
                    'title_en': movie_info.get('title_en', ''),
                    'genre': movie_info.get('genre', ''),
                    'poster_url': movie_info.get('poster_url', ''),
                    'rating': movie_info.get('rating', 0),
                    'release_date': movie_info.get('release_date'),
                    'cf_score': rec['cf_score'],
                    'reason': reason,
                    'cf_used': True,
                    'similar_users': rec['contributors']
                })
            
            conn.close()
            
            print(f"\n✨ Total: {len(final_recommendations)} เรื่อง (ตรงแนวที่เลือก)")
            print(f"{'='*60}\n")
            
            return jsonify({
                "status": "success",
                "data": final_recommendations,
                "cf_enabled": True,
                "similar_users": [
                    {'user_id': int(uid), 'similarity': float(sim)}
                    for uid, sim in similar_users.head(5).items()
                ],
                "message": f"CF: แนะนำเฉพาะแนว {', '.join(favorite_genres)}"
            })
        
        else:
            # CF ไม่มีหนังแนะนำ → ให้หนังแนวที่เลือก
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
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/health')
def health():
    return jsonify({
        "status": "ok",
        "message": "Hybrid CF + Content-Based API"
    })

@app.route('/cf-analysis/<int:user_id>')
def cf_analysis(user_id):
    """
    แสดงรายละเอียดการทำงาน CF
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # ดึงพฤติกรรมทั้ง 3 แบบ
        cursor.execute("SELECT user_id, movie_id, rating as score FROM ratings")
        ratings_data = cursor.fetchall()
        
        cursor.execute("SELECT user_id, movie_id, 4.0 as score FROM favorites")
        favorites_data = cursor.fetchall()
        
        cursor.execute("SELECT user_id, movie_id, 3.0 as score FROM view_history")
        views_data = cursor.fetchall()
        
        all_behaviors = ratings_data + favorites_data + views_data
        
        if not all_behaviors:
            return jsonify({
                "status": "error",
                "message": "ไม่มีข้อมูลพฤติกรรม"
            }), 404
        
        df = pd.DataFrame(all_behaviors)
        df = df.groupby(['user_id', 'movie_id'])['score'].max().reset_index()
        df.columns = ['user_id', 'movie_id', 'rating']
        
        if user_id not in df['user_id'].values:
            return jsonify({
                "status": "error",
                "message": f"User {user_id} ยังไม่มีข้อมูล"
            }), 404
        
        # สร้าง matrix
        user_movie_matrix = df.pivot_table(
            index='user_id',
            columns='movie_id',
            values='rating'
        ).fillna(0)
        
        # คำนวณ similarity
        user_similarity = cosine_similarity(user_movie_matrix)
        user_similarity_df = pd.DataFrame(
            user_similarity,
            index=user_movie_matrix.index,
            columns=user_movie_matrix.index
        )
        
        # หา Top 5 users คล้ายกัน
        similar_users = (
            user_similarity_df[user_id]
            .sort_values(ascending=False)
            .drop(user_id)
            .head(5)
        )
        
        # คำนวณหนังที่ดูร่วมกัน
        similar_users_data = []
        for other_user_id, similarity in similar_users.items():
            user_a_movies = set(user_movie_matrix.loc[user_id][user_movie_matrix.loc[user_id] > 0].index)
            user_b_movies = set(user_movie_matrix.loc[other_user_id][user_movie_matrix.loc[other_user_id] > 0].index)
            common_movies = len(user_a_movies & user_b_movies)
            
            similar_users_data.append({
                'user_id': int(other_user_id),
                'similarity': float(similarity),
                'common_movies': common_movies
            })
        
        # หนังที่เคยดู
        rated_movies = user_movie_matrix.loc[user_id]
        rated_movies = rated_movies[rated_movies > 0].index.tolist()
        
        # หนังที่ยังไม่เคยดู
        all_movies = user_movie_matrix.columns.tolist()
        unrated_movies = [m for m in all_movies if m not in rated_movies]
        
        # คำนวณคะแนนแนะนำ
        recommendations = []
        
        for movie_id in unrated_movies:
            score = 0
            sim_sum = 0
            evidence = []
            
            for other_user_id, similarity in similar_users.items():
                rating = user_movie_matrix.loc[other_user_id, movie_id]
                
                if rating > 0:
                    score += similarity * rating
                    sim_sum += similarity
                    evidence.append({
                        'user_id': int(other_user_id),
                        'similarity': float(similarity),
                        'rating': float(rating)
                    })
            
            if sim_sum > 0:
                cf_score = score / sim_sum
                
                if cf_score >= 3.5:
                    recommendations.append({
                        'movie_id': int(movie_id),
                        'cf_score': float(cf_score),
                        'evidence': evidence[:3]
                    })
        
        # เรียงตามคะแนน
        recommendations = sorted(
            recommendations,
            key=lambda x: x['cf_score'],
            reverse=True
        )[:10]
        
        # ดึงชื่อหนัง
        if recommendations:
            movie_ids = [r['movie_id'] for r in recommendations]
            movies_df = pd.read_sql(f"""
                SELECT id, title, title_en, genre, rating
                FROM movies
                WHERE id IN ({','.join(map(str, movie_ids))})
            """, conn)
            
            movie_dict = movies_df.set_index('id').to_dict('index')
            
            for rec in recommendations:
                movie_info = movie_dict.get(rec['movie_id'], {})
                rec['title'] = movie_info.get('title', 'Unknown')
                rec['title_en'] = movie_info.get('title_en', '')
                rec['genre'] = movie_info.get('genre', '')
                rec['rating'] = movie_info.get('rating', 0)
        
        conn.close()
        
        return jsonify({
            "status": "success",
            "data": {
                "target_user_id": user_id,
                "total_users": len(user_movie_matrix),
                "similar_users": similar_users_data,
                "recommendations": recommendations,
                "avg_similarity": float(similar_users.mean())
            }
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    
if __name__ == "__main__":
    print("🚀 Starting Hybrid Recommendation API...")

    app.run(host="0.0.0.0", port=10000)
