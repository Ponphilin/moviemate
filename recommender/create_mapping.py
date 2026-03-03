import mysql.connector
import re

def create_mapping():
    """
    Auto-map MovieLens movies to TMDB movies
    โดยจับคู่จากชื่อหนัง + ปี
    """
    
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="moviemate"
    )
    
    cursor = conn.cursor(dictionary=True)
    
    print("🔗 เริ่มจับคู่ MovieLens ↔ TMDB...")
    
    # ดึงหนังจาก MovieLens
    cursor.execute("SELECT movie_id, title FROM movies_ml")
    ml_movies = cursor.fetchall()
    
    mapped_count = 0
    not_found_count = 0
    
    for ml_movie in ml_movies:
        ml_id = ml_movie['movie_id']
        ml_title = ml_movie['title']
        
        # แยกชื่อและปี
        # ตัวอย่าง: "Toy Story (1995)"
        match = re.match(r'(.+?)\s*\((\d{4})\)', ml_title)
        
        if not match:
            # ไม่มีปี ใช้ชื่ออย่างเดียว
            title_clean = ml_title.strip()
            year = None
        else:
            title_clean = match.group(1).strip()
            year = int(match.group(2))
        
        # ค้นหาใน TMDB
        if year:
            # ค้นหาแบบมีปี
            query = """
                SELECT id, tmdb_id FROM movies 
                WHERE title LIKE %s 
                AND year = %s
                LIMIT 1
            """
            cursor.execute(query, (f"%{title_clean}%", year))
        else:
            # ค้นหาแบบไม่มีปี
            query = """
                SELECT id, tmdb_id FROM movies 
                WHERE title LIKE %s
                LIMIT 1
            """
            cursor.execute(query, (f"%{title_clean}%",))
        
        tmdb_movie = cursor.fetchone()
        
        if tmdb_movie:
            # พบ! บันทึก mapping
            insert_query = """
                INSERT IGNORE INTO movielens_tmdb_map (ml_movie_id, tmdb_movie_id, movie_id)
                VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (
                ml_id,
                tmdb_movie['tmdb_id'],
                tmdb_movie['id']
            ))
            conn.commit()
            
            mapped_count += 1
            if mapped_count % 100 == 0:
                print(f"✅ Mapped {mapped_count} movies...")
        else:
            not_found_count += 1
            if not_found_count % 50 == 0:
                print(f"⚠️  Not found: {not_found_count} movies (e.g., {ml_title})")
    
    cursor.close()
    conn.close()
    
    print(f"\n✅ เสร็จสิ้น!")
    print(f"   Mapped: {mapped_count} movies ({mapped_count/len(ml_movies)*100:.1f}%)")
    print(f"   Not found: {not_found_count} movies")
    print(f"\n💡 หนังที่ไม่พบส่วนใหญ่เป็นหนังเก่าหรือไม่มีใน TMDB")


if __name__ == '__main__':
    create_mapping()