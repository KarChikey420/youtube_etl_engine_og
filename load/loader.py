import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL DB config
DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT", 5432)
}

def insert_video_stats(stats, db_config=DB_CONFIG):
    """
    Inserts or updates video stats into the PostgreSQL database.

    Args:
        stats (list): List of dictionaries containing video metadata and statistics
        db_config (dict): Dictionary with DB connection parameters
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("🔄 Inserting video stats into the database...")

        # SQL query with upsert (ON CONFLICT) logic
        insert_query = """
            INSERT INTO youtube_videos (video_id, title, published_at, views, likes, comments)
            VALUES %s
            ON CONFLICT (video_id) DO UPDATE SET
                views = EXCLUDED.views,
                likes = EXCLUDED.likes,
                comments = EXCLUDED.comments,
                recorded_at = CURRENT_TIMESTAMP;
        """

        # Prepare values for batch insert
        values = [
            (
                v['videoId'],
                v['title'],
                v['published_at'],
                v['views'],
                v['likes'],
                v['comments']
            )
            for v in stats
        ]

        execute_values(cursor, insert_query, values)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Video data inserted/updated successfully.")

    except Exception as e:
        print(f"❌ Error inserting video stats: {e}")
