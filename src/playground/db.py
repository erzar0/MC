import sqlite3
import pandas as pd
import io

def upsert_maps(db_path: str, csv_path: str) -> int:
    df: pd.DataFrame = pd.read_csv(csv_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS maps (
        id INTEGER PRIMARY KEY,
        platform TEXT,
        title TEXT,
        category TEXT,
        url TEXT,
        author_id INTEGER,
        author TEXT,
        author_rank TEXT,
        author_level INTEGER,
        author_subs INTEGER,
        views INTEGER,
        downloads INTEGER,
        favorites INTEGER,
        diamonds INTEGER,
        comments INTEGER,
        tags TEXT,
        download_mirrors TEXT,
        description TEXT,
        gallery_urls TEXT,
        updated_at TEXT,
        -- Tracking Columns (Not in CSV)
        processing_status TEXT DEFAULT 'TODO', 
        local_path TEXT,
        last_processed_at TEXT
    )
    """)

    sql = """
    INSERT INTO maps (
        id, platform, title, category, url, author_id, author, author_rank, 
        author_level, author_subs, views, downloads, favorites, diamonds, 
        comments, tags, download_mirrors, description, gallery_urls, updated_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(id) DO UPDATE SET
        platform=excluded.platform,
        title=excluded.title,
        category=excluded.category,
        url=excluded.url,
        author_id=excluded.author_id,
        author=excluded.author,
        author_rank=excluded.author_rank,
        author_level=excluded.author_level,
        author_subs=excluded.author_subs,
        views=excluded.views,
        downloads=excluded.downloads,
        favorites=excluded.favorites,
        diamonds=excluded.diamonds,
        comments=excluded.comments,
        tags=excluded.tags,
        download_mirrors=excluded.download_mirrors,
        description=excluded.description,
        gallery_urls=excluded.gallery_urls,
        updated_at=excluded.updated_at
    """

    data_tuples = df.to_records(index=False).tolist()
    cursor.executemany(sql, data_tuples)
    
    conn.commit()
    conn.close()

    return len(df) # Upserted count

conn = setup_and_upsert('minecraft_ml.db', csv_data)