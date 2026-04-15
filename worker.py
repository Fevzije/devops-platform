import time
import psycopg2
import os

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

print("🚀 Worker started...")

while True:
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT id, content FROM messages WHERE processed = false;")
        rows = cur.fetchall()

        if rows:
            print(f"📦 Found {len(rows)} messages to process")

        for row in rows:
            print(f"⚙️ Processing message {row[0]}: {row[1]}")

            cur.execute(
                "UPDATE messages SET processed = true WHERE id = %s;",
                (row[0],)
            )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print("❌ Worker error:", e)

    time.sleep(3)
