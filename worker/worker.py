import os
import time
import psycopg2

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "appdb")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "apppass")


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


print("Worker started", flush=True)

while True:
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, content FROM messages
            WHERE processed = FALSE
            ORDER BY id ASC
        """)
        rows = cur.fetchall()

        if rows:
            print(f"Found {len(rows)} messages", flush=True)

        for row in rows:
            msg_id, content = row
            print(f"Processing message {msg_id}: {content}", flush=True)

            cur.execute(
                "UPDATE messages SET processed = TRUE WHERE id = %s",
                (msg_id,)
            )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Worker error: {e}", flush=True)

    time.sleep(3)
