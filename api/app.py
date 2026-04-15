import os
import time
from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)

APP_VERSION = os.getenv("APP_VERSION", "v1")
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


def wait_for_db(max_retries=10, delay=2):
    for i in range(max_retries):
        try:
            conn = get_conn()
            conn.close()
            print(f"Connected to DB on try {i+1}")
            return
        except Exception as e:
            print(f"DB not ready yet ({i+1}/{max_retries}): {e}")
            time.sleep(delay)

    raise Exception("Could not connect to DB after retries")


def init_db():
    wait_for_db()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            processed BOOLEAN DEFAULT FALSE
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


@app.route("/health", methods=["GET"])
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok", "version": APP_VERSION}), 200
    except Exception as e:
        return jsonify({"status": "error", "details": str(e)}), 500


@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "api", "version": APP_VERSION}), 200


@app.route("/messages", methods=["POST"])
def create_message():
    data = request.get_json()

    if not data or "content" not in data:
        return jsonify({"error": "content is required"}), 400

    content = data["content"]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO messages (content) VALUES (%s) RETURNING id",
        (content,)
    )
    message_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"id": message_id, "content": content}), 201


@app.route("/messages", methods=["GET"])
def list_messages():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, content, processed FROM messages ORDER BY id ASC")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {"id": row[0], "content": row[1], "processed": row[2]}
        for row in rows
    ]), 200


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000)
