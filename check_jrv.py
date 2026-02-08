import db
import json

def check():
    conn = db.get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM actas WHERE jrv='941'").fetchall()
        for r in rows:
            print(f"ID: {r['id']}, JRV: {r['jrv']}, ORIGEN: {r['origen']}, NIVEL: {r['nivel']}, FILEPATH: {r['filepath']}")
    finally:
        conn.close()

if __name__ == "__main__":
    check()
