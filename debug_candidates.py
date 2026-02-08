
import db
import json

try:
    conn = db.get_db_connection()
    
    print("--- DIPUTADOS CANDIDATES ---")
    rows = conn.execute("SELECT DISTINCT candidato FROM resultados WHERE acta_id IN (SELECT id FROM actas WHERE nivel = 'DIPUTADOS') LIMIT 50").fetchall()
    for r in rows:
        print(r['candidato'])

    print("\n--- ALCALDE CANDIDATES ---")
    rows = conn.execute("SELECT DISTINCT candidato FROM resultados WHERE acta_id IN (SELECT id FROM actas WHERE nivel = 'ALCALDE') LIMIT 50").fetchall()
    for r in rows:
        print(r['candidato'])
        
    conn.close()
except Exception as e:
    print(e)
