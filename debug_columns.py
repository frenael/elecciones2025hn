
import db

try:
    conn = db.get_db_connection()
    print("--- SEARCHING FOR 'COLUMN' IN CANDIDATES ---")
    rows = conn.execute("SELECT DISTINCT candidato FROM resultados WHERE candidato LIKE '%COLUM%'").fetchall()
    for r in rows:
        print(f"Found: {r['candidato']}")
        
    print("--- ALL CANDIDATES FOR DIPUTADOS ---")
    rows = conn.execute("SELECT DISTINCT candidato FROM resultados WHERE acta_id IN (SELECT id FROM actas WHERE nivel='DIPUTADOS')").fetchall()
    for r in rows:
        print(r['candidato'])
        
    conn.close()
except Exception as e:
    print(e)
