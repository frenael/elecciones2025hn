import csv
import sqlite3
import os

CSV_PATH = "resultados_2025 (frenael).csv"
DB_PATH = "auditoria.db"

def normalize_candidate(name):
    name = name.upper().strip()
    if name == "P. LIBERAL": return "LIBERAL"
    if name == "P. NACIONAL": return "NACIONAL"
    return name

def run():
    if not os.path.exists(CSV_PATH):
        print(f"Error: {CSV_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Data structure: { (jrv, origen): { 'votes': {}, 'summary': {} } }
    data_buffer = {}

    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            jrv = row['JRV']
            origen = row['ORIGEN'] # ESCRUTINIO or TREP
            candidate = row['CANDIDATO_RUBRO']
            try: votes = int(row['VOTOS'])
            except: votes = 0
            
            key = (jrv, origen)
            if key not in data_buffer:
                data_buffer[key] = {'votes': {}, 'summary': {}}
            
            if candidate in ["VOTOS BLANCOS", "VOTOS NULOS", "GRAN TOTAL"]:
                slug = candidate.lower().replace(" ", "_")
                data_buffer[key]['summary'][slug] = votes
            else:
                norm_cand = normalize_candidate(candidate)
                data_buffer[key]['votes'][norm_cand] = votes

    print(f"Loaded {len(data_buffer)} Acta entries from CSV.")

    # Insert into DB
    for (jrv, origen), payload in data_buffer.items():
        # Ensure Acta exists
        cursor.execute("""
            INSERT OR IGNORE INTO actas (jrv, origen, nivel, estado)
            VALUES (?, ?, 'PRESIDENTE', 'OFICIAL')
        """, (jrv, origen))
        
        # Get ID
        cursor.execute("SELECT id FROM actas WHERE jrv=? AND origen=? AND nivel='PRESIDENTE'", (jrv, origen))
        acta_id = cursor.fetchone()[0]
        
        # Insert Summary
        r = payload['summary']
        cursor.execute("""
            INSERT OR REPLACE INTO resumenes (acta_id, votos_validos, votos_blancos, votos_nulos, gran_total)
            VALUES (?, ?, ?, ?, ?)
        """, (acta_id, 0, r.get('votos_blancos', 0), r.get('votos_nulos', 0), r.get('gran_total', 0)))
        
        # Insert Votes
        for cand, v in payload['votes'].items():
            cursor.execute("""
                INSERT OR REPLACE INTO resultados (acta_id, candidato, votos)
                VALUES (?, ?, ?)
            """, (acta_id, cand, v))
            
    conn.commit()
    conn.close()
    print("Database updated successfully.")

if __name__ == "__main__":
    run()
