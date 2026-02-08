import sqlite3
import json
import os
import db

def normalize_party(p_name):
    p_upper = p_name.upper().strip()
    if 'NACIONAL' in p_upper: return 'NACIONAL'
    if 'LIBERAL' in p_upper and 'LIBRE' not in p_upper: return 'LIBERAL'
    if 'LIBRE' in p_upper or 'LIBERTAD' in p_upper: return 'LIBRE'
    if 'SALVADOR' in p_upper or 'PSH' in p_upper: return 'PSH'
    if 'INNOVACION' in p_upper or 'PINU' in p_upper or 'SOCIAL' in p_upper: return 'PINU'
    if 'DEMOCRATA' in p_upper or ' DC' in p_upper or p_upper == 'DC': return 'DC'
    return p_upper

def import_official_data():
    base_dir = os.path.dirname(__file__)
    json_path = os.path.join(base_dir, 'data', 'diputados_oficial.json')
    
    if not os.path.exists(json_path):
        print("Json not found!")
        return

    print("Loading Official Data...")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    conn = db.get_db_connection()
    c = conn.cursor()
    
    count = 0
    for jrv, parties in data.items():
        # Get Acta ID
        # Note: 'ESCRUTINIO' is the key in DB for 'OFICIAL' origin
        row = c.execute("SELECT id FROM actas WHERE jrv=? AND nivel='DIPUTADOS' AND origen='ESCRUTINIO'", (jrv,)).fetchone()
        if not row:
            # Maybe create it?
            # User reset DB, so maybe acta is missing if processor didn't create it?
            # processor should have created it from the 'bad' JSONs. 
            # If not, let's skip or create.
            # print(f"Skipping {jrv} (No Acta)")
            continue
            
        acta_id = row['id']
        
        # CLEAR existing results for this acta to avoid duplicates with 'bad' data
        c.execute("DELETE FROM resultados WHERE acta_id=?", (acta_id,))
        
        # Insert New
        for p_name, candidates in parties.items():
            party_key = normalize_party(p_name)
            
            # Sort by ID to get relative index 1..N
            # Keys are strings "1", "9" etc.
            sorted_ids = sorted(candidates.keys(), key=lambda x: int(x))
            
            for idx, cid in enumerate(sorted_ids, start=1):
                cand_data = candidates[cid]
                votes = cand_data.get('votes', 0)
                
                # Construct key: "PARTIDO - DIP N"
                final_key = f"{party_key} - DIP {idx}"
                
                c.execute("INSERT INTO resultados (acta_id, candidato, votos) VALUES (?, ?, ?)", (acta_id, final_key, votes))
        
        
        # Recalculate Totals (Inline to avoid DB Lock from new connection)
        # Sum candidates
        tm = c.execute("SELECT SUM(votos) FROM resultados WHERE acta_id=?", (acta_id,)).fetchone()[0] or 0
        
        # Get Resumen
        res = c.execute("SELECT votos_blancos, votos_nulos FROM resumenes WHERE acta_id=?", (acta_id,)).fetchone()
        if res:
            blancos = res['votos_blancos']
            nulos = res['votos_nulos']
            gran = tm + blancos + nulos
            c.execute("UPDATE resumenes SET gran_total=?, votos_validos=? WHERE acta_id=?", (gran, tm, acta_id))
        
        count += 1
        if count % 100 == 0: 
            conn.commit() # Commit periodically
            print(f"Imported {count}...")

    conn.commit()
    conn.close()
    print(f"Finished. Imported {count} actas.")

if __name__ == "__main__":
    import_official_data()
