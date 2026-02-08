import sqlite3
import json
import os
import db
import glob

def normalize_candidate(name):
    # Simple normalization if needed, but official JSON usually has full names
    return name.strip().upper()

def import_president_data():
    base_dir = os.path.dirname(__file__)
    json_dir = os.path.join(base_dir, 'data', 'JSON')
    
    conn = db.get_db_connection()
    c = conn.cursor()
    
    # Get all *-PRESIDENTE.json files
    files = glob.glob(os.path.join(json_dir, "*-PRESIDENTE.json"))
    print(f"Found {len(files)} President JSON files.")
    
    count = 0
    for file_path in files:
        filename = os.path.basename(file_path)
        jrv = filename.split('-')[0]
        
        # Load JSON
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue
            
        # Verify structure
        if 'resultados' not in data:
            print(f"Skipping {filename}: No 'resultados' key")
            continue
            
        # Get Acta ID (Official/Escrutinio)
        row = c.execute("SELECT id FROM actas WHERE jrv=? AND nivel='PRESIDENTE' AND origen='ESCRUTINIO'", (jrv,)).fetchone()
        if not row:
            # print(f"Skipping {jrv} (No Acta in DB)")
            continue
            
        acta_id = row['id']
        
        # CLEAR existing results
        c.execute("DELETE FROM resultados WHERE acta_id=?", (acta_id,))
        
        # Insert New
        validos = 0
        for item in data['resultados']:
            partido = item.get('partido', 'UNKNOWN')
            # For President, we usually use Partido as key or candidate?
            # Existing specific logic uses Candidate Name or Party?
            # In processor.py: key = f"{partido}" if candidate empty, else "{partido} ({candidato})"
            # But let's check what the frontend expects. 
            # Frontend uses `key` to match. 
            # If I look at `processor.py`, it uses: key = f"{partido}" ... if candidate: key += f" ({candidato})"
            # However, `db.py` normalizes keys.
            # Let's use simple Party Name for now as that's what seems to be used in Comparison view (keys are DC, Libre, etc.)
            
            # WAIT. Comparison view uses keys like "DC", "LIBRE".
            # processor.py Line 157: key = f"{partido}"
            # Let's map strict party names from JSON to our standard keys.
            
            p_upper = partido.upper()
            key = "UNKNOWN"
            if 'NACIONAL' in p_upper: key = 'NACIONAL'
            elif 'LIBERAL' in p_upper and 'LIBRE' not in p_upper: key = 'LIBERAL'
            elif 'LIBRE' in p_upper: key = 'LIBRE'
            elif 'INNOVACION' in p_upper: key = 'PINU'
            elif 'DEMOCRATA' in p_upper: key = 'DC'
            else: key = p_upper # Fallback
            
            v_str = str(item.get('votos', '0')).replace(',', '')
            try: votos = int(v_str)
            except: votos = 0
            
            c.execute("INSERT INTO resultados (acta_id, candidato, votos) VALUES (?, ?, ?)", (acta_id, key, votos))
            validos += votos
            
        # Update Resumen
        # JSON has stats
        stats = data.get('estadisticas', {}).get('distribucion_votos', {})
        def clean_int(val):
            try: return int(str(val).replace(',', ''))
            except: return 0
            
        r_validos = clean_int(stats.get('validos', 0))
        r_nulos = clean_int(stats.get('nulos', 0))
        r_blancos = clean_int(stats.get('blancos', 0))
        gran_total = r_validos + r_nulos + r_blancos
        
        # If JSON summary is missing/zero, use calculated
        if r_validos == 0 and validos > 0: r_validos = validos
        if gran_total == 0: gran_total = validos

        # Update DB Resumen
        # Check if exists
        res = c.execute("SELECT * FROM resumenes WHERE acta_id=?", (acta_id,)).fetchone()
        if res:
             c.execute("UPDATE resumenes SET votos_validos=?, votos_blancos=?, votos_nulos=?, gran_total=? WHERE acta_id=?", 
                       (r_validos, r_blancos, r_nulos, gran_total, acta_id))
        else:
             c.execute("INSERT INTO resumenes (acta_id, votos_validos, votos_blancos, votos_nulos, gran_total) VALUES (?, ?, ?, ?, ?)",
                       (acta_id, r_validos, r_blancos, r_nulos, gran_total))
        
        count += 1
        if count % 100 == 0: 
            conn.commit()
            print(f"Imported {count}...")

    conn.commit()
    conn.close()
    print(f"Finished. Imported {count} President actas.")

if __name__ == "__main__":
    import_president_data()
