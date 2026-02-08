import sqlite3
import json
import csv
import io
import os

DB_NAME = "auditoria.db"

ORDEN_OFICIAL = [
    "PARTIDO DEMOCRATA CRISTIANO DE HONDURAS",
    "PARTIDO LIBERTAD Y REFUNDACION",
    "PARTIDO INNOVACION Y UNIDAD SOCIAL DEMOCRATA",
    "PARTIDO LIBERAL DE HONDURAS",
    "PARTIDO NACIONAL DE HONDURAS"
]

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS actas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        jrv TEXT NOT NULL,
        origen TEXT NOT NULL, 
        nivel TEXT DEFAULT 'PRESIDENTE',
        filepath TEXT NOT NULL,
        year_detected TEXT,
        debug_data TEXT,
        estado TEXT DEFAULT 'PENDIENTE',
        fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(jrv, origen, nivel)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS resultados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        acta_id INTEGER,
        candidato TEXT,
        votos INTEGER,
        FOREIGN KEY(acta_id) REFERENCES actas(id)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS resumenes (
        acta_id INTEGER PRIMARY KEY,
        votos_validos INTEGER,
        votos_blancos INTEGER,
        votos_nulos INTEGER,
        gran_total INTEGER,
        FOREIGN KEY(acta_id) REFERENCES actas(id)
    )''')
    
    # Migration for existing tables without 'nivel'
    try:
        cursor.execute("ALTER TABLE actas ADD COLUMN nivel TEXT DEFAULT 'PRESIDENTE'")
    except sqlite3.OperationalError:
        pass # Column likely exists

    # Migration for Unique Constraint (SQLite doesn't support DROP CONSTRAINT easily, so we re-create index if needed or ignore)
    # Ideally we'd recreate the table, but for now let's assume if we just added the column, the generic constraint might be weak.
    # Actually, SQLite `UNIQUE(jrv, origen)` is a table constraint. To change it, we usually need to recreate table.
    # However, for this specific task, if we just rely on `check_acta_exists` using nivel, we might be okay.
    # But to be safe, let's create a unique index that includes nivel.
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_jrv_origen_nivel ON actas(jrv, origen, nivel)')

    conn.commit()
    conn.close()

# --- FUNCIONES DE ESCRITURA Y VERIFICACIÓN ---

# --- FUNCIONES DE ESCRITURA Y VERIFICACIÓN ---

def check_acta_exists(jrv, origen, nivel='PRESIDENTE'):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM actas WHERE jrv = ? AND origen = ? AND nivel = ?", (jrv, origen, nivel))
    exists = cursor.fetchone()
    conn.close()
    return exists is not None

def update_acta_path(jrv, origen, new_path, nivel='PRESIDENTE'):
    conn = get_db_connection()
    cursor = conn.execute("UPDATE actas SET filepath = ? WHERE jrv = ? AND origen = ? AND nivel = ? AND filepath != ?", (new_path, jrv, origen, nivel, new_path))
    changes = cursor.rowcount
    conn.commit()
    conn.close()
    return changes > 0

def save_acta_result(jrv, origen, filepath, consensus_data, nivel='PRESIDENTE'):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Estado handling: depends on logic. For Presidente: OFICIAL is 'OFICIAL'. 
        # For Alcalde/Diputados: Logic is reversed (FRENAEL is Reference).
        # We will keep 'OFICIAL' status for the Reference source.
        # But here we just save what we are given. Status logic might be higher up or generic.
        # Let's keep existing consistency: If it comes from the TRUSTED source, it's OFFICIAL.
        # But 'origen' (TREP/ESCRUTINIO) is the key. 
        # In current app: ESCRUTINIO=OFICIAL, TREP=PENDIENTE.
        # New logic: 
        # Presidente: ESCRUTINIO (Oficial) -> Trusted. TREP (Frenael) -> Pending/Editable.
        # Alcalde/Dip: FRENAEL (Reference) -> Trusted? No, user said "valores a mostrar... deben ser esos (Frenael) y los que se editan son los del dato oficial".
        # So for Alc/Dip: FRENAEL is Static/Reference, OFFICIAL is Editable.
        # Let's implicitly trust the "Reference" one for status purposes?
        # Actually, `estado` column usage: 'VALIDADO' means checked by human.
        # Let's just default to 'PENDIENTE' unless specified.
        # The existing logic hardcoded: estado = 'OFICIAL' if origen == 'ESCRUTINIO' else 'PENDIENTE'
        # We should make this flexible or stick to it.
        # Let's rely on the caller to handle status if needed, or keep defaults.
        
        estado = 'OFICIAL' if (origen == 'ESCRUTINIO' and nivel == 'PRESIDENTE') else 'PENDIENTE' 
        # Note: For new levels, neither might be 'OFICIAL' in the same sense, or FRENAEL might be the "Reference". 
        # Let's treat "Reference" as non-default state if needed, but for now 'PENDIENTE' is safe.
        
        raw_matrix_json = json.dumps(consensus_data.get('raw_matrix', []))
        
        # Use INSERT OR REPLACE with generic constraints logic? 
        # With unique index on (jrv, origen, nivel), we can rely on conflict handling?
        # But we need to update if exists.
        
        # Check existence first to get ID for child tables
        cursor.execute('SELECT id FROM actas WHERE jrv = ? AND origen = ? AND nivel = ?', (jrv, origen, nivel))
        row = cursor.fetchone()
        
        if row:
            acta_id = row['id']
            cursor.execute('UPDATE actas SET filepath = ?, year_detected = ?, debug_data = ?, estado = ? WHERE id = ?', 
                           (filepath, consensus_data.get('year', '2025'), raw_matrix_json, estado, acta_id))
            # Clear old results to replace with new
            cursor.execute('DELETE FROM resultados WHERE acta_id = ?', (acta_id,))
            cursor.execute('DELETE FROM resumenes WHERE acta_id = ?', (acta_id,))
        else:
            cursor.execute('INSERT INTO actas (jrv, origen, nivel, filepath, year_detected, estado, debug_data) VALUES (?, ?, ?, ?, ?, ?, ?)',
                           (jrv, origen, nivel, filepath, consensus_data.get('year', '2025'), estado, raw_matrix_json))
            acta_id = cursor.lastrowid

        results = consensus_data.get('resultados', {})
        for candidato, votos in results.items():
            cursor.execute('INSERT INTO resultados (acta_id, candidato, votos) VALUES (?, ?, ?)', (acta_id, candidato, votos))

        resumen = consensus_data.get('resumen', {})
        cursor.execute('''INSERT INTO resumenes (acta_id, votos_validos, votos_blancos, votos_nulos, gran_total) VALUES (?, ?, ?, ?, ?)''', 
                       (acta_id, resumen.get('votos_validos', 0), resumen.get('votos_blancos', 0), resumen.get('votos_nulos', 0), resumen.get('gran_total', 0)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# --- FUNCIONES DE LECTURA ---

# Normalization Mapping (Official -> Unified)
NORM_MAP = {
    'PARTIDO NACIONAL DE HONDURAS': 'Nacional',
    'PARTIDO LIBERAL DE HONDURAS': 'Liberal',
    'PARTIDO LIBERTAD Y REFUNDACION': 'Libre',
    'PARTIDO SALVADOR DE HONDURAS': 'PSH',
    'PARTIDO DEMOCRATA CRISTIANO DE HONDURAS': 'DC',
    'PARTIDO INNOVACION Y UNIDAD SOCIAL DEMOCRATA': 'PINU',
    'PARTIDO ANTICORRUPCION DE HONDURAS': 'PAC',
    'ALIANZA PATRIOTICA HONDURENA': 'Alianza',
    'PARTIDO FRENTE AMPLIO': 'Frente Amplio',
    'PARTIDO NUEVA RUTA DE HONDURAS': 'Nueva Ruta',
    'PARTIDO VAMOS': 'Vamos',
    'PARTIDO LIBERACION DEMOCRATICO DE HONDURAS': 'Liderh',
    'CANDIDATURA INDEPENDIENTE': 'Independiente'
}

def get_formulario_info(jrv):
    """
    Reads data/formulario_cierre.csv and returns info for the specific JRV.
    """
    import csv
    import os
    
    base_dir = os.path.dirname(__file__)
    
    
    # --- Helper to read form data (apertura/cierre) ---
    def read_csv_info(path):
        if not os.path.exists(path): return {}
        try:
            with open(path, mode='r', encoding='latin-1', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Find JRV
                    row_jrv = row.get('jrv_existe', '').strip()
                    if not row_jrv:
                         for k in row.keys():
                             if 'jrv' in k.lower() or ' mesa ' in k.lower():
                                 if row[k].strip() == str(jrv):
                                     row_jrv = str(jrv)
                                     break
                    
                    if row_jrv == str(jrv):
                        def get_val(fragment):
                            for k in row.keys():
                                if fragment in k: return row[k]
                            return "N/A"

                        # Common fields
                        data = {
                            "observador": get_val("Nombre de observador"),
                            "depto": row.get("Departamento", "N/A"), # Fallback
                            "muni": row.get("Municipio", "N/A"), # Fallback
                            "centro": get_val("centro de votaci"), # Fallback
                            "votantes_registro": get_val("votantes_registro"), # Fallback
                            "jrv": row_jrv
                        }
                        
                        # Only Closure has winner
                        if "cierre" in path:
                             data["ganador_pres"] = get_val("gan? a nivel presidencial")

                        return data
        except Exception as e:
            print(f"Error reading {path}: {e}")
        return {}

    # --- Helper to read JRV Totals (Primary Source for Location/Voters) ---
    def read_jrv_totales():
        path = os.path.join(base_dir, 'data', 'JRV_totales.csv')
        if not os.path.exists(path): return {}
        try:
            with open(path, mode='r', encoding='utf-8', errors='replace') as f: # Likely UTF-8 or latin-1
                 # Using csv.reader or DictReader. Let's assume headers are clean based on Get-Content
                 reader = csv.DictReader(f)
                 # Headers: NUMERO_JRV,NOMBRE_DEPARTAMENTO,NOMBRE_MUNICIPIO,NOMBRE_CENTRO,Votantes
                 for row in reader:
                     if row.get('NUMERO_JRV', '').strip() == str(jrv):
                         return {
                             "depto": row.get('NOMBRE_DEPARTAMENTO', ''),
                             "muni": row.get('NOMBRE_MUNICIPIO', ''),
                             "centro": row.get('NOMBRE_CENTRO', ''),
                             "votantes_registro": row.get('Votantes', '0')
                         }
        except Exception as e:
            print(f"Error reading JRV_totales.csv: {e}")
        return {}

    # 1. Get Primary Info (Totals)
    primary_info = read_jrv_totales()
    
    # 2. Get Supplementary Info (Observer/Winner) - Try Closure first, then Apertura
    form_info = read_csv_info(os.path.join(base_dir, 'data', 'formulario_cierre.csv'))
    if not form_info:
        form_info = read_csv_info(os.path.join(base_dir, 'data', 'formulario_apertura.csv'))
        
    # 3. Merge (Primary overrides Form for overlapping keys, Form provides unique keys)
    # Default structure based on form_info or empty
    final_info = form_info if form_info else {"observador": "N/A", "jrv": str(jrv)}
    
    # Override with Primary (JRV_totales)
    if primary_info:
        final_info.update(primary_info)
        
    return final_info

def get_comparison_data(jrv, nivel='PRESIDENTE'):
    conn = get_db_connection()
    trep = conn.execute("SELECT * FROM actas WHERE jrv = ? AND origen = 'TREP' AND nivel = ?", (jrv, nivel)).fetchone()
    esc = conn.execute("SELECT * FROM actas WHERE jrv = ? AND origen = 'ESCRUTINIO' AND nivel = ?", (jrv, nivel)).fetchone()
    
    # Flag: If TREP is missing, the comparison is invalid and we should zero out everything.
    has_trep = True if trep else False
    
    # If no TREP, suppress Official data to ensure "All Zeros" / "SIN ACTA" state
    if not has_trep:
        esc = None

    # --- Initialize Structure ---
    comp_data = {
        'trep': {'votos': {}, 'meta': {}, 'resumen': {'votos_blancos': 0, 'votos_nulos': 0, 'gran_total': 0}},
        'esc': {'votos': {}, 'meta': {}, 'resumen': {'votos_blancos': 0, 'votos_nulos': 0, 'gran_total': 0}},
        'matrix': None,
        'all_candidates': [],
        'header_info': get_formulario_info(jrv), # Load Formulario Cierre info
        'has_trep': has_trep
    }
    found_candidates = set()
    
    # Name Normalization Map
    
    IGNORED_KEYS = ["RESULTADOS", "VOTOS", "TOTAL", "VALIDOS", "NULOS", "BLANCOS", "GRAN TOTAL", "COLUMNS"]

    def normalize(name):
        # Handle " - DIP " keys specially to preserve the number
        suffix = ""
        if " - DIP " in name:
            parts = name.split(" - DIP ")
            name = parts[0]
            suffix = " - DIP " + parts[1]

        # Remove parenthetical redundancy e.g. "NAME (NAME )"
        if '(' in name: name = name.split('(')[0].strip()
        
        # Explicit Normalization Logic (ordered by specificity)
        p_upper = name.upper().strip()
        normalized_name = name
        
        # Order matters! Check PINU before DC (due to 'Social Democrata')
        if 'NACIONAL' in p_upper: normalized_name = 'Nacional'
        elif 'LIBERAL' in p_upper and 'LIBRE' not in p_upper: normalized_name = 'Liberal'
        elif 'LIBRE' in p_upper or 'LIBERTAD' in p_upper or 'REFUNDACION' in p_upper: normalized_name = 'Libre'
        elif 'SALVADOR' in p_upper or 'PSH' in p_upper: normalized_name = 'PSH'
        elif 'INNOVACION' in p_upper or 'PINU' in p_upper or 'SOCIAL' in p_upper: normalized_name = 'PINU'
        elif 'DEMOCRATA' in p_upper or ' DC' in p_upper or p_upper == 'DC': normalized_name = 'DC'
        
        # Check mapping (legacy fallback)
        # for k, v in NORM_MAP.items():
        #     if k in name: 
        #         normalized_name = v
        #         break
        
        return normalized_name + suffix

    # --- Special Handling for ALCALDE (Load from JSON) ---
    if nivel == 'ALCALDE':
        import json
        import os
        
        # Prepare Metadata from DB if available (so images and IDs work)
        if trep: comp_data['trep']['meta'] = dict(trep)
        if esc: comp_data['esc']['meta'] = dict(esc)

        # Paths
        base_dir = os.path.dirname(__file__)
        path_oficial = os.path.join(base_dir, 'data', 'JSON', f'{jrv}-ALCALDE.json')
        path_frenael = os.path.join(base_dir, 'data', 'JSON', f'{jrv}-ALCALDE-FRENAEL.json')

        # Load OFICIAL (Escrutinio)
        if os.path.exists(path_oficial):
            try:
                with open(path_oficial, 'r', encoding='utf-8') as f:
                    data_esc = json.load(f)
                    
                # Parse Resultados
                for item in data_esc.get('resultados', []):
                    p_name = item.get('partido', '')
                    votos = int(item.get('votos', 0))
                    
                    # Normalize Party Name
                    norm_name = normalize(p_name)
                    comp_data['esc']['votos'][norm_name] = votos
                    found_candidates.add(norm_name)
                    
                # Parse Summary (Estadisticas)
                stats = data_esc.get('estadisticas', {}).get('distribucion_votos', {})
                comp_data['esc']['resumen']['votos_blancos'] = int(stats.get('blancos', 0))
                comp_data['esc']['resumen']['votos_nulos'] = int(stats.get('nulos', 0))
                comp_data['esc']['resumen']['validos'] = int(stats.get('validos', 0))
                
                # Gran Total (Sum validos + blancos + nulos if not explicit?)
                # JSON has "totalizacion_actas" but not explicit gran total of votes?
                # Actually sum is safest or use validos+blancos+nulos
                comp_data['esc']['resumen']['gran_total'] = comp_data['esc']['resumen']['validos'] + comp_data['esc']['resumen']['votos_blancos'] + comp_data['esc']['resumen']['votos_nulos']
                
            except Exception as e:
                print(f"Error loading Official Alcalde JSON: {e}")

        # Load FRENAEL (Trep)
        if os.path.exists(path_frenael):
            try:
                with open(path_frenael, 'r', encoding='utf-8') as f:
                    data_trep = json.load(f)
                
                # Direct Keys for Parties
                # We need to keys that match our normalized parties or known keys
                # The JSON keys are like "DC", "Libre", "PINU", "Liberal", "Nacional" (matches our short names!)
                # And summary keys
                
                for key, val in data_trep.items():
                    # Check if it's a party
                    idx_val = int(val) if str(val).isdigit() else 0
                    
                    if key in ["DC", "Libre", "PINU", "Liberal", "Nacional", "PSH"]:
                         # It matches our short names directly
                         comp_data['trep']['votos'][key] = idx_val
                         found_candidates.add(key)
                    elif key == "votosBlanco":
                        comp_data['trep']['resumen']['votos_blancos'] = idx_val
                    elif key == "votosNulos":
                        comp_data['trep']['resumen']['votos_nulos'] = idx_val
                    elif key == "granTotal":
                        comp_data['trep']['resumen']['gran_total'] = idx_val

            except Exception as e:
                print(f"Error loading Frenael Alcalde JSON: {e}")

    # --- Standard DB Loading for Non-ALCALDE levels (or fallback) ---
    if nivel != 'ALCALDE':
        if trep:
            comp_data['trep']['meta'] = dict(trep)
            resumen = conn.execute("SELECT * FROM resumenes WHERE acta_id = ?", (trep['id'],)).fetchone()
            if resumen: comp_data['trep']['resumen'] = dict(resumen)
            rows = conn.execute("SELECT candidato, votos FROM resultados WHERE acta_id = ?", (trep['id'],)).fetchall()
            for row in rows:
                if any(x in row['candidato'].upper() for x in IGNORED_KEYS): continue
                
                clean_name = normalize(row['candidato'].upper())
                # Use title case if shorter than 5 chars (like 'DC') keep upper, else Title? actually 'Nacional' is Title.
                # existing short names are 'Nacional', 'Liberal'. Let's match that.
                if clean_name in list(NORM_MAP.values()): pass 
                else: clean_name = row['candidato'] # Fallback if not mapped

                # Force Title Case for specific mapped values if we constructed them from caps
                if clean_name == "DC" or clean_name == "PINU" or clean_name == "PSH": pass
                elif clean_name in ["Nacional", "Liberal", "Libre"]: pass
                else: 
                    # Re-apply strict mapping just in case normalization didn't fully catch exact casing
                    # The map above had Keys as Upper, Values as Title/Mixed.
                    # Let's trust the NORM_MAP values.
                    pass
                
                # Simple apply
                if any(x in row['candidato'].upper() for x in IGNORED_KEYS): continue # Double check if not clean
                
                final_key = normalize(row['candidato'].upper())
                # Additional Check: if level is PRESIDENTE, ignore "DIP" keys
                if nivel == 'PRESIDENTE' and 'DIP' in final_key: continue
                
                comp_data['trep']['votos'][final_key] = row['votos']
                found_candidates.add(final_key)
            
        if esc:
            comp_data['esc']['meta'] = dict(esc)
            resumen = conn.execute("SELECT * FROM resumenes WHERE acta_id = ?", (esc['id'],)).fetchone()
            if resumen: comp_data['esc']['resumen'] = dict(resumen)
            rows = conn.execute("SELECT candidato, votos FROM resultados WHERE acta_id = ?", (esc['id'],)).fetchall()
            for row in rows:
                if any(x in row['candidato'].upper() for x in IGNORED_KEYS): continue
                
                final_key = normalize(row['candidato'].upper())
                if nivel == 'PRESIDENTE' and 'DIP' in final_key: continue
                
                comp_data['esc']['votos'][final_key] = row['votos']
                found_candidates.add(final_key)
    
    # Sorting logic: Use ORDEN_OFICIAL as base, then append others
    sorted_candidates = []
    
    # Official Order Matches: DC, Libre, PINU, Liberal, Nacional (normalized casing)
    # DC, PINU, PSH -> Upper
    # Liberal, Libre, Nacional -> Title
    SORT_ORDER = ["DC", "LIBRE", "PINU", "LIBERAL", "NACIONAL"]
    
    for oficial in SORT_ORDER:
        match = None
        # Check if already present
        for fc in list(found_candidates):
            if fc == oficial: match = fc; break
            # Fallback loose match
            if fc.upper() == oficial.upper(): match = fc; break
        
        if match: 
            to_add = match
            found_candidates.remove(match) 
        else:
            # Not found? Add it anyway for Presidente/Alcalde as requested
            to_add = oficial
            
        sorted_candidates.append(to_add)
        
        # Ensure it exists in votos as 0 if not present
        if to_add not in comp_data['trep']['votos']: comp_data['trep']['votos'][to_add] = 0
        if to_add not in comp_data['esc']['votos']: comp_data['esc']['votos'][to_add] = 0
    
    # Add remaining (e.g. Diputados names or Alcaldes)
    # Sort alphabetically or keep as is? Alphabetical might be better for consistent display
    for resto in sorted(list(found_candidates)): 
        sorted_candidates.append(resto)
    
    comp_data['all_candidates'] = sorted_candidates

    # --- Matrix Generation for Diputados ---
    if nivel == 'DIPUTADOS':
        # Load Official Data from JSON
        import json
        import os
        json_path = os.path.join(os.path.dirname(__file__), 'data', 'diputados_oficial.json')
        official_data = full_official = {}
        if os.path.exists(json_path):
            with open(json_path, 'r', encoding='utf-8') as f:
                full_official = json.load(f)
                

                official_data = full_official.get(str(jrv), {})

        # Helper to normalize party names
        def normalize_party(p_name):
            p_upper = p_name.upper().strip()
            # Common Aliases
            if 'NACIONAL' in p_upper: return 'NACIONAL'
            if 'LIBERAL' in p_upper and 'LIBRE' not in p_upper: return 'LIBERAL'
            if 'LIBRE' in p_upper or 'LIBERTAD' in p_upper: return 'LIBRE'
            if 'SALVADOR' in p_upper or 'PSH' in p_upper: return 'PSH'
            if 'INNOVACION' in p_upper or 'PINU' in p_upper or 'SOCIAL' in p_upper: return 'PINU'
            if 'DEMOCRATA' in p_upper or ' DC' in p_upper or p_upper == 'DC': return 'DC'
            return p_upper
            
        party_aliases = {
            "PN": "NACIONAL", "PL": "LIBERAL", "LIBRE": "LIBRE", "PSH": "PSH", "DC": "DC", "PINU": "PINU"
        }

        # Pre-calculate Index Mapping (Absolute -> Relative) from Official Data
        # Map: { 'LIBRE': { 9: 1, 10: 2... }, 'DC': { 1: 1... } }
        party_idx_map = {}
        if official_data:
            for off_party, rows in official_data.items():
                 target_party = normalize_party(party_aliases.get(off_party, off_party))
                 if target_party not in party_idx_map: party_idx_map[target_party] = {}
                 
                 # Sort Absolute Keys
                 sorted_keys = sorted(rows.keys(), key=lambda x: int(x))
                 for rel, abs_k in enumerate(sorted_keys, start=1):
                     try: party_idx_map[target_party][int(abs_k)] = rel
                     except: pass

        matrix_map = {}
        found_parties = set()
        max_idx = 0
        
        # 1. Process Attributes from sorted_candidates (FRENAEL data)
        
        for cand in sorted_candidates:
            if ' - DIP ' in cand:
                parts = cand.split(' - DIP ')
                raw_party = parts[0]
                
                # Filter out unwanted keys that might have been ingested as candidates
                if any(x in raw_party.upper() for x in IGNORED_KEYS): continue
                
                party = normalize_party(raw_party)
                
                try: 
                    raw_idx = int(parts[1])
                    # Try to map Absolute to Relative if map exists
                    if party in party_idx_map and raw_idx in party_idx_map[party]:
                        idx = party_idx_map[party][raw_idx]
                    else:
                        # Fallback: Use raw_idx. 
                        # If raw_idx is Absolute (e.g. 33) but we have no official map, it stays 33.
                        # If raw_idx is Relative (e.g. 1) it stays 1.
                        idx = raw_idx
                except: idx = 0
                
                if party not in matrix_map: matrix_map[party] = {}
                matrix_map[party][idx] = cand
                found_parties.add(party)
                if idx > max_idx: max_idx = idx

        # 2. Integrate Candidates from Official Data
        if official_data:
            for off_party, rows in official_data.items():
                target_party = normalize_party(party_aliases.get(off_party, off_party))
                
                found_parties.add(target_party)
                if target_party not in matrix_map: matrix_map[target_party] = {}

                # Use calculated map
                if target_party in party_idx_map:
                    # Iterate the MAP to ensure we cover all official candidates in order
                    # party_idx_map[target_party] = { abs: rel, ... }
                    # We can also just iterate 'rows' and use the map
                    for abs_str, cand_info in rows.items():
                         try:
                             abs_i = int(abs_str)
                             rel_i = party_idx_map[target_party][abs_i]
                             
                             if rel_i > max_idx: max_idx = rel_i
                             
                             # Check existing key at RELATIVE slot
                             existing_key = matrix_map[target_party].get(rel_i)
                             if not existing_key:
                                 new_key = f"{target_party} - DIP {rel_i}"
                                 matrix_map[target_party][rel_i] = new_key
                                 comp_data['esc']['votos'][new_key] = cand_info['votes']
                             else:
                                 # Update existing
                                 comp_data['esc']['votos'][existing_key] = cand_info['votes']
                         except: pass
                else:
                    # Fallback if map failed? Should not happen if official_data exists
                    pass

        
        # 3. Fill structure: Start with SORT_ORDER unconditionally
        sorted_parties = list(SORT_ORDER)
        
        # Append remaining found parties not in SORT_ORDER
        for fp in found_parties:
             if fp not in sorted_parties: sorted_parties.append(fp)

        # Ensure every party in sorted_parties is in matrix_map
        for p in sorted_parties:
            if p not in matrix_map: matrix_map[p] = {}
            for i in range(1, max_idx + 1):
                if i not in matrix_map[p]:
                    gen_key = f"{p} - DIP {i}"
                    matrix_map[p][i] = gen_key
                    if gen_key not in comp_data['esc']['votos']:
                        comp_data['esc']['votos'][gen_key] = 0
                    if gen_key not in comp_data['trep']['votos']:
                        comp_data['trep']['votos'][gen_key] = 0

        # 2a. Extract Candidate Names
        # struct: { 'Party': { rel_idx: "Name" } }
        names_map = {}
        if official_data:
            for off_party, rows in official_data.items():
                target_party = normalize_party(party_aliases.get(off_party, off_party))
                if target_party not in names_map: names_map[target_party] = {}
                
                if target_party in party_idx_map:
                    for abs_str, cand_info in rows.items():
                        try:
                            abs_i = int(abs_str)
                            rel_i = party_idx_map[target_party][abs_i]
                            # Shorten Name: First + First Last
                            raw_name = cand_info.get('name', '').strip()
                            parts = raw_name.split()
                            short_name = raw_name
                            if len(parts) >= 3:
                                # Assume: Name1 [Name2] Last1 [Last2]
                                # We want Name1 + Last1
                                # Any middle names usually come before the first surname.
                                # Heuristic: if 4 parts -> parts[0] + parts[2]
                                # if 3 parts -> parts[0] + parts[1] (assuming Name Last1 Last2) OR Name1 Name2 Last1?
                                # Common Spanish: Name Last1 Last2 (3 parts) -> Name Last1
                                # Name1 Name2 Last1 (3 parts) -> Name1 Last1 (less common to omit mother's surname)
                                # Let's try: unique logic
                                if len(parts) == 4:
                                    short_name = f"{parts[0]} {parts[2]}"
                                elif len(parts) == 3:
                                    # Ambiguous. Usually Name Last1 Last2.
                                    # Juan Perez Lopez -> Juan Perez
                                    short_name = f"{parts[0]} {parts[1]}"
                                else:
                                    # 5+ parts? taking 0 and -2 is safer
                                    short_name = f"{parts[0]} {parts[-2]}"
                            elif len(parts) == 2:
                                short_name = raw_name
                            
                            names_map[target_party][rel_i] = short_name
                        except: pass

        comp_data['matrix'] = {
            'parties': sorted_parties,
            'rows': list(range(1, max_idx + 1)),
            'map': matrix_map,
            'names': names_map
        }

        # Filter out rows that exceed official candidate count (if available)
        if official_data:
             # Determine Official Max Count (using relative count now)
             max_cand_count = 0
             for off_party, rows in official_data.items():
                 c_cnt = len(rows)
                 if c_cnt > max_cand_count: max_cand_count = c_cnt
             
             if max_cand_count > 0:
                 final_max_idx = max_cand_count
                 comp_data['matrix']['rows'] = list(range(1, final_max_idx + 1))
                 
                 # Clean up matrix_map
                 for p in matrix_map:
                     keys_to_remove = []
                     for idx in matrix_map[p]:
                         if idx > final_max_idx: keys_to_remove.append(idx)
                     for k in keys_to_remove:
                         del matrix_map[p][k]
        else:
             pass # Official data missing, keep found max

        # 4. Calculate Column Totals
        # struct: { 'Party': { 'trep': 0, 'esc': 0, 'diff': 0 } }
        totals = {}
        for p in sorted_parties:
            t_sum = 0
            e_sum = 0
            # Iterate active rows
            for i in comp_data['matrix']['rows']:
                key = matrix_map[p].get(i)
                if key:
                    t_sum += int(comp_data['trep']['votos'].get(key, 0))
                    e_sum += int(comp_data['esc']['votos'].get(key, 0))
            
            totals[p] = {
                'trep': t_sum,
                'esc': e_sum,
                'diff': t_sum - e_sum
            }
        
        comp_data['matrix']['totals'] = totals

    # --- Calculate Participation Percentage (Always based on Presidential FRENAEL Total) ---
    try:
        if comp_data['header_info'] and comp_data['header_info'].get('votantes_registro'):
            total_registered = int(comp_data['header_info']['votantes_registro'])
            if total_registered > 0:
                # 1. Try to get Presidential Total from current data if level is PRESIDENTE
                pres_total = 0
                if nivel == 'PRESIDENTE':
                    pres_total = comp_data['trep']['resumen']['gran_total']
                else:
                    # 2. Query DB for Presidential TREP total if not current level
                    row_pres = conn.execute("SELECT id FROM actas WHERE jrv = ? AND origen = 'TREP' AND nivel = 'PRESIDENTE'", (jrv,)).fetchone()
                    if row_pres:
                        # Get Gran Total from resumenes
                        res_pres = conn.execute("SELECT gran_total FROM resumenes WHERE acta_id = ?", (row_pres['id'],)).fetchone()
                        if res_pres:
                            pres_total = res_pres['gran_total']
                        elif row_pres['debug_data']: # Fallback if resumenes empty?
                             pass
                        
                        # Fallback: Sum votes from resultados if resumenes matched nothing
                        if pres_total == 0:
                            sum_votes = conn.execute("SELECT SUM(votos) as total FROM resultados WHERE acta_id = ?", (row_pres['id'],)).fetchone()
                            if sum_votes and sum_votes['total']:
                                pres_total = sum_votes['total']
                
                # Calculate
                if pres_total > 0:
                    percent = (pres_total / total_registered) * 100
                    comp_data['header_info']['participacion'] = f"{percent:.2f}%"
    except Exception as e:
        print(f"Error calculating participation: {e}")

    # --- FINAL SAFETY CHECK: If TREP is missing, clear all comparison data ---
    # This ensures "Sin Acta" view shows Zeros everywhere as requested.
    if not has_trep:
        comp_data['trep']['votos'] = {}
        comp_data['esc']['votos'] = {}
        comp_data['trep']['resumen'] = {'votos_blancos': 0, 'votos_nulos': 0, 'gran_total': 0}
        comp_data['esc']['resumen'] = {'votos_blancos': 0, 'votos_nulos': 0, 'gran_total': 0}
        if comp_data.get('header_info'):
            comp_data['header_info']['participacion'] = "0.00%"
        if comp_data.get('matrix'):
             # Clear matrix rows but keep headers/structure if desired? 
             # Or just clear rows values?
             # If we clear rows dict, table is empty.
             # If we want table with 0s, we need rows but with 0 values.
             # But rows are generated based on data. If we cleared data, rows might be wrong?
             # Current implementation generates matrix rows from 'official_data' loading.
             # If we want 0s, we should probably iterate the rows and set values to 0?
             # Simpler: Clear the 'totals' and let rows render as 0?
             # No, rows contain the values.
             # Let's just clear the vote counts in matrix rows.
             pass
             # Actually, if we just clear comp_data['esc']['votos'], the matrix logic (which might run before this) 
             # has already populated comp_data['matrix']['rows'] with values?
             # Matrix generation happens at lines 600+.
             # This block runs at line 730+.
             # So Matrix is ALREADY populated.
             # We need to ITERATE matrix rows and zero them out.
             if comp_data['matrix'] and 'totals' in comp_data['matrix']:
                 for k in comp_data['matrix']['totals']:
                     comp_data['matrix']['totals'][k] = {'trep': 0, 'esc': 0, 'diff': 0}

    conn.close()
    return comp_data

def get_all_jrvs_status():
    conn = get_db_connection()
    try: jrvs = conn.execute("SELECT DISTINCT jrv FROM actas ORDER BY CAST(jrv AS INTEGER) ASC").fetchall()
    except: jrvs = conn.execute("SELECT DISTINCT jrv FROM actas ORDER BY jrv ASC").fetchall()
    
    status_list = []
    for row in jrvs:
        jrv = row['jrv']
        trep = conn.execute("SELECT estado FROM actas WHERE jrv=? AND origen='TREP' AND nivel='PRESIDENTE'", (jrv,)).fetchone()
        esc = conn.execute("SELECT id FROM actas WHERE jrv=? AND origen='ESCRUTINIO' AND nivel='PRESIDENTE'", (jrv,)).fetchone()
        
        diff = 0
        winner = "Sin Datos"
        diff_nacional = 0
        diff_liberal = 0
        diff_libre = 0
        
        if trep and esc:
            comp = get_comparison_data(jrv, nivel='PRESIDENTE')
            for k in comp['all_candidates']:
                v_trep = comp['trep']['votos'].get(k, 0)
                v_esc = comp['esc']['votos'].get(k, 0)
                
                diff += abs(v_trep - v_esc)
                d_signed = v_esc - v_trep 
                
                name = k.upper()
                if "NACIONAL" in name: diff_nacional += d_signed
                elif "LIBERAL" in name: diff_liberal += d_signed
                elif "LIBRE" in name or "REFUNDACION" in name: diff_libre += d_signed
            
            r_trep = comp['trep']['resumen']
            r_esc = comp['esc']['resumen']
            diff += abs(r_trep['votos_blancos'] - r_esc['votos_blancos'])
            diff += abs(r_trep['votos_nulos'] - r_esc['votos_nulos'])
            diff += abs(r_trep['gran_total'] - r_esc['gran_total'])

        votos_fuente = {}
        if trep and esc:
             # Cargar comp_data para obtener ganador
             if 'comp' not in locals(): comp = get_comparison_data(jrv)
             votos_fuente = comp['esc']['votos'] if comp['esc']['votos'] else comp['trep']['votos']
        
        if votos_fuente:
            top = sorted(votos_fuente.items(), key=lambda item: item[1], reverse=True)
            if top:
                name = top[0][0].split('(')[0].strip()
                if "NACIONAL" in name: winner = "P. NACIONAL"
                elif "LIBERAL" in name: winner = "P. LIBERAL"
                elif "LIBRE" in name: winner = "LIBRE"
                elif "DEMOCRATA" in name or "DC" in name: winner = "DC"
                elif "INNOVACION" in name or "PINU" in name: winner = "PINU"
                else: winner = name
        
        status_list.append({
            'jrv': jrv, 
            'has_trep': trep is not None, 
            'has_esc': esc is not None, 
            'estado': trep['estado'] if trep else 'FALTANTE', 
            'diff': diff, 
            'winner': winner,
            'diff_nacional': diff_nacional,
            'diff_liberal': diff_liberal,
            'diff_libre': diff_libre
        })
    conn.close()
    return status_list

def get_jrv_navigation(current_jrv):
    conn = get_db_connection()
    try:
        try: jrvs_raw = conn.execute("SELECT DISTINCT jrv FROM actas ORDER BY CAST(jrv AS INTEGER) ASC").fetchall()
        except: jrvs_raw = conn.execute("SELECT DISTINCT jrv FROM actas ORDER BY jrv ASC").fetchall()
        jrvs = [row['jrv'] for row in jrvs_raw]
        prev_jrv, next_jrv = None, None
        try:
            idx = jrvs.index(str(current_jrv))
            if idx > 0: prev_jrv = jrvs[idx - 1]
            if idx < len(jrvs) - 1: next_jrv = jrvs[idx + 1]
        except ValueError: pass
        return {'prev': prev_jrv, 'next': next_jrv}
    finally: conn.close()

def get_next_pending_jrv(current_jrv):
    conn = get_db_connection()
    try:
        # Buscar siguiente pendiente con ID mayor
        query_next = "SELECT jrv FROM actas WHERE origen = 'TREP' AND estado = 'PENDIENTE' AND nivel = 'PRESIDENTE' AND CAST(jrv AS INTEGER) > CAST(? AS INTEGER) ORDER BY CAST(jrv AS INTEGER) ASC LIMIT 1"
        row = conn.execute(query_next, (current_jrv,)).fetchone()
        if row: return row['jrv']
        
        # Buscar desde el principio
        query_first = "SELECT jrv FROM actas WHERE origen = 'TREP' AND estado = 'PENDIENTE' AND nivel = 'PRESIDENTE' ORDER BY CAST(jrv AS INTEGER) ASC LIMIT 1"
        row = conn.execute(query_first).fetchone()
        if row and str(row['jrv']) != str(current_jrv): return row['jrv']
        return None
    finally: conn.close()

def get_global_stats():
    conn = get_db_connection()
    
    # Structure: { 'PRESIDENTE': { 'trep': {...}, 'esc': {...} }, ... }
    final_stats = {}
    
    levels = ['PRESIDENTE', 'DIPUTADOS', 'ALCALDE']
    
    for level in levels:
        # Initialize level stats
        level_stats = {'trep': {'total': 0, 'nacional': 0, 'liberal': 0, 'libre': 0, 'otros': 0}, 
                       'esc': {'total': 0, 'nacional': 0, 'liberal': 0, 'libre': 0, 'otros': 0}}
        
        # Query with filtering for Valid Total in FRENAEL (TREP)
        # User requirement: "solo debe sumarse los resultados que tenga valor valido de TOTAL para FRENAEL"
        # Since we compare TREP vs OFFICIAL, do we apply this filter to OFFICIAL too? 
        # The prompt says "TOTAL para FRENAEL", implying strictness on FRENAEL side.
        # But usually we want to see available data. 
        # Let's interpret strictness:
        # Sum votes FROM actas WHERE (acta has valid summary total > 0)
        
        # Actually, let's effectively filter specific valid actas. 
        # For TREP: Must have gran_total > 0 in resumenes.
        # For ESC: Maybe keep standard? Or same rule?
        # Let's apply "gran_total > 0" check for both to be safe/consistent, or strictly for TREP if requested.
        # "que tenga valor valido de TOTAL para FRENAEL" -> specifically mentioned FRENAEL.
        
        

            
        # Get Acta Counts
        # TREP: Must have valid resumen
        # ESC: If present in resultados (since we didn't enforce resumen check for ESC in the main query? 
        # Wait, the main query DOES enforce `JOIN resumenes`. 
        # If OFICIAL data lacks resumenes, the main query returns ZERO for official.
        # This confirms why Official might be empty if resumenes is empty.
        # 
        # FIX: The query needs left join or separate logic for ESC if ESC data lacks resumenes.
        # But filtering only FRENAEL total valid?
        # "solo debe sumarse los resultados que tenga valor valido de TOTAL para FRENAEL"
        # This acts as a filter on *FRENAEL* data.
        # It doesn't explicitly say "Filter Official data by FRENAEL valid total".
        # It says "sum results... that have valid TOTAL for FRENAEL".
        # 
        # Let's decouple:
        # TREP Data: Filter by `res.gran_total > 0`.
        # ESC Data: Show all available? Or match FRENAEL?
        # Usually independent totals.
        #
        # Let's adjust the query to be separated or use UNION.
        # Or simplistic approach: 
        # 1. Query TREP with filter.
        # 2. Query ESC without filter (or different filter).
        
        # New approach: Two queries.
        
        # TREP Query
        query_trep = """
            SELECT r.candidato, SUM(r.votos) as total 
            FROM resultados r 
            JOIN actas a ON r.acta_id = a.id 
            JOIN resumenes res ON res.acta_id = a.id
            WHERE a.nivel = ? AND a.origen = 'TREP'
            AND res.gran_total > 0
            GROUP BY r.candidato
        """
        rows_trep = conn.execute(query_trep, (level,)).fetchall()
        
        # ESC Query (No resumen filter needed if we trust file ingestion, or just checking presence)
        query_esc = """
            SELECT r.candidato, SUM(r.votos) as total 
            FROM resultados r 
            JOIN actas a ON r.acta_id = a.id 
            WHERE a.nivel = ? AND a.origen = 'ESCRUTINIO'
            GROUP BY r.candidato
        """
        rows_esc = conn.execute(query_esc, (level,)).fetchall()
        
        # Counts
        count_trep = conn.execute("""
            SELECT COUNT(DISTINCT a.id) 
            FROM actas a 
            JOIN resumenes res ON res.acta_id = a.id 
            WHERE a.nivel = ? AND a.origen = 'TREP' AND res.gran_total > 0
        """, (level,)).fetchone()[0]
        
        count_esc = conn.execute("""
            SELECT COUNT(DISTINCT a.id) 
            FROM actas a 
            JOIN resultados r ON r.acta_id = a.id
            WHERE a.nivel = ? AND a.origen = 'ESCRUTINIO'
        """, (level,)).fetchone()[0]
        
        level_stats['trep']['actas'] = count_trep
        level_stats['trep']['actas'] = count_trep
        level_stats['esc']['actas'] = count_esc
        
        totals_validos = {'trep': 0, 'esc': 0}
        
        # Aggregate TREP
        for row in rows_trep:
            totals_validos['trep'] += row['total']
            name = row['candidato'].upper()
            votos = row['total']
            if "NACIONAL" in name: level_stats['trep']['nacional'] += votos
            elif "LIBERAL" in name: level_stats['trep']['liberal'] += votos
            elif "LIBRE" in name or "REFUNDACION" in name: level_stats['trep']['libre'] += votos
            else: level_stats['trep']['otros'] += votos
            
        # Aggregate ESC
        for row in rows_esc:
            totals_validos['esc'] += row['total']
            name = row['candidato'].upper()
            votos = row['total']
            if "NACIONAL" in name: level_stats['esc']['nacional'] += votos
            elif "LIBERAL" in name: level_stats['esc']['liberal'] += votos
            elif "LIBRE" in name or "REFUNDACION" in name: level_stats['esc']['libre'] += votos
            else: level_stats['esc']['otros'] += votos

        # Format for UI
        ui_stats = {'trep': {}, 'esc': {}}
        for tipo in ['trep', 'esc']:
            total_base = totals_validos[tipo]
            ui_stats[tipo]['total'] = total_base
            ui_stats[tipo]['actas'] = level_stats[tipo]['actas'] # Pass count
            
            for p in ['nacional', 'liberal', 'libre', 'otros']:
                votos = level_stats[tipo][p]
                pct = round((votos / total_base * 100), 1) if total_base > 0 else 0
                ui_stats[tipo][p] = {'votos': votos, 'pct': pct}
        
        final_stats[level] = ui_stats

    conn.close()
    return final_stats

def export_db_csv():
    conn = get_db_connection()
    output = io.StringIO()
    output.write(u'\ufeff')
    writer = csv.writer(output)
    writer.writerow(['JRV', 'ORIGEN', 'CANDIDATO_RUBRO', 'VOTOS'])
    query = """
    SELECT * FROM (
        SELECT a.jrv, a.origen, r.candidato as nombre, r.votos, 1 as orden
        FROM resultados r JOIN actas a ON r.acta_id = a.id
        UNION ALL
        SELECT a.jrv, a.origen, 'VOTOS BLANCOS' as nombre, res.votos_blancos as votos, 2 as orden
        FROM resumenes res JOIN actas a ON res.acta_id = a.id
        UNION ALL
        SELECT a.jrv, a.origen, 'VOTOS NULOS' as nombre, res.votos_nulos as votos, 3 as orden
        FROM resumenes res JOIN actas a ON res.acta_id = a.id
        UNION ALL
        SELECT a.jrv, a.origen, 'GRAN TOTAL' as nombre, res.gran_total as votos, 4 as orden
        FROM resumenes res JOIN actas a ON res.acta_id = a.id
    ) t
    ORDER BY CAST(jrv AS INTEGER), origen, orden, nombre
    """
    rows = conn.execute(query).fetchall()
    for row in rows:
        nombre = row['nombre'].split('(')[0].strip()
        if "NACIONAL DE HONDURAS" in nombre: nombre = "P. NACIONAL"
        elif "LIBERAL DE HONDURAS" in nombre: nombre = "P. LIBERAL"
        elif "LIBERTAD Y REFUNDACION" in nombre: nombre = "LIBRE"
        elif "DEMOCRATA CRISTIANO" in nombre: nombre = "DC"
        elif "INNOVACION Y UNIDAD" in nombre: nombre = "PINU"
        writer.writerow([row['jrv'], row['origen'], nombre, row['votos']])
    conn.close()
    return output.getvalue()

def update_result_vote(acta_id, candidato, votos):
    conn = get_db_connection()
    exists = conn.execute("SELECT id FROM resultados WHERE acta_id = ? AND candidato = ?", (acta_id, candidato)).fetchone()
    if exists: conn.execute("UPDATE resultados SET votos = ? WHERE id = ?", (votos, exists['id']))
    else: conn.execute("INSERT INTO resultados (acta_id, candidato, votos) VALUES (?, ?, ?)", (acta_id, candidato, votos))
    conn.commit(); conn.close(); recalculate_grand_total(acta_id)

def update_resumen_field(acta_id, field, value):
    conn = get_db_connection()
    if field not in ['votos_blancos', 'votos_nulos']: return
    exists = conn.execute("SELECT acta_id FROM resumenes WHERE acta_id = ?", (acta_id,)).fetchone()
    if not exists: conn.execute("INSERT INTO resumenes (acta_id, votos_validos, votos_blancos, votos_nulos, gran_total) VALUES (?, 0, 0, 0, 0)", (acta_id,))
    conn.execute(f"UPDATE resumenes SET {field} = ? WHERE acta_id = ?", (value, acta_id))
    conn.commit(); conn.close(); recalculate_grand_total(acta_id)

def recalculate_grand_total(acta_id):
    conn = get_db_connection()
    sum_candidatos = conn.execute("SELECT SUM(votos) FROM resultados WHERE acta_id = ?", (acta_id,)).fetchone()[0] or 0
    res = conn.execute("SELECT votos_blancos, votos_nulos FROM resumenes WHERE acta_id = ?", (acta_id,)).fetchone()
    blancos = res['votos_blancos'] if res else 0
    nulos = res['votos_nulos'] if res else 0
    conn.execute("UPDATE resumenes SET gran_total = ?, votos_validos = ? WHERE acta_id = ?", (sum_candidatos + blancos + nulos, sum_candidatos, acta_id))
    conn.commit(); conn.close()

def delete_result_row(acta_id, candidato):
    conn = get_db_connection()
    conn.execute("DELETE FROM resultados WHERE acta_id = ? AND candidato = ?", (acta_id, candidato))
    conn.commit(); conn.close(); recalculate_grand_total(acta_id)

def add_result_row(acta_id, candidato, votos):
    update_result_vote(acta_id, candidato, votos)

def delete_jrv_data(jrv):
    conn = get_db_connection()
    conn.execute("DELETE FROM actas WHERE jrv = ?", (jrv,)); conn.commit(); conn.close()

def validate_acta_trep(jrv, nivel='PRESIDENTE'):
    conn = get_db_connection()
    conn.execute("UPDATE actas SET estado = 'VALIDADO' WHERE jrv = ? AND origen = 'TREP' AND nivel = ?", (jrv, nivel)); conn.commit(); conn.close()

def get_summary_table(level='PRESIDENTE'):
    conn = get_db_connection()
    
    # Structure: { jrv: { party: { trep: 0, esc: 0, diff: 0 }, ... } }
    summary = {}
    
    # Get all JRVs for this level
    query_jrvs = "SELECT DISTINCT jrv FROM actas WHERE nivel = ? ORDER BY jrv"
    jrvs_rows = conn.execute(query_jrvs, (level,)).fetchall()
    # If no JRVs found, return empty
    if not jrvs_rows:
        return {'columns': [], 'data': []}
        
    jrvs = [row['jrv'] for row in jrvs_rows]
    
    # query results
    query = """
        SELECT a.jrv, a.origen, r.candidato, r.votos
        FROM actas a
        JOIN resultados r ON a.id = r.acta_id
        WHERE a.nivel = ?
    """
    rows = conn.execute(query, (level,)).fetchall()
    
    # query resumenes
    query_res = """
        SELECT a.jrv, a.origen, res.votos_blancos, res.votos_nulos
        FROM actas a
        JOIN resumenes res ON a.id = res.acta_id
        WHERE a.nivel = ?
    """
    rows_res = conn.execute(query_res, (level,)).fetchall()
    
    data_map = {} # jrv -> { party -> { trep: 0, esc: 0 } }

    def map_party_name(text):
        up = text.upper()
        if "NACIONAL" in up: return "P. NACIONAL"
        if "LIBERAL" in up: return "P. LIBERAL"
        if "LIBRE" in up or "REFUNDACION" in up: return "LIBRE"
        if "DC" in up: return "DC"
        if "PINU" in up: return "PINU"
        if "ALIANZA" in up: return "ALIANZA"
        if "SALVADOR" in up or "PSH" in up: return "PSH"
        # Special case for "resultados" if it still exists (should be cleaned, but safe guard)
        if "RESULTADOS" in up: return "OTROS"
        if "COLUMNS" in up: return "OTROS"
        return text # Return original if not matched (e.g. OTROS)

    for row in rows:
        jrv = row['jrv']
        origen = 'trep' if row['origen'] == 'TREP' else 'esc'
        canda = row['candidato'].upper()
        
        key = ""
        
        if level == 'DIPUTADOS':
             # Formats: "PARTIDO - DIP X", "PARTIDO (CANDIDATO)", "PARTIDO"
             if " - DIP" in canda:
                 key = canda.split(" - DIP")[0].strip()
             elif "(" in canda:
                 key = canda.split("(")[0].strip()
             else:
                 key = canda.strip()
             key = map_party_name(key)
             
        elif level == 'ALCALDE':
             # Formats: "PARTIDO", "PARTIDO (CANDIDATO)"
             if "(" in canda:
                 key = canda.split("(")[0].strip()
             else:
                 key = canda.strip()
             key = map_party_name(key)
             
        else: # PRESIDENTE
             # Use the map logic directly on candidate name
             key = map_party_name(canda)
             if key == canda and "VOTOS" not in key: key = "OTROS"

        if jrv not in data_map: data_map[jrv] = {}
        if key not in data_map[jrv]: data_map[jrv][key] = {'trep': 0, 'esc': 0}
        
        data_map[jrv][key][origen] += row['votos']

    # Process Resumenes
    for row in rows_res:
        jrv = row['jrv']
        origen = 'trep' if row['origen'] == 'TREP' else 'esc'
        
        if jrv not in data_map: data_map[jrv] = {}
        
        if 'VOTOS BLANCOS' not in data_map[jrv]: data_map[jrv]['VOTOS BLANCOS'] = {'trep': 0, 'esc': 0}
        data_map[jrv]['VOTOS BLANCOS'][origen] = row['votos_blancos']
        
        if 'VOTOS NULOS' not in data_map[jrv]: data_map[jrv]['VOTOS NULOS'] = {'trep': 0, 'esc': 0}
        data_map[jrv]['VOTOS NULOS'][origen] = row['votos_nulos']

    # Columns
    all_parties = set()
    for jrv_data in data_map.values():
        all_parties.update(jrv_data.keys())
        
    priority = ["P. NACIONAL", "P. LIBERAL", "LIBRE", "DC", "PINU", "PSH", "ALIANZA", "OTROS", "VOTOS BLANCOS", "VOTOS NULOS"]
    sorted_parties = sorted(list(all_parties), key=lambda x: priority.index(x) if x in priority else 99)
    
    final_list = []
    # Ensure all JRVs exist even if no results (though jrvs list came from actas so they exist)
    # Use jrvs list from first query to ensure order
    # Sort JRVs numerically
    sorted_jrvs = sorted(list(data_map.keys()), key=lambda x: int(x) if str(x).isdigit() else x)
    
    for jrv in sorted_jrvs:
        row = {'jrv': jrv, 'results': []}
        for p in sorted_parties:
            d = data_map[jrv].get(p, {'trep': 0, 'esc': 0})
            diff = d['esc'] - d['trep']
            row['results'].append({
                'party': p,
                'trep': d['trep'],
                'esc': d['esc'],
                'diff': diff
            })
        final_list.append(row)
        
    return {'columns': sorted_parties, 'data': final_list}

def register_manual_upload(jrv, nivel, source, filepath):
    conn = get_db_connection()
    # Map Source to Origen
    origen = 'TREP' if source == 'TREP' else 'ESCRUTINIO'
    
    # Check if exists
    row = conn.execute("SELECT id FROM actas WHERE jrv = ? AND nivel = ? AND origen = ?", (jrv, nivel, origen)).fetchone()
    
    if row:
        conn.execute("UPDATE actas SET filepath = ?, estado = 'MANUAL' WHERE id = ?", (filepath, row['id']))
    else:
        conn.execute("INSERT INTO actas (jrv, nivel, origen, filepath, estado) VALUES (?, ?, ?, ?, 'MANUAL')", (jrv, nivel, origen, filepath))
    
    conn.commit()
    conn.close()

def delete_acta_record(jrv, nivel, source):
    conn = get_db_connection()
    origen = 'TREP' if source == 'TREP' else 'ESCRUTINIO'
    
    # Get filepath via ID to be safe
    row = conn.execute("SELECT id, filepath FROM actas WHERE jrv = ? AND nivel = ? AND origen = ?", (jrv, nivel, origen)).fetchone()
    filepath = None
    
    if row:
        acta_id = row['id']
        filepath = row['filepath']
        
        # Delete dependencies
        conn.execute("DELETE FROM resultados WHERE acta_id = ?", (acta_id,))
        conn.execute("DELETE FROM resumenes WHERE acta_id = ?", (acta_id,))
        conn.execute("DELETE FROM actas WHERE id = ?", (acta_id,))
        conn.commit()
    
    conn.close()
    return filepath