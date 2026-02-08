
def get_level_summary(level='PRESIDENTE'):
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
             # Wait, if map_party_name returns original, it might be "P. DEMOCRATA CRISTIANO" (starts with P.)
             # Logic above for DC checks "DC".
             # If exact match fails, fallback to OTROS?
             # But "VOTOS BLANCOS" logic is separate.
             # Let's trust map_party_name behaves. "OTROS" as fallback if truly unknown.

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
    # Filter out columns that are clearly candidate names if any slipped through? no, unlikely.
    
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
