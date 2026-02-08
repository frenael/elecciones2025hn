import os
import json
import logging
import re
from db import save_acta_result, check_acta_exists, update_acta_path

logging.basicConfig(
    filename='frenael_debug.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER_TREP = os.path.join(BASE_DIR, 'data', 'ACTAS', 'FRENAEL')
FOLDER_ESCRUTINIO = os.path.join(BASE_DIR, 'data', 'ACTAS', 'OFICIAL')
FOLDER_JSON_ESC = os.path.join(BASE_DIR, 'data', 'JSON')
FOLDER_DIP_FRENAEL = os.path.join(BASE_DIR, 'data', 'json_diputados', 'processed')

def get_jrv_from_filename(filename):
    match = re.search(r'\d+', filename)
    return match.group(0) if match else None

def scan_folders():
    for f in [FOLDER_TREP, FOLDER_ESCRUTINIO, FOLDER_JSON_ESC, FOLDER_DIP_FRENAEL]:
        os.makedirs(f, exist_ok=True)
    
    inventory = {}
    
    # 1. Scan Standard JSON Folder
    if os.path.exists(FOLDER_JSON_ESC):
        for f in os.listdir(FOLDER_JSON_ESC):
            j = get_jrv_from_filename(f)
            if not j: continue
            if j not in inventory: inventory[j] = {}
            
            # Clasificar archivo JSON
            if f.endswith('-PRESIDENTE.json'):
                inventory[j]['json_pres_oficial'] = f
            elif f.endswith('-ALCALDE.json'):
                inventory[j]['json_alc_oficial'] = f
            elif f.endswith('-DIPUTADOS.json'):
                inventory[j]['json_dip_oficial'] = f
            elif f.endswith('-ALCALDE-FRENAEL.json'):
                inventory[j]['json_alc_frenael'] = f
            elif f.endswith('-DIPUTADOS-FRENAEL.json'):
                inventory[j]['json_dip_frenael_legacy'] = f

    # 2. Scan Special Diputados Folder (p_processed_XXXX.json)
    if os.path.exists(FOLDER_DIP_FRENAEL):
        for f in os.listdir(FOLDER_DIP_FRENAEL):
            if f.startswith('p_processed_') and f.endswith('.json'):
                j = get_jrv_from_filename(f)
                if not j: continue
                if j not in inventory: inventory[j] = {}
                # Set as the PRIMARY source for dip_frenael
                inventory[j]['json_dip_frenael'] = os.path.join(FOLDER_DIP_FRENAEL, f)

    valid_exts = ('.jpg', '.jpeg', '.png', '.pdf')
    for folder, source_key in [(FOLDER_TREP, 'trep'), (FOLDER_ESCRUTINIO, 'esc')]:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if not f.lower().endswith(valid_exts): continue
                
                j = get_jrv_from_filename(f)
                if not j: continue
                
                if j not in inventory: inventory[j] = {}
                
                U = f.upper()
                if 'PRESIDENTE' in U:
                    inventory[j][source_key] = f
                elif 'ALCALDE' in U:
                    inventory[j][f'{source_key}_alc'] = f
                elif 'DIPUTADOS' in U:
                    inventory[j][f'{source_key}_dip'] = f

    return inventory

# --- NUEVA FUNCIÓN: REFRESCAR RUTAS ---
def refresh_file_paths():
    """
    Escanea las carpetas de imágenes y actualiza la BD con los nombres reales.
    """
    inventory = scan_folders()
    updated_count = 0
    
    for jrv, files in inventory.items():
        # Actualizar TREP
        if 'trep' in files:
            if update_acta_path(jrv, 'TREP', f"data/ACTAS/FRENAEL/{files['trep']}", nivel='PRESIDENTE'): updated_count += 1
        if 'trep_alc' in files:
            if update_acta_path(jrv, 'TREP', f"data/ACTAS/FRENAEL/{files['trep_alc']}", nivel='ALCALDE'): updated_count += 1
        if 'trep_dip' in files:
            if update_acta_path(jrv, 'TREP', f"data/ACTAS/FRENAEL/{files['trep_dip']}", nivel='DIPUTADOS'): updated_count += 1
        
        # Actualizar ESCRUTINIO
        if 'esc' in files:
            if update_acta_path(jrv, 'ESCRUTINIO', f"data/ACTAS/OFICIAL/{files['esc']}", nivel='PRESIDENTE'): updated_count += 1
        if 'esc_alc' in files:
            if update_acta_path(jrv, 'ESCRUTINIO', f"data/ACTAS/OFICIAL/{files['esc_alc']}", nivel='ALCALDE'): updated_count += 1
        if 'esc_dip' in files:
            if update_acta_path(jrv, 'ESCRUTINIO', f"data/ACTAS/OFICIAL/{files['esc_dip']}", nivel='DIPUTADOS'): updated_count += 1
                
    return updated_count

def load_json_data(filename_or_path, source_type):
    # Determine full path: if it has directory separator, use as is; else join with JSON dir
    if os.path.sep in filename_or_path:
        path = filename_or_path
    else:
        path = os.path.join(FOLDER_JSON_ESC, filename_or_path)

    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        flat_results = {}
        
        # Check if it's the simple Diputados format (Party -> List)
        # It's a dict where values are lists of strings
        is_simple_list_format = False
        if isinstance(data, dict):
            # rudimentary check: first value is a list?
            first_val = next(iter(data.values())) if data else None
            if isinstance(first_val, list):
               is_simple_list_format = True
        
        if is_simple_list_format:
            # Parse { "Party": ["v1", "v2"...] }
            for party, votes_list in data.items():
                if not isinstance(votes_list, list): continue
                # Normalize party name if needed, but db.py handles normalization
                # Just produce "Party - DIP 1", "Party - DIP 2"
                for idx, vote_val in enumerate(votes_list, start=1):
                    try: val = int(vote_val)
                    except: val = 0
                    key = f"{party} - DIP {idx}"
                    flat_results[key] = val
            
            # Manually construct return object
            return {
                'resultados': flat_results,
                'resumen': {'votos_validos': 0, 'votos_blancos': 0, 'votos_nulos': 0, 'gran_total': 0},
                'raw_matrix': [],
                'year': '2025'
            }
        
        # Original Logic for Full JSONs
        resumen = {'votos_validos': 0, 'votos_nulos': 0, 'votos_blancos': 0, 'gran_total': 0}

        # Parsing logic based on source type
        if source_type in ['PRESIDENTE_OFICIAL', 'ALCALDE_OFICIAL', 'DIPUTADOS_OFICIAL']:
            # Estructura Oficial CNE (Resultados + Estadisticas)
            for item in data.get('resultados', []):
                partido = item.get('partido', 'UNKNOWN')
                candidato = item.get('candidato', '') # A veces vacio para diputados
                key = f"{partido}"
                if candidato: key += f" ({candidato})"
                
                # Limpiar votos (pueden venir con comas "1,234")
                v_str = str(item.get('votos', '0')).replace(',', '')
                try: v_int = int(v_str)
                except: v_int = 0
                
                flat_results[key] = v_int

            stats = data.get('estadisticas', {}).get('distribucion_votos', {})
            def clean_int(val):
                try: return int(str(val).replace(',', ''))
                except: return 0
                
            resumen = {
                'votos_validos': clean_int(stats.get('validos', 0)),
                'votos_nulos': clean_int(stats.get('nulos', 0)),
                'votos_blancos': clean_int(stats.get('blancos', 0)),
                'gran_total': clean_int(stats.get('validos', 0)) + clean_int(stats.get('nulos', 0)) + clean_int(stats.get('blancos', 0))
            }
            
        elif source_type == 'ALCALDE_FRENAEL':
            # Estructura Plana Frenael (Keys directos)
            for k, v in data.items():
                if k in ['actasRecibidas', 'actasNoUtilizadas', 'actasUtilizadas', 'ciudadanosVotaron', 'miembrosJRV', 'totalVotantes']: continue
                if k in ['votosBlanco', 'votosNulos', 'granTotal']:
                   pass # Handle later
                else:
                    # Es un partido
                    try: flat_results[k] = int(str(v).replace(',', ''))
                    except: flat_results[k] = 0
            
            resumen['votos_blancos'] = int(str(data.get('votosBlanco', 0)).replace(',', ''))
            resumen['votos_nulos'] = int(str(data.get('votosNulos', 0)).replace(',', ''))
            # granTotal a veces es 0 en el JSON, calcularlo
            validos = sum(flat_results.values())
            resumen['votos_validos'] = validos
            resumen['gran_total'] = validos + resumen['votos_blancos'] + resumen['votos_nulos']

        elif source_type == 'DIPUTADOS_FRENAEL':
            # Estructura Arrays Frenael {"Nacional": ["1", "2"...]}
            for partido, votos_array in data.items():
                if not isinstance(votos_array, list): continue
                # Guardar cada candidato individualmente: "Nacional - 1", "Nacional - 2"
                for idx, v_val in enumerate(votos_array):
                    key = f"{partido} - DIP {idx+1}"
                    # Handling "no se puede leer" or numbers
                    try: 
                        clean_v = str(v_val).replace(',', '')
                        if not clean_v.isdigit(): val = 0
                        else: val = int(clean_v)
                    except: val = 0
                    flat_results[key] = val
            # No summary info in this JSON usually?
            # Si no hay resumen, calculamos sumas
            validos = sum(flat_results.values())
            resumen['votos_validos'] = validos
            # No hay blancos/nulos en este JSON específico visto en el ejemplo?
            # El ejemplo array no mostraba resumen. Asumiremos 0 por ahora.

        return { "year": "2025", "resultados": flat_results, "resumen": resumen }
    except Exception as e:
        logging.error(f"Error JSON {filename_or_path}: {e}")
        return None

def process_batch_generator(dummy1, dummy2): 
    inventory = scan_folders()
    yield "data: Iniciando Carga MultNivel...\n\n"
    
    count_processed = 0
    for jrv, files in inventory.items():
        # --- PROCESO PRESIDENTE (Legacy Logic) ---
        if 'json_pres_oficial' in files:
            data_pkg = load_json_data(files['json_pres_oficial'], 'PRESIDENTE_OFICIAL')
            if data_pkg:
                esc_path = f"data/ACTAS/OFICIAL/{files['esc']}" if 'esc' in files else ""
                save_acta_result(jrv, 'ESCRUTINIO', esc_path, data_pkg, nivel='PRESIDENTE')
                
                # TREP para Pres se inicializa con copia de Oficial si no existe
                if not check_acta_exists(jrv, 'TREP', nivel='PRESIDENTE'):
                    trep_path = f"data/ACTAS/FRENAEL/{files['trep']}" if 'trep' in files else ""
                    save_acta_result(jrv, 'TREP', trep_path, data_pkg, nivel='PRESIDENTE')
                    count_processed += 1
        
        # --- PROCESO ALCALDE ---
        if 'json_alc_oficial' in files:
            data_pkg = load_json_data(files['json_alc_oficial'], 'ALCALDE_OFICIAL')
            if data_pkg:
                path = f"data/ACTAS/OFICIAL/{files['esc_alc']}" if 'esc_alc' in files else ""
                save_acta_result(jrv, 'ESCRUTINIO', path, data_pkg, nivel='ALCALDE')
        
        if 'json_alc_frenael' in files:
            data_pkg = load_json_data(files['json_alc_frenael'], 'ALCALDE_FRENAEL')
            if data_pkg:
                path = f"data/ACTAS/FRENAEL/{files['trep_alc']}" if 'trep_alc' in files else ""
                save_acta_result(jrv, 'TREP', path, data_pkg, nivel='ALCALDE')

        # --- PROCESO DIPUTADOS ---
        if 'json_dip_oficial' in files:
            data_pkg = load_json_data(files['json_dip_oficial'], 'DIPUTADOS_OFICIAL')
            if data_pkg:
                path = f"data/ACTAS/OFICIAL/{files['esc_dip']}" if 'esc_dip' in files else ""
                save_acta_result(jrv, 'ESCRUTINIO', path, data_pkg, nivel='DIPUTADOS')
        
        if 'json_dip_frenael' in files:
            data_pkg = load_json_data(files['json_dip_frenael'], 'DIPUTADOS_FRENAEL')
            if data_pkg:
                path = f"data/ACTAS/FRENAEL/{files['trep_dip']}" if 'trep_dip' in files else ""
                save_acta_result(jrv, 'TREP', path, data_pkg, nivel='DIPUTADOS')

        yield f"data: Probando JRV {jrv}...\n\n"
        
    yield f"data: ACTUALIZACION COMPLETADA.\n\n"