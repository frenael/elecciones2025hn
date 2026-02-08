
import csv
import json
import os

CSV_PATH = r"C:\web_antigravity\actas_frenal_comparacion\data\actas_cna_frenael_v2\base_datos_votos_diputados.csv"
JSON_PATH = r"C:\web_antigravity\actas_frenal_comparacion\data\diputados_oficial.json"

def process_csv():
    data = {}
    
    with open(CSV_PATH, 'r', encoding='ISO-8859-1') as f: # Likely Latin-1 due to Spanish chars
        reader = csv.DictReader(f)
        for row in reader:
            jrv = row['JRV'].strip()
            party = row['Partido'].strip()
            num_cand = row['Num_Candidato'].strip()
            name = row['Nombre_Candidato'].strip()
            votes = row['Votos'].strip()
            
            # Skip invalid rows or headers inside data
            if not jrv.isdigit(): continue
            if "esca" in num_cand: continue 
            
            try:
                votes = int(float(votes))
                num_cand = int(float(num_cand))
            except ValueError:
                continue

            # Normalize Party Names to match system (Simple mapping)
            # System uses: NACIONAL, LIBERAL, LIBRE, PSH, DC, PINU, etc.
            # CSV uses: PARTIDO DEMOCRATA CRISTIANO DE HONDURAS
            
            party_map = {
                "PARTIDO DEMOCRATA CRISTIANO DE HONDURAS": "DC",
                "PARTIDO NACIONAL DE HONDURAS": "NACIONAL",
                "PARTIDO LIBERAL DE HONDURAS": "LIBERAL",
                "PARTIDO LIBERTAD Y REFUNDACION": "LIBRE",
                "PARTIDO INNOVACION Y UNIDAD": "PINU",
                "PARTIDO SALVADOR DE HONDURAS": "PSH"
                # Add others if needed
            }
            
            short_party = party_map.get(party, party)
            
            if jrv not in data: data[jrv] = {}
            if short_party not in data[jrv]: data[jrv][short_party] = {}
            
            data[jrv][short_party][num_cand] = {
                "name": name,
                "votes": votes
            }
            
    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Processed {len(data)} JRVs into {JSON_PATH}")

if __name__ == "__main__":
    process_csv()
