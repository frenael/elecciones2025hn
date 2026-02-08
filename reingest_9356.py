from processor import load_json_data
from db import save_acta_result

jrv = '9356'
print(f"Re-ingesting JRV {jrv}...")

# Files based on listing
trep_file = f"{jrv}-DIPUTADOS-FRENAEL.json"
esc_file = f"{jrv}-DIPUTADOS.json"

print(f"Loading TREP: {trep_file}")
data_trep = load_json_data(trep_file, 'DIPUTADOS_FRENAEL')
if data_trep:
    print("Saving TREP...")
    # Passing empty path as we just want to update votes
    save_acta_result(jrv, 'TREP', '', data_trep, nivel='DIPUTADOS')

print(f"Loading ESC: {esc_file}")
data_esc = load_json_data(esc_file, 'DIPUTADOS_OFICIAL')
if data_esc:
    print("Saving ESC...")
    save_acta_result(jrv, 'ESCRUTINIO', '', data_esc, nivel='DIPUTADOS')

print("Done.")
