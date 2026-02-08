from db import get_db_connection
import os

print("--- DEBUGGING ALCALDE IMAGES ---")
conn = get_db_connection()

# Get a few examples of ALCALDE actas
rows = conn.execute("SELECT id, jrv, origen, filepath FROM actas WHERE nivel = 'ALCALDE' LIMIT 5").fetchall()

if not rows:
    print("No records found for nivel='ALCALDE'.")
else:
    for row in rows:
        jrv, origen, path = row['jrv'], row['origen'], row['filepath']
        print(f"JRV: {jrv}, Origen: {origen}")
        print(f"  DB Path: {path}")
        
        # Check if file exists. 
        # The app serves /<path>, which usually maps to current_dir/data/<path> or just <path> if it starts with data/
        # Let's check relative to CWD and inside data/
        
        real_path_1 = os.path.abspath(path)
        real_path_2 = os.path.abspath(os.path.join('data', path))
        
        exists_1 = os.path.exists(path)
        exists_2 = os.path.exists(os.path.join('data', path))
        
        print(f"  Exists (direct): {exists_1} -> {real_path_1}")
        print(f"  Exists (in data/): {exists_2} -> {real_path_2}")

conn.close()
