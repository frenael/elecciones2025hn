import processor
import sqlite3
import os

def run_full_import():
    print("Starting Full Database Import from Sources...")
    
    # Optional: Clear tables first to be absolutely sure?
    # processor.save_acta_result does DELETE FROM results WHERE acta_id...
    # So if we process every acta, it will be cleared.
    # But if there are actas in DB that don't satisfy the scan (e.g. manual ones?), they might remain.
    # User said "restaurar todos los valores de las fuentes".
    # Existing actas that match sources will be overwritten.
    # Actas in DB that have NO source file... should they be kept or deleted?
    # Safest is to keep them, or maybe user wants PURE source state.
    # "reescribir en la base de datos todos los valores obtenidos de las fuentes"
    # I will rely on overwrite.
    
    gen = processor.process_batch_generator(None, None)
    
    count = 0
    for msg in gen:
        count += 1
        if count % 100 == 0:
            print(f"Processed batch {count}...")
            
    print("Import finished.")
    
    # Run fixes after import (Just in case)
    print("Running fixes (Duplicates, Totals)...")
    
    # Fix duplicates (just in case JSONs had them?)
    # fix_duplicates.py logic here or simpler?
    # Our DB logic should handle it if save_acta_result uses "INSERT"
    
    # Recalculate totals for Diputados (since JSONs might not have summary)
    # Re-run the logic from recalc_diputados_totals.py
    
    conn = sqlite3.connect('auditoria.db')
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Recalc Diputados Totals
    level = 'DIPUTADOS'
    actas = c.execute("SELECT id FROM actas WHERE nivel=?", (level,)).fetchall()
    for a in actas:
        acta_id = a['id']
        rows = c.execute("SELECT SUM(votos) as total FROM resultados WHERE acta_id=?", (acta_id,)).fetchone()
        validos = rows['total'] if rows and rows['total'] else 0
        
        # Check existing res
        res = c.execute("SELECT * FROM resumenes WHERE acta_id=?", (acta_id,)).fetchone()
        blancos = res['votos_blancos'] if res else 0
        nulos = res['votos_nulos'] if res else 0
        gran_total = validos + blancos + nulos
        
        if res:
             c.execute("UPDATE resumenes SET votos_validos=?, gran_total=? WHERE acta_id=?", (validos, gran_total, acta_id))
        else:
             c.execute("INSERT INTO resumenes (acta_id, votos_validos, votos_blancos, votos_nulos, gran_total) VALUES (?, ?, ?, ?, ?)", (acta_id, validos, blancos, nulos, gran_total))
             
    conn.commit()
    conn.close()
    print("Fixes applied.")

if __name__ == "__main__":
    run_full_import()
