
import db

def clean_data():
    conn = db.get_db_connection()
    
    # 1. Remove rows where candidatostarts with "resultados"
    print("Deleting invalid 'resultados' rows...")
    conn.execute("DELETE FROM resultados WHERE candidato LIKE 'resultados%'")
    
    # 2. Check for DIP candidates in ALCALDE level
    print("Deleting DIP candidates from ALCALDE level...")
    # Get IDs of ALCALDE actas
    conn.execute("DELETE FROM resultados WHERE acta_id IN (SELECT id FROM actas WHERE nivel='ALCALDE') AND candidato LIKE '%- DIP%'")
    
    conn.commit()
    conn.close()
    print("Cleanup done.")
    
if __name__ == "__main__":
    clean_data()
