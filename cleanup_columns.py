
import db

def clean_columns_data():
    conn = db.get_db_connection()
    
    # Remove rows where candidato starts with "columns"
    print("Deleting invalid 'columns' rows...")
    conn.execute("DELETE FROM resultados WHERE candidato LIKE 'columns%'")
    
    conn.commit()
    conn.close()
    print("Cleanup COLUMNS done.")
    
if __name__ == "__main__":
    clean_columns_data()
