import os
import shutil
import sys

# --- CONFIGURACIÓN DE RUTAS ---
# Usamos r'' para indicar "raw strings" y evitar problemas con las barras invertidas de Windows

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 1. Carpeta que sirve de LISTA (La que dicta QUÉ actas buscar)
DIR_REFERENCIA_TREP = os.path.join(BASE_DIR, 'data', 'ACTAS', 'FRENAEL')

# 2. Carpeta FUENTE (Donde están las 19,000+ imágenes)
DIR_ORIGEN_MASIVO = r"C:\@actas_elecciones_2025\actas_cna_img"

# 3. Carpeta DESTINO (Donde se pegarán las copias encontradas)
DIR_DESTINO = os.path.join(BASE_DIR, 'data', 'ACTAS', 'OFICIAL')

def procesar_copiado():
    # 1. Validaciones de seguridad
    if not os.path.exists(DIR_REFERENCIA_TREP):
        print(f"ERROR CRÍTICO: No existe la carpeta de referencia:\n{DIR_REFERENCIA_TREP}")
        return

    if not os.path.exists(DIR_ORIGEN_MASIVO):
        print(f"ERROR CRÍTICO: No existe la carpeta origen con las 19k imágenes:\n{DIR_ORIGEN_MASIVO}")
        return

    # Crear carpeta destino si no existe
    if not os.path.exists(DIR_DESTINO):
        try:
            os.makedirs(DIR_DESTINO)
            print(f"Se creó la carpeta destino: {DIR_DESTINO}")
        except Exception as e:
            print(f"Error al crear carpeta destino: {e}")
            return

    print("--- Iniciando proceso de búsqueda y copiado ---")
    
    # 2. Obtener la lista de nombres de archivo de la carpeta TREP
    # Filtramos para que sean solo archivos (ignorando carpetas internas si las hubiera)
    try:
        lista_archivos_requeridos = [
            f for f in os.listdir(DIR_REFERENCIA_TREP) 
            if os.path.isfile(os.path.join(DIR_REFERENCIA_TREP, f)) and 'PRESIDENTE' in f.upper()
        ]
    except Exception as e:
        print(f"Error leyendo la carpeta de referencia: {e}")
        return

    total_requeridos = len(lista_archivos_requeridos)
    print(f"Se buscarán {total_requeridos} actas basadas en la carpeta 'actas_trep_jpg'.")

    copiados = 0
    no_encontrados = 0

    # 3. Iterar y Copiar
    for archivo in lista_archivos_requeridos:
        
        # Construimos la ruta completa donde DEBERÍA estar el archivo en la carpeta masiva
        ruta_origen = os.path.join(DIR_ORIGEN_MASIVO, archivo)
        ruta_destino = os.path.join(DIR_DESTINO, archivo)

        if os.path.exists(ruta_origen):
            try:
                # copy2 preserva metadatos (fechas de creación, etc.)
                shutil.copy2(ruta_origen, ruta_destino)
                copiados += 1
                
                # Barra de progreso simple cada 100 archivos
                if copiados % 100 == 0:
                    print(f"Procesando... {copiados} actas copiadas.")
                    
            except Exception as e:
                print(f"Error al copiar {archivo}: {e}")
        else:
            no_encontrados += 1
            # Opcional: Descomenta la línea de abajo si quieres ver cuáles faltan en pantalla
            # print(f"FALTANTE: {archivo} no está en la carpeta origen.")

    # 4. Resumen final
    print("-" * 30)
    print("RESUMEN DEL PROCESO:")
    print(f"Total buscados (Ref): {total_requeridos}")
    print(f"Éxito (Copiados):     {copiados}")
    print(f"Faltantes (No hallados): {no_encontrados}")
    print("-" * 30)
    print(f"Verifica los archivos en: {DIR_DESTINO}")

if __name__ == '__main__':
    procesar_copiado()