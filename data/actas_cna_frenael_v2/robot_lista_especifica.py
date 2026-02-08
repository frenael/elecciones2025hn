import os
import time
import csv
import base64
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==========================================
# CONFIGURACIÓN DE ACTIVACIÓN DE NIVELES
# ==========================================
DESCARGAR_PRESIDENCIAL = True
DESCARGAR_DIPUTADOS    = True
DESCARGAR_ALCALDE      = True

# ==========================================
# CONFIGURACIÓN: MODO SOLO USA
# ==========================================
# Si True: Ignora departamentos de Honduras y solo descarga JRV > 19152
MODO_SOLO_USA = True  

# Nombre exacto que sale en el menú desplegable de la web.
# Opciones comunes: "USA", "ESTADOS UNIDOS", "VOTO EXTERIOR"
NOMBRE_DEPARTAMENTO_USA = "USA" 

# Rango donde inician las actas de USA
INICIO_USA = 19153
FIN_USA = 25000 

# ==========================================
# LISTA MAESTRA DE OBJETIVOS
# ==========================================
LISTA_JRVS_OBJETIVO = [
    35, 38, 39, 40, 41, 148, 240, 380, 391, 443, 941, 958, 1183, 1184, 1419, 1422, 1520, 
    2407, 2500, 3046, 3459, 3736, 3747, 3752, 3753, 3754, 3764, 3850, 3914, 3917, 3919, 
    3978, 3980, 4330, 4389, 4828, 5325, 5350, 5373, 5426, 5441, 5442, 5445, 5446, 5448, 
    5449, 5450, 5451, 5452, 5453, 5454, 5455, 5456, 5457, 5542, 5605, 5783, 5785, 5928, 
    6309, 6433, 7012, 7041, 7042, 7047, 7061, 7107, 7217, 7657, 7966, 8716, 9356, 9357, 
    9551, 9700, 9890, 9930, 9947, 9966, 9984, 10013, 10067, 10103, 10151, 10314, 10354, 
    10538, 10539, 10572, 10629, 10645, 10663, 10666, 10708, 10709, 10735, 10873, 10936, 
    10953, 11049, 11058, 11060, 11272, 11347, 11598, 12318, 12319, 12476, 12504, 12533, 
    13006, 13324, 13340, 13341, 13393, 13395, 13404, 13480, 13492, 13907, 15528, 15740, 
    16628, 16740, 16830, 17385, 18177, 18192, 18268, 18543, 18572, 19146, 19156, 19157, 
    19159, 19163
]

# ==========================================
# CONFIGURACIÓN GENERAL
# ==========================================
CARPETA_BASE = r"C:\actas_cna_frenael_v2"
ARCHIVO_LOG_INCIDENCIAS = os.path.join(CARPETA_BASE, "reporte_incidencias_usa.csv")
ARCHIVO_DB_VOTOS = os.path.join(CARPETA_BASE, "base_datos_votos_diputados.csv")
URL = "https://web.sie.evoting.com/"

MODO_FANTASMA = True 

NIVELES = {}
if DESCARGAR_PRESIDENCIAL:
    NIVELES["Presidencial"] = "PRESIDENTE"
if DESCARGAR_DIPUTADOS:
    NIVELES["Diputados"] = "DIPUTADOS"
if DESCARGAR_ALCALDE:
    NIVELES["Corporación Municipal"] = "ALCALDE"

# RANGOS (Solo se usan si MODO_SOLO_USA es False)
RANGOS_DEPARTAMENTOS = {
    "ATLANTIDA": (1, 939),
    "COLON": (940, 1623),
    "COMAYAGUA": (1624, 2764),
    "COPAN": (2765, 3640),
    "CORTES": (3641, 6972),
    "CHOLUTECA": (6973, 8073),
    "EL PARAISO": (8074, 9171),
    "FRANCISCO MORAZAN": (9172, 12573),
    "GRACIAS A DIOS": (12574, 12750),
    "INTIBUCA": (12751, 13321),
    "ISLAS DE LA BAHIA": (13322, 13476),
    "LA PAZ": (13477, 13967),
    "LEMPIRA": (13968, 14763),
    "OCOTEPEQUE": (14764, 15128),
    "OLANCHO": (15129, 16310),
    "SANTA BARBARA": (16311, 17406),
    "VALLE": (17407, 17864),
    "YORO": (17865, 19152)
}

# --- SELECTORES ---
SEL_DEPARTAMENTO = "departamento"
SEL_JRV = "jrv"
XPATH_BTN_DESCARGAR = "//a[contains(., 'Descargar Acta')]" 
XPATH_PORCENTAJE = "//h6[contains(., '%')]"
XPATH_CHIP_VALIDA = "//span[contains(., 'Acta incluida en el cómputo')]"
XPATH_CHIP_NO_COMPUTADA = "//span[contains(., 'Acta no incluida en el cómputo')]"

# --- CLASES CSS SCRAPING ---
CLASS_PARTIDO_O_TITULO = "css-1c2p0c"
CLASS_NUM_CANDIDATO = "css-oruufx"
CLASS_NOM_CANDIDATO = "css-d6k7zy"
CLASS_VOTOS_MARCAS = "css-5z6l66"

def obtener_departamento_por_jrv(numero_jrv):
    # Si estamos en modo SOLO USA, verificamos primero si pertenece a USA
    if numero_jrv >= INICIO_USA:
        return NOMBRE_DEPARTAMENTO_USA
    
    # Si está activado SOLO USA, ignoramos el resto de departamentos
    if MODO_SOLO_USA:
        return None 
        
    # Búsqueda normal (solo si MODO_SOLO_USA es False)
    for depto, (inicio, fin) in RANGOS_DEPARTAMENTOS.items():
        if inicio <= numero_jrv <= fin:
            return depto
            
    return None

def registrar_evento(depto, jrv, nivel, estado, detalle):
    if not os.path.exists(CARPETA_BASE):
        try: os.makedirs(CARPETA_BASE)
        except: pass
        
    existe = os.path.isfile(ARCHIVO_LOG_INCIDENCIAS)
    try:
        with open(ARCHIVO_LOG_INCIDENCIAS, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            if not existe:
                writer.writerow(["Fecha_Hora", "Departamento", "JRV", "Nivel", "Estado", "Detalle"])
            fecha_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([fecha_hora, depto, jrv, nivel, estado, detalle])
    except Exception as e:
        print(f" [ERROR LOG] {e}")

def registrar_voto_diputado(depto, jrv, partido, num_candidato, nombre, votos):
    if not os.path.exists(CARPETA_BASE):
        try: os.makedirs(CARPETA_BASE)
        except: pass

    existe = os.path.isfile(ARCHIVO_DB_VOTOS)
    try:
        with open(ARCHIVO_DB_VOTOS, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            if not existe:
                writer.writerow(["Departamento", "JRV", "Partido", "Num_Candidato", "Nombre_Candidato", "Votos"])
            
            votos_limpio = str(votos).lower().replace("marcas", "").replace("marca", "").strip()
            num_limpio = str(num_candidato).replace("#", "").strip()
            writer.writerow([depto, jrv, partido, num_limpio, nombre, votos_limpio])
    except Exception as e:
        print(f" [ERROR DB VOTOS] {e}")

def click_js(driver, elemento):
    driver.execute_script("arguments[0].click();", elemento)

def preparar_carpetas():
    if not os.path.exists(CARPETA_BASE):
        os.makedirs(CARPETA_BASE)
    for nombre_carpeta in NIVELES.values():
        os.makedirs(os.path.join(CARPETA_BASE, nombre_carpeta, "pdf"), exist_ok=True)
        os.makedirs(os.path.join(CARPETA_BASE, nombre_carpeta, "no_computadas"), exist_ok=True)

def extraer_datos_diputados(driver, depto, jrv):
    print("         > Extrayendo votos...", end="")
    try:
        xpath_busqueda = f"//*[contains(@class, '{CLASS_PARTIDO_O_TITULO}') or contains(@class, '{CLASS_NUM_CANDIDATO}')]"
        elementos = driver.find_elements(By.XPATH, xpath_busqueda)
        partido_actual = "DESCONOCIDO"
        datos_recolectados = 0
        
        for i, elem in enumerate(elementos):
            texto = elem.text.strip()
            clase = elem.get_attribute("class")
            
            if CLASS_PARTIDO_O_TITULO in clase:
                if "Votos en blanco" in texto:
                    partido_actual = "BLANCOS"
                    try:
                        votos_elem = elem.find_element(By.XPATH, "following::h6[1]")
                        registrar_voto_diputado(depto, jrv, "ESPECIALES", "BLANCO", "VOTO BLANCO", votos_elem.text)
                        datos_recolectados += 1
                    except: pass
                elif "Votos nulos" in texto:
                    partido_actual = "NULOS"
                    try:
                        votos_elem = elem.find_element(By.XPATH, "following::h6[1]")
                        registrar_voto_diputado(depto, jrv, "ESPECIALES", "NULO", "VOTO NULO", votos_elem.text)
                        datos_recolectados += 1
                    except: pass
                elif "marcas" in texto: continue
                else: partido_actual = texto
            
            elif CLASS_NUM_CANDIDATO in clase:
                num_candidato = texto
                try:
                    xpath_nombre = f".//following::h6[contains(@class, '{CLASS_NOM_CANDIDATO}')][1]"
                    xpath_votos = f".//following::p[contains(@class, '{CLASS_VOTOS_MARCAS}')][1]"
                    nombre = elem.find_element(By.XPATH, xpath_nombre).text
                    votos = elem.find_element(By.XPATH, xpath_votos).text
                    registrar_voto_diputado(depto, jrv, partido_actual, num_candidato, nombre, votos)
                    datos_recolectados += 1
                except: pass

        print(f" [OK: {datos_recolectados} regs]")
        return True
    except Exception as e:
        print(f" [ERROR DATA] {e}")
        return False

def descargar_acta(driver, url_pdf, carpeta_raiz, carpeta_nivel, nombre_final, depto_log, jrv_log, subcarpeta_destino="pdf"):
    ruta_base_sin_ext = os.path.join(carpeta_raiz, carpeta_nivel, subcarpeta_destino, nombre_final)
    msg_tipo = "" if subcarpeta_destino == "pdf" else "(NC)"
    print(f"         ...Bajando {msg_tipo}...", end="")

    try:
        js_script = """
            var url = arguments[0];
            var callback = arguments[1];
            var xhr = new XMLHttpRequest();
            xhr.open('GET', url, true);
            xhr.responseType = 'blob';
            xhr.onload = function() {
                if (xhr.status === 200) {
                    var reader = new FileReader();
                    reader.readAsDataURL(xhr.response);
                    reader.onloadend = function() {
                        callback(reader.result);
                    }
                } else {
                    callback('Error HTTP: ' + xhr.status);
                }
            };
            xhr.onerror = function() {
                callback('Error de Red');
            };
            xhr.send();
        """
        driver.set_script_timeout(60)
        result = driver.execute_async_script(js_script, url_pdf)
        
        if result and isinstance(result, str) and "base64," in result:
            header, base64_data = result.split(',', 1)
            ext = ".pdf"
            tipo_archivo = "PDF"
            if "image/jpeg" in header or "image/jpg" in header:
                ext = ".jpg"
                tipo_archivo = "IMG"
            elif "image/png" in header:
                ext = ".png"
                tipo_archivo = "IMG"
            
            ruta_final = f"{ruta_base_sin_ext}{ext}"
            binary_data = base64.b64decode(base64_data)
            with open(ruta_final, 'wb') as f:
                f.write(binary_data)
            
            if len(binary_data) < 1000:
                print(f" [CORRUPTO]")
                registrar_evento(depto_log, jrv_log, carpeta_nivel, "ERROR_DATA", "Archivo pequeño")
            else:
                print(f" [{tipo_archivo} OK]")
                registrar_evento(depto_log, jrv_log, carpeta_nivel, "EXITO", f"Guardado en {subcarpeta_destino}")
        else:
            msg = str(result)[:30] if result else "Nula"
            print(f" [FALLO] {msg}")
            registrar_evento(depto_log, jrv_log, carpeta_nivel, "ERROR_JS", msg)
    except Exception as e:
        print(f" [ERROR CRITICO] {e}")
        registrar_evento(depto_log, jrv_log, carpeta_nivel, "ERROR_CRITICO", str(e))

def llenar_formulario(driver, wait, depto_nombre, num_jrv):
    try:
        input_depto = wait.until(EC.element_to_be_clickable((By.ID, SEL_DEPARTAMENTO)))
        input_depto.click()
        input_depto.send_keys(Keys.CONTROL + "a")
        input_depto.send_keys(Keys.DELETE)
        time.sleep(0.2)
        input_depto.send_keys(depto_nombre)
        time.sleep(0.5)
        input_depto.send_keys(Keys.ARROW_DOWN)
        input_depto.send_keys(Keys.ENTER)
        
        input_jrv = wait.until(EC.element_to_be_clickable((By.ID, SEL_JRV)))
        input_jrv.click()
        input_jrv.send_keys(Keys.CONTROL + "a")
        input_jrv.send_keys(Keys.DELETE)
        input_jrv.send_keys(str(num_jrv))
        time.sleep(0.5)
        input_jrv.send_keys(Keys.ARROW_DOWN)
        input_jrv.send_keys(Keys.ENTER)
        return True
    except Exception as e:
        print(f"      [ERROR FORMULARIO] {e}")
        return False

def validar_si_descargar(driver):
    intentos = 0
    razon = "Timeout"
    while intentos < 3: 
        try:
            if len(driver.find_elements(By.XPATH, XPATH_CHIP_VALIDA)) > 0:
                print(" [OK]", end="")
                return True, "NORMAL"
        except: pass

        try:
            if len(driver.find_elements(By.XPATH, XPATH_CHIP_NO_COMPUTADA)) > 0:
                print(" [NC]", end="")
                return True, "SEPARAR"
        except: pass
        
        try:
            elems = driver.find_elements(By.XPATH, XPATH_PORCENTAJE)
            for e in elems:
                txt = e.text.strip()
                if "%" in txt:
                    if txt not in ["(0.00%)", "(0%)", "0.00%"]:
                        print(f" [OK]", end="")
                        return True, "NORMAL"
        except: pass
        
        time.sleep(1)
        intentos += 1
    
    print(" [VACÍA]", end="")
    return False, razon

def procesar_una_jrv(driver, wait, depto_nombre, num_jrv):
    print(f"\n--- JRV {num_jrv} ({depto_nombre}) ---")
    for nombre_boton, nombre_carpeta in NIVELES.items():
        print(f"   > {nombre_boton}: ", end="")
        try:
            try:
                xpath_nivel = f"//button[contains(., '{nombre_boton}')]"
                btn_nivel = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_nivel)))
                click_js(driver, btn_nivel)
                time.sleep(1.5) 
            except:
                print(" [ERROR UI]")
                registrar_evento(depto_nombre, num_jrv, nombre_carpeta, "ERROR_UI", "Pestaña")
                driver.refresh()
                time.sleep(5)
                continue

            if not llenar_formulario(driver, wait, depto_nombre, num_jrv):
                registrar_evento(depto_nombre, num_jrv, nombre_carpeta, "ERROR_FORM", "Input")
                continue
            
            debe_descargar, tipo_validacion = validar_si_descargar(driver)

            if debe_descargar:
                if nombre_boton == "Diputados":
                    extraer_datos_diputados(driver, depto_nombre, num_jrv)

                try:
                    elem_enlace = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, XPATH_BTN_DESCARGAR))
                    )
                    url_pdf = elem_enlace.get_attribute('href')
                    
                    if url_pdf:
                        nombre_final = f"{num_jrv}-{nombre_carpeta}"
                        carpeta_destino = "pdf"
                        if tipo_validacion == "SEPARAR":
                            carpeta_destino = "no_computadas"
                        descargar_acta(driver, url_pdf, CARPETA_BASE, nombre_carpeta, nombre_final, depto_nombre, num_jrv, carpeta_destino)
                    else:
                        print(" -> [LINK ROTO]")
                        registrar_evento(depto_nombre, num_jrv, nombre_carpeta, "ERROR_LINK", "Sin href")
                except:
                    print(f" -> [SIN BOTÓN]")
                    registrar_evento(depto_nombre, num_jrv, nombre_carpeta, "NO_DISPONIBLE", "Botón ausente")
            else:
                print(f" -> [OMITIDO]")
                registrar_evento(depto_nombre, num_jrv, nombre_carpeta, "OMITIDO", tipo_validacion)

        except Exception as e:
            print(f"   [ERROR] {e}")
            registrar_evento(depto_nombre, num_jrv, nombre_carpeta, "CRASH", str(e))
            driver.refresh()
            time.sleep(5)

def iniciar_bot():
    preparar_carpetas()
    options = EdgeOptions()
    if MODO_FANTASMA:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080") 
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--log-level=3")
    prefs = {"plugins.always_open_pdf_externally": True}
    options.add_experimental_option("prefs", prefs)

    print(f"--- INICIANDO ROBOT (SOLO USA) ---")
    print(f"-> Carpeta: {CARPETA_BASE}")
    print(f"-> Objetivos en lista: {len(LISTA_JRVS_OBJETIVO)}")
    print(f"-> Departamento USA: {NOMBRE_DEPARTAMENTO_USA}")
    
    driver = webdriver.Edge(options=options)
    wait = WebDriverWait(driver, 20)
    
    try:
        driver.get(URL)
        time.sleep(8)
    except:
        driver.refresh()
        time.sleep(10)

    # 1. Procesar Lista (Con filtro activo de USA)
    for num_jrv in LISTA_JRVS_OBJETIVO:
        depto = obtener_departamento_por_jrv(num_jrv)
        
        # En MODO_SOLO_USA, 'depto' será None para todos los que no sean USA
        # Así que solo procesará los de USA
        if depto:
            procesar_una_jrv(driver, wait, depto, num_jrv)
        else:
            # No es necesario imprimir advertencia si es intencional
            # print(f"Saltando JRV {num_jrv} (No es USA)")
            pass

    driver.quit()
    print("\nFIN")

if __name__ == "__main__":
    iniciar_bot()