import os
import re
import time
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup  # Importación añadida aquí

# Configuración
BASE = "https://www.flowagility.com"
EVENTS_URL = "https://www.flowagility.com/zone/events"
FLOW_EMAIL = "pilar1959suarez@gmail.com"
FLOW_PASS = "Seattle1"
HEADLESS = True
INCOGNITO = True
MAX_SCROLLS = 10
SCROLL_WAIT_S = 1.5
OUT_DIR = "./output"
UUID_RE = re.compile(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})")

# Crear directorio de salida si no existe
os.makedirs(OUT_DIR, exist_ok=True)

def log(message):
    """Función de logging"""
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=0.5, max_s=1.2):
    """Pausa aleatoria entre min_s y max_s segundos"""
    time.sleep(max(min_s, max_s))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        JavascriptException, StaleElementReferenceException, NoSuchElementException,
        ElementClickInterceptedException, TimeoutException
    )
    return webdriver, By, Options, WebDriverWait, EC, JavascriptException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException

def _get_driver():
    webdriver, By, Options, *_ = _import_selenium()
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
    
    # Opciones adicionales para evitar problemas de versión
    opts.add_argument("--disable-gpu")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    # Usar ChromeDriverManager para manejar automáticamente la versión
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except ImportError:
        # Fallback si webdriver_manager no está instalado
        log("webdriver_manager no instalado, usando ChromeDriver del sistema")
        return webdriver.Chrome(options=opts)

def _save_screenshot(driver, name):
    try:
        path = os.path.join(OUT_DIR, name)
        driver.save_screenshot(path)
        log(f"Screenshot -> {path}")
    except Exception:
        pass

def _accept_cookies(driver, By):
    try:
        for sel in (
            '[data-testid="uc-accept-all-button"]',
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            'button[mode="primary"]',
        ):
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            if btns:
                btns[0].click()
                slow_pause(0.8, 1.8)
                return
        driver.execute_script("""
            const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
            if(b) b.click();
        """)
        slow_pause(0.2, 0.5)
    except Exception:
        pass

def _is_login_page(driver):
    return "/user/login" in (driver.current_url or "")

def _login(driver, By, WebDriverWait, EC):
    log("Iniciando login...")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL)
    slow_pause(0.2, 0.4)
    pwd.clear();   pwd.send_keys(FLOW_PASS)
    slow_pause(0.2, 0.4)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause()
    log("Login exitoso")

def _ensure_logged_in(driver, max_tries, By, WebDriverWait, EC):
    for _ in range(max_tries):
        if not _is_login_page(driver):
            return True
        log("Sesión caducada. Reintentando login...")
        _login(driver, By, WebDriverWait, EC)
        slow_pause(0.5, 1.2)
        if not _is_login_page(driver):
            return True
    return False

def _full_scroll(driver):
    last_h = 0
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            break
        last_h = h

def extract_event_details(container_html):
    """Extrae detalles específicos de un evento del HTML"""
    soup = BeautifulSoup(container_html, 'html.parser')
    
    event_data = {}
    
    # ID del evento
    event_container = soup.find('div', class_='group mb-6')
    if event_container:
        event_data['id'] = event_container.get('id', '')
    
    # Información básica
    info_div = soup.find('div', class_='relative flex flex-col w-full pt-1 pb-6 mb-4 border-b border-gray-300')
    if info_div:
        # Fechas
        date_elems = info_div.find_all('div', class_='text-xs')
        if date_elems:
            event_data['fechas'] = date_elems[0].get_text(strip=True)
        
        # Organización
        if len(date_elems) > 1:
            event_data['organizacion'] = date_elems[1].get_text(strip=True)
        
        # Nombre del evento
        name_elem = info_div.find('div', class_='font-caption text-lg text-black truncate -mt-1')
        if name_elem:
            event_data['nombre'] = name_elem.get_text(strip=True)
        
        # Club organizador
        club_elem = info_div.find('div', class_='text-xs mb-0.5 mt-0.5')
        if club_elem:
            event_data['club'] = club_elem.get_text(strip=True)
        
        # Lugar - buscar en todos los divs con text-xs
        location_divs = info_div.find_all('div', class_='text-xs')
        for div in location_divs:
            text = div.get_text(strip=True)
            if '/' in text and ('Spain' in text or 'España' in text):
                event_data['lugar'] = text
                break
    
    # Estado del evento
    status_button = soup.find('div', class_='py-1 px-4 border text-white font-bold rounded text-sm')
    if status_button:
        event_data['estado'] = status_button.get_text(strip=True)
        # Determinar tipo de estado
        if 'Inscribirse' in event_data['estado']:
            event_data['estado_tipo'] = 'inscripcion_abierta'
        elif 'En curso' in event_data['estado']:
            event_data['estado_tipo'] = 'en_curso'
        else:
            event_data['estado_tipo'] = 'desconocido'
    
    # Enlaces
    event_data['enlaces'] = {}
    info_link = soup.find('a', href=lambda x: x and '/info/' in x)
    if info_link:
        event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
    
    participants_link = soup.find('a', href=lambda x: x and '/participants_list' in x)
    if participants_link:
        event_data['enlaces']['participantes'] = urljoin(BASE, participants_link['href'])
    
    runs_link = soup.find('a', href=lambda x: x and '/runs' in x)
    if runs_link:
        event_data['enlaces']['runs'] = urljoin(BASE, runs_link['href'])
    
    # Bandera del país
    flag_div = soup.find('div', class_='text-md')
    if flag_div:
        event_data['pais_bandera'] = flag_div.get_text(strip=True)
    
    return event_data

def main():
    """Función principal"""
    log("=== Scraping FlowAgility - Competiciones de Agility ===")
    
    # Importar Selenium
    (webdriver, By, Options, WebDriverWait, EC, 
     JavascriptException, StaleElementReferenceException, 
     NoSuchElementException, ElementClickInterceptedException, 
     TimeoutException) = _import_selenium()
    
    driver = _get_driver()
    
    try:
        # Login
        _login(driver, By, WebDriverWait, EC)
        
        # Navegar a eventos
        log("Navegando a la página de eventos...")
        driver.get(EVENTS_URL)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Aceptar cookies
        _accept_cookies(driver, By)
        
        # Scroll completo para cargar todos los eventos
        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(2, 3)
        
        # Obtener HTML de la página
        page_html = driver.page_source
        
        # Extraer eventos
        log("Extrayendo información de eventos...")
        events = []
        
        # Buscar todos los contenedores de eventos
        soup = BeautifulSoup(page_html, 'html.parser')
        event_containers = soup.find_all('div', class_='group mb-6')
        
        log(f"Encontrados {len(event_containers)} eventos")
        
        for i, container in enumerate(event_containers, 1):
            try:
                event_data = extract_event_details(str(container))
                events.append(event_data)
                log(f"Procesado evento {i}/{len(event_containers)}: {event_data.get('nombre', 'Sin nombre')}")
            except Exception as e:
                log(f"Error procesando evento {i}: {str(e)}")
                continue
        
        # Guardar resultados
        output_file = os.path.join(OUT_DIR, 'competiciones_agility.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Extracción completada. {len(events)} eventos guardados en {output_file}")
        
        # Mostrar resumen
        print(f"\n{'='*80}")
        print("RESUMEN DE COMPETICIONES ENCONTRADAS:")
        print(f"{'='*80}")
        
        for i, event in enumerate(events, 1):
            print(f"\n{i}. {event.get('nombre', 'Sin nombre')}")
            print(f"   📅 {event.get('fechas', 'Fecha no especificada')}")
            print(f"   🏢 {event.get('organizacion', 'Organización no especificada')}")
            print(f"   🏆 {event.get('club', 'Club no especificado')}")
            print(f"   📍 {event.get('lugar', 'Lugar no especificado')}")
            print(f"   🚦 {event.get('estado', 'Estado no especificado')}")
        
        print(f"\n{'='*80}")
        print(f"Total: {len(events)} competiciones de agility")
        
    except Exception as e:
        log(f"Error durante el scraping: {str(e)}")
        _save_screenshot(driver, "error_screenshot.png")
        
    finally:
        driver.quit()
        log("Navegador cerrado")

if __name__ == "__main__":
    main()