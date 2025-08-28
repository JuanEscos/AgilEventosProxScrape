# -*- coding: utf-8 -*-
"""
Script de Scraping para el Calendario de Eventos de Agility de la RSCE

Propósito:
Este script automatiza la extracción de datos de competiciones de Agility del sitio web
de la Real Sociedad Canina de España (RSCE). Su objetivo principal es mantener
un calendario de eventos actualizado, lo que lo hace ideal para ser ejecutado
diariamente a través de un programador de tareas como `crontab` o GitHub Actions.

Funcionalidad:
1. Navegación y Extracción: Utiliza Selenium para cargar dinámicamente el listado
   de eventos, navegando por las páginas y extrayendo la información clave (nombre,
   fechas, URL y ciudad) de cada competición.

2. Geocodificación: Emplea la librería Geopy para convertir los nombres de las
   ciudades en coordenadas geográficas (latitud y longitud). Esto permite la
   posterior visualización en un mapa.

3. Generación de Archivos:
   - **CSV**: Crea un archivo CSV (`eventos_agility_completo.csv`) con todos los
     datos extraídos y sus coordenadas, ideal para su uso en hojas de cálculo
     o bases de datos.
   - **Mapa Interactivo**: Genera un mapa HTML interactivo (`mapa_agility.html`)
     utilizando Folium, mostrando la ubicación de cada evento con marcadores
     que incluyen información detallada.
   - **Índice HTML**: Produce un archivo HTML sencillo (`indice_eventos.html`) que
     actúa como un contenedor para el mapa, facilitando su visualización en un
     navegador o la integración en un sitio web.

Configuración:
Todas las variables de configuración, como URLs, rutas de archivos y límites de
páginas, se gestionan a través del archivo `.env`, lo que permite una
configuración sencilla sin necesidad de modificar el código.

Autor: [Tu Nombre]
Fecha de Creación: [Fecha de creación]
Última Actualización: [Fecha de última modificación]
"""
# -*- coding: utf-8 -*-
"""
Scraper RSCE Agility -> CSV (con Lat/Lon y Estado)
- Excluye eventos "Anulado"
- Opción de filtrar "desde hoy"
- Paginación robusta 1..N (o solo 1)
- Geocodifica ciudades únicas (Nominatim) y añade Latitud/Longitud
Requiere: selenium, webdriver-manager, bs4, geopy, python-dotenv (opcional)
"""

import os, csv, time, re, datetime
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Geocoding
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# .env (opcional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# Fechas en español
# =========================
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}
DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-záéíóúñ]+),?\s+(\d{4})")

def parse_spanish_date(txt: str) -> Optional[datetime.date]:
    if not txt:
        return None
    t = txt.strip().lower().replace(" de ", " ")
    m = DATE_RE.search(t)
    if not m:
        return None
    try:
        d, mm, y = int(m.group(1)), m.group(2), int(m.group(3))
        mnum = SPANISH_MONTHS.get(mm)
        return datetime.date(y, mnum, d) if mnum else None
    except Exception:
        return None

def parse_date_range(inicio: str, fin: str) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
    return parse_spanish_date(inicio), parse_spanish_date(fin)


# =========================
# Scraper principal
# =========================
class RSCEAgilityCSV:
    def __init__(self):
        # Config desde .env o valores por defecto
        self.URL_BASE = os.getenv(
            "URL_BASE",
            "https://www.rsce.es/eventos-rsce/jsf/jet-engine:eventocuadro/tax/tipos-de-disciplinas:38/meta/fecha-evento!date:2025.1.1-/"
        )
        self.OUTDIR   = os.getenv("CARPETA_DESTINO", "./resultados_agility")
        self.OUTCSV   = os.path.join(self.OUTDIR, os.getenv("NOMBRE_CSV", "eventos_agility_2025.csv"))

        self.SOLO_PRIMERA       = self._to_bool(os.getenv("SOLO_PRIMERA_PAGINA"), False)
        self.MAX_PAGINAS        = int(os.getenv("MAX_PAGINAS", "50"))
        self.APLICAR_FILTRO_UI  = self._to_bool(os.getenv("APLICAR_FILTRO_UI"), True)   # pone "Desde=hoy" en la web
        self.FILTRAR_DESDE_HOY  = self._to_bool(os.getenv("FILTRAR_DESDE_HOY"), True)   # segunda capa de filtro
        self.GEOCODIFICAR       = self._to_bool(os.getenv("GEOCODIFICAR"), True)        # puedes apagarlo para pruebas

        os.makedirs(self.OUTDIR, exist_ok=True)

    @staticmethod
    def _to_bool(v, default=False):
        if v is None:
            return default
        return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "si", "sí")

    # ---------- Selenium ----------
    def _init_driver(self):
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--window-size=1600,1000")
        return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

    def _esperar_listado(self, d):
        WebDriverWait(d, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "jet-listing-grid__item"))
        )
        time.sleep(0.7)

    def _scroll_hasta_el_final(self, d, rounds=3, pause=1.0):
        for _ in range(rounds):
            d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause)

    def _aplicar_filtro_desde_hoy_ui(self, d):
        """Rellena 'Desde' con hoy y 'Hasta' vacío. Pulsa 'Ordenar' si existe."""
        try:
            hoy = datetime.date.today().strftime("%d/%m/%Y")
            desde = d.find_element(By.CSS_SELECTOR, "input.jet-date-range__from")
            hasta = d.find_element(By.CSS_SELECTOR, "input.jet-date-range__to")
            d.execute_script("""
                arguments[0].value = arguments[2];
                arguments[0].dispatchEvent(new Event('input')); arguments[0].dispatchEvent(new Event('change'));
                arguments[1].value = '';
                arguments[1].dispatchEvent(new Event('input')); arguments[1].dispatchEvent(new Event('change'));
            """, desde, hasta, hoy)
            try:
                ordenar = d.find_element(By.XPATH, "//button[normalize-space(.)='Ordenar'] | //input[@value='Ordenar']")
                d.execute_script("arguments[0].click();", ordenar)
            except Exception:
                pass
            self._esperar_listado(d)
            print("[DEBUG] Filtro UI 'Desde=hoy' aplicado")
        except Exception as e:
            print(f"[WARN] No se pudo aplicar filtro UI: {e}")

    def _detectar_total_paginas(self, d) -> int:
        try:
            elems = d.find_elements(By.CSS_SELECTOR, ".jet-filters-pagination__link")
            nums = []
            for el in elems:
                txt = (el.text or "").strip()
                if txt.isdigit():
                    nums.append(int(txt))
            total = max(nums) if nums else 1
            total = max(1, min(total, self.MAX_PAGINAS))
            print(f"[DEBUG] total_pages detectadas: {total}")
            return total
        except Exception:
            return 1

    def _ir_a_pagina(self, d, page_num: int) -> bool:
        try:
            if page_num == 1:
                return True
            btn = WebDriverWait(d, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    f"//div[contains(@class,'jet-filters-pagination__link') and normalize-space(text())='{page_num}']"
                ))
            )
            d.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            d.execute_script("arguments[0].click();", btn)
            self._esperar_listado(d)
            return True
        except Exception:
            # Intento con "Siguiente"
            try:
                nxt = d.find_element(By.CSS_SELECTOR, ".jet-filters-pagination__link.next")
                d.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
                d.execute_script("arguments[0].click();", nxt)
                self._esperar_listado(d)
                return True
            except Exception:
                print("    ⚠️ Paginación no disponible o fin de páginas.")
                return False

    # ---------- Extracción ----------
    def _extraer_eventos(self, html: str) -> List[Tuple[str, str, str, str, str, str]]:
        """
        Devuelve lista de tuplas:
        (nombre, inicio, fin, url, ciudad, estado)  con estado='Anulado' o 'Activo'
        """
        soup = BeautifulSoup(html, "html.parser")
        bloques = soup.select("div.jet-listing-grid__item")
        eventos = []
        for b in bloques:
            h2 = b.find("h2")
            if not h2:
                continue
            a = h2.find("a")
            nombre = h2.get_text(strip=True)
            url = a.get("href", "").strip() if a else ""
            fechas = b.select(".jet-listing-dynamic-field__content")
            inicio = fechas[0].get_text(strip=True) if len(fechas) > 0 else ""
            fin    = fechas[1].get_text(strip=True) if len(fechas) > 1 else ""
            lugar  = b.select_one(".elementor-icon-box-title span")
            ciudad = lugar.get_text(strip=True) if lugar else ""
            # estado (anulado)
            badge  = b.select_one("span.jet-listing-dynamic-terms__link")
            estado = "Anulado" if (badge and "anulado" in badge.get_text(strip=True).lower()) else "Activo"
            if nombre or url:
                eventos.append((nombre, inicio, fin, url, ciudad, estado))
        # debug corto
        print("      Ejemplos:", [f"{e[5]} · {e[0][:48]}" for e in eventos[:3]])
        return eventos

    # ---------- Filtros ----------
    def _filtrar_eventos(self, eventos):
        """
        1) Excluye ANULADOS
        2) (opcional) Filtra por fecha: fin >= hoy, o inicio >= hoy si no hay fin.
           Si ninguna de las dos fechas se puede parsear -> conservamos.
        """
        hoy = datetime.date.today()
        out = []
        for (n,i,f,u,c,estado) in eventos:
            if estado.lower() == "anulado":
                continue
            if not self.FILTRAR_DESDE_HOY:
                out.append((n,i,f,u,c,estado))
                continue
            di, df = parse_date_range(i, f)
            keep = (df and df >= hoy) or (df is None and di and di >= hoy) or (di is None and df is None)
            if keep:
                out.append((n,i,f,u,c,estado))
        return out

    # ---------- Geocoding ----------
    def _geocode_ciudades(self, eventos):
        """
        eventos: (n,i,f,u,c,estado) activos.
        Devuelve dict ciudad -> (lat, lon). Si GEOCODIFICAR=False, todas (None,None).
        """
        cache = {}
        if not self.GEOCODIFICAR:
            return cache
        geolocator = Nominatim(user_agent="agility-mapper-rsce/1.0", timeout=10)
        geocode = RateLimiter(
            geolocator.geocode, min_delay_seconds=1,
            max_retries=2, error_wait_seconds=2, swallow_exceptions=True
        )
        for *_, c, _estado in eventos:
            if c and c not in cache:
                q = f"{c}, España"
                loc = geocode(q)
                cache[c] = (loc.latitude, loc.longitude) if loc else (None, None)
        return cache

    # ---------- CSV ----------
    def _guardar_csv(self, eventos):
        """
        eventos: (n,i,f,u,c,estado) activos (ya filtrados)
        """
        cache = self._geocode_ciudades(eventos)
        with open(self.OUTCSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Nombre","Fecha inicio","Fecha fin","URL","Ciudad","Estado","Latitud","Longitud"])
            for n,i,f,u,c,estado in eventos:
                lat, lon = cache.get(c, (None, None)) if c else (None, None)
                w.writerow([n,i,f,u,c,estado,lat,lon])
        print(f"📁 CSV guardado en: {self.OUTCSV} con {len(eventos)} eventos")

    # ---------- Run ----------
    def run(self):
        print(f"[DEBUG] URL_BASE: {self.URL_BASE}")
        print(f"[DEBUG] SOLO_PRIMERA={self.SOLO_PRIMERA} | APLICAR_FILTRO_UI={self.APLICAR_FILTRO_UI} | FILTRAR_DESDE_HOY={self.FILTRAR_DESDE_HOY} | GEOCODIFICAR={self.GEOCODIFICAR}")

        d = self._init_driver()
        d.get(self.URL_BASE)
        self._esperar_listado(d)

        if self.APLICAR_FILTRO_UI:
            self._aplicar_filtro_desde_hoy_ui(d)

        total_pages = self._detectar_total_paginas(d)
        pages = [1] if self.SOLO_PRIMERA else list(range(1, total_pages + 1))

        eventos_totales = []
        seen_urls = set()

        for p in pages:
            ok = self._ir_a_pagina(d, p)
            if not ok:
                break
            self._scroll_hasta_el_final(d)
            eventos = self._extraer_eventos(d.page_source)

            nuevos = 0
            for ev in eventos:
                url = ev[3]  # URL está en posición 3
                if url and url not in seen_urls:
                    eventos_totales.append(ev)
                    seen_urls.add(url)
                    nuevos += 1
            print(f"    ➕ {nuevos} nuevos en página {p}")

        d.quit()
        print(f"🔍 Total brutos: {len(eventos_totales)}")

        # Filtros: quitar anulados + (opcional) fecha desde hoy
        eventos_final = self._filtrar_eventos(eventos_totales)
        print(f"🔍 Tras filtros (estado/fecha): {len(eventos_final)}")

        self._guardar_csv(eventos_final)


if __name__ == "__main__":
    RSCEAgilityCSV().run()
