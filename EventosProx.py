#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FlowAgility scraper + procesado (dos pasos, un archivo).
Subcomandos:
  - scrape  : descarga eventos y participantes (CSV diarios versionados)
  - process : genera participantes_procesado_YYYY-MM-DD.csv (versionado)
  - all     : scrape + process

Ejemplos:
  python script.py scrape
  python script.py process
  python script.py all
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FlowAgility scraper + procesado (dos pasos, un archivo).
Subcomandos:
  - scrape  : descarga eventos y participantes (CSV diarios versionados)
  - process : genera participantes_procesado_YYYY-MM-DD.csv (versionado)
  - all     : scrape + process

Además: al terminar 'process' (o 'all'), imprime en Terminal las
“pruebas próximas” por fecha (ordenadas y agrupadas).

Requisitos:
  pip install selenium python-dotenv pandas python-dateutil numpy

Variables .env (junto al script):
  FLOW_EMAIL=...
  FLOW_PASS=...
  HEADLESS=true
  INCOGNITO=true
  OUT_DIR=./ListaEventos
  SHOW_CONFIG=true
  CHROME_BINARY=/ruta/a/google-chrome
  CHROMEDRIVER_PATH=/ruta/a/chromedriver
"""

import os, csv, sys, re, traceback, unicodedata, argparse
from datetime import datetime, timedelta
from urllib.parse import urljoin
from pathlib import Path
import time, random

# Terceros
from dotenv import load_dotenv

# ----------------------------- Utilidades ENV -----------------------------
def _env_bool(name, default=False):
    val = os.getenv(name)
    if val is None:
        return bool(default)
    return str(val).strip().lower() in ("1","true","t","yes","y","on")

def _env_int(name, default):
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)

def _env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)

# ----------------------------- Carga .env y Config -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")

BASE       = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"

# Credenciales (OBLIGATORIAS)
FLOW_EMAIL = os.getenv("FLOW_EMAIL")
FLOW_PASS  = os.getenv("FLOW_PASS")
if not FLOW_EMAIL or not FLOW_PASS:
    print("[ERROR] Falta FLOW_EMAIL o FLOW_PASS en .env", file=sys.stderr)
    sys.exit(2)

# Flags/tunables
HEADLESS           = _env_bool("HEADLESS", True)
INCOGNITO          = _env_bool("INCOGNITO", True)
MAX_SCROLLS        = _env_int("MAX_SCROLLS", 24)
SCROLL_WAIT_S      = _env_float("SCROLL_WAIT_S", 0.5)
CLICK_RETRIES      = _env_int("CLICK_RETRIES", 3)
PER_PART_TIMEOUT_S = _env_float("PER_PART_TIMEOUT_S", 6)
RENDER_POLL_S      = _env_float("RENDER_POLL_S", 0.15)
MAX_EVENT_SECONDS  = _env_int("MAX_EVENT_SECONDS", 1800)
OUT_DIR            = os.path.abspath(os.getenv("OUT_DIR", "./ListaEventos"))
os.makedirs(OUT_DIR, exist_ok=True)

DATE_STR = datetime.now().strftime("%Y-%m-%d")
UUID_RE  = re.compile(r"/zone/events/([0-9a-fA-F-]{36})(?:/.*)?$")

def _print_effective_config():
    if str(os.getenv("SHOW_CONFIG", "false")).lower() not in ("1","true","yes","on","t"):
        return
    print("=== Config efectiva ===")
    print(f"FLOW_EMAIL           = {FLOW_EMAIL}")
    print(f"HEADLESS             = {HEADLESS}")
    print(f"INCOGNITO            = {INCOGNITO}")
    print(f"MAX_SCROLLS          = {MAX_SCROLLS}")
    print(f"SCROLL_WAIT_S        = {SCROLL_WAIT_S}")
    print(f"CLICK_RETRIES        = {CLICK_RETRIES}")
    print(f"PER_PART_TIMEOUT_S   = {PER_PART_TIMEOUT_S}")
    print(f"RENDER_POLL_S        = {RENDER_POLL_S}")
    print(f"MAX_EVENT_SECONDS    = {MAX_EVENT_SECONDS}")
    print(f"OUT_DIR              = {OUT_DIR}")
    print(f"CHROME_BINARY        = {os.getenv('CHROME_BINARY') or ''}")
    print(f"CHROMEDRIVER_PATH    = {os.getenv('CHROMEDRIVER_PATH') or ''}")
    print("=======================")

def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def next_free_path(path: str) -> str:
    """Si path existe, devuelve path con sufijo _v2, _v3, … libre."""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 2
    while True:
        cand = f"{base}_v{i}{ext}"
        if not os.path.exists(cand):
            return cand
        i += 1

# ============================== PARTE 1: SCRAPER ==============================
from selenium.webdriver.chrome.service import Service
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
    # Import local para evitar exigir Selenium cuando solo se procesa CSV
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")

    # Permitir override del binario/driver desde .env
    chrome_bin = os.getenv("CHROME_BINARY")
    if chrome_bin:
        opts.binary_location = chrome_bin  # Debe apuntar a chrome.exe

    driver_path = os.getenv("CHROMEDRIVER_PATH", "").strip()

    if driver_path:
        # Selenium 4: hay que usar Service
        service = Service(executable_path=driver_path)
        return webdriver.Chrome(service=service, options=opts)

    # Si no se especifica CHROMEDRIVER_PATH, usa el que esté en el PATH
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
                #time.sleep(0.2)
                time.sleep(random.uniform(1, 2))  # espera entre 1.5 y 3.5 segundos
                return
        driver.execute_script("""
            const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
            if(b) b.click();
        """)
        time.sleep(random.uniform(0.1, 0.2))  # espera entre 1.5 y 3.5 segundos
    except Exception:
        pass

def _is_login_page(driver):
    return "/user/login" in (driver.current_url or "")

def _login(driver, By, WebDriverWait, EC):
    log("Login…")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL)
    pwd.clear();   pwd.send_keys(FLOW_PASS)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    log("Login OK.")

def _ensure_logged_in(driver, max_tries, By, WebDriverWait, EC):
    for _ in range(max_tries):
        if not _is_login_page(driver):
            return True
        log("Sesión caducada. Reintentando login…")
        _login(driver, By, WebDriverWait, EC)
        time.sleep(random.uniform(0.5, 1))  # espera entre 1.5 y 3.5 segundos
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

def _collect_event_urls(driver, By, WebDriverWait, EC):
    driver.get(EVENTS_URL)
    WebDriverWait(driver, 25).until(lambda d: d.find_element(By.TAG_NAME, "body"))
    _accept_cookies(driver, By)
    _full_scroll(driver)

    by_uuid = {}
    anchors = driver.find_elements(By.TAG_NAME, "a")
    for a in anchors:
        href = a.get_attribute("href") or ""
        if not href:
            continue
        if href.startswith("/"): href = urljoin(BASE, href)
        if "flowagility.com/zone/events/" not in href:
            continue
        m = UUID_RE.search(href)
        if not m:
            continue
        uuid = m.group(1)
        is_plist = href.rstrip("/").endswith("participants_list")
        base_url = f"{BASE}/zone/events/{uuid}"
        d = by_uuid.get(uuid, {"base": base_url, "plist": None})
        if is_plist:
            d["plist"] = href
        else:
            d["base"] = base_url
        by_uuid[uuid] = d
    return by_uuid

EMOJI_RE = re.compile(
    "[\U0001F1E6-\U0001F1FF\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+"
)

def _clean(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = EMOJI_RE.sub("", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:;")

_BAD_JUDGE = re.compile(r"(aguardar|por\s+confirmar|tbd|to\s+be\s+confirmed)", re.I)
def _looks_like_name(s: str) -> bool:
    s = _clean(s)
    if not s or _BAD_JUDGE.search(s):
        return False
    if s.lower() in {"nombre","name","jueces","juezes","jueces:","juezes:"}:
        return False
    return bool(re.search(r"[A-Za-zÁÉÍÓÚÜáéíóúüñÑ]", s)) and len(s) >= 3

def _extract_judges_anywhere(driver, By):
    names = []
    # Grid
    try:
        hdrs = driver.find_elements(
            By.XPATH,
            "//div[contains(@class,'font-bold') and contains(@class,'text-sm') and contains(@class,'border-b')]"
        )
        for h in hdrs:
            if re.search(r"\bjuece[sz]\b", (h.text or ""), flags=re.I):
                grid = h.find_element(By.XPATH, "./ancestor::div[contains(@class,'grid')][1]")
                vals = grid.find_elements(
                    By.XPATH, ".//div[contains(@class,'font-bold') and contains(@class,'text-sm') and contains(@class,'text-black')]"
                )
                for v in vals:
                    t = _clean(v.text)
                    if _looks_like_name(t):
                        names.append(t)
    except Exception:
        pass
    # .rules
    try:
        rules_blocks = driver.find_elements(By.CSS_SELECTOR, "div.rules, .rules")
        for rb in rules_blocks:
            txt_block = rb.get_attribute("textContent") or rb.text or ""
            if not re.search(r"\bjuece[sz]\b", txt_block, flags=re.I):
                continue
            lis = rb.find_elements(By.XPATH, ".//li")
            for li in lis:
                t = _clean(li.get_attribute("textContent") or "")
                if _looks_like_name(t):
                    names.append(t)
            if names:
                return list(dict.fromkeys(names))
            lines = [_clean(x) for x in (txt_block or "").splitlines() if _clean(x)]
            idx = next((i for i, ln in enumerate(lines) if re.search(r"\bjuece[sz]\b", ln, re.I)), -1)
            if idx != -1:
                for ln in lines[idx+1 : idx+30]:
                    if re.search(r"\b(evento|organizador|localiz|inscrip|condicion|pruebas|prices|precios)\b", ln, re.I):
                        break
                    if _looks_like_name(ln):
                        names.append(ln)
                if names:
                    return list(dict.fromkeys(names))
    except Exception:
        pass
    # texto global
    try:
        body = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        lines = [_clean(x) for x in body.splitlines()]
        idx = next((i for i, ln in enumerate(lines) if re.search(r"\bjuece[sz]\b", ln, re.I)), -1)
        if idx != -1:
            for ln in lines[idx+1 : idx+30]:
                if re.search(r"\b(evento|organizador|localiz|inscrip|condicion|pruebas|prices|precios)\b", ln, re.I):
                    break
                if _looks_like_name(ln):
                    names.append(ln)
    except Exception:
        pass
    # uniq
    out = []
    seen = set()
    for n in names:
        k = unicodedata.normalize("NFKD", _clean(n)).encode("ascii", "ignore").decode("ascii").casefold()
        if k and k not in seen:
            seen.add(k); out.append(_clean(n))
    return out

def _scrape_event_info(driver, base_event_url, plist_url, By, WebDriverWait, EC):
    def _nonempty_lines(s):
        return [ln.strip() for ln in (s or "").splitlines() if ln and ln.strip()]
    def _best_title_fallback():
        heads = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "h1, h2, [role='heading']") if e.text.strip()]
        heads = [h for h in heads if h.lower() != "flowagility"]
        if heads:
            heads.sort(key=len, reverse=True)
            return heads[0]
        try:
            tmeta = driver.execute_script("return (document.querySelector(\"meta[property='og:title']\")||{}).content || ''")
            tmeta = (tmeta or "").strip()
            if tmeta and tmeta.lower() != "flowagility":
                return tmeta
        except Exception:
            pass
        t = (driver.title or "").strip()
        return t if t.lower() != "flowagility" else "N/D"
    def _read_header():
        try:
            hdr = driver.find_element(By.ID, "event_header")
            lines = _nonempty_lines(hdr.text)
            lines = [ln for ln in lines if ln.lower() != "flowagility"]
            return lines[:6]
        except Exception:
            return []
    def _body_text():
        try:
            return driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        except Exception:
            return ""

    def _get_organizer(header_lines, body):
        try:
            headers = driver.find_elements(By.XPATH, "//div[contains(@class,'font-bold') and contains(@class,'text-sm') and contains(@class,'border-b')]")
            for h in headers:
                if h.text.strip().lower() in ("organizador","organizer"):
                    grid = h.find_element(By.XPATH, "./ancestor::div[contains(@class,'grid')][1]")
                    labs = grid.find_elements(By.CSS_SELECTOR, "div.text-gray-500.text-sm")
                    for lab in labs:
                        if lab.text.strip().lower() in ("nombre","name"):
                            val = lab.find_element(By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm') and contains(@class,'text-black')][1]")
                            v = _clean(val.text)
                            if v:
                                return v
        except Exception:
            pass
        m = re.search(r"(Organiza|Organizer|Organizador)\s*[:\-]\s*(.+)", body, flags=re.I)
        if m:
            v = _clean(m.group(2).splitlines()[0])
            if v:
                return v
        if len(header_lines) >= 4:
            candidate = _clean(header_lines[3])
            if candidate and candidate not in header_lines[:3]:
                return candidate
        return "N/D"

    def _get_location(header_lines, body):
        country_terms = {
            "spain","españa","portugal","france","francia","italy","italia","germany","alemania",
            "belgium","bélgica","belgica","netherlands","holanda","países bajos","paises bajos",
            "czech republic","república checa","republica checa","slovakia","eslovaquia","poland","polonia",
            "austria","switzerland","suiza","hungary","hungría","hungria","romania","rumanía","rumania",
            "bulgaria","greece","grecia","united kingdom","reino unido","uk","ireland","irlanda",
            "norway","noruega","sweden","suecia","denmark","dinamarca","finland","finlandia",
            "estonia","latvia","lithuania","croatia","croacia","slovenia","eslovenia","serbia",
            "bosnia","montenegro","north macedonia","macedonia","albania","turkey","turquía","turquia",
            "usa","estados unidos","canada","canadá","canada"
        }
        for ln in header_lines:
            if " / " in ln and not re.search(r"\b(FCI|RSCE|RFEC|FED)\b", ln, flags=re.I):
                right = ln.split("/")[-1].strip().lower()
                if right in country_terms:
                    return ln.strip()
        ciudad = re.search(r"(Ciudad|City)\s*[:\-]\s*(.+)", body, flags=re.I)
        pais   = re.search(r"(Pa[ií]s|Country)\s*[:\-]\s*(.+)", body, flags=re.I)
        c = _clean(ciudad.group(2).splitlines()[0]) if ciudad else ""
        p = _clean(pais.group(2).splitlines()[0])   if pais   else ""
        if c or p:
            return f"{c} / {p}".strip(" /")
        return "N/D"

    def _get_dates(header_lines, body):
        if header_lines:
            return header_lines[0]
        ini = re.search(r"(Fecha de inicio|Start date)\s*[:\-]\s*(.+)", body, flags=re.I)
        fin = re.search(r"(Fecha de fin|End date)\s*[:\-]\s*(.+)", body, flags=re.I)
        if ini or fin:
            a = _clean(ini.group(2).splitlines()[0]) if ini else ""
            b = _clean(fin.group(2).splitlines()[0]) if fin else ""
            return f"{a} – {b}".strip(" –")
        meses = r"(Ene|Feb|Mar|Abr|May|Jun|Jul|Ago|Sep|Oct|Nov|Dic|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        m = re.search(rf"\b{meses}\s+\d{{1,2}}\s*-\s*\d{{1,2}}\b", body)
        return _clean(m.group(0)) if m else "N/D"

    data = {
        "event_url": base_event_url,
        "title": "N/D", "organizer": "N/D", "location": "N/D", "dates": "N/D",
        "header_1": "", "header_2": "", "header_3": "", "header_4": "", "header_5": "", "header_6": "",
        "judges": "N/D",
    }

    driver.get(base_event_url)
    WebDriverWait(driver, 20).until(lambda d: d.find_element(By.TAG_NAME, "body"))
    _accept_cookies(driver, By)
    time.sleep(random.uniform(0.2, 0.3))  # espera entre 0.2 y 0.3 segundos

    header_lines = _read_header()
    body_txt     = _body_text()

    title = header_lines[2] if len(header_lines) >= 3 and header_lines[2].lower() != "flowagility" else _best_title_fallback()
    data.update({
        "title": title,
        "dates": _get_dates(header_lines, body_txt),
        "location": _get_location(header_lines, body_txt),
        "organizer": _get_organizer(header_lines, body_txt),
        **{f"header_{i+1}": (header_lines[i] if i < len(header_lines) else "") for i in range(6)}
    })

    jlist = _extract_judges_anywhere(driver, By)
    if jlist:
        data["judges"] = " | ".join(jlist)

    # Fallback a participants_list si faltan datos
    need_fb = any(not str(data[k]).strip() or data[k] == "N/D" for k in ("organizer","judges","header_1"))
    if need_fb:
        alt = plist_url or (base_event_url.rstrip("/") + "/participants_list")
        try:
            driver.get(alt)
            WebDriverWait(driver, 15).until(lambda d: d.find_element(By.TAG_NAME, "body"))
            time.sleep(random.uniform(0.2, 0.3))  # espera entre 0.2 y 0.3 segundos
            header_lines = _read_header()
            body_txt     = _body_text()
            if data["dates"]     == "N/D": data["dates"]     = _get_dates(header_lines, body_txt)
            if data["location"]  == "N/D": data["location"]  = _get_location(header_lines, body_txt)
            if data["organizer"] == "N/D": data["organizer"] = _get_organizer(header_lines, body_txt)
            for i in range(6):
                if not data[f"header_{i+1}"]:
                    data[f"header_{i+1}"] = header_lines[i] if i < len(header_lines) else ""
            if data["judges"] == "N/D":
                jlist2 = _extract_judges_anywhere(driver, By)
                if jlist2:
                    data["judges"] = " | ".join(jlist2)
        except Exception:
            pass

    for k in ("title","organizer","location","dates","judges"):
        data[k] = _clean(data[k]) if data[k] else "N/D"
        if not data[k]:
            data[k] = "N/D"
    for i in range(1,7):
        data[f"header_{i}"] = _clean(data[f"header_{i}"])

    return data

JS_MAP_PARTICIPANT_RICH = r"""
const pid = arguments[0];
const root = document.getElementById(pid);
if (!root) return null;

const txt = el => (el && el.textContent) ? el.textContent.trim() : null;

function classListArray(el){
  if (!el) return [];
  const cn = el.className;
  if (!cn) return [];
  if (typeof cn === 'string') return cn.trim().split(/\s+/);
  if (typeof cn === 'object' && 'baseVal' in cn) return String(cn.baseVal).trim().split(/\s+/);
  return String(cn).trim().split(/\s+/);
}
function hasAll(el, toks){
  const set = new Set(classListArray(el));
  return toks.every(t => set.has(t));
}
function isHeader(el){
  const arr = classListArray(el);
  return (arr.includes('border-b') && arr.includes('border-gray-400'))
      || (arr.includes('font-bold') && arr.includes('text-sm') && arr.some(c => /^mt-/.test(c)));
}
function isLabel(el){ return (classListArray(el).includes('text-gray-500') && classListArray(el).includes('text-sm')); }
function isStrong(el){
  const arr = classListArray(el);
  return (arr.includes('font-bold') && arr.includes('text-sm'));
}
function nextStrong(el){
  let cur = el;
  for (let i=0;i<8;i++){
    cur = cur && cur.nextElementSibling;
    if (!cur) break;
    if (isStrong(cur)) return cur;
  }
  return null;
}

const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, null);
let node = walker.currentNode;
let currentDay = null;
let tmpFecha = null;
let tmpMangas = null;

const fields = {};
const schedule = [];

const simpleFieldLabels = new Set([
  "Dorsal","Guía","Guia","Perro","Raza","Edad","Género","Genero",
  "Altura (cm)","Altura","Nombre de Pedigree","Nombre de Pedrigree",
  "País","Pais","Licencia","Equipo","Club","Federación","Federacion"
]);

while (node){
  if (isHeader(node)){
    const t = txt(node); if (t) currentDay = t;
  } else if (isLabel(node)){
    const label = (txt(node) || "");
    const valueEl = nextStrong(node);
    const value = txt(valueEl) || "";

    const l = label.toLowerCase();
    if (l.startsWith("fecha"))       { tmpFecha  = value; }
    else if (l.startsWith("mangas")) { tmpMangas = value; }
    else if (simpleFieldLabels.has(label) && value && (fields[label] == null || fields[label] === "")) {
      fields[label] = value;
    }

    if (tmpFecha !== null && tmpMangas !== null){
      schedule.push({ day: currentDay || "", fecha: tmpFecha, mangas: tmpMangas });
      tmpFecha = null; tmpMangas = null;
    }
  }
  node = walker.nextNode();
}
return { fields, schedule };
"""

def _collect_booking_ids(driver):
    try:
        ids = driver.execute_script("""
            return Array.from(
              document.querySelectorAll("[phx-click='booking_details_show']")
            ).map(el => el.getAttribute("phx-value-booking_id"))
             .filter(Boolean);
        """) or []
    except Exception:
        ids = []
    seen, out = set(), []
    for x in ids:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _click_toggle_by_pid(driver, pid, By, WebDriverWait, EC, TimeoutException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException):
    sel = f"[phx-click='booking_details_show'][phx-value-booking_id='{pid}']"
    for _ in range(6):
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            WebDriverWait(driver, 8).until(lambda d: d.find_element(By.ID, pid))
            return driver.find_element(By.ID, pid)
        except (StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException):
            time.sleep(random.uniform(0.2, 0.3))  # espera entre 0.2 y 0.3 segundos
            driver.execute_script("window.scrollBy(0, 120);")
            time.sleep(random.uniform(0.2, 0.3))  # espera entre 0.2 y 0.3 segundos
            continue
    return None

def _fallback_map_participant(driver, pid, By):
    labels = driver.find_elements(
        By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'text-gray-500') and contains(@class,'text-sm')]"
    )
    values = driver.find_elements(
        By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'font-bold') and contains(@class,'text-sm')]"
    )
    fields = {}
    for lab_el, val_el in zip(labels, values):
        lt = _clean(lab_el.text or "")
        vt = _clean(val_el.text or "")
        if lt and vt and lt not in fields:
            fields[lt] = vt

    headers = driver.find_elements(
        By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'border-b') and contains(@class,'border-gray-400')]"
    )
    schedule = []
    for h in headers:
        fecha = h.find_elements(
            By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm')][1]"
        )
        mangas = h.find_elements(
            By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm')][2]"
        )
        schedule.append({
            "day": _clean(h.text or ""),
            "fecha": _clean(fecha[0].text if fecha else ""),
            "mangas": _clean(mangas[0].text if mangas else "")
        })
    return {"fields": fields, "schedule": schedule}

def _write_csv(path, rows, header=None):
    """Modo 'w' siempre; si ya existía hoy, path vendrá versionado por next_free_path()."""
    if not rows:
        return
    if not header:
        keys = set()
        for r in rows: keys.update(r.keys())
        header = sorted(keys)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def _build_participants_header(rows):
    base = [
        "event_uuid","event_title","participants_url","BinomID","Dorsal","Guía","Perro","Raza",
        "Edad","Género","Altura (cm)","Nombre de Pedigree","País","Licencia","Club","Federación","Equipo"
    ]
    max_i = 0
    for r in rows:
        for k in r.keys():
            m = re.match(r"Fecha (\d+)", k)
            if m:
                max_i = max(max_i, int(m.group(1)))
    for i in range(1, max_i+1):
        base.extend([f"Día {i}", f"Fecha {i}", f"Mangas {i}"])
    return base

def scrape_main():
    _print_effective_config()

    (webdriver, By, Options, WebDriverWait, EC,
     JavascriptException, StaleElementReferenceException,
     NoSuchElementException, ElementClickInterceptedException, TimeoutException) = _import_selenium()

    csv_event = next_free_path(os.path.join(OUT_DIR, f"events_{DATE_STR}.csv"))
    csv_part  = next_free_path(os.path.join(OUT_DIR, f"participants_{DATE_STR}.csv"))

    driver = _get_driver()
    try:
        _login(driver, By, WebDriverWait, EC)
        urls_by_uuid = _collect_event_urls(driver, By, WebDriverWait, EC)
        log(f"Eventos (UUIDs) encontrados: {len(urls_by_uuid)}")

        all_event_rows = []
        all_part_rows  = []

        for uuid, pair in urls_by_uuid.items():
            start_event_ts = time.time()
            base_url = pair["base"]
            plist    = pair["plist"]

            ev = _scrape_event_info(driver, base_url, plist, By, WebDriverWait, EC)
            ev["uuid"] = uuid
            all_event_rows.append(ev)

            if plist:
                # participants_list
                for attempt in range(1, 4):
                    driver.get(plist)
                    WebDriverWait(driver, 25).until(lambda d: d.find_element(By.TAG_NAME, "body"))
                    _accept_cookies(driver, By)

                    # Esperas
                    # Después de cargar la página
                    time.sleep(random.uniform(2, 3))  # espera entre 0.2 y 0.3 segundos
                    start = time.time()
                    state = "timeout"
                    while time.time() - start < 20:
                        if _is_login_page(driver):
                            state = "login"; break
                        toggles = driver.find_elements(By.CSS_SELECTOR, "[phx-click='booking_details_show']")
                        if toggles:
                            state = "toggles"; break
                        hints = (
                            "//p[contains(., 'No hay') or contains(., 'No results') or contains(., 'Sin participantes')]",
                            "//div[contains(., 'No hay') or contains(., 'No results') or contains(., 'Sin participantes')]",
                        )
                        if any(driver.find_elements(By.XPATH, xp) for xp in hints):
                            state = "empty"; break
                        time.sleep(0.25)

                    if state == "login":
                        if not _ensure_logged_in(driver, 2, By, WebDriverWait, EC):
                            log(f"No se pudo relogar para {plist}. Siguiente evento.")
                            break
                        else:
                            continue
                    if state == "timeout":
                        log(f"participants_list tardó demasiado: {plist} (intento {attempt}/3)")
                        try: driver.refresh()
                        except Exception: pass
                        time.sleep(random.uniform(1, 1.3))  # espera entre 0.2 y 0.3 segundos
                        if attempt < 3:
                            continue
                        else:
                            break
                    if state == "empty":
                        log(f"participants_list sin participantes: {plist}")
                        break

                    # Con toggles, seguimos
                    booking_ids = _collect_booking_ids(driver)
                    total = len(booking_ids)
                    log(f"Toggles/participantes detectados: {total}")

                    for idx, pid in enumerate(booking_ids, start=1):
                        if idx % 25 == 0 or idx == total:
                            log(f"  - Progreso participantes: {idx}/{total}")
                        if not pid:
                            continue

                        block_el = _click_toggle_by_pid(
                            driver, pid, By, WebDriverWait, EC, TimeoutException,
                            StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException
                        )
                        if not block_el:
                            continue

                        painted = False
                        end = time.time() + PER_PART_TIMEOUT_S
                        while time.time() < end:
                            try:
                                strongs = block_el.find_elements(
                                    By.XPATH, ".//div[contains(@class,'font-bold') and contains(@class,'text-sm')]"
                                )
                                if strongs:
                                    painted = True
                                    break
                            except StaleElementReferenceException:
                                block_el = _click_toggle_by_pid(
                                    driver, pid, By, WebDriverWait, EC, TimeoutException,
                                    StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException
                                )
                            time.sleep(RENDER_POLL_S)
                        if not painted:
                            continue

                        try:
                            payload = driver.execute_script(JS_MAP_PARTICIPANT_RICH, pid)
                        except JavascriptException:
                            payload = _fallback_map_participant(driver, pid, By)

                        if not payload or not isinstance(payload, dict):
                            continue

                        fields = (payload.get("fields") or {})
                        schedule = (payload.get("schedule") or [])

                        def pick(keys, default="No disponible"):
                            for k in keys:
                                v = fields.get(k)
                                if v: return _clean(v)
                            return default

                        row = {
                            "participants_url": plist,
                            "BinomID": pid,
                            "Dorsal": pick(["Dorsal"]),
                            "Guía": pick(["Guía","Guia"]),
                            "Perro": pick(["Perro"]),
                            "Raza": pick(["Raza"]),
                            "Edad": pick(["Edad"]),
                            "Género": pick(["Género","Genero"]),
                            "Altura (cm)": pick(["Altura (cm)","Altura"]),
                            "Nombre de Pedigree": pick(["Nombre de Pedigree","Nombre de Pedrigree"]),
                            "País": pick(["País","Pais"]),
                            "Licencia": pick(["Licencia"]),
                            "Club": pick(["Club"]),
                            "Federación": pick(["Federación","Federacion"]),
                            "Equipo": pick(["Equipo"]),
                            "event_uuid": uuid,
                            "event_title": ev.get("title","N/D"),
                        }

                        for i, item in enumerate(schedule, start=1):
                            row[f"Día {i}"]    = _clean(item.get("day")   or "")
                            row[f"Fecha {i}"]  = _clean(item.get("fecha") or "")
                            row[f"Mangas {i}"] = _clean(item.get("mangas")or "")

                        if any(v not in ("No disponible","") for k,v in row.items()
                               if not (k.startswith("Día ") or k.startswith("Fecha ") or k.startswith("Mangas "))):
                            all_part_rows.append(row)

                        if idx % 100 == 0 and idx < total:
                            driver.execute_script("window.scrollBy(0, -200);")
                            time.sleep(0.1)

                    break  # fin participants para este evento

            if time.time() - start_event_ts > MAX_EVENT_SECONDS:
                log(f"Evento {uuid} superó {MAX_EVENT_SECONDS}s. Continuo con el siguiente.")
                continue

        # Guardar CSVs
        _write_csv(
            csv_event,
            all_event_rows,
            header=[
                "uuid","event_url","title","organizer","location","dates",
                "header_1","header_2","header_3","header_4","header_5","header_6",
                "judges"
            ],
        )
        part_header = _build_participants_header(all_part_rows)
        _write_csv(csv_part, all_part_rows, header=part_header)

        # Resumen
        print("\n--- RESUMEN SCRAPE ---")
        print(f"Eventos guardados:      {len(all_event_rows)} -> {csv_event}")
        print(f"Participantes guardados:{len(all_part_rows)} -> {csv_part}")
        if all_event_rows:
            ej = all_event_rows[0]
            print(f"Ejemplo evento: {ej.get('title','N/D')} | {ej.get('location','N/D')} | {ej.get('dates','N/D')}")
        print("-----------------------\n")

    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()
        _save_screenshot(driver, f"error_{int(time.time())}.png")
        sys.exit(1)
    finally:
        driver.quit()

# ============================== PARTE 2: PROCESADO ==============================
import pandas as pd
import numpy as np
from dateutil import parser

def _resolve_csv(preferred_today_pattern: str, fallback_pattern: str, extra_dirs=()):
    parent = Path(OUT_DIR)
    search_dirs = [parent, *map(Path, extra_dirs)]
    def glob_all(pattern):
        out = []
        for d in search_dirs:
            if d.exists():
                out += list(d.glob(pattern))
        return out

    cand_today = glob_all(preferred_today_pattern)
    if cand_today:
        cand_today.sort(key=lambda p: p.stat().st_mtime)
        return cand_today[-1]

    candidates = glob_all(fallback_pattern)
    if not candidates:
        raise FileNotFoundError(
            f"No se encontró ningún CSV con patrón '{preferred_today_pattern}' ni '{fallback_pattern}' "
            f"en: " + ", ".join(str(d) for d in search_dirs)
        )
    def date_key(p: Path):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
        return m.group(1) if m else "0000-00-00"
    candidates.sort(key=lambda p: (date_key(p), p.stat().st_mtime))
    return candidates[-1]

def to_spanish_dd_mm_yyyy(val):
    if not isinstance(val, str) or not val.strip():
        return val
    try:
        dt = parser.parse(val, dayfirst=True, fuzzy=True)
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return val

def strip_accents(s):
    if not isinstance(s, str):
        return s
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

VALID_GRADO = {"G3","G2","G1","PRE","PROM","COMP","ROOKIES","TRIATHLON"}
VALID_CAT   = {"I","L","M","S","XS","20","30","40","50","60"}
VALID_EXTRA = {"J12","J15","J19","SEN","PA","MST","ESP"}

GRADO_SYNS = {
    r"\bG\s*3\b": "G3", r"\bG\s*2\b": "G2", r"\bG\s*1\b": "G1",
    r"\bGRADO\s*3\b": "G3", r"\bGRADO\s*2\b": "G2", r"\bGRADO\s*1\b": "G1",
    r"\bPRE\b": "PRE", r"\bPRE\s*AGILITY\b": "PRE", r"\bPREAGILITY\b": "PRE",
    r"\bPROM\b": "PROM", r"\bPROMO(?!c)": "PROM", r"\bPROMOCION\b": "PROM",
    r"\bCOMP\b": "COMP", r"\bCOMPET(ICI[OÓ]N|ITION)?\b": "COMP",
    r"\bROOK(IE|IES)?\b": "ROOKIES",
    r"\bTRIAT(H?L)ON\b": "TRIATHLON", r"\bTRIATLON\b": "TRIATHLON",
}
CAT_SYNS = {
    r"\bXS(MALL)?\b": "XS", r"\bX[-\s]?SMALL\b": "XS", r"\bTOY\b": "XS", r"\bEXTRA\s*SMALL\b": "XS",
    r"\bS(MALL)?\b": "S",
    r"\bM(EDIUM)?\b": "M",
    r"\bL(ARGE)?\b": "L",
    r"\bI(NTER(MEDIATE)?)?\b": "I", r"\bINTERMED(IO|IA|IATE)\b": "I",
}
EXTRA_SYNS = {
    r"\bJ\s*1\s*2\b": "J12", r"\bJUNIOR\s*12\b": "J12", r"\bJ12\b": "J12",
    r"\bJ\s*1\s*5\b": "J15", r"\bJUNIOR\s*15\b": "J15", r"\bJ15\b": "J15",
    r"\bJ\s*1\s*9\b": "J19", r"\bJUNIOR\s*19\b": "J19", r"\bJ19\b": "J19",
    r"\bSEN(IOR)?\b": "SEN", r"\bPA(RA(GILITY)?)?\b": "PA",
    r"\bM(Á|A)STER\b": "MST", r"\bMST\b": "MST",
    r"\bESP(ECIAL)?\b": "ESP",
}

def robust_parse_mangas(manga_val, federacion_val):
    grado = None; cat = None; extra = None
    raw = manga_val if isinstance(manga_val, str) else ""
    txt = strip_accents(raw).upper()
    txt = re.sub(r"[|,;]+", " ", txt)
    paren = re.findall(r"\(([^)]+)\)", txt)

    heights = re.findall(r"\b(20|30|40|50|60)\b", txt)
    if heights and heights[0] in VALID_CAT:
        cat = heights[0]

    if cat is None:
        for pat, canon in CAT_SYNS.items():
            if re.search(pat, txt):
                cat = canon
                break

    for source in [txt] + paren:
        if source is None: 
            continue
        src = str(source)
        for pat, canon in EXTRA_SYNS.items():
            if re.search(pat, src):
                extra = canon
                break
        if extra:
            break

    for pat, canon in GRADO_SYNS.items():
        if re.search(pat, txt):
            grado = canon
            break

    if "/" in txt and (grado is None or (cat is None and extra is None)):
        m = re.match(r"^\s*([^/()]+)?\s*/\s*([^(]+?)\s*(?:\(([^)]+)\))?\s*$", txt)
        if m:
            before = m.group(1).strip() if m.group(1) else ""
            after  = m.group(2).strip() if m.group(2) else ""
            inpar  = m.group(3).strip() if m.group(3) else ""

            if grado is None:
                for pat, canon in GRADO_SYNS.items():
                    if re.search(pat, before):
                        grado = canon
                        break
                if grado is None and before in VALID_GRADO:
                    grado = before

            if cat is None:
                h = re.search(r"\b(20|30|40|50|60)\b", after)
                if h:
                    cat = h.group(1)
                else:
                    for pat, canon in CAT_SYNS.items():
                        if re.search(pat, after):
                            cat = canon
                            break
                if cat is None and after in VALID_CAT:
                    cat = after

            if extra is None and inpar:
                for pat, canon in EXTRA_SYNS.items():
                    if re.search(pat, inpar):
                        extra = canon
                        break
                if extra is None and inpar in VALID_EXTRA:
                    extra = inpar

    fed = strip_accents(str(federacion_val or "")).upper().strip()
    if fed.startswith("FED"):
        if "/" in txt:
            after = re.split(r"/", txt, maxsplit=1)[1]
            letras = re.sub(r"[^A-ZÑ ]+", " ", after).strip()
            num = re.search(r"\b(20|30|40|50|60)\b", after)
            talla = None
            for pat, canon in CAT_SYNS.items():
                if re.search(pat, after):
                    talla = canon
                    break

            if grado is None and letras:
                assigned = False
                for pat, canon in GRADO_SYNS.items():
                    if re.search(pat, letras):
                        grado = canon
                        assigned = True
                        break
                if not assigned and letras in VALID_GRADO:
                    grado = letras

            if cat is None:
                if num:
                    cat = num.group(1)
                elif talla:
                    cat = talla

            if extra is None and paren:
                for src in paren:
                    for pat, canon in EXTRA_SYNS.items():
                        if re.search(pat, src):
                            extra = canon
                            break
                    if extra:
                        break

    if grado not in VALID_GRADO:
        grado = ""
    if cat not in VALID_CAT:
        cat = ""
    if extra not in VALID_EXTRA:
        extra = ""

    return grado, cat, extra

def process_main():
    _print_effective_config()

    events_csv = _resolve_csv(preferred_today_pattern=f"events_{DATE_STR}*.csv",
                              fallback_pattern="events_*.csv",
                              extra_dirs=[OUT_DIR])
    parts_csv  = _resolve_csv(preferred_today_pattern=f"participants_{DATE_STR}*.csv",
                              fallback_pattern="participants_*.csv",
                              extra_dirs=[OUT_DIR])

    output_csv = next_free_path(os.path.join(OUT_DIR, f"participantes_procesado_{DATE_STR}.csv"))

    print("Leyendo de:", events_csv)
    print("Leyendo de:", parts_csv)
    print("Guardando en:", output_csv)

    # Carga
    events = pd.read_csv(events_csv, dtype=str).replace({"": np.nan})
    participants = pd.read_csv(parts_csv, dtype=str).replace({"": np.nan})

    # Seguridad: uuid único
    if "uuid" in events.columns:
        events = events.drop_duplicates(subset=["uuid"])

    # Base participants
    pt_cols = ["event_uuid","event_title","BinomID","Dorsal","Guía","Perro","Raza","Edad","Género",
               "Altura (cm)","Licencia","Club","Federación"]
    faltan = [c for c in pt_cols if c not in participants.columns]
    if faltan:
        raise ValueError(f"Faltan columnas en participants: {faltan}")

    pt_sel = participants[pt_cols].copy()

    # Edad → años (float)
    def edad_to_years_numeric(s):
        if pd.isna(s): 
            return np.nan
        if isinstance(s, (int, float)):
            return float(s)
        text = str(s).lower().strip().replace(",", ".")
        years = 0.0; months = 0.0
        my = re.search(r"(\d+(?:\.\d+)?)\s*a(?:ño|nios|ños)?", text)
        if my: years = float(my.group(1))
        mm = re.search(r"(\d+(?:\.\d+)?)\s*m(?:es|eses)?", text)
        if mm: months = float(mm.group(1))
        if my or mm: 
            return years + months/12.0
        try:
            return float(text)
        except Exception:
            return np.nan
    pt_sel["Edad"] = pt_sel["Edad"].apply(edad_to_years_numeric)

    # Fechas 1..6 normalizadas (si existen)
    fecha_cols = [f"Fecha {i}" for i in range(1,7) if f"Fecha {i}" in participants.columns]
    for c in fecha_cols:
        pt_sel[c] = participants[c].apply(to_spanish_dd_mm_yyyy)

    # Mangas -> Grado, Cat, CatExtra
    mangas_cols = [c for c in participants.columns if c.startswith("Mangas")]
    if mangas_cols:
        first_manga = participants[mangas_cols].bfill(axis=1).iloc[:, 0]
    else:
        first_manga = pd.Series([np.nan]*len(participants), index=participants.index)
    fed_series = participants["Federación"] if "Federación" in participants.columns else pd.Series([np.nan]*len(participants), index=participants.index)

    parsed = [robust_parse_mangas(mv, fv) for mv, fv in zip(first_manga, fed_series)]
    grado, cat, catextra = zip(*parsed) if parsed else ([], [], [])
    pt_sel["Grado"] = list(grado)
    pt_sel["Cat"] = [str(c) if c is not None else "" for c in cat]
    pt_sel["CatExtra"] = list(catextra)

    # Events y unión
    sel_cols = ["uuid","event_url","title","organizer","location","dates"]
    for col in sel_cols:
        if col not in events.columns:
            events[col] = np.nan
    ev_sel = events[sel_cols].copy()
    ev_sel["dates"] = ev_sel["dates"].apply(to_spanish_dd_mm_yyyy)

    merged = pt_sel.merge(ev_sel, left_on="event_uuid", right_on="uuid", how="left")

    # Fallback de título
    mask_missing = merged["title"].isna() | (merged["title"].astype(str).str.strip().eq("N/D"))
    merged.loc[mask_missing, "title"] = merged.loc[mask_missing, "event_title"].fillna("N/D")

    # Orden y salida segura
    src_cols = ["event_url","title","organizer","location","dates",
                "BinomID","Dorsal","Guía","Perro","Raza","Edad","Género","Altura (cm)",
                "Licencia","Club","Federación","Grado","Cat","CatExtra"] + fecha_cols
    final = merged.reindex(columns=src_cols, fill_value="").copy()

    RENAME_MAP = {
        "title": "PruebaNom",
        "organizer": "Organiza",
        "location": "Lugar",
        "dates": "Fechas",
        "Guía": "Guia",
        "Género": "SexoPerro",
        "Altura (cm)": "AlturaPerro",
        "Federación": "Federacion",
    }
    final.rename(columns=RENAME_MAP, inplace=True)

    target_cols = ["event_url","PruebaNom","Organiza","Lugar","Fechas",
                   "BinomID","Dorsal","Guia","Perro","Raza","Edad","SexoPerro","AlturaPerro",
                   "Licencia","Club","Federacion","Grado","Cat","CatExtra"] + fecha_cols
    final = final.reindex(columns=target_cols, fill_value="")

    print("Titles N/D tras fallback:", (final["PruebaNom"]=="N/D").sum())

    # Mantener 1:1 con participants (mismo índice)
    final = final.reindex(index=participants.index)
    if len(final) != len(participants):
        print(f"AVISO: Filas finales {len(final)} != participants {len(participants)}. Guardo igualmente para inspección.")

    final.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"OK -> {output_csv} | filas = {len(final)}")

    # ----------- BLOQUE EXTRA: “Pruebas próximas” por fecha -----------
    # Nos basamos en el CSV de eventos (únicos) para no duplicar por participante.
    try:
        _print_upcoming_from_events(ev_sel)
    except Exception as e:
        print(f"(Aviso) No se pudieron listar 'pruebas próximas': {e}")

def _parse_start_date_from_spanish_range(s: str):
    """
    Recibe 'Fechas' (p.ej. '31-07-2024 – 04-08-2024' o '31/07/2024-04/08/2024' o '31-07-2024')
    y devuelve la fecha de INICIO como datetime.date. Si no puede, devuelve None.
    """
    if not isinstance(s, str) or not s.strip():
        return None
    txt = s.replace("—", "-").replace("–", "-")
    # Intenta coger el primer bloque que parezca fecha
    # Separadores comunes
    parts = re.split(r"\s*[-–—aA]\s*|\s+al\s+|\s*hasta\s+", txt)
    # parts ahora tiene candidatos; probamos el primero que parezca fecha
    for chunk in parts:
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            dt = parser.parse(chunk, dayfirst=True, fuzzy=True)
            return dt.date()
        except Exception:
            continue
    # Último intento: extraer dd-mm-yyyy explícito
    m = re.search(r"\b(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b", txt)
    if m:
        try:
            return parser.parse(m.group(1), dayfirst=True, fuzzy=True).date()
        except Exception:
            pass
    return None

def _print_upcoming_from_events(ev_df: pd.DataFrame, horizon_days: int = 60):
    """
    Toma el DataFrame de eventos con columnas: uuid/event_url/title/organizer/location/dates
    y muestra las pruebas cuyo inicio está en [hoy, hoy+horizon]
    """
    print("\n===== PRUEBAS PRÓXIMAS =====")
    today = datetime.now().date()
    horizon = today + timedelta(days=horizon_days)

    # Evitar duplicados por event_url/título
    df = ev_df.copy()
    if "event_url" in df.columns:
        df = df.drop_duplicates(subset=["event_url"])
    elif "title" in df.columns:
        df = df.drop_duplicates(subset=["title"])

    # Parseo de inicio
    starts = []
    for i, row in df.iterrows():
        s = str(row.get("dates") or "")
        start_date = _parse_start_date_from_spanish_range(s)
        starts.append(start_date)
    df["start_date"] = starts

    mask = df["start_date"].notna() & (df["start_date"] >= today) & (df["start_date"] <= horizon)
    up = df.loc[mask].sort_values("start_date")

    if up.empty:
        print(f"No hay pruebas en los próximos {horizon_days} días.")
        print("============================\n")
        return

    # Agrupar por semana natural (Lunes-domingo) o por fecha exacta
    # Aquí usamos fecha exacta.
    for d, grp in up.groupby("start_date", sort=True):
        print(f"\n>>> {d.strftime('%d-%m-%Y')}")
        for _, r in grp.iterrows():
            title = (r.get("title") or "N/D")
            loc   = (r.get("location") or "N/D")
            url   = (r.get("event_url") or "")
            print(f"  · {title}  —  {loc}  {('[' + url + ']') if url else ''}")
    print("\n============================\n")

# ============================== CLI / Entry point ==============================
def main():
    parser = argparse.ArgumentParser(description="FlowAgility scraper + procesado (+ próximas)")
    parser.add_argument("cmd", choices=["scrape","process","all"], nargs="?", default="all", help="Qué ejecutar")

    args = parser.parse_args()

    if args.cmd == "scrape":
        scrape_main()
    elif args.cmd == "process":
        process_main()
    else:  # all
        scrape_main()
        process_main()

if __name__ == "__main__":
    main()
