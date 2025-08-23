#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FlowAgility scraper + procesado (dos pasos, un archivo).
Subcomandos:
  - scrape  : descarga eventos y participantes (CSV diarios con reanudación)
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

  # NUEVO (opcional):
  RESUME=true                 # reanudar si existe progreso del día
  SLOW_MIN_S=1.0              # pausa mínima aleatoria entre acciones clave
  SLOW_MAX_S=3.0              # pausa máxima aleatoria entre acciones clave
  MAX_SCROLLS=24
  SCROLL_WAIT_S=2.0
  PER_PART_TIMEOUT_S=10
  RENDER_POLL_S=0.25
  MAX_EVENT_SECONDS=1800
"""

import os, csv, sys, re, traceback, unicodedata, argparse, json
from datetime import datetime, timedelta
from urllib.parse import urljoin
from pathlib import Path
import time, random

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
SCROLL_WAIT_S      = _env_float("SCROLL_WAIT_S", 2.0)
CLICK_RETRIES      = _env_int("CLICK_RETRIES", 3)
PER_PART_TIMEOUT_S = _env_float("PER_PART_TIMEOUT_S", 10)
RENDER_POLL_S      = _env_float("RENDER_POLL_S", 0.25)
MAX_EVENT_SECONDS  = _env_int("MAX_EVENT_SECONDS", 1800)
OUT_DIR            = os.path.abspath(os.getenv("OUT_DIR", "./ListaEventos"))
RESUME             = _env_bool("RESUME", True)
SLOW_MIN_S         = _env_float("SLOW_MIN_S", 1.0)
SLOW_MAX_S         = _env_float("SLOW_MAX_S", 3.0)

os.makedirs(OUT_DIR, exist_ok=True)

DATE_STR = datetime.now().strftime("%Y-%m-%d")
UUID_RE  = re.compile(r"/zone/events/([0-9a-fA-F-]{36})(?:/.*)?$")

# Ficheros del día (estables para reanudar)
CSV_EVENT_PATH = os.path.join(OUT_DIR, f"events_{DATE_STR}.csv")
CSV_PART_PATH  = os.path.join(OUT_DIR, f"participants_{DATE_STR}.csv")
PROGRESS_PATH  = os.path.join(OUT_DIR, f"progress_{DATE_STR}.json")

# ----------------------------- Utilidades logging / pausas -----------------------------
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
    print(f"RESUME               = {RESUME}")
    print(f"SLOW_MIN_S           = {SLOW_MIN_S}")
    print(f"SLOW_MAX_S           = {SLOW_MAX_S}")
    print(f"CHROME_BINARY        = {os.getenv('CHROME_BINARY') or ''}")
    print(f"CHROMEDRIVER_PATH    = {os.getenv('CHROMEDRIVER_PATH') or ''}")
    print("=======================")

def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def slow_pause(min_s=None, max_s=None):
    """Pausa aleatoria para no saturar; usa .env si no se indica."""
    a = SLOW_MIN_S if min_s is None else float(min_s)
    b = SLOW_MAX_S if max_s is None else float(max_s)
    if b < a: a, b = b, a
    time.sleep(random.uniform(a, b))

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

# ============================== Progreso (resume) ==============================
def _load_progress():
    if RESUME and os.path.exists(PROGRESS_PATH):
        try:
            with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"done_events": [], "last_part_index": {}, "total_found": 0}

def _save_progress(state):
    tmp = PROGRESS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PROGRESS_PATH)

def _is_event_done(state, uuid):
    return uuid in set(state.get("done_events", []))

def _get_last_part_index(state, uuid):
    return int(state.get("last_part_index", {}).get(uuid, 0))

def _set_last_part_index(state, uuid, idx):
    state.setdefault("last_part_index", {})[uuid] = int(idx)
    _save_progress(state)

def _mark_event_done(state, uuid):
    de = set(state.get("done_events", []))
    if uuid not in de:
        de.add(uuid)
        state["done_events"] = sorted(de)
        _save_progress(state)

# ============================== CSV seguro (append + cabecera si no existe) ==============================
def _ensure_csv_header(path, header):
    exist = os.path.exists(path)
    if not exist:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()

def _append_csv_row(path, header, row_dict):
    _ensure_csv_header(path, header)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writerow({k: row_dict.get(k, "") for k in header})

# ============================== PARTE 1: SCRAPER ==============================
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
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")

    chrome_bin = os.getenv("CHROME_BINARY")
    if chrome_bin:
        opts.binary_location = chrome_bin

    driver_path = os.getenv("CHROMEDRIVER_PATH", "").strip()
    if driver_path:
        service = Service(executable_path=driver_path)
        return webdriver.Chrome(service=service, options=opts)
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
    log("Login…")
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
    log("Login OK.")

def _ensure_logged_in(driver, max_tries, By, WebDriverWait, EC):
    for _ in range(max_tries):
        if not _is_login_page(driver):
            return True
        log("Sesión caducada. Reintentando login…")
        _login(driver, By, WebDriverWait, EC)
        slow_pause(0.5, 1.2)
        if not _is_login_page(driver):
            return True
    return False

def _full_scroll(driver):
    last_h = 0
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)  # ritmo de scroll configurable
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            break
        last_h = h

def _collect_event_urls(driver, By, WebDriverWait, EC):
    driver.get(EVENTS_URL)
    WebDriverWait(driver, 25).until(lambda d: d.find_element(By.TAG_NAME, "body"))
    _accept_cookies(driver, By)
    _full_scroll(driver)
    slow_pause()

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
    slow_pause(0.4, 0.8)

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
            slow_pause(0.4, 0.8)
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

# --- JS para mapear participante ---
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
            slow_pause(0.2, 0.5)
            driver.execute_script("window.scrollBy(0, 120);")
            slow_pause(0.2, 0.5)
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

# ----------------------------- Cabeceras CSV canónicas -----------------------------
EVENT_HEADER = [
    "uuid","event_url","title","organizer","location","dates",
    "header_1","header_2","header_3","header_4","header_5","header_6",
    "judges"
]

# Para facilitar append, fijamos 6 bloques Día/Fecha/Mangas
PART_BASE = [
    "participants_url","BinomID","Dorsal","Guía","Perro","Raza","Edad","Género",
    "Altura (cm)","Nombre de Pedigree","País","Licencia","Club","Federación","Equipo",
    "event_uuid","event_title"
]
PART_SLOTS = []
for i in range(1, 7):
    PART_SLOTS += [f"Día {i}", f"Fecha {i}", f"Mangas {i}"]
PART_HEADER = PART_BASE + PART_SLOTS

def scrape_main():
    _print_effective_config()

    (webdriver, By, Options, WebDriverWait, EC,
     JavascriptException, StaleElementReferenceException,
     NoSuchElementException, ElementClickInterceptedException, TimeoutException) = _import_selenium()

    # Si no reanudamos, versiona los nombres para no pisar
    csv_event = CSV_EVENT_PATH if RESUME else next_free_path(CSV_EVENT_PATH)
    csv_part  = CSV_PART_PATH  if RESUME else next_free_path(CSV_PART_PATH)

    # Asegura cabeceras si reanudamos o empezamos
    _ensure_csv_header(csv_event, EVENT_HEADER)
    _ensure_csv_header(csv_part, PART_HEADER)

    # Carga/crea progreso
    state = _load_progress()

    driver = _get_driver()
    try:
        _login(driver, By, WebDriverWait, EC)
        urls_by_uuid = _collect_event_urls(driver, By, WebDriverWait, EC)
        log(f"Eventos (UUIDs) encontrados: {len(urls_by_uuid)}")
        state["total_found"] = len(urls_by_uuid)
        _save_progress(state)

        # --- Itera eventos (respetando progreso) ---
        for uuid, pair in urls_by_uuid.items():
            if _is_event_done(state, uuid):
                # Ya se terminó este evento anteriormente
                continue

            start_event_ts = time.time()
            base_url = pair["base"]
            plist    = pair["plist"]

            # 1) INFO DEL EVENTO -> append inmediato
            ev = _scrape_event_info(driver, base_url, plist, By, WebDriverWait, EC)
            ev_row = {
                "uuid": uuid,
                "event_url": ev.get("event_url",""),
                "title": ev.get("title","N/D"),
                "organizer": ev.get("organizer","N/D"),
                "location": ev.get("location","N/D"),
                "dates": ev.get("dates","N/D"),
                "header_1": ev.get("header_1",""),
                "header_2": ev.get("header_2",""),
                "header_3": ev.get("header_3",""),
                "header_4": ev.get("header_4",""),
                "header_5": ev.get("header_5",""),
                "header_6": ev.get("header_6",""),
                "judges": ev.get("judges","N/D"),
            }
            _append_csv_row(csv_event, EVENT_HEADER, ev_row)
            slow_pause()  # pausa tras guardar

            # 2) PARTICIPANTES (si hay participants_list)
            if plist:
                # Detecta lista y participantes
                for attempt in range(1, 4):
                    driver.get(plist)
                    WebDriverWait(driver, 25).until(lambda d: d.find_element(By.TAG_NAME, "body"))
                    _accept_cookies(driver, By)
                    slow_pause(1.0, 2.0)

                    start = time.time()
                    state_page = "timeout"
                    while time.time() - start < 20:
                        if _is_login_page(driver):
                            state_page = "login"; break
                        toggles = driver.find_elements(By.CSS_SELECTOR, "[phx-click='booking_details_show']")
                        if toggles:
                            state_page = "toggles"; break
                        hints = (
                            "//p[contains(., 'No hay') or contains(., 'No results') or contains(., 'Sin participantes')]",
                            "//div[contains(., 'No hay') or contains(., 'No results') or contains(., 'Sin participantes')]",
                        )
                        if any(driver.find_elements(By.XPATH, xp) for xp in hints):
                            state_page = "empty"; break
                        time.sleep(0.25)

                    if state_page == "login":
                        if not _ensure_logged_in(driver, 2, By, WebDriverWait, EC):
                            log(f"No se pudo relogar para {plist}. Siguiente evento.")
                            break
                        else:
                            continue
                    if state_page == "timeout":
                        log(f"participants_list tardó demasiado: {plist} (intento {attempt}/3)")
                        try: driver.refresh()
                        except Exception: pass
                        slow_pause(0.8, 1.3)
                        if attempt < 3:
                            continue
                        else:
                            break
                    if state_page == "empty":
                        log(f"participants_list sin participantes: {plist}")
                        break

                    # Con toggles, seguimos
                    booking_ids = _collect_booking_ids(driver)
                    total = len(booking_ids)
                    log(f"Toggles/participantes detectados: {total}")

                    # Reanudar dentro del evento
                    start_idx = _get_last_part_index(state, uuid) + 1  # 1-based
                    if start_idx > total:
                        start_idx = total + 1

                    for idx, pid in enumerate(booking_ids, start=1):
                        if idx < start_idx:
                            continue  # ya procesado

                        if idx % 25 == 0 or idx == total:
                            log(f"  - Progreso participantes: {idx}/{total}")

                        if not pid:
                            _set_last_part_index(state, uuid, idx)
                            continue

                        block_el = _click_toggle_by_pid(
                            driver, pid, By, WebDriverWait, EC, TimeoutException,
                            StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException
                        )
                        if not block_el:
                            _set_last_part_index(state, uuid, idx)
                            slow_pause(0.2, 0.5)
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
                            _set_last_part_index(state, uuid, idx)
                            slow_pause(0.2, 0.5)
                            continue

                        try:
                            payload = driver.execute_script(JS_MAP_PARTICIPANT_RICH, pid)
                        except Exception:
                            payload = None
                        if not payload or not isinstance(payload, dict):
                            # Fallback
                            payload = _fallback_map_participant(driver, pid, By)

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
                        # Rellena slots fijos 1..6
                        for i in range(1, 7):
                            day = schedule[i-1]["day"] if i-1 < len(schedule) else ""
                            fec = schedule[i-1]["fecha"] if i-1 < len(schedule) else ""
                            man = schedule[i-1]["mangas"] if i-1 < len(schedule) else ""
                            row[f"Día {i}"]    = _clean(day)
                            row[f"Fecha {i}"]  = _clean(fec)
                            row[f"Mangas {i}"] = _clean(man)

                        # Escribe inmediatamente
                        _append_csv_row(csv_part, PART_HEADER, row)
                        # Guarda progreso por participante
                        _set_last_part_index(state, uuid, idx)
                        slow_pause()  # respiración entre participantes

                    # terminado el bucle de participantes
                    break  # evento con participants_list ya tratado

            # Marcar evento finalizado (con o sin participantes)
            _mark_event_done(state, uuid)

            if time.time() - start_event_ts > MAX_EVENT_SECONDS:
                log(f"Evento {uuid} superó {MAX_EVENT_SECONDS}s (continuo con el siguiente).")
                continue

        # Resumen (lee tamaños de archivo como índice rápido)
        ev_count = sum(1 for _ in open(csv_event, "r", encoding="utf-8-sig")) - 1 if os.path.exists(csv_event) else 0
        pt_count = sum(1 for _ in open(csv_part,  "r", encoding="utf-8-sig")) - 1 if os.path.exists(csv_part)  else 0
        print("\n--- RESUMEN SCRAPE ---")
        print(f"Eventos guardados (líneas):      {ev_count} -> {csv_event}")
        print(f"Participantes guardados (líneas):{pt_count} -> {csv_part}")
        print(f"Progreso en: {PROGRESS_PATH}")
        print("-----------------------\n")

    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()
        try: _save_screenshot(driver, f"error_{int(time.time())}.png")
        except Exception: pass
        sys.exit(1)
    finally:
        try: driver.quit()
        except Exception: pass

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
    try:
        _print_upcoming_from_events(ev_sel)
    except Exception as e:
        print(f"(Aviso) No se pudieron listar 'pruebas próximas': {e}")

def _parse_start_date_from_spanish_range(s: str):
    if not isinstance(s, str) or not s.strip():
        return None
    txt = s.replace("—", "-").replace("–", "-")
    parts = re.split(r"\s*[-–—aA]\s*|\s+al\s+|\s*hasta\s+", txt)
    for chunk in parts:
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            dt = parser.parse(chunk, dayfirst=True, fuzzy=True)
            return dt.date()
        except Exception:
            continue
    m = re.search(r"\b(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b", txt)
    if m:
        try:
            return parser.parse(m.group(1), dayfirst=True, fuzzy=True).date()
        except Exception:
            pass
    return None

def _print_upcoming_from_events(ev_df: pd.DataFrame, horizon_days: int = 60):
    print("\n===== PRUEBAS PRÓXIMAS =====")
    today = datetime.now().date()
    horizon = today + timedelta(days=horizon_days)

    df = ev_df.copy()
    if "event_url" in df.columns:
        df = df.drop_duplicates(subset=["event_url"])
    elif "title" in df.columns:
        df = df.drop_duplicates(subset=["title"])

    starts = []
    for _, row in df.iterrows():
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
    parser = argparse.ArgumentParser(description="FlowAgility scraper + procesado (+ próximas) con pausas y reanudación")
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
