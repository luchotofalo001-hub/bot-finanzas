import os
import io
import re
import time
import asyncio
import logging
import threading
import hashlib
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from google import genai
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pypdf

# ==================== LOGS ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
plt.ioff()

# ==================== SERVIDOR WEB PARA RENDER ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot activo")
    def log_message(self, format, *args):
        pass

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()

threading.Thread(target=run_web_server, daemon=True).start()

# ==================== CONFIGURACIÓN ====================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")
LUCHO_TELEGRAM_ID = 8429535344
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")

ALERTA_HORA_INICIO = 7
ALERTA_HORA_FIN = 22
ALERTA_INTERVALO_HORAS = 3
UMBRAL_LIQUIDACION_PCT = 15.0
UMBRAL_GASTO_INUSUAL = 2.2
UMBRAL_GASTO_HORMIGA_ARS = 15000.0
UMBRAL_AUTO_APROBAR = 3  # Si se confirmó 3 o más veces, se registra directo sin preguntar
UMBRAL_MOVIMIENTO_BRUSCO_PCT = 5.0
TICKERS_MERCADO_GRAL = ["SPY", "QQQ", "DIA", "BTC-USD", "ETH-USD"]

ai_client = genai.Client(api_key=GEMINI_API_KEY)

# Estado en memoria para importaciones interactivas
importaciones_activas = {}

_DB_POOL = None
_YF_CACHE = {}
_YF_TTL_SEG = 180
_YF_CACHE_MAX = 80

def _db_pool():
    global _DB_POOL
    if _DB_POOL is None:
        _DB_POOL = SimpleConnectionPool(1, 6, dsn=DATABASE_URL, connect_timeout=8)
    return _DB_POOL

class _PooledConn:
    def __init__(self):
        self.conn = _db_pool().getconn()
    def __enter__(self):
        return self.conn
    def __exit__(self, exc_type, exc, tb):
        try:
            if self.conn and not self.conn.closed:
                if exc_type:
                    self.conn.rollback()
        except Exception:
            pass
        try:
            _db_pool().putconn(self.conn)
        except Exception:
            try:
                self.conn.close()
            except Exception:
                pass
        return False

def get_db_connection():
    return _PooledConn()

def yf_history(symbol: str, start=None, period=None, auto_adjust=True, interval=None):
    key = (str(symbol), str(start), str(period), bool(auto_adjust), str(interval))
    now = time.time()
    hit = _YF_CACHE.get(key)
    if hit and now - hit[0] < _YF_TTL_SEG:
        return hit[1].copy()
    t = yf.Ticker(symbol)
    kwargs = {"auto_adjust": auto_adjust}
    if interval:
        kwargs["interval"] = interval
    if period:
        h = t.history(period=period, **kwargs)
    else:
        h = t.history(start=start, **kwargs)
    _YF_CACHE[key] = (now, h)
    if len(_YF_CACHE) > _YF_CACHE_MAX:
        viejo = min(_YF_CACHE.items(), key=lambda kv: kv[1][0])[0]
        _YF_CACHE.pop(viejo, None)
    return h

def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS movimientos (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        tipo VARCHAR(20),
                        monto NUMERIC,
                        categoria VARCHAR(50),
                        descripcion TEXT
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS portafolio_inversiones (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ticker VARCHAR(20),
                        cantidad NUMERIC,
                        precio_compra NUMERIC,
                        monto_total_usd NUMERIC,
                        tipo_posicion VARCHAR(10) DEFAULT 'SPOT',
                        apalancamiento NUMERIC DEFAULT 1,
                        precio_liquidacion NUMERIC DEFAULT NULL
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trades_cerrados (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ticker VARCHAR(20),
                        tipo_posicion VARCHAR(10) DEFAULT 'SPOT',
                        pnl_usd NUMERIC,
                        roi_pct NUMERIC DEFAULT NULL,
                        monto_invertido NUMERIC DEFAULT NULL,
                        descripcion TEXT,
                        fecha_apertura TIMESTAMP DEFAULT NULL
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS presupuestos (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        categoria VARCHAR(50),
                        monto_limite NUMERIC,
                        mes INTEGER,
                        anio INTEGER,
                        UNIQUE(user_id, categoria, mes, anio)
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS objetivos (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        descripcion TEXT,
                        tipo VARCHAR(30),
                        monto_objetivo NUMERIC,
                        fecha_limite DATE,
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS alertas_enviadas (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        tipo_alerta VARCHAR(50),
                        clave VARCHAR(100),
                        hash_alerta VARCHAR(64),
                        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mapeo_conceptos (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        patron_clave VARCHAR(100),
                        categoria VARCHAR(50),
                        descripcion_limpia TEXT,
                        usos_exitosos INTEGER DEFAULT 1,
                        UNIQUE(user_id, patron_clave)
                    );
                """)

                cursor.execute("ALTER TABLE mapeo_conceptos ADD COLUMN IF NOT EXISTS usos_exitosos INTEGER DEFAULT 1;")
                cursor.execute("ALTER TABLE mapeo_conceptos ADD COLUMN IF NOT EXISTS usos_rechazados INTEGER DEFAULT 0;")
                cursor.execute("ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS tipo_posicion VARCHAR(10) DEFAULT 'SPOT';")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS apalancamiento NUMERIC DEFAULT 1;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS precio_liquidacion NUMERIC DEFAULT NULL;")
                cursor.execute("ALTER TABLE trades_cerrados ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE trades_cerrados ADD COLUMN IF NOT EXISTS fecha_apertura TIMESTAMP;")

                cursor.execute("UPDATE movimientos SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))
                cursor.execute("UPDATE portafolio_inversiones SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))
                cursor.execute("UPDATE trades_cerrados SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_mov_user_fecha ON movimientos (user_id, fecha);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_mov_user_tipo ON movimientos (user_id, tipo);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_mapeo_user_clave ON mapeo_conceptos (user_id, patron_clave);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_inv_user ON portafolio_inversiones (user_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_tc_user_fecha ON trades_cerrados (user_id, fecha);")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mapeo_frases (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        frase VARCHAR(180),
                        comando VARCHAR(80),
                        usos INTEGER DEFAULT 1,
                        UNIQUE(user_id, frase)
                    );
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frases_user ON mapeo_frases (user_id, frase);")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS preferencias_usuario (
                        user_id BIGINT,
                        clave VARCHAR(40),
                        valor VARCHAR(40),
                        PRIMARY KEY (user_id, clave)
                    );
                """)
                conn.commit()
        logger.info("Base de datos inicializada correctamente.")
    except Exception as e:
        logger.error(f"Error en init_db: {e}")

init_db()

# ==================== UTILIDADES DE TIEMPO ====================
def ahora_argentina():
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=3)

def en_horario_alertas():
    h = ahora_argentina().hour
    return ALERTA_HORA_INICIO <= h < ALERTA_HORA_FIN

def es_dia_habil_arg() -> bool:
    return ahora_argentina().weekday() < 5

def get_pref(user_id: int, clave: str, default: str = "") -> str:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT valor FROM preferencias_usuario WHERE user_id = %s AND clave = %s;",
                    (user_id, clave),
                )
                row = cursor.fetchone()
                return str(row[0]) if row and row[0] is not None else default
    except Exception as e:
        logger.warning(f"get_pref: {e}")
        return default

def set_pref(user_id: int, clave: str, valor: str):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO preferencias_usuario (user_id, clave, valor)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (user_id, clave) DO UPDATE SET valor = EXCLUDED.valor;""",
                (user_id, clave, valor),
            )
            conn.commit()

def alertas_activas(user_id: int) -> bool:
    return get_pref(user_id, "alertas", "on") != "off"

# ==================== FORMATEO LIMPIO TELEGRAM ====================
def limpiar_estilo_telegram(texto: str) -> str:
    if not texto:
        return ""
    texto = re.sub(r"#{2,6}\s*", "", texto)
    texto = re.sub(r"^\s*[*•-]\s*\*\*(.*?)(?:\*\*:?|\*\*)\s*", r"• \1: ", texto, flags=re.MULTILINE)
    texto = re.sub(r"::\s*", ": ", texto)
    texto = re.sub(r"^\s*[*]\s+", r"• ", texto, flags=re.MULTILINE)
    texto = texto.replace("**", "")
    texto = re.sub(r"\n\s*---\s*\n", "\n\n", texto)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    return texto.strip()

# ==================== NORMALIZACIÓN DE CATEGORÍAS ====================
MAPA_CANONICO_CATEGORIAS = {
    "tarjetas de credito": "Tarjeta de crédito",
    "tarjetas de crédito": "Tarjeta de crédito",
    "tarjeta de credito": "Tarjeta de crédito",
    "tarjeta de crédito": "Tarjeta de crédito",
    "pago de tarjeta de credito": "Tarjeta de crédito",
    "pago de tarjeta": "Tarjeta de crédito",
    "pago tarjetas": "Tarjeta de crédito",
    "pago tarjeta": "Tarjeta de crédito",
    "tarjetas": "Tarjeta de crédito",
    "supermercados": "Supermercado",
    "supermercado": "Supermercado",
    "servicios basicos": "Servicios",
    "servicios básicos": "Servicios",
    "servicio": "Servicios",
    "servicios": "Servicios",
    "transferencia familiar": "Transferencias",
    "ingresos familiares": "Transferencias",
    "transferencias": "Transferencias",
    "transferencia": "Transferencias",
    "devolucion": "Devoluciones",
    "devolución": "Devoluciones",
    "panaderia": "Comida",
    "panadería": "Comida",
    "verduleria": "Comida",
    "verdulería": "Comida",
    "fiambreria": "Comida",
    "fiambrería": "Comida",
    "comida": "Comida",
    "cafe": "Comida",
    "café": "Comida",
    "delivery": "Delivery",
    "bazar": "Hogar",
    "hogar": "Hogar",
    "regalos": "Regalos",
    "regalo": "Regalos",
    "flores": "Regalos",
    "farmacia": "Farmacia",
    "transporte": "Transporte",
    "uber": "Transporte",
    "sube": "Transporte",
    "entretenimiento": "Entretenimiento",
    "cine": "Entretenimiento",
    "suscripciones": "Suscripciones",
    "suscripcion": "Suscripciones",
    "suscripción": "Suscripciones",
    "impuestos": "Impuestos",
    "impuesto": "Impuestos",
    "inversiones": "Inversiones",
    "inversion": "Inversiones",
    "inversión": "Inversiones",
    "sueldo": "Sueldo",
    "mascotas": "Mascotas",
    "mascota": "Mascotas",
    "arena": "Mascotas",
    "deportes": "Deportes",
    "futbol": "Deportes",
    "fútbol": "Deportes",
    "cuidado personal": "Cuidado personal",
    "barberia": "Cuidado personal",
    "barbería": "Cuidado personal",
    "compras": "Compras",
    "compra": "Compras",
}

def _emprolijar_texto(txt: str) -> str:
    t = " ".join(str(txt or "").split()).strip()
    if not t:
        return t
    minus = {"de", "del", "la", "el", "los", "las", "y", "a", "en", "para", "por"}
    parts = t.split(" ")
    out = []
    for i, w in enumerate(parts):
        wl = w.lower()
        if i > 0 and wl in minus:
            out.append(wl)
        else:
            out.append(wl[:1].upper() + wl[1:] if wl else w)
    return " ".join(out)

CATEGORIAS_VALIDAS = {
    "Tarjeta de crédito", "Supermercado", "Servicios", "Transferencias", "Devoluciones",
    "Comida", "Delivery", "Hogar", "Regalos", "Farmacia", "Transporte", "Entretenimiento",
    "Suscripciones", "Impuestos", "Inversiones", "Sueldo", "Mascotas", "Deportes",
    "Cuidado personal", "Compras", "Ingresos", "Ingresos financieros", "Varios",
}

def categoria_reconocida_por_python(texto: str) -> bool:
    tlow = " ".join(str(texto or "").split()).strip().lower()
    if not tlow:
        return False
    if "|" in tlow or "," in tlow:
        izq = tlow.split("|", 1)[0].split(",", 1)[0].strip()
        tlow = izq
    if tlow in MAPA_CANONICO_CATEGORIAS:
        return True
    for alias in sorted(MAPA_CANONICO_CATEGORIAS, key=len, reverse=True):
        if tlow == alias or tlow.startswith(alias + " "):
            return True
    return False

def emprolijar_categoria_con_gemini(user_text: str):
    prompt = (
        "El usuario escribió esto para clasificar un gasto o ingreso. "
        "Devolvé SOLO: CATEGORIA|DESCRIPCION\n"
        f"Texto: {user_text}"
    )
    res = llamar_gemini(prompt, "Clasificador breve. Responde solo CATEGORIA|DESCRIPCION.")
    partes = [p.strip() for p in (res or "").split("|")]
    cat = normalizar_categoria(partes[0]) if partes and partes[0] else "Varios"
    desc = _emprolijar_texto(partes[1]) if len(partes) > 1 and partes[1] else _emprolijar_texto(user_text)
    return cat, desc

def parsear_categoria_descripcion_usuario(texto: str):
    raw = " ".join(str(texto or "").split()).strip()
    if not raw:
        return "Varios", "Sin descripción"
    if "|" in raw:
        izq, der = raw.split("|", 1)
        cat = normalizar_categoria(izq)
        desc = _emprolijar_texto(der) or cat
        return cat, desc
    if "," in raw:
        izq, der = raw.split(",", 1)
        if len(izq.split()) <= 4:
            cat = normalizar_categoria(izq)
            desc = _emprolijar_texto(der) or cat
            return cat, desc
    tlow = raw.lower()
    alias_ord = sorted(MAPA_CANONICO_CATEGORIAS.items(), key=lambda x: len(x[0]), reverse=True)
    for alias, canon in alias_ord:
        if tlow == alias:
            return canon, canon
        if tlow.startswith(alias + " "):
            resto = raw[len(alias):].strip(" ,:-")
            return canon, _emprolijar_texto(resto) or canon
    partes = raw.split(None, 1)
    cat = normalizar_categoria(partes[0])
    desc = _emprolijar_texto(partes[1]) if len(partes) > 1 else cat
    return cat, desc or cat

def normalizar_categoria(cat: str) -> str:
    c_low = cat.strip().lower()
    return MAPA_CANONICO_CATEGORIAS.get(c_low, cat.strip().capitalize())

# ==================== REGLAS FIJAS PYTHON (0 tokens) ====================
# Alias de comercio / concepto → (categoria, descripcion). Orden: el match más largo gana.
REGLAS_COMERCIO_NL = [
    (("pedidosya market", "pedidos ya market", "pya market"), "Supermercado", "PedidosYa Market"),
    (("pedidosya extra", "pedidos ya extra"), "Delivery", "PedidosYa Extra"),
    (("pedidosya", "pedidos ya", "peya", "pedidosya delivery"), "Delivery", "PedidosYa Delivery"),
    (("rappi pro",), "Suscripciones", "Rappi Pro"),
    (("rappi",), "Delivery", "Rappi"),
    (("uber eats",), "Delivery", "Uber Eats"),
    (("uber shopper",), "Supermercado", "Uber Shopper"),
    (("uber",), "Transporte", "Uber"),
    (("cabify", "didi"), "Transporte", "Viaje app"),
    (("sube",), "Transporte", "Tarjeta SUBE"),
    (("farmacity", "farmacity reintegro"), "Farmacia", "Farmacity"),
    (("cinemark", "hoyts", "cinema"), "Entretenimiento", "Cine"),
    (("metrogas",), "Servicios", "Metrogas"),
    (("edesur",), "Servicios", "Edesur"),
    (("aysa",), "Servicios", "AySA"),
    (("telecentro", "fibertel", "personal flow", "flow"), "Servicios", "Internet"),
    (("netflix", "spotify", "youtube premium", "icloud", "openai", "chatgpt plus"), "Suscripciones", "Suscripción"),
    (("havas", "havas media"), "Sueldo", "Havas Media Argentina S.A."),
    (("fornodelpaese", "forno del paese"), "Comida", "Forno del Paese"),
    (("pettish",), "Hogar", "Pettish Bazar"),
    (("fima premium", "fima"), "Inversiones", "FIMA Premium"),
    (("compra dolares", "compra de dolares", "compra de dólares", "compra dólares"), "Inversiones", "Compra de dólares"),
    (("delfina silva", "delfina"), "Transferencias", "Transferencia de Delfina Silva"),
    (("marina luz", "ugariza", "mama", "mamá"), "Transferencias", "Transferencia familiar"),
    (("pago tarjeta master", "pago mastercard", "pago visa", "pago de tarjeta"), "Tarjeta de crédito", "Pago tarjeta"),
    (("panaderia", "panadería", "pan"), "Comida", "Panadería"),
    (("verduleria", "verdulería", "verdura"), "Comida", "Verdulería"),
    (("fiambreria", "fiambrería"), "Comida", "Fiambrería"),
    (("cafe", "café", "cafeteria", "cafetería"), "Comida", "Café"),
    (("super", "chino", "dia ", "carrefour", "coto", "jumbo", "vea", "disco"), "Supermercado", "Supermercado"),
    (("barberia", "barbería", "pelo", "corte de pelo"), "Cuidado personal", "Barbería"),
    (("arena", "piedras gato", "veterinaria"), "Mascotas", "Mascotas"),
    (("nafta", "ypf", "shell", "axion", "estacion de servicio"), "Transporte", "Combustible"),
]

VERBO_GASTO = (
    r"registr(a|á|ar|ame)|anot(a|á|ar|ame)|carg(a|á|ar|ame)|"
    r"gast(e|é|o|ar|aste)|pagu(e|é)|pag(o|ué|ue)|un pago|"
    r"compr(e|é|o|ar)|saqu(e|é)|transfer(i|í)|debito|débito|"
    r"me cobraron|me descontaron|salida"
)
VERBO_INGRESO = (
    r"ingreso|cobr(e|é|o)|me depositaron|me transfirieron|"
    r"sueldo|haberes|salario|n[oó]mina|acreditaron|"
    r"me devolvieron|reintegro|devoluci[oó]n"
)
VERBO_MOVIMIENTO = VERBO_GASTO + r"|" + VERBO_INGRESO

STOP_CLAVE_NL = {
    "REGISTRA", "REGISTRAR", "REGISTRAME", "ANOTA", "ANOTAR", "ANOTAME", "CARGA", "CARGAR",
    "GASTE", "GASTE", "GASTO", "PAGUE", "PAGO", "COMPRE", "COMPRA", "SAQUE",
    "INGRESO", "COBRE", "SUELDO", "UN", "UNA", "EL", "LA", "LOS", "LAS", "DE", "DEL",
    "EN", "POR", "CON", "PARA", "HOY", "AYER", "ARS", "PESOS", "PESO", "PLATA",
    "QUE", "ME", "MI", "LO", "LE", "SE", "AL", "THE", "AND",
}


def _parsear_numero_ars(bruto: str, sufijo: str = ""):
    s = (bruto or "").strip().replace("$", "").replace(" ", "")
    if not s:
        return None
    try:
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            izq, der = s.split(",", 1)
            if len(der) <= 2:
                s = izq.replace(".", "") + "." + der
            else:
                s = s.replace(",", "")
        elif s.count(".") == 1:
            izq, der = s.split(".")
            if len(der) == 3 and len(izq) <= 3:
                s = izq + der
        else:
            s = s.replace(".", "")
        val = float(s)
    except Exception:
        return None
    suf = (sufijo or "").lower().strip()
    if suf in ("k", "mil") and val < 100000:
        val *= 1000
    return val if val > 0 else None


def extraer_monto_ars(texto: str):
    low = (texto or "").lower()
    low = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", " ", low)
    low = re.sub(r"\b(20[2-3]\d)\b", " ", low)
    patrones = [
        r"\$\s*(\d[\d\.]*)([,]\d{1,2})?\s*(k|mil)?",
        r"(?:de|por)\s+(\d[\d\.]*)([,]\d{1,2})?\s*(k|mil)?",
        r"\b(\d[\d\.]*)([,]\d{1,2})?\s*(k|mil)?\s*(?:ars|pesos)?\b",
    ]
    mejor = None
    for pat in patrones:
        for m in re.finditer(pat, low):
            entero = m.group(1) or "0"
            dec = m.group(2) or ""
            suf = m.group(3) if m.lastindex and m.lastindex >= 3 else ""
            val = _parsear_numero_ars(entero + dec, suf)
            if val is None:
                continue
            if 1900 <= val <= 2100 and not suf:
                continue
            if mejor is None or val > mejor:
                mejor = val
    return mejor


def extraer_fecha_movimiento(texto: str):
    low = (texto or "").lower()
    if re.search(r"\bayer\b", low):
        return (ahora_argentina() - timedelta(days=1)).strftime("%Y-%m-%d")
    if re.search(r"\bhoy\b", low):
        return ahora_argentina().strftime("%Y-%m-%d")
    m_f = re.search(r"\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\b", low)
    if not m_f:
        return None
    d, mo = int(m_f.group(1)), int(m_f.group(2))
    an = m_f.group(3)
    if an:
        an = int(an)
        if an < 100:
            an += 2000
    else:
        an = ahora_argentina().year
    try:
        return datetime(an, mo, d).strftime("%Y-%m-%d")
    except Exception:
        return None


def detectar_tipo_movimiento(texto: str) -> str:
    low = (texto or "").lower()
    if re.search(r"\b(" + VERBO_INGRESO + r")\b", low):
        return "INGRESO"
    if re.search(r"\b(reintegro|devoluci[oó]n|me devolvieron|anulaci[oó]n)\b", low):
        return "INGRESO"
    return "GASTO"


def extraer_clave_nl(texto: str) -> str:
    clave_bank = extraer_clave_comercio(texto)
    if clave_bank:
        return clave_bank
    t = re.sub(r"\$?\d[\d\.,]*", " ", (texto or "").upper())
    t = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", " ", t)
    t = re.sub(r"[^A-ZÁÉÍÓÚÜÑ0-9\s]", " ", t)
    tokens = [
        tok for tok in t.split()
        if tok not in STOP_CLAVE_NL
        and tok not in PALABRAS_PROHIBIDAS_PATRON
        and len(tok) >= 3
        and not tok.isdigit()
    ]
    return " ".join(tokens[:3]).strip()


def match_regla_comercio(texto: str):
    low = " " + re.sub(r"\s+", " ", (texto or "").lower()) + " "
    mejor = None
    mejor_len = 0
    for aliases, cat, desc in REGLAS_COMERCIO_NL:
        for alias in aliases:
            if f" {alias} " in low or low.strip() == alias:
                if len(alias) > mejor_len:
                    mejor = (cat, desc, alias.upper())
                    mejor_len = len(alias)
    return mejor


def descripcion_limpia_desde_texto(texto: str, fallback: str = "") -> str:
    t = (texto or "").lower()
    t = re.sub(r"\b(" + VERBO_MOVIMIENTO + r")\b", " ", t)
    t = re.sub(r"\$?\d[\d\.,]*", " ", t)
    t = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", " ", t)
    t = re.sub(r"\b(hoy|ayer|ars|pesos|un|una|el|la|los|las|de|del|en|por|con|para)\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return _emprolijar_texto(t)[:80] or fallback


def parece_consulta_no_registro(texto: str) -> bool:
    low = (texto or "").lower().strip()
    if re.search(r"\b(vacaciones|viaje|viajar|presupuesto|activar|desactivar|alertas)\b", low):
        return True
    if re.search(r"\b(cu[aá]nto|c[oó]mo vengo|radiograf|resumen|gr[aá]fico|an[aá]lisis|torta|benchmark)\b", low):
        return True
    if re.search(r"\b(btc|eth|spy|nvda|meli|cartera|long|short|apalanc)\b", low) and re.search(r"\b(usd|u\$d|d[oó]lar)\b", low):
        return True
    return False


def parece_intento_registro(texto: str, tiene_regla: bool, tiene_cat: bool, tiene_mapeo: bool) -> bool:
    low = (texto or "").lower()
    if parece_consulta_no_registro(texto):
        return False
    if re.search(r"\b(" + VERBO_MOVIMIENTO + r")\b", low):
        return True
    if extraer_monto_ars(texto) and (tiene_regla or tiene_cat or tiene_mapeo or "$" in (texto or "")):
        return True
    return False


def buscar_mapeo_por_texto(user_id: int, texto: str):
    if not user_id:
        return None, None, ""
    claves = []
    k1 = extraer_clave_comercio(texto)
    k2 = extraer_clave_nl(texto)
    if k1:
        claves.append(k1)
    if k2 and k2 not in claves:
        claves.append(k2)
    hit = match_regla_comercio(texto)
    if hit and hit[2] not in claves:
        claves.append(hit[2])
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for clave in claves:
                    if not clave:
                        continue
                    cursor.execute(
                        """SELECT categoria, descripcion_limpia, patron_clave,
                                  COALESCE(usos_exitosos,1), COALESCE(usos_rechazados,0)
                           FROM mapeo_conceptos
                           WHERE user_id = %s AND patron_clave = %s;""",
                        (user_id, clave),
                    )
                    row = cursor.fetchone()
                    if row and int(row[4] or 0) == 0:
                        return row[0], row[1], row[2]
                blob = " " + re.sub(r"\s+", " ", (texto or "").upper()) + " "
                cursor.execute(
                    """SELECT categoria, descripcion_limpia, patron_clave,
                              COALESCE(usos_rechazados,0)
                       FROM mapeo_conceptos WHERE user_id = %s;""",
                    (user_id,),
                )
                candidatos = []
                for cat, desc, patron, rech in cursor.fetchall():
                    if int(rech or 0) > 0 or not patron or len(str(patron)) < 3:
                        continue
                    p = f" {str(patron).upper()} "
                    if p in blob:
                        candidatos.append((len(str(patron)), cat, desc, patron))
                if candidatos:
                    candidatos.sort(reverse=True)
                    _, cat, desc, patron = candidatos[0]
                    return cat, desc, patron
    except Exception as e:
        logger.warning(f"buscar_mapeo_por_texto: {e}")
    return None, None, (claves[0] if claves else "")


def aprender_patron_clasificacion(user_id: int, texto: str, categoria: str, descripcion: str, clave_extra: str = ""):
    if not user_id:
        return
    vistas = []
    for cand in (clave_extra, extraer_clave_comercio(texto), extraer_clave_nl(texto)):
        c = (cand or "").strip().upper()
        if c and c not in vistas:
            vistas.append(c)
    for clave in vistas:
        guardar_aprendizaje_concepto(user_id, clave, categoria, descripcion)


def clasificar_categoria_python(texto: str):
    hit = match_regla_comercio(texto)
    if hit:
        return hit[0], hit[1], hit[2], True
    low = (texto or "").lower()
    if re.search(r"\b(sueldo|haberes|salario|n[oó]mina|liquidaci[oó]n)\b", low) or "havas" in low:
        return "Sueldo", "Sueldo", "SUELDO", True
    if categoria_reconocida_por_python(texto):
        cat, desc = parsear_categoria_descripcion_usuario(texto)
        return cat, desc, extraer_clave_nl(texto), True
    for canon in sorted(CATEGORIAS_VALIDAS, key=len, reverse=True):
        if str(canon).lower() in low:
            desc = descripcion_limpia_desde_texto(texto, canon)
            return canon, desc, extraer_clave_nl(texto), True
    return None, None, extraer_clave_nl(texto), False


def clasificar_con_gemini_movimiento(texto: str):
    cats = ", ".join(sorted(CATEGORIAS_VALIDAS))
    prompt = (
        "Clasificá este gasto o ingreso personal de Argentina. "
        "Devolvé EXACTAMENTE una línea: TIPO|CATEGORIA|DESCRIPCION\n"
        "TIPO es GASTO o INGRESO. CATEGORIA una de: "
        f"{cats}. DESCRIPCION corta y prolija, sin monto.\n"
        f"Texto: {texto}"
    )
    res = llamar_gemini(
        prompt,
        "Clasificador de movimientos. Solo TIPO|CATEGORIA|DESCRIPCION. No inventes montos.",
    )
    partes = [p.strip() for p in (res or "").replace("\n", " ").split("|")]
    tipo = "GASTO"
    cat = "Varios"
    desc = descripcion_limpia_desde_texto(texto, "Movimiento")
    if partes and partes[0].upper() in ("GASTO", "INGRESO"):
        tipo = partes[0].upper()
        if len(partes) > 1 and partes[1]:
            cat = normalizar_categoria(partes[1])
        if len(partes) > 2 and partes[2]:
            desc = _emprolijar_texto(re.sub(r"\$?\d[\d\.,]*", " ", partes[2]))[:80] or desc
    elif partes:
        cat = normalizar_categoria(partes[0])
        if len(partes) > 1 and partes[1]:
            desc = _emprolijar_texto(partes[1])[:80] or desc
    if cat not in CATEGORIAS_VALIDAS:
        cat = normalizar_categoria(cat)
        if cat not in CATEGORIAS_VALIDAS:
            cat = "Varios"
    return tipo, cat, desc


def parsear_registro_manual(texto: str, user_id: int = None, usar_gemini: bool = False):
    raw = (texto or "").strip()
    if not raw or raw.startswith("/"):
        return None
    if parece_consulta_no_registro(raw):
        return None

    monto = extraer_monto_ars(raw)
    fecha = extraer_fecha_movimiento(raw)
    tipo = detectar_tipo_movimiento(raw)

    cat_db, desc_db, clave_db = buscar_mapeo_por_texto(user_id, raw) if user_id else (None, None, "")
    cat_py, desc_py, clave_py, ok_py = clasificar_categoria_python(raw)
    tiene_cat = bool(cat_db or ok_py)

    if not parece_intento_registro(raw, bool(match_regla_comercio(raw)), tiene_cat, bool(cat_db)):
        return None
    if monto is None or monto <= 0:
        return None

    origen = "python"
    if cat_db:
        cat, desc, clave = normalizar_categoria(cat_db), (desc_db or cat_db), clave_db
        origen = "memoria"
    elif ok_py:
        cat, desc, clave = cat_py, desc_py, clave_py
        origen = "python"
    elif usar_gemini:
        try:
            tipo_ia, cat, desc = clasificar_con_gemini_movimiento(raw)
            if tipo == "GASTO" and tipo_ia == "INGRESO":
                tipo = "INGRESO"
            clave = extraer_clave_nl(raw) or extraer_clave_comercio(raw)
            origen = "gemini"
            aprender_patron_clasificacion(user_id, raw, cat, desc, clave)
        except Exception as e:
            logger.warning(f"Gemini clasificador movimiento: {e}")
            cat, desc, clave = "Varios", descripcion_limpia_desde_texto(raw, "Movimiento"), extraer_clave_nl(raw)
            origen = "fallback"
    else:
        return {
            "tipo": tipo,
            "monto": monto,
            "categoria": "Varios",
            "descripcion": descripcion_limpia_desde_texto(raw, "Movimiento"),
            "fecha": fecha,
            "clave": extraer_clave_nl(raw),
            "origen": "incompleto",
            "necesita_ia": True,
        }

    if not desc:
        desc = descripcion_limpia_desde_texto(raw, cat)
    return {
        "tipo": tipo,
        "monto": float(monto),
        "categoria": str(cat)[:50],
        "descripcion": str(desc)[:80],
        "fecha": fecha,
        "clave": clave or extraer_clave_nl(raw),
        "origen": origen,
        "necesita_ia": False,
    }

# ==================== APRENDIZAJE Y MAPEO DE CONCEPTOS BANCARIOS ====================
PALABRAS_PROHIBIDAS_PATRON = {
    "COMPRA", "DEBITO", "CREDITO", "TARJ", "TARJETA", "SUC", "SUCURSAL", "PAGO", "PAGOS",
    "TRANSFERENCIA", "TRANSFERENCIAS", "TERCERO", "TERCEROS", "DEBIN", "RECURRENTE", "RECURRENTES",
    "ACREDITAMIENTO", "ACREDITACION", "HABERES", "SERVICIO", "SERVICIOS", "ANULACION", "ANULACIONES",
    "DEVOLUCION", "DEVO", "DEV", "ELECTRON", "GALICIA", "BANCO", "COELSA", "PERCEPCION", "RETENCION",
    "IMPUESTO", "IMPUESTOS", "VARIOS", "RESUMEN", "EXTRACTO", "STATEMENT", "SHOPPER", "PAYU", "MERPAGO",
    "MERCADOPAGO", "MERCADO", "VENTA", "DOLARES", "MONEDA", "EXTRANJERA", "OPERACION", "VIAJES", "BUSES",
    "INTERES", "CAPITALIZADO", "PROMOCION", "REINTEGRO", "REINTEGROS"
}

def _es_hash_debin(token: str) -> bool:
    t = str(token).upper()
    if len(t) < 8:
        return False
    if re.fullmatch(r"[A-Z0-9]{10,}", t) and any(c.isdigit() for c in t) and any(c.isalpha() for c in t):
        return True
    return False

def extraer_clave_comercio(concepto_crudo: str) -> str:
    raw = str(concepto_crudo)
    texto = raw.upper()
    texto = texto.replace("*", " ")
    texto = re.sub(r"\d{4}X+\d{2,4}", " ", texto)
    texto = re.sub(r"\b\d{6,}\b", " ", texto)
    texto = re.sub(r"[^A-Z0-9\s]", " ", texto)

    tokens = texto.split()
    tokens_utiles = [
        t for t in tokens
        if t not in PALABRAS_PROHIBIDAS_PATRON
        and len(t) >= 3
        and not t.isdigit()
        and not _es_hash_debin(t)
        and t not in {"A001", "A171", "A368", "A371", "A429", "A603", "A736", "A738", "A760", "A762", "A894", "A068"}
    ]

    if re.search(r"PEDIDOSYA\s*\*?\s*MARKET|PEDIDOS YA\s*\*?\s*MARKET", texto):
        return "PEDIDOSYA MARKET"
    if "PEDIDOSYA" in tokens or "PEDIDOSYA" in texto.replace(" ", ""):
        local = ""
        m_loc = re.search(r"PEDIDOSYA\s*\*?\s*([A-Z][A-Z0-9 ]{2,40})", texto)
        if m_loc:
            local = m_loc.group(1).strip()
            local = re.sub(r"\bA\d{3}\b", " ", local)
            local = re.sub(r"\b\d{3,}\b", " ", local)
            local = re.sub(r"\b(DE|DEL|LA|EL|LOS|LAS)\b", " ", local)
            local = " ".join(local.split()[:3]).strip()
        if local and "MARKET" not in local and "EXTRA" not in local:
            return f"PEDIDOSYA {local}"
        if "EXTRA" in texto:
            return "PEDIDOSYA EXTRA"
        return "PEDIDOSYA DELIVERY"

    if "UBER" in tokens or "UBER" in texto:
        if "SHOPPER" in texto:
            return "UBER SHOPPER"
        return "UBER"
    if "SUBE" in tokens or ("TRANSPORTE" in texto and "SUBE" in texto):
        return "SUBE"
    if "FARMACITY" in texto:
        if "REINTEGRO" in texto:
            return "FARMACITY REINTEGRO"
        return "FARMACITY"
    if "CINEMARK" in tokens or "CINEMARK" in texto:
        return "CINEMARK"
    if "METROGAS" in tokens:
        return "METROGAS"
    if "EDESUR" in tokens:
        return "EDESUR"
    if "HAVAS" in tokens:
        return "HAVAS MEDIA"
    if "FORNODELPAESE" in texto or "FORNO DEL PAESE" in texto:
        return "FORNODELPAESE"
    if "PETTISH" in tokens:
        return "PETTISH"
    if "RAPPI" in tokens:
        return "RAPPI PRO" if "PRO" in tokens else "RAPPI"
    if "FIMA" in tokens:
        return "FIMA PREMIUM"
    if "DOLAR" in texto or "DOLARES" in texto:
        return "COMPRA DOLARES"
    if "DELFINA" in tokens and "SILVA" in tokens:
        return "DELFINA SILVA"
    if "UGARRIZA" in tokens and not any(x in texto for x in ["TOFALO", "LUCIANO", "20454793820"]):
        return "MARINA LUZ (MAMA)"
    if "MASTER" in tokens and "TARJETA" in texto:
        return "PAGO TARJETA MASTER"

    if not tokens_utiles:
        return ""
    clave_fallback = " ".join(tokens_utiles[:2])
    genericas = {
        "TRANSPORTE", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE", "ENERO", "FEBRERO",
        "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "VARIOS", "PERSONAL", "PAY"
    }
    if clave_fallback in genericas or all(p in genericas for p in clave_fallback.split()):
        return ""
    return clave_fallback

def sugerir_clasificacion_por_concepto(concepto: str):
    clave = extraer_clave_comercio(concepto)
    texto = str(concepto).upper()
    if clave == "PEDIDOSYA MARKET":
        return "Supermercado", "PedidosYa Market", clave
    if clave == "PEDIDOSYA EXTRA":
        return "Delivery", "PedidosYa Extra", clave
    if clave.startswith("PEDIDOSYA"):
        desc = clave.replace("PEDIDOSYA", "PedidosYa").strip()
        return "Delivery", desc or "PedidosYa Delivery", clave
    if clave == "HAVAS MEDIA":
        return "Sueldo", "Havas Media Argentina S.A.", clave
    if clave == "FIMA PREMIUM":
        if "RESCATE" in texto:
            return "Inversiones", "Rescate FIMA Premium", clave
        return "Inversiones", "Suscripción FIMA Premium", clave
    if clave == "COMPRA DOLARES":
        return "Inversiones", "Compra de dólares", clave
    if clave == "SUBE":
        if "ANULACION" in texto:
            return "Transporte", "Devolución SUBE", clave
        return "Transporte", "Tarjeta SUBE", clave
    if clave == "UBER":
        if "REINTEGRO" in texto:
            return "Transporte", "Reintegro Uber", clave
        return "Transporte", "Uber", clave
    if clave == "FARMACITY REINTEGRO":
        return "Farmacia", "Reintegro Farmacity", clave
    if clave == "FARMACITY":
        return "Farmacia", "Farmacity", clave
    if clave == "FORNODELPAESE":
        return "Comida", "Forno del Paese", clave
    if clave == "CINEMARK":
        return "Entretenimiento", "Cinemark", clave
    if clave == "METROGAS":
        return "Servicios", "Metrogas", clave
    if clave == "EDESUR":
        return "Servicios", "Edesur", clave
    if clave == "PETTISH":
        return "Hogar", "Pettish Bazar", clave
    if clave == "MARINA LUZ (MAMA)":
        return "Transferencias", "Transferencia de mamá", clave
    if clave == "DELFINA SILVA":
        return "Transferencias", "Transferencia de Delfina Silva", clave
    if clave == "PAGO TARJETA MASTER":
        return "Tarjeta de crédito", "Pago Tarjeta Mastercard", clave
    return None, None, clave

CLAVES_COMERCIO_CONOCIDO = {
    "PEDIDOSYA MARKET", "PEDIDOSYA EXTRA", "SUBE", "UBER", "UBER SHOPPER",
    "FARMACITY", "FARMACITY REINTEGRO", "CINEMARK", "METROGAS", "EDESUR",
    "HAVAS MEDIA", "FORNODELPAESE", "PETTISH", "FIMA PREMIUM", "COMPRA DOLARES",
    "MARINA LUZ (MAMA)", "PAGO TARJETA MASTER", "RAPPI PRO", "RAPPI",
}

def clasificacion_es_auto_aprobable(usos: int, rechazos: int, clave: str = "") -> bool:
    if int(rechazos or 0) > 0:
        return False
    if not clave:
        return False
    umbral = 1 if (clave in CLAVES_COMERCIO_CONOCIDO or str(clave).startswith("PEDIDOSYA")) else UMBRAL_AUTO_APROBAR
    return usos >= umbral

def buscar_clasificacion_previa(user_id: int, concepto: str):
    cat_reglas, desc_reglas, clave = sugerir_clasificacion_por_concepto(concepto)
    if not clave:
        return None, None, "", 0, 0, False
    cat_db, desc_db, usos, rechazos = None, None, 0, 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT categoria, descripcion_limpia,
                              COALESCE(usos_exitosos, 1), COALESCE(usos_rechazados, 0)
                       FROM mapeo_conceptos WHERE user_id = %s AND patron_clave = %s;""",
                    (user_id, clave)
                )
                res = cursor.fetchone()
                if res:
                    cat_db, desc_db, usos, rechazos = res[0], res[1], int(res[2]), int(res[3])
    except Exception as e:
        logger.warning(f"buscar_clasificacion_previa sin DB: {e}")

    if cat_db:
        auto_ok = clasificacion_es_auto_aprobable(usos, rechazos, clave)
        return cat_db, desc_db, clave, usos, rechazos, auto_ok
    if cat_reglas:
        return cat_reglas, desc_reglas, clave, 0, 0, False
    return None, None, clave, 0, 0, False

def guardar_aprendizaje_concepto(user_id: int, clave: str, categoria: str, descripcion: str):
    if not clave or len(clave) < 3 or clave in PALABRAS_PROHIBIDAS_PATRON or _es_hash_debin(clave.replace(" ", "")):
        return
    categoria = normalizar_categoria(categoria)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT categoria, COALESCE(usos_exitosos,1), COALESCE(usos_rechazados,0) FROM mapeo_conceptos WHERE user_id = %s AND patron_clave = %s;",
                (user_id, clave)
            )
            prev = cursor.fetchone()
            if prev:
                cat_prev = normalizar_categoria(str(prev[0]))
                usos = int(prev[1])
                rechazos = int(prev[2])
                if cat_prev != categoria:
                    cursor.execute(
                        """UPDATE mapeo_conceptos
                           SET categoria = %s, descripcion_limpia = %s,
                               usos_exitosos = 1, usos_rechazados = usos_rechazados + 1
                           WHERE user_id = %s AND patron_clave = %s;""",
                        (categoria, descripcion, user_id, clave)
                    )
                else:
                    cursor.execute(
                        """UPDATE mapeo_conceptos
                           SET descripcion_limpia = %s, usos_exitosos = usos_exitosos + 1
                           WHERE user_id = %s AND patron_clave = %s;""",
                        (descripcion, user_id, clave)
                    )
            else:
                cursor.execute("""
                    INSERT INTO mapeo_conceptos (user_id, patron_clave, categoria, descripcion_limpia, usos_exitosos, usos_rechazados)
                    VALUES (%s, %s, %s, %s, 1, 0)
                    ON CONFLICT (user_id, patron_clave)
                    DO UPDATE SET
                        categoria = EXCLUDED.categoria,
                        descripcion_limpia = EXCLUDED.descripcion_limpia,
                        usos_exitosos = mapeo_conceptos.usos_exitosos + 1;
                """, (user_id, clave, categoria, descripcion))
            conn.commit()

def normalizar_frase(texto: str) -> str:
    t = (texto or "").lower().strip()
    if t.startswith("/"):
        t = t[1:]
    t = re.sub(r"[¿?¡!.,;:\"']+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:180]

COMANDOS_FRASE = {
    "gastos", "vs", "fijos", "delivery", "mes", "resumen", "riesgo", "objetivos",
    "mensual", "spy", "grafico", "activos", "analisis", "precio", "excel",
    "briefing", "torta", "silencio", "ayuda", "help", "pie",
}
_ALIAS_CMD = {
    "análisis": "analisis", "gráfico": "grafico", "grafica": "grafico",
    "técnico": "analisis", "tecnico": "analisis", "cotizacion": "precio",
    "cotización": "precio", "cartera": "resumen", "balance": "resumen",
}

def sanitizar_comando_derivado(texto_cmd: str) -> str:
    t = re.sub(r"\s+", " ", (texto_cmd or "").strip().lstrip("/")).strip()
    if not t:
        return ""
    parts = t.split()
    cmd = _ALIAS_CMD.get(parts[0].lower().strip(".,:;"), parts[0].lower().strip(".,:;"))
    if cmd not in COMANDOS_FRASE:
        return ""
    args = parts[1:]
    if cmd == "analisis":
        tk = ""
        tf = "diario"
        for a in args:
            al = a.lower().strip(".,")
            if al in ("diario", "1d"):
                tf = "diario"
            elif al in ("semanal", "1w", "semana"):
                tf = "semanal"
            elif al in ("4h", "4hs"):
                tf = "4h"
            elif re.fullmatch(r"[A-Za-z]{1,6}\d{0,2}", a) and not tk:
                tk = a.upper().replace("-USD", "")
        return f"analisis {tk} {tf}".strip() if tk else "analisis"
    if cmd == "precio" and args:
        tk = args[0].upper().replace("-USD", "")
        if re.fullmatch(r"[A-Z]{1,6}\d{0,2}", tk):
            return f"precio {tk}"
        return "precio"
    if cmd == "torta" and args and re.search(r"cartera|invers|usd|activo", " ".join(args), re.I):
        return "torta cartera"
    if cmd == "spy":
        per = " ".join(args[:2]).lower()
        return f"spy {per}".strip()
    return cmd

def plantilla_de_frase(frase: str, comando: str) -> str:
    parts = (comando or "").split()
    if len(parts) < 2:
        return ""
    tk = parts[1].upper()
    nueva = frase
    nueva = re.sub(rf"\b{re.escape(tk.lower())}\b", "{TK}", nueva)
    if "{TK}" not in nueva and tk == "GOOGL":
        nueva = re.sub(r"\bgoogle\b", "{TK}", nueva)
    if "{TK}" not in nueva:
        return ""
    return nueva

def guardar_frase_comando(user_id: int, texto: str, comando: str):
    frase = normalizar_frase(texto)
    cmd = sanitizar_comando_derivado(comando)
    if not frase or not cmd or frase == cmd or len(frase) < 3:
        return
    if frase.split()[0] == cmd.split()[0] and len(frase.split()) == 1:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO mapeo_frases (user_id, frase, comando, usos)
                       VALUES (%s, %s, %s, 1)
                       ON CONFLICT (user_id, frase)
                       DO UPDATE SET comando = EXCLUDED.comando, usos = mapeo_frases.usos + 1;""",
                    (user_id, frase, cmd),
                )
                tpl = plantilla_de_frase(frase, cmd)
                if tpl and "{TK}" in tpl:
                    cmd_tpl = cmd
                    partes = cmd.split()
                    if len(partes) >= 2:
                        cmd_tpl = cmd.replace(partes[1], "{TK}")
                    cursor.execute(
                        """INSERT INTO mapeo_frases (user_id, frase, comando, usos)
                           VALUES (%s, %s, %s, 1)
                           ON CONFLICT (user_id, frase)
                           DO UPDATE SET comando = EXCLUDED.comando, usos = mapeo_frases.usos + 1;""",
                        (user_id, "tpl:" + tpl, cmd_tpl),
                    )
                conn.commit()
    except Exception as e:
        logger.warning(f"guardar_frase_comando: {e}")

def buscar_comando_por_frase(user_id: int, texto: str) -> str:
    frase = normalizar_frase(texto)
    if not frase:
        return ""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT comando FROM mapeo_frases WHERE user_id = %s AND frase = %s;",
                    (user_id, frase),
                )
                row = cursor.fetchone()
                if row and row[0]:
                    cursor.execute(
                        "UPDATE mapeo_frases SET usos = COALESCE(usos,1)+1 WHERE user_id = %s AND frase = %s;",
                        (user_id, frase),
                    )
                    conn.commit()
                    return sanitizar_comando_derivado(str(row[0]))
                cursor.execute(
                    "SELECT frase, comando FROM mapeo_frases WHERE user_id = %s AND frase LIKE 'tpl:%%';",
                    (user_id,),
                )
                for frase_tpl, comando in cursor.fetchall():
                    tpl = str(frase_tpl)[4:]
                    rx = "^" + re.escape(tpl).replace(r"\{TK\}", r"([a-z0-9]{1,10})") + "$"
                    m = re.fullmatch(rx, frase)
                    if not m:
                        continue
                    tk = m.group(1).upper()
                    cmd = str(comando).replace("{TK}", tk)
                    cursor.execute(
                        "UPDATE mapeo_frases SET usos = COALESCE(usos,1)+1 WHERE user_id = %s AND frase = %s;",
                        (user_id, frase_tpl),
                    )
                    conn.commit()
                    return sanitizar_comando_derivado(cmd)
    except Exception as e:
        logger.warning(f"buscar_comando_por_frase: {e}")
    return ""

def extraer_comando_de_respuesta(reply: str) -> str:
    texto = reply or ""
    m = re.search(r"COMANDO:\s*/?([^\n\r]+)", texto, re.I)
    if m:
        limpio = sanitizar_comando_derivado(m.group(1))
        if limpio:
            return limpio
    m2 = re.search(
        r"\b(analisis|análisis|precio|gastos|resumen|spy|torta|riesgo|mes|objetivos|mensual|excel|briefing|fijos|delivery|vs)\b(?:\s+([A-Za-z0-9]{1,8}))?(?:\s+(diario|semanal|4h|4hs))?",
        texto,
        re.I,
    )
    if m2:
        blob = " ".join(p for p in m2.groups() if p)
        return sanitizar_comando_derivado(blob)
    return ""

# Frases fijas (0 tokens). Las más específicas van primero.
REGLAS_INTENTO = [
    (r"\bspy\b|\bbenchmark\b|contra el\s*s&?p|vs\s*spy", "spy"),
    (r"torta\s*(de\s*)?(la\s*)?(cartera|invers|usd|activos)|pie\s*cartera", "torta cartera"),
    (r"\btorta\b|\bpie\b|grafico de torta|gráfico de torta", "torta"),
    (r"ciclo anterior|contra el ciclo|vs el (ciclo|mes)|compar(a|ame|ar)\s+(los\s+)?ciclos?", "vs"),
    (r"^vs$", "vs"),
    (r"gastos?\s+fijos?|servicios recurrentes|\bfijos\b|\brecurrentes\b", "fijos"),
    (r"\bdelivery\b|pedidos\s*ya(?!\s*market)|gasto(s)? de delivery", "delivery"),
    (r"rendimiento mensual|mes a mes|como me fue cada mes|tasa mensual|\bmensual\b", "mensual"),
    (r"radiograf[ií]a|gastos hormiga|como vengo de gastos|resumen de gastos|\bgastos\b|\bfinanzas\b", "gastos"),
    (r"presupuesto\s+\S+", None),  # lo maneja el regex de set
    (r"\bpresupuestos?\b|\bdel mes\b|^mes$", "mes"),
    (r"\bobjetivos?\b|\bmetas?\b", "objetivos"),
    (r"\briesgo\b|\brisk\b|drawdown", "riesgo"),
    (r"desactivar\s+alertas|apagar\s+alertas|alertas\s+off", "silencio"),
    (r"activar\s+alertas|prender\s+alertas|alertas\s+on", "activar alertas"),
    (r"\bbriefing\b|\bresumen de mercado\b|\bpremarket\b", "briefing"),
    (r"\bresumen\b|\bcartera\b|\bbalance\b|como esta mi cartera", "resumen"),
    (r"\bexcel\b|exportar planilla", "excel"),
    (r"\ban[aá]lisis\b|\bt[eé]cnico\b", "analisis"),
    (r"\bgrafico\b|\bgráfico\b|\bcurva\b", "grafico"),
]

def resolver_intencion(texto: str) -> str:
    low = normalizar_frase(texto)
    if not low:
        return ""
    for pat, cmd in REGLAS_INTENTO:
        if cmd is None:
            continue
        if re.search(pat, low):
            return cmd
    return ""

_STOP_TICKER_NL = {
    "EN", "DE", "DEL", "EL", "LA", "LOS", "LAS", "UN", "UNA", "ME", "MI", "AL",
    "DIARIO", "SEMANAL", "SEMANA", "4H", "4HS", "1D", "1W", "FIBO", "FIBONACCI",
    "TECNICO", "TECNICA", "ANALISIS", "ANALIZA", "ANALIZAME", "ANALIZAR", "AT",
    "COMO", "VES", "VER", "HOY", "AYER", "POR", "FAVOR", "GRAFICO", "CHART",
}

def extraer_pedido_analisis(texto: str):
    low = normalizar_frase(texto)
    if not low:
        return None
    if not re.search(r"\b(analiz\w*|an[aá]lisis|c[oó]mo ves|como ves|fijate|f[ií]jate|mira|chart|t[eé]cnico de)\b", low):
        return None
    tf = "diario"
    if re.search(r"\b(4h|4hs)\b", low):
        tf = "4h"
    elif re.search(r"\b(semanal|semana|1w|weekly)\b", low):
        tf = "semanal"
    for tok in re.findall(r"[a-z0-9.\-]{2,12}", low):
        u = tok.upper().replace("-USD", "")
        if u in _STOP_TICKER_NL or u.isdigit():
            continue
        if re.fullmatch(r"[A-Z]{1,6}\d{0,2}", u):
            return u, tf
    return None

def registrar_rechazo_concepto(user_id: int, clave: str):
    if not clave or len(clave) < 3:
        return
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """UPDATE mapeo_conceptos
                   SET usos_rechazados = COALESCE(usos_rechazados, 0) + 1
                   WHERE user_id = %s AND patron_clave = %s;""",
                (user_id, clave)
            )
            conn.commit()

def es_sueldo_haberes(categoria: str = "", descripcion: str = "") -> bool:
    txt = f"{categoria} {descripcion}".lower()
    return any(k in txt for k in ["sueldo", "haberes", "havas", "acreditamiento de haberes"])

def obtener_fechas_sueldo(user_id: int):
    with get_db_connection() as conn:
        df = pd.read_sql(
            """SELECT fecha, categoria, descripcion FROM movimientos
               WHERE user_id = %s AND tipo = 'INGRESO' ORDER BY fecha ASC;""",
            conn, params=(user_id,)
        )
    if df.empty:
        return []
    fechas = []
    for _, row in df.iterrows():
        if es_sueldo_haberes(str(row.get("categoria") or ""), str(row.get("descripcion") or "")):
            f = pd.to_datetime(row["fecha"]).tz_localize(None).normalize()
            fechas.append(f)
    fechas = sorted(set(fechas))
    return fechas

def resolver_ciclo_havas(user_id: int, fecha_ref=None):
    fecha_ref = pd.to_datetime(fecha_ref or ahora_argentina()).tz_localize(None).normalize() if not isinstance(fecha_ref, pd.Timestamp) else fecha_ref.normalize()
    sueldos = obtener_fechas_sueldo(user_id)
    if not sueldos:
        inicio = pd.Timestamp(datetime(fecha_ref.year, fecha_ref.month, 1))
        return inicio, fecha_ref + pd.Timedelta(days=1), "calendario (sin sueldo Havas cargado)"
    inicio = sueldos[0]
    fin = fecha_ref + pd.Timedelta(days=1)
    for i, f in enumerate(sueldos):
        nxt = sueldos[i + 1] if i + 1 < len(sueldos) else None
        if f <= fecha_ref and (nxt is None or fecha_ref < nxt):
            inicio = f
            fin = nxt if nxt is not None else (fecha_ref + pd.Timedelta(days=1))
            break
    if fecha_ref < sueldos[0]:
        inicio = fecha_ref - pd.Timedelta(days=30)
        fin = sueldos[0]
    etiqueta = f"ciclo Havas {inicio.strftime('%d/%m')} → {(fin - pd.Timedelta(days=1)).strftime('%d/%m/%Y')}"
    return inicio, fin, etiqueta

def asignar_ciclo_havas(df, sueldos):
    if df.empty:
        df = df.copy()
        df["ciclo_id"] = []
        df["ciclo_inicio"] = []
        return df
    df = df.copy()
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.tz_localize(None)
    if not sueldos:
        df["ciclo_id"] = df["fecha"].dt.to_period("M").astype(str)
        df["ciclo_inicio"] = df["fecha"].dt.to_period("M").dt.start_time
        return df
    sueldos = sorted(sueldos)
    def _cid(f):
        inicio = sueldos[0]
        for i, s in enumerate(sueldos):
            nxt = sueldos[i + 1] if i + 1 < len(sueldos) else None
            if s <= f and (nxt is None or f < nxt):
                inicio = s
                break
        if f < sueldos[0]:
            inicio = sueldos[0] - pd.Timedelta(days=30)
        return inicio.strftime("%Y-%m-%d")
    df["ciclo_inicio"] = df["fecha"].apply(lambda x: pd.Timestamp(_cid(x)))
    df["ciclo_id"] = df["ciclo_inicio"].dt.strftime("%Y-%m-%d")
    return df

TAGS_AHORRO = {
    "inversion", "inversión", "inversiones", "ahorro", "usdt", "crypto", "cripto",
    "broker", "dolares", "dólares", "dolar", "dólar", "usd", "fima"
}
TAGS_DEUDA = {
    "tarjeta de crédito", "tarjeta de credito", "pago de tarjeta", "pago tarjeta",
    "pago tarjetas",
}

CATS_NETEAR_DEVOL = {
    "comida", "delivery", "supermercado", "farmacia", "transporte",
    "entretenimiento", "hogar", "regalos", "mascotas", "compras",
}
TAGS_DEVOL = ("reintegro", "devol", "anulacion", "anulación", "dev.compra", "devo")
TAGS_FIJO = (
    "metrogas", "edesur", "aysa", "telecentro", "personal", "internet",
    "luz", "gas", "abl", "expensas", "alquiler", "rappi pro", "suscrip",
    "netflix", "spotify", "youtube", "icloud", "openai",
)

def _es_devolucion_row(row) -> bool:
    if str(row.get("tipo", "")).upper() != "INGRESO":
        return False
    blob = f"{row.get('categoria', '')} {row.get('descripcion', '')}".lower()
    cat = str(row.get("categoria", "")).lower()
    if any(t in blob for t in TAGS_DEVOL):
        return True
    return cat in CATS_NETEAR_DEVOL

def netear_devoluciones_en_consumo(df_consumo, df_ingresos):
    if df_consumo is None or df_consumo.empty:
        return df_consumo
    if df_ingresos is None or df_ingresos.empty:
        return df_consumo
    reint = df_ingresos[df_ingresos.apply(_es_devolucion_row, axis=1)].copy()
    if reint.empty:
        return df_consumo
    por_cat = reint.groupby(reint["categoria"].apply(lambda c: normalizar_categoria(c)))["monto"].sum()
    out = df_consumo.copy()
    out["categoria"] = out["categoria"].apply(normalizar_categoria)
    descuentos = []
    for cat, monto_dev in por_cat.items():
        mask = out["categoria"] == cat
        if not mask.any():
            continue
        resto = float(monto_dev)
        idxs = list(out.loc[mask].sort_values("monto", ascending=False).index)
        for i in idxs:
            if resto <= 0:
                break
            actual = float(out.at[i, "monto"])
            baja = min(actual, resto)
            out.at[i, "monto"] = actual - baja
            resto -= baja
            descuentos.append((cat, baja))
    out = out[out["monto"] > 0.5]
    return out

def _cargar_movimientos_ciclo(user_id: int):
    with get_db_connection() as conn:
        df = pd.read_sql(
            "SELECT fecha, tipo, monto, categoria, descripcion FROM movimientos WHERE user_id = %s;",
            conn, params=(user_id,),
        )
    if df.empty:
        return df
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["categoria"] = df["categoria"].apply(normalizar_categoria)
    sueldos = obtener_fechas_sueldo(user_id)
    return asignar_ciclo_havas(df, sueldos)

def texto_comparar_ciclos(user_id: int) -> str:
    df = _cargar_movimientos_ciclo(user_id)
    if df.empty:
        return "No hay movimientos para comparar ciclos."
    inicio, fin, etq = resolver_ciclo_havas(user_id)
    ciclos = sorted(df["ciclo_id"].dropna().unique())
    if not ciclos:
        return "Todavía no hay ciclos Havas para comparar."
    actual_id = inicio.strftime("%Y-%m-%d")
    if actual_id not in ciclos:
        actual_id = ciclos[-1]
    prevs = [c for c in ciclos if c < actual_id]
    prev_id = prevs[-1] if prevs else None

    g = df[(df["tipo"] == "GASTO")].copy()
    blob = (g["categoria"].astype(str) + " " + g["descripcion"].astype(str)).str.lower()
    g = g[~blob.apply(lambda x: any(t in x for t in TAGS_AHORRO))]
    g_act = g[g["ciclo_id"] == actual_id]
    tot_act = float(g_act["monto"].sum()) if not g_act.empty else 0.0
    por_act = g_act.groupby("categoria")["monto"].sum() if not g_act.empty else pd.Series(dtype=float)

    lineas = [f"📊 ESTE CICLO VS ANTERIOR", f"• Actual: {etq}", ""]
    if prev_id is None:
        lineas.append("Todavía no hay un ciclo anterior completo para comparar.")
        lineas.append(f"Consumo de este ciclo: ${tot_act:,.0f} ARS")
        return "\n".join(lineas)

    g_prev = g[g["ciclo_id"] == prev_id]
    tot_prev = float(g_prev["monto"].sum()) if not g_prev.empty else 0.0
    por_prev = g_prev.groupby("categoria")["monto"].sum() if not g_prev.empty else pd.Series(dtype=float)
    delta = tot_act - tot_prev
    pct = (delta / tot_prev * 100.0) if tot_prev else 0.0
    em = "🔴" if delta > 0 else ("🟢" if delta < 0 else "🟡")
    lineas.append(f"• Ciclo anterior: {prev_id}")
    lineas.append(f"• Consumo actual:    ${tot_act:,.0f}")
    lineas.append(f"• Consumo anterior:  ${tot_prev:,.0f}")
    lineas.append(f"• {em} Variación:     ${delta:+,.0f} ({pct:+.1f}%)")
    lineas.append("")
    cats = sorted(set(por_act.index) | set(por_prev.index), key=lambda c: -float(por_act.get(c, 0)))
    lineas.append("Por rubro:")
    for cat in cats[:12]:
        a = float(por_act.get(cat, 0))
        p = float(por_prev.get(cat, 0))
        if a == 0 and p == 0:
            continue
        d = a - p
        pc = (d / p * 100.0) if p else 100.0
        mark = "▲" if d > 0 else ("▼" if d < 0 else "=")
        lineas.append(f"• {cat}: ${a:,.0f} vs ${p:,.0f}  {mark}{abs(pc):.0f}%")
    # Delivery highlight
    d_act = float(por_act.get("Delivery", 0))
    d_prev = float(por_prev.get("Delivery", 0))
    lineas.append("")
    if d_prev > 0 or d_act > 0:
        dd = d_act - d_prev
        lineas.append(f"🛵 Delivery: ${d_act:,.0f} vs ${d_prev:,.0f} ({dd:+,.0f})")
        if d_prev and d_act > d_prev * 1.2:
            lineas.append("⚠️ Delivery +20% vs el ciclo anterior.")
    return "\n".join(lineas)

def texto_gastos_fijos(user_id: int) -> str:
    df = _cargar_movimientos_ciclo(user_id)
    if df.empty:
        return "No hay movimientos para detectar fijos."
    g = df[df["tipo"] == "GASTO"].copy()
    if g.empty:
        return "No hay gastos cargados."
    g["blob"] = (g["categoria"].astype(str) + " " + g["descripcion"].astype(str)).str.lower()
    g["es_fijo_tag"] = g["blob"].apply(lambda x: any(t in x for t in TAGS_FIJO) or any(
        k in x for k in ("servicio", "suscrip")
    ))
    por_cat_ciclo = g.groupby(["categoria", "ciclo_id"])["monto"].sum().reset_index()
    n_ciclos_cat = por_cat_ciclo.groupby("categoria")["ciclo_id"].nunique()
    cats_repetidas = set(n_ciclos_cat[n_ciclos_cat >= 2].index)
    # también comercios que se repiten aunque el monto cambie
    g["clave"] = g["descripcion"].fillna("").str.upper().str.slice(0, 40)
    n_ciclos_desc = g.groupby("clave")["ciclo_id"].nunique()
    claves_rep = set(n_ciclos_desc[n_ciclos_desc >= 2].index)

    cand = g[g["es_fijo_tag"] | g["categoria"].isin(cats_repetidas) | g["clave"].isin(claves_rep)].copy()
    # descartar hormiga one-off comida
    skip = {"comida", "delivery", "regalos", "entretenimiento", "hogar"}
    cand = cand[~cand["categoria"].str.lower().isin(skip) | cand["es_fijo_tag"]]
    if cand.empty:
        return "No detecté gastos fijos todavía (hace falta verlos en más de un ciclo, o que sean servicios/suscripciones)."

    lineas = ["📌 GASTOS FIJOS (monto puede variar)", ""]
    grp = cand.groupby("categoria")
    inicio, fin, etq = resolver_ciclo_havas(user_id)
    actual_id = inicio.strftime("%Y-%m-%d")
    for cat, sub in sorted(grp, key=lambda kv: -kv[1]["monto"].sum()):
        ciclos_n = sub["ciclo_id"].nunique()
        prom = float(sub.groupby("ciclo_id")["monto"].sum().mean())
        ult = float(sub[sub["ciclo_id"] == actual_id]["monto"].sum()) if actual_id in set(sub["ciclo_id"]) else float(sub.groupby("ciclo_id")["monto"].sum().iloc[-1])
        mn = float(sub.groupby("ciclo_id")["monto"].sum().min())
        mx = float(sub.groupby("ciclo_id")["monto"].sum().max())
        lineas.append(f"• {cat}")
        lineas.append(f"  {ciclos_n} ciclo(s) · promedio ${prom:,.0f} · último ${ult:,.0f} · rango ${mn:,.0f}–${mx:,.0f}")
    lineas.append("")
    lineas.append("Se marcan fijos si aparecen en 2+ ciclos o si son servicio/suscripción/luz/gas, aunque el importe cambie.")
    return "\n".join(lineas)

def texto_alerta_delivery(user_id: int) -> str:
    df = _cargar_movimientos_ciclo(user_id)
    if df.empty:
        return "No hay datos de Delivery."
    inicio, fin, etq = resolver_ciclo_havas(user_id)
    g = df[(df["tipo"] == "GASTO") & (df["categoria"] == "Delivery")].copy()
    ing = df[(df["tipo"] == "INGRESO") & (df["categoria"] == "Delivery")]
    gast_act = float(g[g["ciclo_id"] == inicio.strftime("%Y-%m-%d")]["monto"].sum()) if not g.empty else 0.0
    dev_act = float(ing[ing["ciclo_id"] == inicio.strftime("%Y-%m-%d")]["monto"].sum()) if not ing.empty else 0.0
    neto = max(0.0, gast_act - dev_act)
    lineas = [f"🛵 DELIVERY — {etq}", f"• Gastado: ${gast_act:,.0f}", f"• Devoluciones: ${dev_act:,.0f}", f"• Neto: ${neto:,.0f}"]
    # presupuesto si existe
    ahora = ahora_argentina()
    with get_db_connection() as conn:
        dfp = pd.read_sql(
            "SELECT monto_limite FROM presupuestos WHERE user_id = %s AND LOWER(categoria) LIKE %s ORDER BY id DESC LIMIT 1;",
            conn, params=(user_id, "%delivery%"),
        )
    if not dfp.empty:
        lim = float(dfp.iloc[0]["monto_limite"])
        pct = neto / lim * 100 if lim else 0
        em = "🟢" if pct < 70 else ("🟡" if pct < 95 else "🔴")
        lineas.append(f"• Tope: ${lim:,.0f}  {em} {pct:.0f}%")
        if pct >= 80:
            lineas.append("⚠️ Ya usaste el 80%+ del presupuesto de Delivery.")
    else:
        lineas.append("Tip: 'Presupuesto Delivery 80000' para avisarte al 80%.")
    return "\n".join(lineas)

# ==================== CONCILIACIÓN ENTRE BANCO Y MERCADO PAGO ====================
def buscar_coincidencia_previa_db(user_id: int, monto: float, fecha_str: str, origen_nuevo: str = "EXCEL"):
    if origen_nuevo != "PDF_MP":
        return None

    try:
        f_dt = datetime.strptime(fecha_str, "%Y-%m-%d")
        f_min = (f_dt - timedelta(days=2)).strftime("%Y-%m-%d")
        f_max = (f_dt + timedelta(days=2)).strftime("%Y-%m-%d")
    except Exception:
        f_min, f_max = fecha_str, fecha_str

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, fecha, tipo, monto, categoria, descripcion 
                FROM movimientos 
                WHERE user_id = %s 
                  AND tipo = 'GASTO' 
                  AND ABS(monto - %s) < 0.05 
                  AND fecha >= %s::date AND fecha <= %s::date
                  AND (UPPER(descripcion) LIKE '%%DEBIN%%' OR UPPER(descripcion) LIKE '%%30703088534%%' OR UPPER(descripcion) LIKE '%%MERCADOLIBRE%%')
                ORDER BY id DESC LIMIT 1;
            """, (user_id, monto, f_min, f_max))
            res = cursor.fetchone()
            if res:
                return {
                    "id": res[0],
                    "fecha": res[1].strftime("%Y-%m-%d") if hasattr(res[1], "strftime") else str(res[1])[:10],
                    "tipo": res[2],
                    "monto": float(res[3]),
                    "categoria": res[4],
                    "descripcion": res[5]
                }
    return None

def borrar_movimiento_por_id_db(user_id: int, mov_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM movimientos WHERE id = %s AND user_id = %s;", (mov_id, user_id))
            conn.commit()

# ==================== PARSER EXTRACTOS (FECHAS CORREGIDAS DD/MM/YYYY) ====================
def parsear_extracto_bancario_excel(file_bytes: bytes) -> list:
    try:
        df_raw = pd.read_excel(io.BytesIO(file_bytes), header=None)
    except Exception:
        return []

    header_idx = None
    for idx, row in df_raw.iterrows():
        row_str = " ".join([str(v).lower() for v in row if pd.notnull(v)])
        if ("fecha" in row_str or "date" in row_str) and any(w in row_str for w in ["concepto", "descripcion", "detalle", "importe", "monto", "debito", "movimiento", "saldo"]):
            header_idx = idx
            break

    if header_idx is not None:
        df = df_raw.iloc[header_idx + 1:].copy()
        df.columns = [str(c).strip().lower() for c in df_raw.iloc[header_idx]]
    else:
        df = df_raw.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]

    cols = list(df.columns)
    col_fecha = next((c for c in cols if any(k in c for k in ["fecha", "date"])), None)
    col_desc = next((c for c in cols if any(k in c for k in ["concepto", "descrip", "detalle", "movimiento", "referencia"])), None)
    col_deb = next((c for c in cols if any(k in c for k in ["debito", "débito", "egreso", "salida"])), None)
    col_cred = next((c for c in cols if any(k in c for k in ["credito", "crédito", "ingreso", "entrada"])), None)
    col_monto = next((c for c in cols if any(k in c for k in ["importe", "monto", "total"])), None)

    movimientos = []
    for _, r in df.iterrows():
        f_raw = r[col_fecha] if col_fecha and pd.notnull(r[col_fecha]) else None
        desc_val = str(r[col_desc]).strip() if col_desc and pd.notnull(r[col_desc]) else "Sin concepto"
        if pd.isnull(f_raw) or str(f_raw).lower() in ["nan", "none", "fecha", ""]:
            continue

        try:
            if isinstance(f_raw, (datetime, pd.Timestamp)):
                f_limpia = f_raw.strftime('%Y-%m-%d')
            else:
                f_limpia = pd.to_datetime(f_raw, dayfirst=True).strftime('%Y-%m-%d')
        except Exception:
            f_cand = str(f_raw).split()[0]
            f_limpia = f_cand if re.match(r"^\d{4}-\d{2}-\d{2}$", f_cand) else ahora_argentina().strftime('%Y-%m-%d')

        monto = 0.0
        tipo = "GASTO"

        def _limpiar_num(val):
            s = str(val).replace("$", "").replace("ARS", "").replace(" ", "").strip()
            if "," in s and "." in s:
                if s.find(".") < s.find(","):
                    s = s.replace(".", "").replace(",", ".")
                else:
                    s = s.replace(",", "")
            elif "," in s:
                s = s.replace(",", ".")
            return float(s)

        if col_deb and col_cred:
            val_deb = _limpiar_num(r[col_deb]) if pd.notnull(r[col_deb]) and str(r[col_deb]).strip() != "" else 0.0
            val_cred = _limpiar_num(r[col_cred]) if pd.notnull(r[col_cred]) and str(r[col_cred]).strip() != "" else 0.0
            if val_deb and abs(val_deb) > 0:
                monto = abs(val_deb)
                tipo = "GASTO"
            elif val_cred and abs(val_cred) > 0:
                monto = abs(val_cred)
                tipo = "INGRESO"
        elif col_monto:
            val_m = _limpiar_num(r[col_monto]) if pd.notnull(r[col_monto]) and str(r[col_monto]).strip() != "" else 0.0
            if val_m < 0:
                monto = abs(val_m)
                tipo = "GASTO"
            elif val_m > 0:
                monto = val_m
                tipo = "INGRESO"

        desc_low = desc_val.lower()
        if "luciano tofalo" in desc_low:
            continue

        if monto > 0:
            movimientos.append({
                "fecha": f_limpia,
                "concepto": desc_val,
                "monto": monto,
                "tipo": tipo
            })

    return movimientos

def parsear_extracto_pdf_mercadopago(file_bytes: bytes) -> list:
    try:
        reader = pypdf.PdfReader(io.BytesIO(file_bytes))
        texto_completo = ""
        for page in reader.pages:
            t = page.extract_text() or ""
            texto_completo += t + "\n"
    except Exception as e:
        logger.error(f"Error leyendo PDF: {e}")
        return []

    patron = re.compile(
        r"(\d{2}-\d{2}-\d{4})\s*\|\s*(.*?)\s*\|\s*(\d+)\s*\|\s*(\$?\s*-?[\d\.,]+)\s*\|\s*\$?\s*[\d\.,]+",
        re.MULTILINE
    )

    movs_crudos = []
    ids_operacion = {}

    for match in patron.finditer(texto_completo):
        fecha_str, desc, op_id, valor_str = match.groups()
        desc = " ".join(desc.split())

        val_clean = valor_str.replace("$", "").replace(" ", "").strip()
        if "," in val_clean and "." in val_clean:
            val_clean = val_clean.replace(".", "").replace(",", ".")
        elif "," in val_clean:
            val_clean = val_clean.replace(",", ".")
        
        try:
            monto = float(val_clean)
        except Exception:
            continue

        d, m, y = fecha_str.split("-")
        fecha_iso = f"{y}-{m}-{d}"

        item = {
            "fecha": fecha_iso,
            "concepto": desc,
            "id_op": op_id,
            "monto": abs(monto),
            "tipo": "INGRESO" if monto > 0 else "GASTO"
        }
        movs_crudos.append(item)
        ids_operacion.setdefault(op_id, []).append(item)

    movimientos_finales = []
    for item in movs_crudos:
        desc_low = item["concepto"].lower()

        if "rendimiento" in desc_low:
            continue

        if "luciano tofalo" in desc_low:
            continue

        if item["tipo"] == "INGRESO":
            hermanos = ids_operacion.get(item["id_op"], [])
            hay_salida_espejo = any(h["tipo"] == "GASTO" and abs(h["monto"] - item["monto"]) < 0.01 for h in hermanos)
            if hay_salida_espejo:
                continue

        movimientos_finales.append(item)

    return movimientos_finales

# ==================== CONSULTAS DE MERCADO EN VIVO ====================
CRIPTOS_COMUNES = {
    "BTC", "ETH", "SOL", "BNB", "ADA", "XRP", "DOGE", "SUI", 
    "PAXG", "NEXO", "AVAX", "DOT", "LINK", "NEAR", "RENDER", "PEPE"
}

def normalizar_ticker_yf(ticker: str):
    ticker = ticker.strip().upper()
    if ticker in CRIPTOS_COMUNES:
        return f"{ticker}-USD"
    return ticker

def consultar_datos_mercado(ticker: str):
    ticker = ticker.strip().upper()
    simbolos_a_probar = [normalizar_ticker_yf(ticker)]
    if not ticker.endswith("-USD") and ticker not in CRIPTOS_COMUNES:
        simbolos_a_probar.append(f"{ticker}-USD")

    for sym in simbolos_a_probar:
        try:
            t = yf.Ticker(sym)
            df_hist = yf_history(sym, period="5d")
            if df_hist is not None and not df_hist.empty:
                df_hist = df_hist.dropna(subset=['Close'])
                if not df_hist.empty:
                    last_price = float(df_hist['Close'].iloc[-1])
                    day_high = float(df_hist['High'].iloc[-1])
                    day_low = float(df_hist['Low'].iloc[-1])
                    prev_close = float(df_hist['Close'].iloc[-2]) if len(df_hist) > 1 else float(df_hist['Open'].iloc[-1])
                    var_usd = last_price - prev_close
                    var_pct = (var_usd / prev_close * 100) if prev_close else 0.0
                    return {
                        "ticker": sym,
                        "precio": last_price,
                        "prev_close": prev_close,
                        "var_usd": var_usd,
                        "var_pct": var_pct,
                        "day_high": day_high,
                        "day_low": day_low
                    }

            fi = getattr(t, "fast_info", None)
            if fi:
                last_price = None
                try:
                    last_price = getattr(fi, "last_price", None) or fi.get("last_price", None)
                except Exception:
                    pass

                if last_price and not np.isnan(last_price):
                    prev_close = None
                    try:
                        prev_close = getattr(fi, "previous_close", None) or fi.get("previous_close", last_price)
                    except Exception:
                        prev_close = last_price

                    var_usd = (last_price - prev_close) if prev_close else 0.0
                    var_pct = (var_usd / prev_close * 100) if prev_close else 0.0
                    return {
                        "ticker": sym,
                        "precio": float(last_price),
                        "prev_close": float(prev_close) if prev_close else None,
                        "var_usd": float(var_usd),
                        "var_pct": float(var_pct),
                        "day_high": getattr(fi, "day_high", None) if hasattr(fi, "day_high") else None,
                        "day_low": getattr(fi, "day_low", None) if hasattr(fi, "day_low") else None
                    }
        except Exception as e:
            logger.warning(f"Intento fallido con ticker {sym}: {e}")
            continue
    return None

def obtener_precio_actual(ticker: str):
    datos = consultar_datos_mercado(ticker)
    if datos:
        return datos["precio"], datos["ticker"]
    return None, ticker

# ==================== CÁLCULO DE FECHAS ====================
def resolver_fecha_inicio(periodo_str: str, fecha_compra_db: str = None):
    p = periodo_str.strip().lower() if periodo_str else ""
    hoy = datetime.now()
    
    if p in ["todo", "max", "historico", "histórico", "desde el inicio", "desde siempre", "total"]:
        if fecha_compra_db:
            return fecha_compra_db, f"Histórico total (desde {fecha_compra_db})"
        return (hoy - timedelta(days=365 * 3)).strftime('%Y-%m-%d'), "Histórico"

    m_meses = re.search(r"(\d+)\s*(?:mes|meses|mo)", p)
    m_dias = re.search(r"(\d+)\s*(?:dia|dias|d)", p)
    m_anos = re.search(r"(\d+)\s*(?:ano|anos|año|años|y)", p)
    
    if m_meses:
        cant_meses = int(m_meses.group(1))
        return (hoy - timedelta(days=cant_meses * 30)).strftime('%Y-%m-%d'), f"Últimos {cant_meses} meses"
    elif m_dias:
        cant_dias = int(m_dias.group(1))
        return (hoy - timedelta(days=cant_dias)).strftime('%Y-%m-%d'), f"Últimos {cant_dias} días"
    elif m_anos:
        cant_anos = int(m_anos.group(1))
        return (hoy - timedelta(days=cant_anos * 365)).strftime('%Y-%m-%d'), f"Últimos {cant_anos} año(s)"
    elif p in ["1m", "1mo", "un mes"]:
        return (hoy - timedelta(days=30)).strftime('%Y-%m-%d'), "Último mes"
    elif p in ["2m", "2mo", "dos meses"]:
        return (hoy - timedelta(days=60)).strftime('%Y-%m-%d'), "Últimos 2 meses"
    elif p in ["3m", "3mo", "tres meses"]:
        return (hoy - timedelta(days=90)).strftime('%Y-%m-%d'), "Últimos 3 meses"
    elif p in ["1y", "1a", "un año"]:
        return (hoy - timedelta(days=365)).strftime('%Y-%m-%d'), "Último año"
    elif p in ["ytd", "year to date", "este año", "este ano"]:
        return datetime(hoy.year, 1, 1).strftime('%Y-%m-%d'), f"YTD {hoy.year}"
    elif p in ["mtd", "este mes"]:
        return datetime(hoy.year, hoy.month, 1).strftime('%Y-%m-%d'), f"MTD {hoy.month:02d}/{hoy.year}"
    elif p in ["wtd", "esta semana"]:
        return (hoy - timedelta(days=hoy.weekday())).strftime('%Y-%m-%d'), "Esta semana"
    
    if fecha_compra_db:
        return fecha_compra_db, f"Desde tu primera inversión ({fecha_compra_db})"
    
    return (hoy - timedelta(days=180)).strftime('%Y-%m-%d'), "Últimos 6 meses"

# ==================== OPERACIONES DE MOVIMIENTOS ARS ====================
def guardar_movimiento(user_id: int, tipo: str, monto: float, categoria: str, descripcion: str, fecha_str: str = None):
    tipo = tipo.strip().upper()
    categoria = str(normalizar_categoria(categoria))[:50]
    descripcion = (descripcion or "").strip()[:240]
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_str:
                cursor.execute(
                    """INSERT INTO movimientos (user_id, fecha, tipo, monto, categoria, descripcion)
                       VALUES (%s, %s, %s, %s, %s, %s);""",
                    (user_id, fecha_str, tipo, float(monto), categoria, descripcion)
                )
            else:
                cursor.execute(
                    """INSERT INTO movimientos (user_id, fecha, tipo, monto, categoria, descripcion)
                       VALUES (%s, NOW(), %s, %s, %s, %s);""",
                    (user_id, tipo, float(monto), categoria, descripcion)
                )
            conn.commit()

# ==================== MOTOR CUANTITATIVO DE FINANZAS PERSONALES (ARS) ====================
def calcular_metricas_finanzas_completas(user_id: int, meses_lookback: int = 6):
    with get_db_connection() as conn:
        df_mov = pd.read_sql(
            """SELECT fecha, tipo, monto, categoria, descripcion 
               FROM movimientos 
               WHERE user_id = %s 
               AND fecha > NOW() - INTERVAL '%s months'
               ORDER BY fecha ASC;""",
            conn, params=(user_id, meses_lookback)
        )
    
    if df_mov.empty:
        return "No tenés movimientos registrados en los últimos meses para analizar."

    df_mov['fecha'] = pd.to_datetime(df_mov['fecha']).dt.tz_localize(None)
    df_mov['categoria'] = df_mov['categoria'].apply(normalizar_categoria)
    sueldos = obtener_fechas_sueldo(user_id)
    df_mov = asignar_ciclo_havas(df_mov, sueldos)
    df_mov['mes_periodo'] = df_mov['ciclo_id']

    def es_registro_ahorro(row):
        cat = str(row['categoria']).lower().strip()
        desc = str(row['descripcion']).lower().strip()
        blob = f"{cat} {desc}"
        if any(t in blob for t in TAGS_AHORRO):
            return True
        if "fima" in blob or "compra de dólar" in blob or "compra de dolar" in blob:
            return True
        return False

    def es_pago_deuda(row):
        cat = normalizar_categoria(str(row.get("categoria", "")))
        if cat == "Tarjeta de crédito":
            return True
        blob = f"{row.get('categoria','')} {row.get('descripcion','')}".lower()
        if any(t in blob for t in TAGS_DEUDA):
            return True
        return False

    def es_capital_tercero(row):
        cat = str(row['categoria']).lower()
        desc = str(row['descripcion']).lower()
        if row['tipo'] != 'INGRESO':
            return False
        if es_sueldo_haberes(cat, desc):
            return False
        blob = f"{cat} {desc}"
        if any(t in blob for t in TAGS_AHORRO):
            return True
        monto = float(row.get("monto") or 0)
        if "delfina" in blob and monto >= 100000:
            return True
        return False

    df_mov['es_ahorro'] = df_mov.apply(es_registro_ahorro, axis=1)
    df_mov['es_capital'] = df_mov.apply(es_capital_tercero, axis=1)
    df_mov['es_deuda'] = df_mov.apply(es_pago_deuda, axis=1)

    df_gastos_totales = df_mov[df_mov['tipo'] == 'GASTO'].copy()
    df_ingresos_totales = df_mov[df_mov['tipo'] == 'INGRESO'].copy()

    df_consumo = df_gastos_totales[~df_gastos_totales['es_ahorro']].copy()
    df_salidas_ahorro = df_gastos_totales[df_gastos_totales['es_ahorro']].copy()
    df_pagos_deuda = df_gastos_totales[df_gastos_totales['es_deuda'] & ~df_gastos_totales['es_ahorro']].copy()
    df_consumo = netear_devoluciones_en_consumo(df_consumo, df_ingresos_totales)

    df_ingresos_reales = df_ingresos_totales[~df_ingresos_totales['es_ahorro'] & ~df_ingresos_totales['es_capital']].copy()
    df_entradas_ahorro = df_ingresos_totales[df_ingresos_totales['es_ahorro'] | df_ingresos_totales['es_capital']].copy()

    cant_meses_reales = max(1, df_mov['ciclo_id'].nunique())

    tot_ingresos = float(df_ingresos_reales['monto'].sum()) if not df_ingresos_reales.empty else 0.0
    tot_consumo = float(df_consumo['monto'].sum()) if not df_consumo.empty else 0.0
    tot_ahorro_derivado = float(df_salidas_ahorro['monto'].sum()) if not df_salidas_ahorro.empty else 0.0
    tot_capital = float(df_entradas_ahorro['monto'].sum()) if not df_entradas_ahorro.empty else 0.0
    tot_deuda = float(df_pagos_deuda['monto'].sum()) if not df_pagos_deuda.empty else 0.0

    prom_ingreso_mensual = tot_ingresos / cant_meses_reales
    prom_consumo_mensual = tot_consumo / cant_meses_reales
    prom_ahorro_derivado = tot_ahorro_derivado / cant_meses_reales
    
    superavit_mensual_prom = prom_ingreso_mensual - prom_consumo_mensual
    tasa_ahorro_pct = ((tot_ingresos - tot_consumo) / tot_ingresos * 100.0) if tot_ingresos > 0 else 0.0

    df_hormiga = df_consumo[df_consumo['monto'] <= UMBRAL_GASTO_HORMIGA_ARS] if not df_consumo.empty else pd.DataFrame()
    tot_hormiga = float(df_hormiga['monto'].sum()) if not df_hormiga.empty else 0.0
    prom_hormiga_mes = tot_hormiga / cant_meses_reales
    pct_hormiga_sobre_consumo = (tot_hormiga / tot_consumo * 100.0) if tot_consumo > 0 else 0.0
    compras_hormiga_por_mes = len(df_hormiga) / cant_meses_reales

    cat_totales = df_consumo.groupby('categoria')['monto'].sum().sort_values(ascending=False) if not df_consumo.empty else pd.Series()
    
    inicio_ciclo, fin_ciclo, etq_ciclo = resolver_ciclo_havas(user_id)
    mask_ciclo = (df_consumo['fecha'] >= inicio_ciclo) & (df_consumo['fecha'] < fin_ciclo)
    df_mes_actual = df_consumo[mask_ciclo] if not df_consumo.empty else pd.DataFrame()
    consumo_mes_actual = float(df_mes_actual['monto'].sum()) if not df_mes_actual.empty else 0.0
    dias_ciclo = max(1, (min(pd.Timestamp(ahora_argentina()), fin_ciclo - pd.Timedelta(days=1)) - inicio_ciclo).days + 1)
    dia_del_mes = dias_ciclo
    dias_en_mes = max(28, (fin_ciclo - inicio_ciclo).days)
    proyeccion_mes_actual = (consumo_mes_actual / dia_del_mes * dias_en_mes) if dia_del_mes > 0 else consumo_mes_actual
    desvio_pct_vs_prom = ((proyeccion_mes_actual - prom_consumo_mensual) / prom_consumo_mensual * 100.0) if prom_consumo_mensual > 0 else 0.0

    resumen_cartera = obtener_resumen_portafolio(user_id)
    patrimonio_usd = float(resumen_cartera['total_actual']) if resumen_cartera else 0.0
    patrimonio_ars_aprox = patrimonio_usd * 1300.0
    runway_meses = (patrimonio_ars_aprox / prom_consumo_mensual) if prom_consumo_mensual > 0 else 0.0

    lineas = [
        f"📊 RADIOGRAFÍA FINANCIERA ({cant_meses_reales} ciclos Havas→Havas)",
        f"• Ciclo actual: {etq_ciclo}",
        "",
        "💵 Flujo de Caja y Ahorro",
        f"• Ingresos habituales:     ${prom_ingreso_mensual:,.0f} ARS/mes",
        f"• Costo de vida (consumo): ${prom_consumo_mensual:,.0f} ARS/mes",
        f"• Derivado a Ahorro/USDT:  ${prom_ahorro_derivado:,.0f} ARS/mes",
        f"• Capital recibido (no sueldo): ${tot_capital / cant_meses_reales:,.0f} ARS/mes",
        f"• Capacidad neta de ahorro: ${superavit_mensual_prom:+,.0f} ARS/mes",
        f"• Tasa de ahorro real:     {tasa_ahorro_pct:.1f}% del ingreso"
    ]

    em_ahorro = "🟢 Excelente capacidad de capitalización" if tasa_ahorro_pct >= 30 else ("🟡 Ahorro moderado" if tasa_ahorro_pct >= 10 else "🔴 Margen de ahorro muy ajustado")
    lineas.append(f"• Diagnóstico: {em_ahorro}")

    lineas.append("")
    lineas.append("🐜 Gastos Hormiga en Consumo (≤ $15.000 ARS)")
    lineas.append(f"• Fuga total acumulada: ${tot_hormiga:,.0f} ARS ({len(df_hormiga)} compras)")
    lineas.append(f"• Impacto mensual:      ${prom_hormiga_mes:,.0f} ARS/mes ({pct_hormiga_sobre_consumo:.1f}% de tus gastos de vida)")
    lineas.append(f"• Frecuencia:           ~{compras_hormiga_por_mes:.0f} compras chicas al mes")
    
    if not df_hormiga.empty:
        top_h_cat = df_hormiga.groupby('categoria')['monto'].sum().sort_values(ascending=False).head(3)
        top_str = ", ".join([f"{c} (${v:,.0f})" for c, v in top_h_cat.items()])
        lineas.append(f"• Principales focos:    {top_str}")

    lineas.append("")
    lineas.append("🏆 Salidas Principales de Consumo (Ley de Pareto)")
    acum = 0.0
    for cat, val in cat_totales.head(4).items():
        pct = (val / tot_consumo * 100.0) if tot_consumo > 0 else 0.0
        prom_cat = val / cant_meses_reales
        lineas.append(f"• {cat:<18} → ${prom_cat:,.0f} ARS/mes ({pct:.1f}%)")
        acum += pct
    if tot_consumo > 0:
        lineas.append(f"   (Estas categorías explican el {acum:.0f}% de tus gastos reales de vida)")

    lineas.append("")
    lineas.append("⏱️ Control del Mes en Curso y Runway")
    signo_d = "+" if desvio_pct_vs_prom >= 0 else ""
    em_d = "🔴" if desvio_pct_vs_prom > 15 else ("🟢" if desvio_pct_vs_prom < -5 else "🟡")
    lineas.append(f"• Consumido este ciclo: ${consumo_mes_actual:,.0f} ARS (día {dia_del_mes}/{dias_en_mes})")
    lineas.append(f"• Ritmo proyectado:     {em_d} {signo_d}{desvio_pct_vs_prom:.1f}% vs promedio histórico")
    if runway_meses > 0:
        lineas.append(f"• Runway de respaldo:   {runway_meses:.1f} meses de costo de vida cubiertos con tu cartera")

    try:
        deliv = texto_alerta_delivery(user_id)
        if deliv:
            lineas.append("")
            lineas.append(deliv)
    except Exception:
        pass

    return "\n".join(lineas)

# ==================== RENDIMIENTO MENSUAL EXACTO (CONSISTENTE CON /SPY) ====================
def calcular_rendimiento_por_meses(user_id: int, anio: int = None):
    with get_db_connection() as conn:
        df_tc = pd.read_sql(
            "SELECT fecha, pnl_usd, monto_invertido FROM trades_cerrados WHERE user_id = %s ORDER BY fecha ASC;",
            conn, params=(user_id,)
        )
        df_inv = pd.read_sql(
            "SELECT monto_total_usd FROM portafolio_inversiones WHERE user_id = %s;",
            conn, params=(user_id,)
        )

    if df_tc.empty:
        return "No tenés historial de trades cerrados para calcular rendimientos mensuales."

    cap_abierto = float(df_inv['monto_total_usd'].sum()) if not df_inv.empty else 0.0
    cap_tc_pico = float(df_tc['monto_invertido'].max()) if 'monto_invertido' in df_tc and pd.notnull(df_tc['monto_invertido'].max()) else 2000.0
    capital_ref = max(cap_abierto, cap_tc_pico, 2500.0)

    df_tc['fecha'] = pd.to_datetime(df_tc['fecha'])

    if anio is not None:
        df_tc = df_tc[df_tc['fecha'].dt.year == anio]
        if df_tc.empty:
            return f"No tenés trades cerrados registrados en el año {anio}."

    df_tc['periodo'] = df_tc['fecha'].dt.to_period('M')

    agrupado = df_tc.groupby('periodo').agg(
        pnl_mes=('pnl_usd', 'sum'),
        cant_ops=('pnl_usd', 'count'),
        ganadores=('pnl_usd', lambda s: (s > 0).sum())
    ).reset_index()

    fecha_min = df_tc['fecha'].min().strftime('%Y-%m-%d')
    spy_hist = None
    try:
        hspy = yf_history("SPY", start=fecha_min, auto_adjust=True)
        if hspy is not None and not hspy.empty:
            spy_hist = hspy['Close'].dropna()
            spy_hist.index = pd.to_datetime(spy_hist.index).tz_localize(None)
    except Exception:
        spy_hist = None

    mapa_meses = {
        1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
    }

    titulo_anio = f" ({anio})" if anio else " (Histórico)"
    lineas = [
        f"📅 RENDIMIENTO MENSUAL DE CARTERA{titulo_anio}",
        f"• Base de capital de referencia: ${capital_ref:,.2f} USD",
        ""
    ]

    for _, row in agrupado.iterrows():
        periodo = row['periodo']
        nombre_mes = f"{mapa_meses.get(periodo.month, str(periodo.month))} {periodo.year}"
        pnl_m = float(row['pnl_mes'])
        ret_pct = (pnl_m / capital_ref) * 100.0
        
        signo = "+" if ret_pct >= 0 else ""
        emoji = "🟢" if ret_pct > 0 else ("🔴" if ret_pct < 0 else "⚪")
        wr = (row['ganadores'] / row['cant_ops'] * 100) if row['cant_ops'] > 0 else 0.0

        extra_spy = ""
        if spy_hist is not None:
            mask_m = (spy_hist.index.year == periodo.year) & (spy_hist.index.month == periodo.month)
            sub_spy = spy_hist.loc[mask_m]
            if len(sub_spy) >= 2:
                r_spy = (sub_spy.iloc[-1] / sub_spy.iloc[0] - 1.0) * 100.0
                signo_s = "+" if r_spy >= 0 else ""
                extra_spy = f" | SPY: {signo_s}{r_spy:.1f}%"

        lineas.append(
            f"{emoji} {nombre_mes:<8} → {signo}{ret_pct:>5.2f}% ({signo}${pnl_m:>7,.2f} USD) | {row['cant_ops']} ops (WR: {wr:.0f}%){extra_spy}"
        )

    pnl_acumulado = float(df_tc['pnl_usd'].sum())
    ret_total = (pnl_acumulado / capital_ref) * 100.0
    signo_tot = "+" if ret_total >= 0 else ""
    lineas.append("")
    tag_tot = f"Total {anio}" if anio else "Total histórico acumulado"
    lineas.append(f"🏆 {tag_tot}: {signo_tot}{ret_total:.2f}% ({signo_tot}${pnl_acumulado:,.2f} USD)")

    return "\n".join(lineas)

# ==================== EXPORTACIÓN A EXCEL ====================
def generar_excel_completo(user_id: int):
    try:
        with get_db_connection() as conn:
            df_mov = pd.read_sql("SELECT fecha, tipo, monto, categoria, descripcion FROM movimientos WHERE user_id = %s ORDER BY fecha DESC;", conn, params=(user_id,))
            df_inv = pd.read_sql("SELECT fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion FROM portafolio_inversiones WHERE user_id = %s ORDER BY fecha DESC;", conn, params=(user_id,))
            df_tc = pd.read_sql("SELECT fecha, fecha_apertura, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion FROM trades_cerrados WHERE user_id = %s ORDER BY fecha DESC;", conn, params=(user_id,))

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df_mov.to_excel(writer, sheet_name='Movimientos_ARS', index=False)
            df_inv.to_excel(writer, sheet_name='Cartera_Abierta', index=False)
            df_tc.to_excel(writer, sheet_name='Trades_Cerrados', index=False)
        buf.seek(0)
        return buf
    except Exception as e:
        logger.error(f"Error generando Excel: {e}")
        return None

# ==================== GRÁFICO CONSOLIDADO: EVOLUCIÓN CARTERA ====================
def generar_grafico_evolucion_cartera_consolidada(user_id: int, periodo_solicitado: str = "", modo: str = "usd"):
    try:
        with get_db_connection() as conn:
            df_inv = pd.read_sql(
                "SELECT fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento FROM portafolio_inversiones WHERE user_id = %s ORDER BY fecha ASC;",
                conn, params=(user_id,)
            )
            df_tc = pd.read_sql(
                "SELECT fecha, fecha_apertura, ticker, tipo_posicion, pnl_usd, monto_invertido, descripcion FROM trades_cerrados WHERE user_id = %s ORDER BY fecha ASC;",
                conn, params=(user_id,)
            )

        if df_inv.empty and df_tc.empty:
            return None

        fechas_candidatas = []
        if not df_inv.empty:
            fechas_candidatas.append(df_inv['fecha'].min())
        if not df_tc.empty:
            fechas_candidatas.append(df_tc['fecha'].min())
            if 'fecha_apertura' in df_tc.columns and df_tc['fecha_apertura'].notna().any():
                fechas_candidatas.append(df_tc['fecha_apertura'].min())

        primera_fecha_global = min(fechas_candidatas).strftime('%Y-%m-%d')
        fecha_start, desc_periodo = resolver_fecha_inicio(periodo_solicitado, primera_fecha_global)

        tickers_unicos = list(df_inv['ticker'].unique()) if not df_inv.empty else []
        precios_hist = {}
        for tk in tickers_unicos:
            if tk in ["USDT", "USDC", "DAI", "USD"]:
                continue
            sym = normalizar_ticker_yf(tk)
            try:
                h = yf_history(sym, start=fecha_start)
                if not h.empty:
                    s = h['Close'].ffill().bfill()
                    s.index = pd.to_datetime(s.index).tz_localize(None)
                    precios_hist[tk] = s
            except Exception:
                pass

        fechas_rango = pd.date_range(start=fecha_start, end=datetime.now().strftime('%Y-%m-%d'), freq='D')
        df_precios = pd.DataFrame(index=fechas_rango)
        for tk, s in precios_hist.items():
            df_precios[tk] = s
        df_precios = df_precios.ffill().bfill()

        serie_capital_abierto = pd.Series(0.0, index=fechas_rango)
        serie_valor_mercado = pd.Series(0.0, index=fechas_rango)
        serie_pnl_cerrado_acum = pd.Series(0.0, index=fechas_rango)

        if not df_tc.empty:
            df_tc['fecha_d'] = pd.to_datetime(df_tc['fecha']).dt.tz_localize(None).dt.floor('D')
            f_a_list = []
            for _, tr in df_tc.iterrows():
                f_a = tr['fecha_apertura'] if 'fecha_apertura' in tr and pd.notnull(tr['fecha_apertura']) else None
                if pd.isnull(f_a) and 'descripcion' in tr and tr['descripcion']:
                    m_ap = re.search(r"Apertura\s+(\d{4}-\d{2}-\d{2})", str(tr['descripcion']))
                    if m_ap:
                        f_a = m_ap.group(1)
                f_a = pd.to_datetime(f_a).tz_localize(None).floor('D') if pd.notnull(f_a) else tr['fecha_d']
                f_a_list.append(f_a)
            df_tc['fecha_a'] = f_a_list

            incr = pd.Series(0.0, index=fechas_rango)
            for _, tr in df_tc.iterrows():
                pnl = float(tr['pnl_usd'] or 0)
                f_c = tr['fecha_d']
                f_a = tr['fecha_a']
                if pd.isnull(f_a) or pd.isnull(f_c):
                    continue
                if f_a > f_c:
                    f_a = f_c

                mask = (fechas_rango >= f_a) & (fechas_rango <= f_c)
                n = int(mask.sum())
                if n <= 1:
                    if f_c in incr.index:
                        incr.loc[f_c] += pnl
                else:
                    incr.loc[mask] += pnl / n

            serie_pnl_cerrado_acum = incr.cumsum()

        for _, pos in df_inv.iterrows():
            pos_fecha = pd.to_datetime(pos['fecha']).tz_localize(None).floor('D')
            tk = pos['ticker']
            cant = float(pos['cantidad'])
            ppc = float(pos['precio_compra']) if pos['precio_compra'] else 1.0
            margen = float(pos['monto_total_usd'])
            tipo = str(pos['tipo_posicion']).upper() if pos['tipo_posicion'] else "SPOT"
            lev = float(pos['apalancamiento']) if pos['apalancamiento'] else 1.0

            mascara = fechas_rango >= pos_fecha
            serie_capital_abierto[mascara] += margen

            if tk in ["USDT", "USDC", "DAI", "USD"] or tk not in df_precios.columns:
                serie_valor_mercado[mascara] += margen
            else:
                spot_t = df_precios[tk].loc[mascara]
                if tipo == "SHORT":
                    val_t = np.maximum(0.0, margen + margen * ((ppc - spot_t) / ppc) * lev)
                elif tipo == "LONG":
                    val_t = np.maximum(0.0, margen + margen * ((spot_t - ppc) / ppc) * lev)
                else:
                    val_t = cant * spot_t
                serie_valor_mercado[mascara] += val_t

        serie_pnl_flotante = serie_valor_mercado - serie_capital_abierto
        serie_pnl_total_usd = serie_pnl_flotante + serie_pnl_cerrado_acum

        pnl_base_inicio = float(serie_pnl_total_usd.iloc[0]) if len(serie_pnl_total_usd) else 0.0
        serie_pnl_periodo = serie_pnl_total_usd - pnl_base_inicio
        pnl_final_periodo = serie_pnl_periodo.iloc[-1] if len(serie_pnl_periodo) else 0.0

        sspy = None
        try:
            hspy = yf_history("SPY", start=fecha_start, auto_adjust=True)
            if hspy is not None and not hspy.empty:
                sspy = hspy["Close"].replace([np.inf, -np.inf], np.nan)
                sspy.index = pd.to_datetime(sspy.index).tz_localize(None)
                sspy = sspy.reindex(fechas_rango).ffill().bfill()
        except Exception:
            sspy = None

        modo = (modo or "usd").lower()
        fig, ax = plt.subplots(figsize=(10, 5.2))

        if modo in ("pct", "percent", "%", "spy"):
            cap_abierto_total = float(df_inv['monto_total_usd'].sum()) if not df_inv.empty else 2000.0
            cap_tc_pico = float(df_tc['monto_invertido'].max()) if not df_tc.empty and 'monto_invertido' in df_tc and pd.notnull(df_tc['monto_invertido'].max()) else 2000.0
            base_capital = max(cap_abierto_total, cap_tc_pico, 2500.0)

            serie_cartera_pct = (serie_pnl_periodo / base_capital) * 100.0
            ret_c = float(serie_cartera_pct.iloc[-1]) if len(serie_cartera_pct) else 0.0
            if not np.isfinite(ret_c):
                ret_c = 0.0

            color_linea = "#00b06f" if ret_c >= 0 else "#e04050"
            ax.plot(fechas_rango, serie_cartera_pct, label=f"Tu cartera ({ret_c:+.1f}%)", color=color_linea, linewidth=2.4)
            ax.fill_between(fechas_rango, serie_cartera_pct, 0, where=(serie_cartera_pct >= 0), alpha=0.15, color="#00b06f")
            ax.fill_between(fechas_rango, serie_cartera_pct, 0, where=(serie_cartera_pct < 0), alpha=0.15, color="#e04050")

            if sspy is not None and float(sspy.dropna().iloc[0]) > 0:
                base = float(sspy.dropna().iloc[0])
                serie_spy_pct = (sspy / base - 1.0) * 100.0
                spy_ret = float(serie_spy_pct.dropna().iloc[-1])
                if np.isfinite(spy_ret):
                    ax.plot(fechas_rango, serie_spy_pct, label=f"SPY ({spy_ret:+.1f}%)", color="#5b8def", linewidth=1.8, linestyle="--")

            ax.axhline(0, color="gray", linestyle="--", linewidth=1.1, alpha=0.7)
            ax.set_title(f"Rendimiento %: tu cartera vs SPY\n{desc_periodo}", fontsize=12, fontweight='bold', pad=12)
            ax.set_ylabel("Rendimiento acumulado (%)")
        else:
            color_linea = "#00b06f" if pnl_final_periodo >= 0 else "#e04050"
            ax.plot(fechas_rango, serie_pnl_periodo, label=f"PnL Período ({pnl_final_periodo:+,.2f} USD)", color=color_linea, linewidth=2.4)
            ax.fill_between(fechas_rango, serie_pnl_periodo, 0, where=(serie_pnl_periodo >= 0), alpha=0.15, color="#00b06f")
            ax.fill_between(fechas_rango, serie_pnl_periodo, 0, where=(serie_pnl_periodo < 0), alpha=0.15, color="#e04050")
            ax.axhline(0, color="gray", linestyle="--", linewidth=1.1, alpha=0.7)
            ax.set_title(f"Evolución de cartera en USD (PnL del período)\n{desc_periodo}", fontsize=12, fontweight='bold', pad=12)
            ax.set_ylabel("Ganancia / Pérdida acumulada (USD)")

        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(loc="upper left", frameon=True)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200)
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Error generando curva consolidada de cartera: {e}", exc_info=True)
        return None

# ==================== GRÁFICO POR ACTIVOS ====================
def generar_grafico_evolucion_por_activos(user_id: int, periodo_solicitado: str = "", tickers_filtro: list = None):
    try:
        with get_db_connection() as conn:
            df = pd.read_sql(
                "SELECT ticker, MIN(fecha) as primera_compra, AVG(precio_compra) as ppc_real, MAX(tipo_posicion) as tipo_pos, MAX(apalancamiento) as lev FROM portafolio_inversiones WHERE user_id = %s GROUP BY ticker;",
                conn, params=(user_id,)
            )
        
        if df.empty:
            return None

        if tickers_filtro and len(tickers_filtro) > 0:
            tickers_clean = [t.strip().upper() for t in tickers_filtro if t.strip()]
            df = df[df['ticker'].isin(tickers_clean)]
            if df.empty:
                return None
        
        primera_fecha_db = df['primera_compra'].min().strftime('%Y-%m-%d')
        fecha_start, desc_periodo = resolver_fecha_inicio(periodo_solicitado, primera_fecha_db)
        
        fechas_rango = pd.date_range(start=fecha_start, end=datetime.now().strftime('%Y-%m-%d'), freq='D')
        colores = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#17becf', '#bcbd22']
        idx_color = 0
        
        fig, ax = plt.subplots(figsize=(10.5, 5.5))
        hay_series = False

        for _, row in df.iterrows():
            tk = row['ticker']
            if tk in ["USDT", "USDC", "DAI", "USD"]:
                continue
            
            f_compra = pd.to_datetime(row['primera_compra']).tz_localize(None).floor('D')
            tipo_pos = str(row['tipo_pos']).upper() if row['tipo_pos'] else "SPOT"
            lev = float(row['lev']) if row['lev'] else 1.0

            sym = normalizar_ticker_yf(tk)
            try:
                h = yf_history(sym, start=fecha_start)
                if not h.empty:
                    s = h['Close'].ffill().bfill()
                    s.index = pd.to_datetime(s.index).tz_localize(None)
                    s = s.reindex(fechas_rango).ffill().bfill()
                    
                    if f_compra > fechas_rango[0]:
                        s[fechas_rango < f_compra] = np.nan
                    
                    s_valida = s.dropna()
                    if s_valida.empty:
                        continue
                    
                    precio_inicial_serie = s_valida.iloc[0]
                    
                    if tipo_pos == "SHORT":
                        serie_rend = 100.0 + ((precio_inicial_serie - s) / precio_inicial_serie) * lev * 100.0
                    elif tipo_pos == "LONG":
                        serie_rend = 100.0 + ((s - precio_inicial_serie) / precio_inicial_serie) * lev * 100.0
                    else:
                        serie_rend = (s / precio_inicial_serie) * 100.0
                    
                    ultimo_val = serie_rend.dropna().iloc[-1]
                    rend_pct = ultimo_val - 100.0
                    signo = "+" if rend_pct >= 0 else ""
                    tag_lev = f" [{lev:.0f}x]" if lev > 1 else ""
                    
                    c = colores[idx_color % len(colores)]
                    ax.plot(fechas_rango, serie_rend, label=f"{tk}{tag_lev} ({signo}{rend_pct:.1f}%)", linewidth=2.2, color=c)
                    idx_color += 1
                    hay_series = True
            except Exception as e:
                logger.error(f"Error procesando serie de {tk}: {e}")

        if not hay_series:
            return None

        ax.axhline(y=100, color="gray", linestyle=":", linewidth=1.4, alpha=0.8, label="Base 100 (Inicio)")
        filtro_sub = f" ({', '.join(df['ticker'].tolist())})" if tickers_filtro else ""
        ax.set_title(f"Rendimiento Relativo de tus Activos (Base 100 = Inicio){filtro_sub}\n{desc_periodo}", fontsize=12, fontweight='bold', pad=12)
        ax.set_xlabel("Fecha")
        ax.set_ylabel("Rendimiento Relativo (Base 100)")
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(loc="upper left", frameon=True)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200)
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Error generando comparativa por activos: {e}", exc_info=True)
        return None

generar_grafico_por_activos = generar_grafico_evolucion_por_activos

# ==================== EVOLUCIÓN ACTIVO INDIVIDUAL ====================
def generar_grafico_evolucion_activo(user_id: int, ticker: str, periodo_solicitado: str = ""):
    ticker = ticker.strip().upper()
    simbolo = normalizar_ticker_yf(ticker)
    fecha_compra_db = None
    ppc_referencia = None
    
    try:
        with get_db_connection() as conn:
            df_inv = pd.read_sql(
                "SELECT fecha, cantidad, monto_total_usd FROM portafolio_inversiones WHERE user_id = %s AND UPPER(ticker) = %s ORDER BY fecha ASC;",
                conn, params=(user_id, ticker)
            )
            if not df_inv.empty and df_inv['cantidad'].sum() > 0:
                fecha_compra_db = df_inv['fecha'].iloc[0].strftime('%Y-%m-%d')
                ppc_referencia = df_inv['monto_total_usd'].sum() / df_inv['cantidad'].sum()
    except Exception as e:
        logger.error(f"Error consultando fecha de activo {ticker}: {e}")

    fecha_start, desc_periodo = resolver_fecha_inicio(periodo_solicitado, fecha_compra_db)

    try:
        df_hist = yf_history(simbolo, start=fecha_start)
        if df_hist.empty and not simbolo.endswith("-USD"):
            simbolo = f"{simbolo}-USD"
            df_hist = yf_history(simbolo, start=fecha_start)

        if df_hist.empty:
            return None

        df_hist = df_hist.dropna(subset=['Close'])
        df_hist.index = pd.to_datetime(df_hist.index).tz_localize(None)
        serie = df_hist['Close'].ffill().bfill()

        plt.figure(figsize=(9.5, 5))
        plt.plot(serie.index, serie, label=f"Precio {ticker} (USD)", color="#1f77b4", linewidth=2.2)
        plt.fill_between(serie.index, serie, alpha=0.12, color="#1f77b4")
        
        if ppc_referencia:
            plt.axhline(y=ppc_referencia, color="#d62728", linestyle="--", linewidth=1.6, label=f"Tu PPC (${ppc_referencia:,.2f})")

        plt.title(f"Evolución Histórica: {ticker}\n{desc_periodo}", fontsize=12, fontweight='bold', pad=12)
        plt.xlabel("Fecha")
        plt.ylabel("Precio en USD")
        plt.grid(True, linestyle="--", alpha=0.35)
        plt.legend(loc="upper left", frameon=True)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200)
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Error graficando activo {ticker}: {e}")
        return None

# ==================== MOTOR CUANTITATIVO TÉCNICO ====================
def calcular_rsi_serie(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calcular_macd(series, fast=12, slow=26, signal=9):
    ema_f = series.ewm(span=fast, adjust=False).mean()
    ema_s = series.ewm(span=slow, adjust=False).mean()
    linea = ema_f - ema_s
    senal = linea.ewm(span=signal, adjust=False).mean()
    hist = linea - senal
    return linea, senal, hist

def calcular_atr(df, period=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    prev = close.shift(1)
    tr = pd.concat([(high - low), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def calcular_pivots_y_niveles(df, ventana=4):
    highs = df['High'].values
    lows = df['Low'].values
    precio_actual = float(df['Close'].dropna().iloc[-1])
    n = len(df)
    
    pivots_h = []
    pivots_l = []
    
    for i in range(ventana, n - ventana):
        if all(highs[i] >= highs[i - j] for j in range(1, ventana + 1)) and \
           all(highs[i] >= highs[i + j] for j in range(1, ventana + 1)):
            pivots_h.append((df.index[i], highs[i]))
            
        if all(lows[i] <= lows[i - j] for j in range(1, ventana + 1)) and \
           all(lows[i] <= lows[i + j] for j in range(1, ventana + 1)):
            pivots_l.append((df.index[i], lows[i]))

    resistencias = sorted([val for _, val in pivots_h if val > precio_actual])
    soportes = sorted([val for _, val in pivots_l if val < precio_actual], reverse=True)

    res_inmediata = resistencias[0] if resistencias else None
    res_segunda = resistencias[1] if len(resistencias) > 1 else None
    sop_inmediato = soportes[0] if soportes else None
    sop_segundo = soportes[1] if len(soportes) > 1 else None

    ult_highs = [val for _, val in pivots_h[-3:]]
    ult_lows = [val for _, val in pivots_l[-3:]]
    
    estructura_txt = "Consolidación / lateral"
    estructura_bias = "lateral"

    if ult_highs and ult_lows:
        last_high = ult_highs[-1]
        last_low = ult_lows[-1]
        prev_close = float(df['Close'].dropna().iloc[-2]) if len(df) > 1 else precio_actual

        if prev_close >= last_low and precio_actual < last_low:
            estructura_txt = f"🔴 Se rompió un piso/mínimo importante (${last_low:,.2f}) — Presión bajista activa"
            estructura_bias = "choch_bajista"
        elif prev_close <= last_high and precio_actual > last_high:
            estructura_txt = f"🟢 Se rompió un techo/máximo importante (${last_high:,.2f}) — Posible giro alcista"
            estructura_bias = "choch_alcista"
        elif len(ult_highs) >= 2 and len(ult_lows) >= 2:
            if ult_highs[-1] < ult_highs[-2] and ult_lows[-1] < ult_lows[-2]:
                estructura_txt = "Estructura BAJISTA sólida (Máximos y mínimos descendentes)"
                estructura_bias = "bajista"
            elif ult_highs[-1] > ult_highs[-2] and ult_lows[-1] > ult_lows[-2]:
                estructura_txt = "Estructura ALCISTA sólida (Máximos y mínimos ascendentes)"
                estructura_bias = "alcista"
            elif precio_actual < ult_highs[-1] and precio_actual > ult_lows[-1]:
                estructura_txt = f"Rango lateral / compresión entre ${last_low:,.2f} y ${last_high:,.2f}"
                estructura_bias = "lateral"

    fibo_info = None
    fibo_niveles = {}
    eventos = [(ts, float(val), "H") for ts, val in pivots_h] + [(ts, float(val), "L") for ts, val in pivots_l]
    eventos.sort(key=lambda x: x[0])
    if len(eventos) >= 2:
        fin = eventos[-1]
        inicio = None
        for ev in reversed(eventos[:-1]):
            if ev[2] != fin[2]:
                inicio = ev
                break
        if inicio is not None:
            p0, p1 = float(inicio[1]), float(fin[1])
            diff = p1 - p0
            if abs(diff) > 0:
                alcista = diff > 0
                retro = {
                    "0.382": p1 - 0.382 * diff,
                    "0.500": p1 - 0.500 * diff,
                    "0.618": p1 - 0.618 * diff,
                    "0.786": p1 - 0.786 * diff,
                }
                ext = {
                    "1.272": p0 + 1.272 * diff,
                    "1.618": p0 + 1.618 * diff,
                }
                dentro = (min(p0, p1) <= precio_actual <= max(p0, p1))
                roto_fin = precio_actual > max(p0, p1) if alcista else precio_actual < min(p0, p1)
                invalidado = precio_actual < min(p0, p1) if alcista else precio_actual > max(p0, p1)
                if invalidado:
                    estado = "el tramo quedó invalidado: el precio volvió detrás del origen"
                    uso = "ninguno"
                elif roto_fin:
                    estado = "el precio superó el extremo del tramo: usar extensiones"
                    uso = "extension"
                elif dentro:
                    estado = "el precio está dentro del tramo: usar retrocesos"
                    uso = "retroceso"
                else:
                    estado = "tramo vigente"
                    uso = "retroceso"
                fibo_info = {
                    "direccion": "alcista" if alcista else "bajista",
                    "desde": p0,
                    "hasta": p1,
                    "desde_fecha": pd.Timestamp(inicio[0]).strftime("%d/%m/%Y"),
                    "hasta_fecha": pd.Timestamp(fin[0]).strftime("%d/%m/%Y"),
                    "uso": uso,
                    "estado": estado,
                    "retrocesos": retro,
                    "extensiones": ext,
                }
                fibo_niveles = {
                    "Ret 0.382": retro["0.382"],
                    "Ret 0.500": retro["0.500"],
                    "Ret 0.618": retro["0.618"],
                    "Ret 0.786": retro["0.786"],
                    "Ext 1.272": ext["1.272"],
                    "Ext 1.618": ext["1.618"],
                }

    return {
        "res_inmediata": res_inmediata,
        "res_segunda": res_segunda,
        "sop_inmediato": sop_inmediato,
        "sop_segundo": sop_segundo,
        "estructura_txt": estructura_txt,
        "estructura_bias": estructura_bias,
        "fibo_niveles": fibo_niveles,
        "fibo_info": fibo_info,
    }

def calcular_tendencias(df, tf_label: str):
    close = df["Close"].dropna()
    px = float(close.iloc[-1])
    if tf_label == "Semanal":
        ventanas = [
            ("muy corto plazo", 8, "~2 meses"),
            ("mediano plazo", 26, "~6 meses"),
            ("largo plazo", 104, "~2 años"),
        ]
    elif tf_label == "4 Horas":
        ventanas = [
            ("muy corto plazo", 12, "~1 semana"),
            ("mediano plazo", 40, "~3-4 semanas"),
            ("largo plazo", 80, "~2 meses"),
        ]
    else:
        ventanas = [
            ("muy corto plazo", 10, "~2 semanas"),
            ("mediano plazo", 70, "~3-4 meses"),
            ("largo plazo", 200, "~10-12 meses"),
        ]
    out = []
    for nombre, barras, etiqueta in ventanas:
        n = min(barras, len(close))
        if n < 5:
            continue
        base = float(close.iloc[-n])
        ret = (px / base - 1.0) * 100.0 if base else 0.0
        media = float(close.iloc[-n:].mean())
        umbral = 2.0 if barras <= 15 else (4.0 if barras <= 80 else 6.0)
        if px > media and ret >= umbral:
            sesgo = "alcista"
        elif px < media and ret <= -umbral:
            sesgo = "bajista"
        elif abs(ret) < umbral:
            sesgo = "lateral"
        elif px >= media:
            sesgo = "alcista débil"
        else:
            sesgo = "bajista débil"
        out.append({
            "nombre": nombre,
            "barras": n,
            "etiqueta": etiqueta,
            "sesgo": sesgo,
            "ret": ret,
            "media": media,
        })
    return out

def calcular_poc_volumen(df, barras_lookback=90, bins_count=40):
    if "Volume" not in df.columns or df["Volume"].sum() == 0:
        return None
    sub = df.iloc[-barras_lookback:] if len(df) >= barras_lookback else df
    min_p = sub["Low"].min()
    max_p = sub["High"].max()
    if min_p == max_p or np.isnan(min_p) or np.isnan(max_p):
        return None

    bins = np.linspace(min_p, max_p, bins_count + 1)
    vol_per_bin = np.zeros(bins_count)
    typical_price = (sub["High"] + sub["Low"] + sub["Close"]) / 3.0
    vol = sub["Volume"].fillna(0).values

    for tp, v in zip(typical_price.values, vol):
        if np.isnan(tp) or np.isnan(v):
            continue
        idx = np.digitize(tp, bins) - 1
        if 0 <= idx < bins_count:
            vol_per_bin[idx] += v

    if vol_per_bin.sum() == 0:
        return None

    poc_idx = np.argmax(vol_per_bin)
    poc_price = (bins[poc_idx] + bins[poc_idx + 1]) / 2.0
    return float(poc_price)

def horizontes_por_timeframe(tf_label: str):
    """Barras hacia adelante según el timeframe real de la serie.

    El bug anterior usaba 11/21/63/126/252 también en semanal: ahí 126 velas
    son ~2.4 años, no 6 meses, e inflaba el retorno promedio.
    """
    tf = (tf_label or "").lower()
    if "sem" in tf or "1w" in tf:
        return [("15 días", 2), ("1 mes", 4), ("3 meses", 13), ("6 meses", 26), ("1 año", 52)]
    if "4" in tf:
        return [("15 días", 30), ("1 mes", 44), ("3 meses", 130), ("6 meses", 260), ("1 año", 520)]
    return [("15 días", 11), ("1 mes", 21), ("3 meses", 63), ("6 meses", 126), ("1 año", 252)]


def backtest_comportamiento_historico(df, condicion="rsi_similar", rsi_actual=None, banda=0.5, tf_label="diario"):
    if df is None or len(df) < 40 or "RSI" not in df.columns or "Close" not in df.columns:
        return None

    rsi = df["RSI"].to_numpy(dtype=float)
    close = df["Close"].to_numpy(dtype=float)
    n = len(close)
    if not np.isfinite(rsi).any():
        return None

    if rsi_actual is None or not np.isfinite(rsi_actual):
        valid = rsi[np.isfinite(rsi)]
        rsi_actual = float(valid[-1]) if len(valid) else 50.0

    eventos_idx = []
    if condicion == "rsi_similar":
        lo = float(rsi_actual) - float(banda)
        hi = float(rsi_actual) + float(banda)
        # Primera vela que entra en la banda. La última no es caso: todavía no tiene retorno.
        for i in range(1, n - 1):
            if not np.isfinite(rsi[i]) or not (lo <= rsi[i] <= hi):
                continue
            prev_in = np.isfinite(rsi[i - 1]) and lo <= rsi[i - 1] <= hi
            if not prev_in:
                eventos_idx.append(i)
        label = f"RSI similar ({lo:.2f} a {hi:.2f}, ±{float(banda):.1f})"
    elif condicion == "sobreventa_rsi":
        for i in range(1, n - 1):
            if np.isfinite(rsi[i]) and np.isfinite(rsi[i - 1]) and rsi[i] <= 40 and rsi[i - 1] > 40:
                eventos_idx.append(i)
        label = "RSI en zona baja / sobreventa (<= 40)"
    elif condicion == "sobrecompra_rsi":
        for i in range(1, n - 1):
            if np.isfinite(rsi[i]) and np.isfinite(rsi[i - 1]) and rsi[i] >= 70 and rsi[i - 1] < 70:
                eventos_idx.append(i)
        label = "RSI en sobrecompra clásica (>= 70)"
    else:
        return None

    if not eventos_idx:
        return None

    desglose = []
    for nombre_h, barras in horizontes_por_timeframe(tf_label):
        rets = []
        for idx in eventos_idx:
            j = idx + int(barras)
            if j < n and close[idx] > 0 and np.isfinite(close[idx]) and np.isfinite(close[j]):
                r = (close[j] / close[idx] - 1.0) * 100.0
                if np.isfinite(r):
                    rets.append(r)
        if not rets:
            continue
        arr = np.asarray(rets, dtype=float)
        desglose.append({
            "horizonte": nombre_h,
            "win_rate": float((arr > 0).mean() * 100.0),
            "avg_ret": float(arr.mean()),
            "mediana": float(np.median(arr)),
            "peor": float(arr.min()),
            "mejor": float(arr.max()),
            "muestras": int(arr.size),
        })

    if not desglose:
        return None

    return {
        "condicion": label,
        "total_eventos": len(eventos_idx),
        "rsi_actual": float(rsi_actual),
        "banda": float(banda),
        "tf_label": tf_label,
        "desglose": desglose,
    }


def historia_rsi_largo(simbolo: str, tf_label: str):
    """Serie larga solo para los casos. El gráfico sigue usando su ventana corta."""
    tf = (tf_label or "").lower()
    try:
        if "sem" in tf:
            df = yf_history(simbolo, start="1999-01-01", interval="1wk")
        elif "4" in tf:
            df = yf_history(simbolo, period="730d", interval="1h")
            if df is not None and not df.empty:
                df = df.resample("4h").agg({
                    "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"
                }).dropna()
        else:
            df = yf_history(simbolo, start="1999-01-01", interval="1d")
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["Close"]).copy()
        df["RSI"] = calcular_rsi_serie(df["Close"], period=14)
        return df
    except Exception as e:
        logger.warning(f"historia_rsi_largo {simbolo}: {e}")
        return None

def detectar_divergencia_rsi(df, window=25):
    if len(df) < window:
        return "Sin datos suficientes"
    
    sub = df.iloc[-window:].copy()
    precios = sub['Close'].values
    rsis = sub['RSI'].values
    rsi_act = float(rsis[-1])
    precio_act = float(precios[-1])
    n = len(precios)
    
    alerta_nivel = f"Neutral ({rsi_act:.1f})"
    if rsi_act >= 70:
        alerta_nivel = f"🔥 SOBRECOMPRA ACTIVA ({rsi_act:.1f})"
    elif rsi_act >= 65:
        alerta_nivel = f"⚠️ Por entrar en SOBRECOMPRA ({rsi_act:.1f} - cerca de techo)"
    elif rsi_act <= 30:
        alerta_nivel = f"❄️ SOBREVENTA ACTIVA ({rsi_act:.1f})"
    elif rsi_act <= 36:
        alerta_nivel = f"⚠️ Por entrar en SOBREVENTA ({rsi_act:.1f} - cerca de piso)"

    mitad = n // 2
    min_idx_1 = np.argmin(precios[:mitad])
    min_idx_2 = mitad + np.argmin(precios[mitad:])
    max_idx_1 = np.argmax(precios[:mitad])
    max_idx_2 = mitad + np.argmax(precios[mitad:])

    p1_min, r1_min = precios[min_idx_1], rsis[min_idx_1]
    p2_min, r2_min = precios[min_idx_2], rsis[min_idx_2]
    p1_max, r1_max = precios[max_idx_1], rsis[max_idx_1]
    p2_max, r2_max = precios[max_idx_2], rsis[max_idx_2]

    barras_desde_max2 = (n - 1) - max_idx_2
    barras_desde_min2 = (n - 1) - min_idx_2

    if precio_act >= p1_max * 0.985 and rsi_act < r1_max - 2.5 and rsi_act > 58:
        return f"🔮 SE ESTÁ FORMANDO UNA POSIBLE DIVERGENCIA BAJISTA (Precio testeando zona de máximos en ${precio_act:,.2f} pero el RSI pierde fuerza en {rsi_act:.1f} vs {r1_max:.1f} previo)"

    if barras_desde_max2 <= 3 and rsi_act >= 50:
        if p2_max > p1_max and r2_max < r1_max - 1.5 and r2_max > 55:
            return f"🔴 DIVERGENCIA BAJISTA CONFIRMADA (Máximo mayor en precio con RSI perdiendo fuerza: {r2_max:.1f} vs {r1_max:.1f})"

    if precio_act <= p1_min * 1.015 and rsi_act > r1_min + 2.0 and rsi_act < 44:
        return f"🔮 SE ESTÁ FORMANDO UNA POSIBLE DIVERGENCIA ALCISTA (Precio testeando mínimos en ${precio_act:,.2f} con RSI aguantando más arriba en {rsi_act:.1f} vs {r1_min:.1f} previo)"

    if barras_desde_min2 <= 3 and rsi_act <= 50:
        if p2_min < p1_min and r2_min > r1_min + 1.5 and r2_min < 45:
            return f"🟢 DIVERGENCIA ALCISTA CONFIRMADA (Mínimo menor en precio con RSI en subida: {r2_min:.1f} vs {r1_min:.1f})"

    return alerta_nivel

def formatear_reporte_tecnico(info):
    tk = info['ticker']
    tf = info['timeframe']
    p = info['precio_actual']
    e20 = info['ema20']
    e50 = info['ema50']
    e200 = info['ema200']
    rsi = info['rsi']
    diag_rsi = info['diagnostico_rsi']
    poc = info.get('poc')
    hist_stat = info.get('hist_stat')
    
    lineas = [f"📊 REPORTE TÉCNICO: {tk} ({tf})", f"• Precio actual: ${p:,.2f} USD", ""]

    lineas.append("📈 Estructura de Mercado y Pivots")
    est = info.get("estructura_txt") or "En desarrollo"
    lineas.append(f"• Estructura: {est}")
    
    sop1 = info.get("sop_inmediato")
    sop2 = info.get("sop_segundo")
    res1 = info.get("res_inmediata")
    res2 = info.get("res_segunda")
    
    if sop1 and not np.isnan(sop1):
        dist_s = ((p - sop1) / p) * 100.0
        lineas.append(f"• Soporte clave: ${sop1:,.2f} (-{dist_s:.1f}%)" + (f" | S2: ${sop2:,.2f}" if sop2 else ""))
    if res1 and not np.isnan(res1):
        dist_r = ((res1 - p) / p) * 100.0
        lineas.append(f"• Resistencia clave: ${res1:,.2f} (+{dist_r:.1f}%)" + (f" | R2: ${res2:,.2f}" if res2 else ""))

    if poc and not np.isnan(poc):
        dist_poc = ((p - poc) / p) * 100.0
        lado_poc = "soporte de volumen institucional" if p >= poc else "resistencia magnética de volumen"
        signo = "+" if dist_poc >= 0 else ""
        lineas.append(f"• POC (Mayor volumen): ${poc:,.2f} ({signo}{dist_poc:.1f}% → {lado_poc})")

    lineas.append("")
    lineas.append("📐 Tendencia por plazo")
    for t in info.get("tendencias") or []:
        em = "🟢" if "alcista" in t["sesgo"] else ("🔴" if "bajista" in t["sesgo"] else "🟡")
        signo = "+" if t["ret"] >= 0 else ""
        lineas.append(
            f"• {em} {t['nombre'].capitalize()} ({t['etiqueta']}): {t['sesgo']} · {signo}{t['ret']:.1f}% en {t['barras']} velas"
        )

    lineas.append("")
    lineas.append("🌊 Medias Móviles")
    if p > e20 and e20 > e50:
        lineas.append(f"• Sesgo dinámico: Alcista sólido (Precio > EMA20 ${e20:,.2f} > EMA50 ${e50:,.2f})")
    elif p < e20 and e20 < e50:
        lineas.append(f"• Sesgo dinámico: Bajista bajo presión (Precio < EMA20 ${e20:,.2f} < EMA50 ${e50:,.2f})")
    else:
        lineas.append(f"• Sesgo dinámico: Mixto / compresión (EMA20 ${e20:,.2f} | EMA50 ${e50:,.2f})")
    if e200 and not np.isnan(e200):
        pos_200 = "soporte macro" if p > e200 else "resistencia macro"
        lineas.append(f"• EMA 200: ${e200:,.2f} ({pos_200})")

    lineas.append("")
    lineas.append("⚡ Momentum")
    lineas.append(f"• RSI 14: {rsi:.1f} — {diag_rsi}")
    macd = info.get("macd")
    macd_hist = info.get("macd_hist")
    if macd is not None:
        cruce = "histograma verde (+)" if (macd_hist or 0) >= 0 else "histograma rojo (-)"
        lineas.append(f"• MACD: {cruce} (Hist: {macd_hist:+.4f})")

    fibo = info.get("fibo_info") or {}
    if fibo:
        lineas.append("")
        dire = "de piso a techo" if fibo.get("direccion") == "alcista" else "de techo a piso"
        lineas.append(f"🎯 Fibonacci — último tramo {dire}")
        lineas.append(
            f"• Tramo: ${fibo['desde']:,.2f} ({fibo['desde_fecha']}) → ${fibo['hasta']:,.2f} ({fibo['hasta_fecha']})"
        )
        lineas.append(f"• Lectura: {fibo.get('estado')}")
        if fibo.get("uso") != "ninguno":
            lineas.append("• Retrocesos de ese tramo (0 = extremo, 1 = origen):")
            for k in ("0.382", "0.500", "0.618", "0.786"):
                v = fibo["retrocesos"].get(k)
                if v is not None and not np.isnan(v):
                    lineas.append(f"   Ret {k}: ${v:,.2f}")
            lineas.append("• Extensiones del mismo tramo (proyectadas más allá del extremo):")
            for k in ("1.272", "1.618"):
                v = fibo["extensiones"].get(k)
                if v is not None and not np.isnan(v):
                    lineas.append(f"   Ext {k}: ${v:,.2f}")
            if fibo.get("uso") == "retroceso":
                lineas.append("• Ahora importa el retroceso. La extensión es objetivo solo si el tramo se retoma.")
            elif fibo.get("uso") == "extension":
                lineas.append("• Ahora importa la extensión. Los retrocesos quedan atrás, como soporte/resistencia rota.")

    if hist_stat:
        lineas.append("")
        lineas.append(f"🧠 Comportamiento Histórico ante: {hist_stat['condicion']}")
        lineas.append(f"• Eventos detectados en su historia: {hist_stat['total_eventos']}")
        lineas.append("• Desglose por horizonte temporal:")
        lineas.append("• Evento = primera vela que entra en la banda. Sin semanas solapadas. Precios ajustados.")
        for item in hist_stat['desglose']:
            em = "🟢" if item['win_rate'] >= 60 else ("🟡" if item['win_rate'] >= 45 else "🔴")
            signo = "+" if item['avg_ret'] >= 0 else ""
            med = item.get("mediana")
            med_txt = f" | Med: {med:+.2f}%" if med is not None else ""
            lineas.append(
                f"   {em} {item['horizonte']:<8} → WR: {item['win_rate']:>5.1f}% | Prom: {signo}{item['avg_ret']:>6.2f}%{med_txt} ({item['muestras']} casos)"
            )

    lineas.append("")
    lineas.append("💡 Conclusión Operativa")
    bias = info.get("estructura_bias") or "lateral"
    confirmada_alcista = "DIVERGENCIA ALCISTA CONFIRMADA" in diag_rsi.upper()
    confirmada_bajista = "DIVERGENCIA BAJISTA CONFIRMADA" in diag_rsi.upper()
    en_formacion = "SE ESTÁ FORMANDO" in diag_rsi.upper() or "EN FORMACIÓN" in diag_rsi.upper()
    if confirmada_alcista or rsi <= 32:
        lineas.append("• Probabilidad alta de rebote técnico. Buscar confirmación sobre EMA20.")
    elif confirmada_bajista or rsi >= 70:
        lineas.append("• Zona de agotamiento de compras. Riesgo alto de pullback a soporte o POC.")
    elif en_formacion:
        lineas.append("• Hay una divergencia en formación. Es aviso, no estadística: el histórico de arriba es el que manda.")
    elif "choch_alcista" in bias:
        lineas.append("• Se rompió un techo/máximo importante. Posible cambio a tendencia alcista; esperar retesteo.")
    elif "choch_bajista" in bias:
        lineas.append("• Se rompió un piso/mínimo importante. Posible cambio a tendencia bajista; ajustar stops.")
    elif bias == "alcista" and p > e20:
        lineas.append("• Tendencia a favor. Mantener stop bajo el último pivot soporte.")
    else:
        lineas.append("• Rango en desarrollo. Operar rebotes en extremos o esperar quiebre con volumen.")

    return "\n".join(lineas)

def generar_grafico_analisis_tecnico(ticker: str, timeframe: str = "diario", con_fibo: bool = False, con_ext: bool = False):
    ticker = ticker.strip().upper()
    simbolo = normalizar_ticker_yf(ticker)
    
    tf_str = timeframe.strip().lower() if timeframe else "diario"
    if "4h" in tf_str or "4 hs" in tf_str or "4 horas" in tf_str or "corto" in tf_str:
        periodo = "60d"
        intervalo = "1h"
        tf_label = "4 Horas"
    elif "sem" in tf_str or "1w" in tf_str:
        periodo = "5y"
        intervalo = "1wk"
        tf_label = "Semanal"
    else:
        periodo = "3y"
        intervalo = "1d"
        tf_label = "Diario"

    try:
        df = yf_history(simbolo, period=periodo, interval=intervalo)
        if df.empty and not simbolo.endswith("-USD"):
            simbolo = f"{simbolo}-USD"
            df = yf_history(simbolo, period=periodo, interval=intervalo)
            
        if df.empty:
            return None, f"No se encontraron datos para {ticker} en {tf_label}"

        df = df.dropna(subset=['Close', 'High', 'Low'])
        if df.empty:
            return None, f"Datos insuficientes para {ticker}"

        if tf_label == "4 Horas":
            df = df.resample('4h').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum'
            }).dropna()

        idx = pd.to_datetime(df.index)
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_convert("UTC").tz_localize(None)
        df.index = idx
        df['Close'] = df['Close'].ffill()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        
        df['RSI'] = calcular_rsi_serie(df['Close'], period=14)
        df['MACD'], df['MACDs'], df['MACDh'] = calcular_macd(df['Close'])
        df['ATR'] = calcular_atr(df)

        niveles_dict = calcular_pivots_y_niveles(df, ventana=4)
        tendencias = calcular_tendencias(df, tf_label)
        poc_price = calcular_poc_volumen(df, barras_lookback=90)
        
        hist_stat = None
        rsi_act = float(df['RSI'].dropna().iloc[-1]) if 'RSI' in df and not df['RSI'].dropna().empty else 50.0
        df_hist = historia_rsi_largo(simbolo, tf_label)
        if df_hist is None or df_hist.empty:
            df_hist = df
        hist_stat = backtest_comportamiento_historico(
            df_hist,
            condicion="rsi_similar",
            rsi_actual=rsi_act,
            banda=0.5,
            tf_label=tf_label,
        )

        vol_txt = None
        if "Volume" in df.columns and df["Volume"].fillna(0).sum() > 0:
            v_now = float(df["Volume"].dropna().iloc[-1])
            v_avg = float(df["Volume"].tail(20).mean())
            if v_avg > 0:
                ratio = v_now / v_avg
                if ratio >= 1.6:
                    vol_txt = f"alto ({ratio:.1f}x vs 20)"
                elif ratio <= 0.6:
                    vol_txt = f"bajo ({ratio:.1f}x vs 20)"
                else:
                    vol_txt = f"normal ({ratio:.1f}x vs 20)"

        estado_rsi_div = detectar_divergencia_rsi(df)

        ult_velas = min(252, len(df))
        sub_df = df.iloc[-ult_velas:].copy()

        fig, (ax1, axm, ax2) = plt.subplots(3, 1, figsize=(11, 8.2), gridspec_kw={'height_ratios': [3.0, 1.0, 1.1]}, sharex=True)
        
        ax1.plot(sub_df.index, sub_df['Close'], label="Precio", color="#ffffff", linewidth=1.5, alpha=0.9)
        ax1.plot(sub_df.index, sub_df['EMA20'], label="EMA 20", color="#29b6f6", linewidth=1.4)
        ax1.plot(sub_df.index, sub_df['EMA50'], label="EMA 50", color="#ffa726", linewidth=1.4)
        if len(df) >= 150:
            ax1.plot(sub_df.index, sub_df['EMA200'], label="EMA 200", color="#ef5350", linewidth=1.8)

        if poc_price and not np.isnan(poc_price):
            ax1.axhline(poc_price, color="#00e676", linestyle="-.", linewidth=1.3, alpha=0.9, label=f"POC Vol (${poc_price:,.2f})")
            
        if niveles_dict["sop_inmediato"] and not np.isnan(niveles_dict["sop_inmediato"]):
            ax1.axhline(niveles_dict["sop_inmediato"], color="#29b6f6", linestyle=":", linewidth=1.2, label=f"Soporte (${niveles_dict['sop_inmediato']:,.2f})")
            
        if niveles_dict["res_inmediata"] and not np.isnan(niveles_dict["res_inmediata"]):
            ax1.axhline(niveles_dict["res_inmediata"], color="#ff5252", linestyle=":", linewidth=1.2, label=f"Resistencia (${niveles_dict['res_inmediata']:,.2f})")

        fibo_niveles = niveles_dict["fibo_niveles"]
        fibo_info = niveles_dict.get("fibo_info") or {}
        if fibo_niveles:
            colores_fibo = {
                "Ret 0.382": "#ab47bc", "Ret 0.500": "#26a69a", "Ret 0.618": "#ffca28",
                "Ret 0.786": "#8d6e63", "Ext 1.272": "#ff8a65", "Ext 1.618": "#ff7043",
            }
            relevantes = set(fibo_niveles)
            if fibo_info.get("uso") == "retroceso":
                relevantes = {k for k in relevantes if k.startswith("Ret")}
            elif fibo_info.get("uso") == "extension":
                relevantes = {k for k in relevantes if k.startswith("Ext")} | {"Ret 0.618"}
            for k, v in fibo_niveles.items():
                if k in relevantes and k in colores_fibo and v is not None and not np.isnan(v):
                    ax1.axhline(v, color=colores_fibo[k], linestyle="--", linewidth=1.1, alpha=0.75, label=f"{k} (${v:,.2f})")
            if fibo_info.get("desde") and fibo_info.get("hasta"):
                try:
                    ax1.scatter(
                        [pd.to_datetime(fibo_info["desde_fecha"], dayfirst=True), pd.to_datetime(fibo_info["hasta_fecha"], dayfirst=True)],
                        [fibo_info["desde"], fibo_info["hasta"]],
                        color="#ffca28", s=28, zorder=5, label="Tramo Fibo",
                    )
                except Exception:
                    pass

        ax1.set_facecolor("#131722")
        fig.patch.set_facecolor("#131722")
        ax1.grid(True, linestyle="--", alpha=0.15, color="#787b86")
        ax1.set_title(f"{ticker} | Analisis Tecnico Cuantitativo ({tf_label})\nEMA 20/50/200 + POC + Pivots + RSI", color="#ffffff", fontsize=12, fontweight='bold', pad=10)
        ax1.tick_params(colors="#787b86")
        ax1.legend(loc="upper left", facecolor="#1e222d", edgecolor="#2a2e39", labelcolor="#d1d4dc", fontsize=8)

        axm.set_facecolor("#131722")
        hist_colors = np.where(sub_df["MACDh"] >= 0, "#26a69a", "#ef5350")
        axm.bar(sub_df.index, sub_df["MACDh"], color=hist_colors, width=1.0, alpha=0.7, label="Hist")
        axm.plot(sub_df.index, sub_df["MACD"], color="#29b6f6", linewidth=1.2, label="MACD")
        axm.plot(sub_df.index, sub_df["MACDs"], color="#ffa726", linewidth=1.1, label="Signal")
        axm.axhline(0, color="#787b86", linewidth=0.7, alpha=0.6)
        axm.grid(True, linestyle="--", alpha=0.15, color="#787b86")
        axm.tick_params(colors="#787b86")
        axm.legend(loc="upper left", facecolor="#1e222d", edgecolor="#2a2e39", labelcolor="#d1d4dc", fontsize=7)

        ax2.set_facecolor("#131722")
        ax2.plot(sub_df.index, sub_df['RSI'], color="#ba68c8", linewidth=1.6, label="RSI 14")
        ax2.axhline(70, color="#ef5350", linestyle=":", linewidth=1.1, alpha=0.8)
        ax2.axhline(30, color="#26a69a", linestyle=":", linewidth=1.1, alpha=0.8)
        ax2.axhline(50, color="#787b86", linestyle="--", linewidth=0.8, alpha=0.5)
        ax2.fill_between(sub_df.index, sub_df['RSI'], 70, where=(sub_df['RSI'] >= 70), color="#ef5350", alpha=0.25)
        ax2.fill_between(sub_df.index, sub_df['RSI'], 30, where=(sub_df['RSI'] <= 30), color="#26a69a", alpha=0.25)
        ax2.set_ylim(10, 90)
        ax2.grid(True, linestyle="--", alpha=0.15, color="#787b86")
        ax2.tick_params(colors="#787b86")
        ax2.legend(loc="upper left", facecolor="#1e222d", edgecolor="#2a2e39", labelcolor="#d1d4dc", fontsize=8)
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200, facecolor=fig.get_facecolor())
        buf.seek(0)
        plt.close()

        precio_actual = float(df['Close'].dropna().iloc[-1])
        ema20_val = float(df['EMA20'].dropna().iloc[-1])
        ema50_val = float(df['EMA50'].dropna().iloc[-1])
        ema200_val = float(df['EMA200'].dropna().iloc[-1]) if 'EMA200' in df and not df['EMA200'].dropna().empty else None
        rsi_val = float(df['RSI'].dropna().iloc[-1])
        atr_val = float(df['ATR'].dropna().iloc[-1]) if pd.notnull(df['ATR'].dropna().iloc[-1]) else None
        atr_pct = (atr_val / precio_actual * 100.0) if atr_val and precio_actual else None

        datos_analisis = {
            "ticker": ticker,
            "timeframe": tf_label,
            "precio_actual": precio_actual,
            "ema20": ema20_val,
            "ema50": ema50_val,
            "ema200": ema200_val,
            "rsi": rsi_val,
            "diagnostico_rsi": estado_rsi_div,
            "sop_inmediato": niveles_dict["sop_inmediato"],
            "sop_segundo": niveles_dict["sop_segundo"],
            "res_inmediata": niveles_dict["res_inmediata"],
            "res_segunda": niveles_dict["res_segunda"],
            "fibo_niveles": fibo_niveles,
            "fibo_info": fibo_info,
            "tendencias": tendencias,
            "poc": poc_price,
            "hist_stat": hist_stat,
            "macd": float(df['MACD'].dropna().iloc[-1]),
            "macd_signal": float(df['MACDs'].dropna().iloc[-1]),
            "macd_hist": float(df['MACDh'].dropna().iloc[-1]),
            "atr": atr_val,
            "atr_pct": atr_pct,
            "estructura_txt": niveles_dict["estructura_txt"],
            "estructura_bias": niveles_dict["estructura_bias"],
            "volumen_txt": vol_txt,
        }
        return buf, datos_analisis
    except Exception as e:
        logger.error(f"Error generando analisis tecnico de {ticker}: {e}", exc_info=True)
        return None, str(e)

# ==================== ESCÁNER DE SEÑALES EN CARTERA ====================
def escanear_cartera_senales(user_id: int):
    with get_db_connection() as conn:
        df_inv = pd.read_sql(
            "SELECT DISTINCT ticker FROM portafolio_inversiones WHERE user_id = %s;",
            conn, params=(user_id,)
        )
    if df_inv.empty:
        return "No tienes activos cargados en cartera para analizar.", []

    tickers = [t.strip().upper() for t in df_inv['ticker'].unique() if t.strip().upper() not in ["USDT", "USDC", "DAI", "USD"]]
    if not tickers:
        return "No tienes activos volátiles en cartera (solo liquidez/stablecoins).", []

    diagnosticos = []
    imagenes_senales = []

    for tk in tickers:
        buf_d, info_d = generar_grafico_analisis_tecnico(tk, "diario")
        buf_w, info_w = generar_grafico_analisis_tecnico(tk, "semanal")

        if not info_d or isinstance(info_d, str):
            continue

        rsi_d = info_d['rsi']
        diag_d = info_d['diagnostico_rsi']
        precio = info_d['precio_actual']
        est_d = info_d.get('estructura_txt', '')
        poc_d = info_d.get('poc')

        rsi_w = info_w['rsi'] if (info_w and not isinstance(info_w, str)) else None
        diag_w = info_w['diagnostico_rsi'] if (info_w and not isinstance(info_w, str)) else "N/A"

        tiene_senal_d = ("DIVERGENCIA" in diag_d.upper()) or (rsi_d >= 70) or (rsi_d <= 30) or (poc_d and abs(precio - poc_d)/precio <= 0.01)
        tiene_senal_w = ("DIVERGENCIA" in diag_w.upper()) or (rsi_w and (rsi_w >= 70 or rsi_w <= 30))

        diag_texto = [f"📌 {tk} (${precio:,.2f} USD)"]
        diag_texto.append(f"• Estructura: {est_d}")
        
        if poc_d:
            dist_p = ((precio - poc_d) / precio) * 100
            diag_texto.append(f"• POC volumen: ${poc_d:,.2f} ({dist_p:+.1f}%)")
        
        avisos_rsi = []
        if "DIVERGENCIA" in diag_d.upper():
            avisos_rsi.append(f"⚡ Diario: {diag_d}")
        elif rsi_d >= 70 or rsi_d <= 30:
            avisos_rsi.append(f"⚠️ Diario: RSI {rsi_d:.1f} ({'Sobrecompra' if rsi_d>=70 else 'Sobreventa'})")
        else:
            avisos_rsi.append(f"RSI Diario {rsi_d:.1f} (neutro)")

        if rsi_w:
            if "DIVERGENCIA" in diag_w.upper():
                avisos_rsi.append(f"⚡ Semanal: {diag_w}")
            elif rsi_w >= 70 or rsi_w <= 30:
                avisos_rsi.append(f"⚠️ Semanal: RSI {rsi_w:.1f}")

        diag_texto.append(f"• Aviso RSI: {' | '.join(avisos_rsi)}")

        if tiene_senal_d and buf_d:
            imagenes_senales.append((buf_d, f"🚨 {tk} (Diario) — Señal técnica activa"))
        elif tiene_senal_w and buf_w:
            imagenes_senales.append((buf_w, f"🚨 {tk} (Semanal) — Señal RSI activa"))

        diagnosticos.append("\n".join(diag_texto))

    resumen_final = "🔍 ESCÁNER DE CARTERA (Diario + Semanal)\n\n" + "\n\n".join(diagnosticos)
    if not imagenes_senales:
        resumen_final += "\n\nℹ️ No se detectaron divergencias ni extremos de RSI críticos."
    else:
        resumen_final += f"\n\n📊 Se detectaron {len(imagenes_senales)} señales claras. Gráficos a continuación."

    return resumen_final, imagenes_senales

# ==================== OPERACIONES BANCARIAS Y CARTERA ====================
def registrar_operacion_inversion(user_id: int, ticker: str, monto_usd: float, precio_compra: float = None, cantidad: float = None, fecha_compra: str = None, tipo_posicion: str = "SPOT", apalancamiento: float = 1.0, precio_liq: float = None):
    ticker = ticker.strip().upper()
    tipo_pos = tipo_posicion.strip().upper() if tipo_posicion else "SPOT"
    lev = float(apalancamiento) if apalancamiento and float(apalancamiento) >= 1 else 1.0
    
    if (not precio_compra or precio_compra <= 0) and (not cantidad or cantidad <= 0):
        precio_mercado, _ = obtener_precio_actual(ticker)
        precio_compra = precio_mercado if precio_mercado else 1.0
    
    posicion_nocional = monto_usd * lev if (monto_usd and monto_usd > 0) else (cantidad * precio_compra)
    
    if cantidad is None or cantidad <= 0:
        cantidad = posicion_nocional / precio_compra if precio_compra > 0 else 0
    elif monto_usd is None or monto_usd <= 0:
        monto_usd = (cantidad * precio_compra) / lev

    if precio_liq is None or precio_liq <= 0:
        if tipo_pos == "LONG" and lev > 1:
            precio_liq = precio_compra * (1 - (1 / lev) * 0.95)
        elif tipo_pos == "SHORT" and lev > 1:
            precio_liq = precio_compra * (1 + (1 / lev) * 0.95)
        else:
            precio_liq = None

    fecha_limpia = None
    if fecha_compra:
        f_cand = fecha_compra.strip().split()[0]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f_cand):
            fecha_limpia = f_cand

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_limpia:
                cursor.execute(
                    """INSERT INTO portafolio_inversiones (user_id, fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                    (user_id, fecha_limpia, ticker, float(cantidad), float(precio_compra), float(monto_usd), tipo_pos, float(lev), float(precio_liq) if precio_liq else None)
                )
            else:
                cursor.execute(
                    """INSERT INTO portafolio_inversiones (user_id, fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion)
                       VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s);""",
                    (user_id, ticker, float(cantidad), float(precio_compra), float(monto_usd), tipo_pos, float(lev), float(precio_liq) if precio_liq else None)
                )
            conn.commit()
    return ticker, cantidad, precio_compra, monto_usd, fecha_limpia, tipo_pos, lev, precio_liq

def registrar_trade_cerrado(user_id: int, ticker: str, pnl_usd: float, tipo_posicion: str = "SPOT", roi_pct: float = None, monto_invertido: float = None, descripcion: str = "", fecha_str: str = None, fecha_apertura: str = None):
    ticker = ticker.strip().upper()
    tipo_pos = tipo_posicion.strip().upper() if tipo_posicion else "SPOT"
    
    fecha_limpia = None
    if fecha_str:
        f_cand = fecha_str.strip().split()[0]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f_cand):
            fecha_limpia = f_cand
    fecha_ap_limpia = None
    if fecha_apertura:
        f_a = str(fecha_apertura).strip().split()[0]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f_a):
            fecha_ap_limpia = f_a
    if not fecha_ap_limpia and descripcion:
        m_ap = re.search(r"Apertura\s+(\d{4}-\d{2}-\d{2})", descripcion)
        if m_ap:
            fecha_ap_limpia = m_ap.group(1)

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_limpia:
                cursor.execute(
                    """INSERT INTO trades_cerrados (user_id, fecha, fecha_apertura, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;""",
                    (user_id, fecha_limpia, fecha_ap_limpia, ticker, tipo_pos, float(pnl_usd), float(roi_pct) if roi_pct else None, float(monto_invertido) if monto_invertido else None, descripcion)
                )
            else:
                cursor.execute(
                    """INSERT INTO trades_cerrados (user_id, fecha, fecha_apertura, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion)
                       VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s) RETURNING id;""",
                    (user_id, fecha_ap_limpia, ticker, tipo_pos, float(pnl_usd), float(roi_pct) if roi_pct else None, float(monto_invertido) if monto_invertido else None, descripcion)
                )
            new_id = cursor.fetchone()[0]
            conn.commit()
            return new_id, ticker, pnl_usd, tipo_pos, roi_pct, fecha_limpia

def cerrar_posicion_abierta_por_id(user_id: int, inv_id: int, pnl_manual: float = None, precio_salida_manual: float = None):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, fecha FROM portafolio_inversiones WHERE id = %s AND user_id = %s;",
                (inv_id, user_id)
            )
            reg = cursor.fetchone()
            if not reg:
                return None, "Posición abierta no encontrada"
            
            _, ticker, cant, ppc, margen, tipo_pos, lev, fecha_ap = reg
            cant, ppc, margen, lev = float(cant), float(ppc), float(margen), float(lev)
            tipo_pos = str(tipo_pos).upper() if tipo_pos else "SPOT"
            
            if pnl_manual is not None:
                pnl_final = float(pnl_manual)
            else:
                spot = float(precio_salida_manual) if precio_salida_manual else obtener_precio_actual(ticker)[0]
                if spot is None:
                    spot = ppc
                if tipo_pos == "SHORT":
                    pnl_final = margen * ((ppc - spot) / ppc) * lev
                elif tipo_pos == "LONG":
                    pnl_final = margen * ((spot - ppc) / ppc) * lev
                else:
                    pnl_final = (cant * spot) - margen
            
            roi_final = (pnl_final / margen * 100) if margen > 0 else 0.0
            fecha_ap_txt = fecha_ap.strftime('%Y-%m-%d') if fecha_ap is not None else None
            desc = f"Apertura {fecha_ap_txt or 'N/D'} | Posición cerrada (Entrada: ${ppc:,.2f})"

            cursor.execute(
                """INSERT INTO trades_cerrados (user_id, fecha, fecha_apertura, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion)
                   VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s) RETURNING id;""",
                (user_id, fecha_ap_txt, ticker, tipo_pos, pnl_final, roi_final, margen, desc)
            )
            nuevo_tc_id = cursor.fetchone()[0]
            cursor.execute("DELETE FROM portafolio_inversiones WHERE id = %s AND user_id = %s;", (inv_id, user_id))
            conn.commit()
            return {
                "tc_id": nuevo_tc_id,
                "ticker": ticker,
                "tipo_pos": tipo_pos,
                "pnl_usd": pnl_final,
                "roi_pct": roi_final,
                "margen": margen
            }, "Posición cerrada exitosamente"

def modificar_inversion_por_id(user_id: int, inv_id: int, campo: str, nuevo_valor: str):
    campo = campo.strip().lower()
    mapa_campos = {
        "ticker": "ticker", "activo": "ticker", "cantidad": "cantidad", "cant": "cantidad",
        "precio": "precio_compra", "ppc": "precio_compra", "precio_compra": "precio_compra",
        "monto": "monto_total_usd", "monto_total_usd": "monto_total_usd", "fecha": "fecha",
        "tipo": "tipo_posicion", "tipo_posicion": "tipo_posicion",
        "apalancamiento": "apalancamiento", "leverage": "apalancamiento",
        "liq": "precio_liquidacion", "precio_liquidacion": "precio_liquidacion"
    }
    col = mapa_campos.get(campo)
    if not col:
        return None, "Campo no reconocido"

    val = nuevo_valor.strip()
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, ticker, monto_total_usd FROM portafolio_inversiones WHERE id = %s AND user_id = %s;", (inv_id, user_id))
            prev = cursor.fetchone()
            if not prev:
                return None, "Inversión no encontrada en tu cartera"
                
            if col in ["cantidad", "precio_compra", "monto_total_usd", "apalancamiento", "precio_liquidacion"]:
                val_num = float(val)
                cursor.execute(f"UPDATE portafolio_inversiones SET {col} = %s WHERE id = %s AND user_id = %s;", (val_num, inv_id, user_id))
            elif col == "fecha":
                cursor.execute("UPDATE portafolio_inversiones SET fecha = %s WHERE id = %s AND user_id = %s;", (val, inv_id, user_id))
            else:
                cursor.execute(f"UPDATE portafolio_inversiones SET {col} = %s WHERE id = %s AND user_id = %s;", (val.upper(), inv_id, user_id))
            conn.commit()
            return prev, f"Campo {col} actualizado a {val}"

def agregar_margen_a_posicion(user_id: int, inv_id: int, margen_extra: float):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, ticker, monto_total_usd, apalancamiento, precio_compra, tipo_posicion FROM portafolio_inversiones WHERE id = %s AND user_id = %s;",
                (inv_id, user_id)
            )
            reg = cursor.fetchone()
            if not reg:
                return None, "Posición no encontrada"
            
            nuevo_margen = float(reg[2]) + float(margen_extra)
            ratio_aumento = nuevo_margen / float(reg[2]) if float(reg[2]) > 0 else 1.0
            nuevo_lev = max(1.0, float(reg[3]) / ratio_aumento)
            
            ppc = float(reg[4])
            tipo_pos = reg[5]
            if tipo_pos == "LONG" and nuevo_lev > 1:
                nuevo_liq = ppc * (1 - (1 / nuevo_lev) * 0.95)
            elif tipo_pos == "SHORT" and nuevo_lev > 1:
                nuevo_liq = ppc * (1 + (1 / nuevo_lev) * 0.95)
            else:
                nuevo_liq = None
            
            cursor.execute(
                """UPDATE portafolio_inversiones 
                   SET monto_total_usd = %s, apalancamiento = %s, precio_liquidacion = %s 
                   WHERE id = %s AND user_id = %s;""",
                (nuevo_margen, nuevo_lev, nuevo_liq, inv_id, user_id)
            )
            conn.commit()
            return reg, f"Margen total: ${nuevo_margen:,.2f} USD (Apalancamiento efectivo: {nuevo_lev:.1f}x)"

def modificar_movimiento_por_id(user_id: int, mov_id: int, campo: str, nuevo_valor: str):
    campo = campo.strip().lower()
    mapa_campos = {
        "monto": "monto", "categoria": "categoria", "descripcion": "descripcion",
        "desc": "descripcion", "fecha": "fecha", "tipo": "tipo"
    }
    col = mapa_campos.get(campo)
    if not col:
        return None, "Campo no reconocido"

    val = nuevo_valor.strip()
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, tipo, monto, categoria FROM movimientos WHERE id = %s AND user_id = %s;", (mov_id, user_id))
            prev = cursor.fetchone()
            if not prev:
                return None, "Movimiento no encontrado"
                
            if col == "monto":
                cursor.execute("UPDATE movimientos SET monto = %s WHERE id = %s AND user_id = %s;", (float(val), mov_id, user_id))
            elif col == "fecha":
                cursor.execute("UPDATE movimientos SET fecha = %s WHERE id = %s AND user_id = %s;", (val, mov_id, user_id))
            elif col == "categoria":
                cursor.execute("UPDATE movimientos SET categoria = %s WHERE id = %s AND user_id = %s;", (normalizar_categoria(val), mov_id, user_id))
            else:
                cursor.execute(f"UPDATE movimientos SET {col} = %s WHERE id = %s AND user_id = %s;", (val.capitalize(), mov_id, user_id))
            conn.commit()
            return prev, f"Campo {col} actualizado a {val}"

def obtener_resumen_portafolio(user_id: int):
    with get_db_connection() as conn:
        df = pd.read_sql(
            "SELECT id, fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion FROM portafolio_inversiones WHERE user_id = %s;",
            conn, params=(user_id,)
        )
    
    if df.empty:
        return None

    posiciones = []
    total_margen_invertido = 0.0
    total_valor_actual = 0.0

    for _, fila in df.iterrows():
        inv_id = int(fila['id'])
        ticker = str(fila['ticker']).strip().upper()
        cant = float(fila['cantidad']) if pd.notnull(fila['cantidad']) else 0.0
        costo_margen = float(fila['monto_total_usd']) if pd.notnull(fila['monto_total_usd']) else 0.0
        
        if pd.notnull(fila['precio_compra']) and float(fila['precio_compra']) > 0:
            ppc = float(fila['precio_compra'])
        elif cant > 0:
            ppc = costo_margen / cant
        else:
            ppc = 1.0

        tipo_pos = str(fila['tipo_posicion']).upper() if pd.notnull(fila['tipo_posicion']) else "SPOT"
        lev = float(fila['apalancamiento']) if pd.notnull(fila['apalancamiento']) and float(fila['apalancamiento']) > 0 else 1.0
        p_liq = float(fila['precio_liquidacion']) if pd.notnull(fila['precio_liquidacion']) else None

        if ticker in ["USDT", "USDC", "DAI", "USD"]:
            spot = 1.0
            valor_actual = costo_margen if costo_margen > 0 else cant
            pnl_usd = 0.0
            pnl_pct = 0.0
        else:
            spot, _ = obtener_precio_actual(ticker)
            if spot is None or np.isnan(spot) or spot <= 0:
                spot = ppc

            if tipo_pos == "SHORT":
                var_precio_pct = (ppc - spot) / ppc if ppc > 0 else 0.0
                pnl_pct = var_precio_pct * lev * 100
                pnl_usd = costo_margen * (var_precio_pct * lev)
                valor_actual = max(0.0, costo_margen + pnl_usd)
            elif tipo_pos == "LONG":
                var_precio_pct = (spot - ppc) / ppc if ppc > 0 else 0.0
                pnl_pct = var_precio_pct * lev * 100
                pnl_usd = costo_margen * (var_precio_pct * lev)
                valor_actual = max(0.0, costo_margen + pnl_usd)
            else:
                valor_actual = cant * spot
                pnl_usd = valor_actual - costo_margen
                pnl_pct = (pnl_usd / costo_margen * 100) if costo_margen > 0 else 0.0

        if np.isnan(valor_actual):
            valor_actual = costo_margen
        if np.isnan(pnl_usd):
            pnl_usd = 0.0
        if np.isnan(pnl_pct):
            pnl_pct = 0.0

        total_margen_invertido += costo_margen
        total_valor_actual += valor_actual

        dist_liq_pct = None
        if p_liq and tipo_pos in ["LONG", "SHORT"] and lev > 1 and spot > 0:
            if tipo_pos == "LONG":
                dist_liq_pct = ((spot - p_liq) / spot * 100)
            else:
                dist_liq_pct = ((p_liq - spot) / spot * 100)
            if np.isnan(dist_liq_pct):
                dist_liq_pct = None

        posiciones.append({
            "id": inv_id,
            "ticker": ticker,
            "tipo_pos": tipo_pos,
            "lev": lev,
            "cantidad": cant,
            "ppc": ppc,
            "spot": spot,
            "costo_margen": costo_margen,
            "valor_actual": valor_actual,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "precio_liq": p_liq,
            "dist_liq_pct": dist_liq_pct,
            "fecha": fila['fecha']
        })

    pnl_total_usd = total_valor_actual - total_margen_invertido
    pnl_total_pct = (pnl_total_usd / total_margen_invertido * 100) if total_margen_invertido > 0 else 0.0

    return {
        "posiciones": posiciones,
        "total_invertido": total_margen_invertido,
        "total_actual": total_valor_actual,
        "pnl_total_usd": pnl_total_usd,
        "pnl_total_pct": pnl_total_pct
    }

def generar_grafico_torta_gastos(user_id: int):
    inicio, fin, etq = resolver_ciclo_havas(user_id)
    with get_db_connection() as conn:
        df = pd.read_sql(
            """SELECT categoria, tipo, monto, descripcion FROM movimientos
               WHERE user_id = %s AND fecha >= %s AND fecha < %s;""",
            conn, params=(user_id, inicio.to_pydatetime(), fin.to_pydatetime()),
        )
    if df.empty:
        return None, etq
    df["categoria"] = df["categoria"].apply(normalizar_categoria)
    gastos = df[df["tipo"] == "GASTO"].copy()
    if gastos.empty:
        return None, etq
    blob = (gastos["categoria"].astype(str) + " " + gastos["descripcion"].astype(str)).str.lower()
    gastos = gastos[~blob.apply(lambda x: any(t in x for t in TAGS_AHORRO))]
    if gastos.empty:
        return None, etq
    # netear devoluciones de las mismas categorías
    ings = df[df["tipo"] == "INGRESO"].copy()
    if not ings.empty:
        ings["categoria"] = ings["categoria"].apply(normalizar_categoria)
        for cat, mdev in ings.groupby("categoria")["monto"].sum().items():
            if str(cat).lower() not in CATS_NETEAR_DEVOL and not any(t in str(cat).lower() for t in TAGS_DEVOL):
                continue
            mask = gastos["categoria"] == cat
            resto = float(mdev)
            for i in list(gastos.loc[mask].index):
                if resto <= 0:
                    break
                actual = float(gastos.at[i, "monto"])
                baja = min(actual, resto)
                gastos.at[i, "monto"] = actual - baja
                resto -= baja
        gastos = gastos[gastos["monto"] > 0.5]
    por = gastos.groupby("categoria")["monto"].sum().sort_values(ascending=False)
    if por.empty:
        return None, etq
    if len(por) > 8:
        top = por.head(7)
        otros = float(por.iloc[7:].sum())
        por = pd.concat([top, pd.Series({"Otros": otros})])
    plt.figure(figsize=(8, 6.2))
    colores = ["#4C78A8", "#F58518", "#54A24B", "#E45756", "#72B7B2", "#EECA3B", "#B279A2", "#FF9DA6"]
    labels = [f"{c}\n${v:,.0f}" for c, v in por.items()]
    plt.pie(por.values, labels=labels, autopct="%1.0f%%", startangle=120,
            colors=colores[:len(por)], wedgeprops=dict(width=0.55, edgecolor="w"),
            textprops={"fontsize": 8})
    plt.title(f"Gastos de vida — {etq}", fontsize=13, pad=16)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=160)
    buf.seek(0)
    plt.close()
    return buf, etq

def generar_grafico_distribucion_inversiones(user_id: int):
    resumen = obtener_resumen_portafolio(user_id)
    if not resumen or not resumen["posiciones"]:
        return None
    
    df = pd.DataFrame(resumen["posiciones"])
    plt.figure(figsize=(7, 6))
    colores = ['#2ca02c', '#1f77b4', '#ff7f0e', '#d62728', '#9467bd', '#8c564b', '#e377c2']
    
    agrup = df.groupby('ticker')['valor_actual'].sum()
    plt.pie(
        agrup.values,
        labels=[f"{tk} ({val:,.0f} USD)" for tk, val in agrup.items()],
        autopct='%1.1f%%',
        startangle=140,
        colors=colores[:len(agrup)],
        wedgeprops=dict(width=0.6, edgecolor='w')
    )
    plt.title('Distribución de Posiciones Abiertas (USD)', fontsize=14, pad=20)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=200)
    buf.seek(0)
    plt.close()
    return buf

# ==================== MÉTRICAS DE RIESGO Y PERFORMANCE TRADING ====================
def calcular_max_drawdown(serie_pnl):
    if serie_pnl is None or len(serie_pnl) < 2:
        return 0.0, 0.0
    peak = serie_pnl.expanding(min_periods=1).max()
    dd = (serie_pnl - peak)
    max_dd = float(dd.min())
    max_dd_pct = float((dd / peak.replace(0, np.nan)).min() * 100) if peak.max() != 0 else 0.0
    return max_dd, max_dd_pct

def calcular_metricas_riesgo_completas(user_id: int):
    resultado = {
        "pnl_realizado": 0.0,
        "cant_trades": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "expectancy": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "max_drawdown_usd": 0.0,
        "max_drawdown_pct": 0.0,
        "sharpe_aprox": 0.0,
        "tiempo_promedio_dias": 0.0,
        "posiciones_riesgo": [],
        "texto": ""
    }

    try:
        with get_db_connection() as conn:
            df_tc = pd.read_sql(
                "SELECT fecha, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido FROM trades_cerrados WHERE user_id = %s ORDER BY fecha ASC;",
                conn, params=(user_id,)
            )
            df_inv = pd.read_sql(
                "SELECT id, fecha, ticker, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion, precio_compra FROM portafolio_inversiones WHERE user_id = %s;",
                conn, params=(user_id,)
            )

        if not df_tc.empty:
            resultado["pnl_realizado"] = float(df_tc['pnl_usd'].sum())
            resultado["cant_trades"] = len(df_tc)
            ganadores = df_tc[df_tc['pnl_usd'] > 0]
            perdedores = df_tc[df_tc['pnl_usd'] < 0]
            resultado["win_rate"] = (len(ganadores) / len(df_tc) * 100) if len(df_tc) > 0 else 0.0
            suma_gan = float(ganadores['pnl_usd'].sum()) if not ganadores.empty else 0.0
            suma_per = abs(float(perdedores['pnl_usd'].sum())) if not perdedores.empty else 0.0
            resultado["profit_factor"] = (suma_gan / suma_per) if suma_per > 0 else (suma_gan if suma_gan > 0 else 0.0)
            resultado["avg_win"] = float(ganadores['pnl_usd'].mean()) if not ganadores.empty else 0.0
            resultado["avg_loss"] = float(perdedores['pnl_usd'].mean()) if not perdedores.empty else 0.0
            resultado["expectancy"] = (resultado["win_rate"]/100 * resultado["avg_win"]) + ((1 - resultado["win_rate"]/100) * resultado["avg_loss"])

            df_tc['fecha_d'] = pd.to_datetime(df_tc['fecha']).dt.tz_localize(None).dt.floor('D')
            pnl_diario = df_tc.groupby('fecha_d')['pnl_usd'].sum().cumsum()
            if len(pnl_diario) >= 2:
                mdd, mdd_pct = calcular_max_drawdown(pnl_diario)
                resultado["max_drawdown_usd"] = mdd
                resultado["max_drawdown_pct"] = mdd_pct

            retornos = df_tc.groupby('fecha_d')['pnl_usd'].sum()
            if len(retornos) > 5 and retornos.std() > 0:
                resultado["sharpe_aprox"] = float((retornos.mean() / retornos.std()) * np.sqrt(252))

        if not df_inv.empty:
            ahora = datetime.now()
            dias = []
            for _, row in df_inv.iterrows():
                f = pd.to_datetime(row['fecha']).tz_localize(None)
                dias.append((ahora - f).days)
            resultado["tiempo_promedio_dias"] = float(np.mean(dias)) if dias else 0.0

            resumen = obtener_resumen_portafolio(user_id)
            if resumen:
                for p in resumen["posiciones"]:
                    if p.get("dist_liq_pct") is not None and p["dist_liq_pct"] < UMBRAL_LIQUIDACION_PCT:
                        resultado["posiciones_riesgo"].append(p)

        lineas = [
            "📊 MÉTRICAS DE RIESGO Y PERFORMANCE (TRADING)",
            "",
            "🏆 Trades Cerrados",
            f"• PnL Realizado: ${resultado['pnl_realizado']:+,.2f} USD",
            f"• Operaciones: {resultado['cant_trades']}  |  Win Rate: {resultado['win_rate']:.1f}%",
            f"• Profit Factor: {resultado['profit_factor']:.2f}",
            f"• Expectancy: ${resultado['expectancy']:+,.2f} por trade",
            f"• Promedio ganancia: ${resultado['avg_win']:+,.2f}  |  Promedio pérdida: ${resultado['avg_loss']:+,.2f}",
            "",
            "📉 Riesgo",
            f"• Max Drawdown: ${resultado['max_drawdown_usd']:+,.2f} USD ({resultado['max_drawdown_pct']:.1f}%)",
            f"• Sharpe aproximado (anualizado): {resultado['sharpe_aprox']:.2f}",
            f"• Tiempo promedio en posición: {resultado['tiempo_promedio_dias']:.0f} días"
        ]
        
        if resultado["posiciones_riesgo"]:
            lineas.append("")
            lineas.append("⚠️ Posiciones cerca de liquidación")
            for p in resultado["posiciones_riesgo"]:
                lineas.append(f"• {p['ticker']} [{p['tipo_pos']} {p['lev']:.0f}x] — Distancia: {p['dist_liq_pct']:.1f}% | Liq: ${p['precio_liq']:,.2f}")

        resultado["texto"] = "\n".join(lineas)
        return resultado
    except Exception as e:
        logger.error(f"Error calculando métricas de riesgo: {e}", exc_info=True)
        resultado["texto"] = "No se pudieron calcular las métricas de riesgo en este momento."
        return resultado

def _serie_spy_ret(fecha_start: str):
    for sym in ("SPY", "SPY.US", "^GSPC"):
        try:
            h = yf_history(sym, start=fecha_start, auto_adjust=True)
            if h is None or h.empty or "Close" not in h.columns:
                continue
            s = h["Close"].replace([np.inf, -np.inf], np.nan).dropna()
            if len(s) < 2:
                continue
            base = float(s.iloc[0])
            last = float(s.iloc[-1])
            if not np.isfinite(base) or not np.isfinite(last) or base <= 0:
                continue
            ret = last / base - 1.0
            if not np.isfinite(ret):
                continue
            return ret
        except Exception as e:
            logger.warning(f"SPY {sym} falló: {e}")
            continue
    return None

def calcular_rendimiento_periodo(user_id: int, periodo: str = "ytd"):
    hoy = datetime.now()
    primera_fecha = "2025-05-01"
    with get_db_connection() as conn:
        df_tc = pd.read_sql("SELECT fecha, pnl_usd, monto_invertido FROM trades_cerrados WHERE user_id = %s ORDER BY fecha ASC;", conn, params=(user_id,))
        df_inv = pd.read_sql("SELECT fecha, monto_total_usd FROM portafolio_inversiones WHERE user_id = %s ORDER BY fecha ASC;", conn, params=(user_id,))

    if not df_tc.empty:
        primera_fecha = df_tc['fecha'].min().strftime('%Y-%m-%d')
    elif not df_inv.empty:
        primera_fecha = df_inv['fecha'].min().strftime('%Y-%m-%d')

    fecha_start, desc = resolver_fecha_inicio(periodo, primera_fecha)
    start_dt = pd.to_datetime(fecha_start)

    pnl_realizado = 0.0
    if not df_tc.empty:
        df_tc["fecha"] = pd.to_datetime(df_tc["fecha"])
        sub = df_tc.loc[df_tc["fecha"] >= start_dt]
        pnl_realizado = float(sub["pnl_usd"].sum()) if not sub.empty else 0.0

    resumen = obtener_resumen_portafolio(user_id)
    pnl_flot = float(resumen["pnl_total_usd"]) if resumen else 0.0
    cap_abierto = float(resumen["total_invertido"]) if resumen else 0.0
    valor_actual = float(resumen["total_actual"]) if resumen else 0.0
    pnl_total = pnl_realizado + pnl_flot

    cap_tc_pico = float(df_tc['monto_invertido'].max()) if not df_tc.empty and 'monto_invertido' in df_tc and pd.notnull(df_tc['monto_invertido'].max()) else 2000.0
    capital_ref = max(cap_abierto, cap_tc_pico, 2500.0)

    ret_pct = (pnl_total / capital_ref * 100.0) if capital_ref > 0 else 0.0

    dias = max(1, (hoy - start_dt.to_pydatetime()).days)
    años = dias / 365.25
    base_cagr = 1.0 + (pnl_total / capital_ref) if capital_ref > 0 else 1.0
    if años > 0 and np.isfinite(base_cagr) and base_cagr > 0:
        cagr = (base_cagr ** (1 / años) - 1) * 100.0
    else:
        cagr = ret_pct
    if not np.isfinite(ret_pct):
        ret_pct = 0.0
    if not np.isfinite(cagr):
        cagr = ret_pct

    spy_ret = _serie_spy_ret(fecha_start)
    spy_pct = (spy_ret * 100.0) if spy_ret is not None and np.isfinite(spy_ret) else None
    alpha = (ret_pct - spy_pct) if spy_pct is not None else None

    lineas = [
        f"📈 RENDIMIENTO — {desc}",
        "",
        f"• Retorno de cartera: {ret_pct:+.2f}%",
        f"• CAGR aprox.: {cagr:+.2f}%",
    ]
    if spy_pct is not None:
        signo_alpha = "+" if alpha >= 0 else ""
        lineas.append(f"• SPY (Benchmark): {spy_pct:+.2f}%")
        lineas.append(f"• Alpha vs SPY: {signo_alpha}{alpha:.2f} pp")
    else:
        lineas.append("• SPY: sin dato válido en este período")

    lineas.extend([
        "",
        f"• PnL realizado en el período: ${pnl_realizado:+,.2f} USD",
        f"• PnL flotante actual: ${pnl_flot:+,.2f} USD",
        f"• PnL total (realizado + flotante): ${pnl_total:+,.2f} USD",
        f"• Capital abierto ahora: ${cap_abierto:,.2f} USD",
        f"• Capital base de referencia: ${capital_ref:,.2f} USD",
        f"• Valor actual abierto: ${valor_actual:,.2f} USD",
        "",
        "Nota: el cálculo porcentual pondera el PnL neto del período contra la base de capital de la cuenta, manteniendo consistencia total con la curva gráfica."
    ])
    return "\n".join(lineas)

def resumen_compacto_para_ia(user_id: int) -> str:
    resumen = obtener_resumen_portafolio(user_id)
    with get_db_connection() as conn:
        df_tc = pd.read_sql(
            "SELECT COUNT(*) AS n, COALESCE(SUM(pnl_usd),0) AS pnl FROM trades_cerrados WHERE user_id = %s;",
            conn, params=(user_id,),
        )
        df_mov = pd.read_sql(
            "SELECT tipo, COUNT(*) n, COALESCE(SUM(monto),0) tot FROM movimientos WHERE user_id = %s GROUP BY tipo;",
            conn, params=(user_id,),
        )
        df_ids = pd.read_sql(
            "SELECT id, ticker, tipo_posicion, apalancamiento, monto_total_usd FROM portafolio_inversiones WHERE user_id = %s ORDER BY id DESC LIMIT 12;",
            conn, params=(user_id,),
        )
        df_last_mov = pd.read_sql(
            "SELECT fecha, tipo, monto, categoria, descripcion FROM movimientos WHERE user_id = %s ORDER BY fecha DESC LIMIT 5;",
            conn, params=(user_id,)
        )
    n_tc = int(df_tc["n"].iloc[0]) if not df_tc.empty else 0
    pnl_tc = float(df_tc["pnl"].iloc[0]) if not df_tc.empty else 0.0
    lineas = [
        f"Trades cerrados: {n_tc} | PnL realizado ${pnl_tc:+,.2f} USD",
    ]
    if resumen:
        lineas.append(
            f"Abiertas: {len(resumen['posiciones'])} | Capital ${resumen['total_invertido']:,.2f} | Valor ${resumen['total_actual']:,.2f} | Flotante ${resumen['pnl_total_usd']:+,.2f}"
        )
        for p in resumen["posiciones"][:10]:
            lineas.append(
                f"ID {p['id']} {p['ticker']} {p['tipo_pos']} {p['lev']:.0f}x margen ${p['costo_margen']:,.0f} PPC ${p['ppc']:,.2f} Spot ${p['spot']:,.2f} PnL {p['pnl_usd']:+.0f}"
            )
    elif not df_ids.empty:
        for _, r in df_ids.iterrows():
            lineas.append(f"ID {int(r['id'])} {r['ticker']} {r['tipo_posicion']} ${float(r['monto_total_usd']):,.0f}")
    if not df_mov.empty:
        for _, r in df_mov.iterrows():
            lineas.append(f"Movimientos {r['tipo']}: {int(r['n'])} / ${float(r['tot']):,.0f} ARS")
    if not df_last_mov.empty:
        lineas.append("Últimos gastos/ingresos:")
        for _, m in df_last_mov.iterrows():
            lineas.append(f"  {m['tipo']} ${float(m['monto']):,.0f} ARS en {m['categoria']} ({m['descripcion']})")
    return "\n".join(lineas)

def llamar_gemini(prompt: str, system_instruction: str) -> str:
    response = ai_client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt[:800],
        config={
            "system_instruction": system_instruction,
            "max_output_tokens": 220,
            "temperature": 0,
        },
    )
    return response.text or ""

# ==================== PRESUPUESTOS ====================
def set_presupuesto(user_id: int, categoria: str, monto_limite: float, mes: int = None, anio: int = None):
    ahora = ahora_argentina()
    mes = mes or ahora.month
    anio = anio or ahora.year
    categoria = normalizar_categoria(categoria)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO presupuestos (user_id, categoria, monto_limite, mes, anio)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, categoria, mes, anio)
                DO UPDATE SET monto_limite = EXCLUDED.monto_limite;
            """, (user_id, categoria, float(monto_limite), mes, anio))
            conn.commit()
    return categoria, monto_limite, mes, anio

def obtener_progreso_presupuestos(user_id: int, mes: int = None, anio: int = None):
    ahora = ahora_argentina()
    mes = mes or ahora.month
    anio = anio or ahora.year
    inicio, fin, etq = resolver_ciclo_havas(user_id)

    with get_db_connection() as conn:
        df_pres = pd.read_sql(
            "SELECT categoria, monto_limite FROM presupuestos WHERE user_id = %s AND mes = %s AND anio = %s;",
            conn, params=(user_id, mes, anio)
        )
        df_gastos = pd.read_sql(
            """SELECT categoria, SUM(monto) as gastado 
               FROM movimientos 
               WHERE user_id = %s AND tipo = 'GASTO' 
               AND fecha >= %s AND fecha < %s
               GROUP BY categoria;""",
            conn, params=(user_id, inicio.to_pydatetime(), fin.to_pydatetime())
        )

    if df_pres.empty:
        return None, "No tenés presupuestos cargados para este mes. Decime por ejemplo: 'Presupuesto Comida 180000'"

    df_pres['categoria'] = df_pres['categoria'].apply(normalizar_categoria)
    if not df_gastos.empty:
        df_gastos['categoria'] = df_gastos['categoria'].apply(normalizar_categoria)
        gastos_dict = df_gastos.groupby('categoria')['gastado'].sum().to_dict()
    else:
        gastos_dict = {}

    lineas = [f"📅 PRESUPUESTOS — {etq}", ""]
    total_limite = 0.0
    total_gastado = 0.0

    for _, row in df_pres.iterrows():
        cat = row['categoria']
        limite = float(row['monto_limite'])
        gastado = float(gastos_dict.get(cat, 0.0))
        pct = (gastado / limite * 100) if limite > 0 else 0.0
        restante = limite - gastado
        emoji = "🟢" if pct < 70 else ("🟡" if pct < 95 else "🔴")
        barra = "▰" * int(min(pct, 100) // 10) + "▱" * (10 - int(min(pct, 100) // 10))
        lineas.append(f"{emoji} {cat}")
        lineas.append(f"   {barra} {pct:.0f}%")
        lineas.append(f"   Gastado: ${gastado:,.0f} / ${limite:,.0f}  →  Resta: ${restante:,.0f}")
        lineas.append("")
        total_limite += limite
        total_gastado += gastado

    pct_total = (total_gastado / total_limite * 100) if total_limite > 0 else 0.0
    lineas.append(f"📦 Total del mes: ${total_gastado:,.0f} / ${total_limite:,.0f} ({pct_total:.0f}%)")
    return "\n".join(lineas), None

# ==================== OBJETIVOS FINANCIEROS ====================
def crear_objetivo(user_id: int, descripcion: str, tipo: str, monto_objetivo: float, fecha_limite: str = None):
    tipo = tipo.strip().upper() if tipo else "CAPITAL"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO objetivos (user_id, descripcion, tipo, monto_objetivo, fecha_limite)
                   VALUES (%s, %s, %s, %s, %s) RETURNING id;""",
                (user_id, descripcion, tipo, float(monto_objetivo), fecha_limite)
            )
            oid = cursor.fetchone()[0]
            conn.commit()
            return oid

def obtener_progreso_objetivos(user_id: int):
    with get_db_connection() as conn:
        df = pd.read_sql(
            "SELECT id, descripcion, tipo, monto_objetivo, fecha_limite FROM objetivos WHERE user_id = %s AND activo = TRUE ORDER BY fecha_creacion;",
            conn, params=(user_id,)
        )
    if df.empty:
        return "No tenés objetivos activos. Podés crear uno diciendo por ejemplo:\n• 'Quiero llegar a 5000 usd de capital'\n• 'Objetivo: ganar 1000 usd este año'"

    resumen = obtener_resumen_portafolio(user_id)
    capital_actual = float(resumen['total_actual']) if (resumen and pd.notnull(resumen.get('total_actual')) and not np.isnan(resumen.get('total_actual'))) else 0.0

    with get_db_connection() as conn:
        df_tc = pd.read_sql("SELECT COALESCE(SUM(pnl_usd), 0) as pnl FROM trades_cerrados WHERE user_id = %s;", conn, params=(user_id,))
    pnl_realizado = float(df_tc['pnl'].iloc[0]) if (not df_tc.empty and pd.notnull(df_tc['pnl'].iloc[0]) and not np.isnan(df_tc['pnl'].iloc[0])) else 0.0

    lineas = ["🎯 TUS OBJETIVOS FINANCIEROS", ""]
    for _, row in df.iterrows():
        oid = int(row['id'])
        desc = row['descripcion']
        tipo = str(row['tipo']).strip().upper() if pd.notnull(row['tipo']) else "CAPITAL"
        objetivo = float(row['monto_objetivo']) if (pd.notnull(row['monto_objetivo']) and float(row['monto_objetivo']) > 0) else 1.0
        f_lim = row['fecha_limite'].strftime('%Y-%m-%d') if pd.notnull(row['fecha_limite']) else "Sin fecha"

        if tipo in ["PNL", "GANANCIA", "TRADES"]:
            actual = pnl_realizado
        else:
            actual = capital_actual

        if actual is None or np.isnan(actual):
            actual = 0.0

        pct = (actual / objetivo * 100) if objetivo > 0 else 0.0
        if np.isnan(pct):
            pct = 0.0
        pct_clamped = max(0.0, min(100.0, pct))

        emoji = "🟢" if pct_clamped >= 100 else ("🟡" if pct_clamped >= 50 else "🔵")
        bloques_llenos = int(round(pct_clamped / 10))
        bloques_vacios = 10 - bloques_llenos
        barra = "▰" * bloques_llenos + "▱" * bloques_vacios

        lineas.append(f"{emoji} {desc}")
        lineas.append(f"   {barra} {pct:.1f}%")
        lineas.append(f"   Actual: ${actual:,.2f} / Objetivo: ${objetivo:,.2f}")
        lineas.append(f"   Fecha límite: {f_lim}  (ID {oid})")
        lineas.append("")

    return "\n".join(lineas)

def desactivar_objetivo(user_id: int, oid: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE objetivos SET activo = FALSE WHERE id = %s AND user_id = %s;", (oid, user_id))
            conn.commit()

# ==================== SISTEMA DE ALERTAS INTELIGENTE ====================
def _hash_alerta(tipo: str, clave: str, detalle: str = "") -> str:
    raw = f"{tipo}|{clave}|{detalle}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def alerta_ya_enviada(user_id: int, hash_a: str, horas_ventana: int = 72) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT id FROM alertas_enviadas 
                   WHERE user_id = %s AND hash_alerta = %s 
                   AND fecha > NOW() - INTERVAL '%s hours';""",
                (user_id, hash_a, horas_ventana)
            )
            return cursor.fetchone() is not None

def registrar_alerta_enviada(user_id: int, tipo: str, clave: str, hash_a: str):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO alertas_enviadas (user_id, tipo_alerta, clave, hash_alerta) VALUES (%s, %s, %s, %s);",
                (user_id, tipo, clave, hash_a)
            )
            conn.commit()

def generar_alertas_para_usuario(user_id: int) -> list:
    alertas = []
    es_fin_de_semana = ahora_argentina().weekday() >= 5

    resumen = obtener_resumen_portafolio(user_id)
    if resumen:
        for p in resumen["posiciones"]:
            dist = p.get("dist_liq_pct")
            if dist is not None and dist < UMBRAL_LIQUIDACION_PCT:
                clave = f"{p['ticker']}_{p['id']}"
                h = _hash_alerta("liquidacion", clave, "")
                if not alerta_ya_enviada(user_id, h, horas_ventana=12):
                    alertas.append({
                        "texto": f"⚠️ LIQUIDACIÓN CERCANA\n{p['ticker']} [{p['tipo_pos']} {p['lev']:.0f}x]\nDistancia actual: {dist:.1f}%\nPrecio liq: ${p['precio_liq']:,.2f} | Spot: ${p['spot']:,.2f}",
                        "tipo": "liquidacion",
                        "clave": clave,
                        "hash": h
                    })

    if resumen:
        tickers = list({p['ticker'] for p in resumen["posiciones"] if p['ticker'] not in ["USDT", "USDC", "DAI", "USD"]})
        
        if es_fin_de_semana:
            tickers = [t for t in tickers if t in CRIPTOS_COMUNES or t.endswith("-USD")]

        for tk in tickers[:8]:
            try:
                _, info = generar_grafico_analisis_tecnico(tk, "diario")
                if info and not isinstance(info, str):
                    p_act = info['precio_actual']
                    rsi = info.get("rsi", 50)
                    diag = info.get("diagnostico_rsi", "")
                    poc = info.get("poc")
                    bias = info.get("estructura_bias")
                    hist = info.get("hist_stat")

                    if poc and not np.isnan(poc) and abs(p_act - poc) / p_act <= 0.008:
                        clave = f"{tk}_test_poc"
                        h = _hash_alerta("poc_test", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=48):
                            alertas.append({
                                "texto": f"🎯 TESTEO DE POC INSTITUCIONAL\n{tk} está testeando su POC de volumen en ${poc:,.2f} (Precio: ${p_act:,.2f}). Zona de alta reacción.",
                                "tipo": "poc_test",
                                "clave": clave,
                                "hash": h
                            })

                    if bias == "choch_alcista":
                        clave = f"{tk}_choch_up"
                        h = _hash_alerta("choch", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=72):
                            alertas.append({
                                "texto": f"🟢 CAMBIO DE ESTRUCTURA\n{tk} rompió un techo/máximo importante en ${p_act:,.2f}. Es posible un cambio a tendencia alcista.",
                                "tipo": "choch",
                                "clave": clave,
                                "hash": h
                            })
                    elif bias == "choch_bajista":
                        clave = f"{tk}_choch_down"
                        h = _hash_alerta("choch", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=72):
                            alertas.append({
                                "texto": f"🔴 CAMBIO DE ESTRUCTURA\n{tk} rompió un piso/mínimo importante en ${p_act:,.2f}. Es posible un cambio a tendencia bajista.",
                                "tipo": "choch",
                                "clave": clave,
                                "hash": h
                            })

                    tipo_senal = None
                    if "SE ESTÁ FORMANDO" in diag.upper() or "EN FORMACIÓN" in diag.upper():
                        tipo_senal = "div_predictiva"
                    elif "DIVERGENCIA ALCISTA" in diag.upper():
                        tipo_senal = "div_alcista"
                    elif "DIVERGENCIA BAJISTA" in diag.upper():
                        tipo_senal = "div_bajista"
                    elif rsi >= 70:
                        tipo_senal = "sobrecompra"
                    elif rsi <= 30:
                        tipo_senal = "sobreventa"

                    if tipo_senal:
                        clave = f"{tk}_{tipo_senal}"
                        h = _hash_alerta("rsi_senal", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=72):
                            extra_stat = ""
                            if hist and hist.get('desglose'):
                                d = {x['horizonte']: x for x in hist['desglose']}
                                h15 = d.get('15 días')
                                h3m = d.get('3 meses')
                                h1y = d.get('1 año')
                                lineas_stat = []
                                if h15:
                                    lineas_stat.append(f"15d: {h15['win_rate']:.0f}% WR ({h15['avg_ret']:+.1f}%)")
                                if h3m:
                                    lineas_stat.append(f"3m: {h3m['win_rate']:.0f}% WR ({h3m['avg_ret']:+.1f}%)")
                                if h1y:
                                    lineas_stat.append(f"1a: {h1y['win_rate']:.0f}% WR ({h1y['avg_ret']:+.1f}%)")
                                extra_stat = "\n📊 Histórico: " + " | ".join(lineas_stat)

                            alertas.append({
                                "texto": f"📡 SEÑAL TÉCNICA (Diario)\n{tk} — RSI {rsi:.1f}\n{diag}{extra_stat}",
                                "tipo": "rsi_senal",
                                "clave": clave,
                                "hash": h
                            })
            except Exception:
                pass

    try:
        with get_db_connection() as conn:
            df_hoy = pd.read_sql(
                """SELECT SUM(monto) as total FROM movimientos 
                   WHERE user_id = %s AND tipo = 'GASTO' 
                   AND fecha::date = CURRENT_DATE;""",
                conn, params=(user_id,)
            )
            df_prom = pd.read_sql(
                """SELECT AVG(diario) as promedio FROM (
                     SELECT fecha::date as d, SUM(monto) as diario 
                     FROM movimientos 
                     WHERE user_id = %s AND tipo = 'GASTO' 
                     AND fecha > NOW() - INTERVAL '30 days'
                     GROUP BY fecha::date
                   ) t;""",
                conn, params=(user_id,)
            )
        total_hoy = float(df_hoy['total'].iloc[0]) if not df_hoy.empty and pd.notnull(df_hoy['total'].iloc[0]) else 0.0
        prom = float(df_prom['promedio'].iloc[0]) if not df_prom.empty and pd.notnull(df_prom['promedio'].iloc[0]) else 0.0
        if prom > 0 and total_hoy > prom * UMBRAL_GASTO_INUSUAL:
            clave = f"gasto_{ahora_argentina().strftime('%Y%m%d')}"
            h = _hash_alerta("gasto_inusual", clave, "")
            if not alerta_ya_enviada(user_id, h, horas_ventana=20):
                alertas.append({
                    "texto": f"💸 GASTO INUSUAL HOY\nGastaste ${total_hoy:,.0f} ARS\nPromedio diario (30d): ${prom:,.0f} ARS\n({total_hoy/prom:.1f}x el promedio)",
                    "tipo": "gasto_inusual",
                    "clave": clave,
                    "hash": h
                })
    except Exception as e:
        logger.error(f"Error alerta gasto: {e}")

    return alertas

def _tickers_cartera_usuario(user_id: int):
    resumen = obtener_resumen_portafolio(user_id)
    if not resumen:
        return []
    out = []
    for p in resumen["posiciones"]:
        tk = str(p.get("ticker") or "").upper()
        if tk in ("USDT", "USDC", "DAI", "USD"):
            continue
        out.append(tk)
    return list(dict.fromkeys(out))

def generar_alertas_movimiento_brusco(user_id: int) -> list:
    alertas = []
    tickers = _tickers_cartera_usuario(user_id)
    if not tickers:
        return alertas
    es_finde = ahora_argentina().weekday() >= 5
    if es_finde:
        tickers = [t for t in tickers if t in CRIPTOS_COMUNES or str(t).endswith("-USD") or t in ("BTC", "ETH", "SOL")]
    dia = ahora_argentina().strftime("%Y%m%d")
    for tk in tickers[:15]:
        try:
            datos = consultar_datos_mercado(tk)
            if not datos:
                continue
            var = float(datos.get("var_pct") or 0)
            if abs(var) < UMBRAL_MOVIMIENTO_BRUSCO_PCT:
                continue
            lado = "ALZA" if var > 0 else "BAJA"
            clave = f"{tk}_{dia}_{lado}"
            h = _hash_alerta("brusco", clave, "")
            if alerta_ya_enviada(user_id, h, horas_ventana=20):
                continue
            em = "🟢" if var > 0 else "🔴"
            alertas.append({
                "texto": (
                    f"{em} MOVIMIENTO BRUSCO {lado} {abs(var):.1f}%\n"
                    f"{datos['ticker']}  ${datos['precio']:,.2f}\n"
                    f"Var día: {var:+.2f}%  (umbral {UMBRAL_MOVIMIENTO_BRUSCO_PCT:.0f}%)"
                ),
                "tipo": "brusco",
                "clave": clave,
                "hash": h,
            })
        except Exception as e:
            logger.warning(f"brusco {tk}: {e}")
    return alertas

_KW_VOL = (
    "fed", "fomc", "powell", "rate cut", "rate hike", "interest rate", "cpi", "nfp",
    "inflation", "sec ", "etf", "ban", "regul", "crypto", "bitcoin", "ethereum",
    "war", "tariff", "treasury", "payroll", "jobs", "yield",
)

def _pct(a, b):
    try:
        if a is None or b is None or float(b) == 0:
            return None
        return (float(a) - float(b)) / float(b) * 100.0
    except Exception:
        return None

def snapshot_briefing_ticker(ticker: str) -> dict:
    sym = normalizar_ticker_yf(ticker)
    out = {"ticker": ticker.upper(), "sym": sym, "precio": None, "var_hoy": None, "var_ayer": None, "var_pre": None}
    try:
        h = yf_history(sym, period="10d")
        if h is not None and not h.empty and "Close" in h.columns:
            c = h["Close"].dropna()
            if len(c) >= 1:
                out["precio"] = float(c.iloc[-1])
            if len(c) >= 2:
                out["var_hoy"] = _pct(c.iloc[-1], c.iloc[-2])
            if len(c) >= 3:
                out["var_ayer"] = _pct(c.iloc[-2], c.iloc[-3])
    except Exception as e:
        logger.warning(f"brief hist {sym}: {e}")
    try:
        t = yf.Ticker(sym)
        info = {}
        try:
            info = t.fast_info if hasattr(t, "fast_info") else {}
        except Exception:
            info = {}
        pre = None
        prev = None
        last = None
        def _g(obj, *keys):
            for k in keys:
                try:
                    if hasattr(obj, k):
                        v = getattr(obj, k)
                        if v is not None:
                            return float(v)
                    if isinstance(obj, dict) and obj.get(k) is not None:
                        return float(obj.get(k))
                except Exception:
                    continue
            return None
        pre = _g(info, "pre_market_price", "preMarketPrice")
        prev = _g(info, "previous_close", "previousClose", "regularMarketPreviousClose")
        last = _g(info, "last_price", "lastPrice", "regularMarketPrice")
        if last and out["precio"] is None:
            out["precio"] = last
        if pre and prev:
            out["var_pre"] = _pct(pre, prev)
            out["pre_px"] = pre
        elif "-USD" in sym or ticker.upper() in CRIPTOS_COMUNES:
            # cripto: variación últimas ~8h vs precio de hace 8h
            try:
                hin = yf.Ticker(sym).history(period="1d", interval="1h")
                if hin is not None and not hin.empty:
                    ser = hin["Close"].dropna()
                    if len(ser) >= 4:
                        out["var_pre"] = _pct(ser.iloc[-1], ser.iloc[0])
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"brief pre {sym}: {e}")
    return out

def titulares_volatilidad() -> list:
    titulos = []
    vistos = set()
    for tk in ("SPY", "QQQ", "BTC-USD", "^VIX"):
        try:
            items = getattr(yf.Ticker(tk), "news", None) or []
        except Exception:
            items = []
        for it in items[:8]:
            title = ""
            if isinstance(it, dict):
                title = it.get("title") or it.get("headline") or ""
                content = it.get("content") if isinstance(it.get("content"), dict) else {}
                if not title and content:
                    title = content.get("title") or ""
            title = str(title).strip()
            if not title:
                continue
            key = title.lower()[:80]
            if key in vistos:
                continue
            vistos.add(key)
            blob = title.lower()
            if any(k in blob for k in _KW_VOL) or tk in ("SPY", "BTC-USD"):
                titulos.append(title[:140])
            if len(titulos) >= 5:
                return titulos
    return titulos[:5]

def _fmt_var(v):
    if v is None:
        return "n/d"
    em = "🟢" if v >= 0 else "🔴"
    return f"{em} {v:+.2f}%"

def texto_briefing_mananero(user_id: int) -> str:
    es_finde = ahora_argentina().weekday() >= 5
    lineas = [f"☀️ BRIEFING {ahora_argentina().strftime('%d/%m %H:%M')} ART", ""]
    lineas.append("Mercado general  (hoy / ayer / pre o 8h crypto)")
    universales = ["BTC-USD", "ETH-USD"] if es_finde else list(TICKERS_MERCADO_GRAL)
    snaps = []
    for tk in universales:
        s = snapshot_briefing_ticker(tk)
        if s.get("precio") is None:
            continue
        snaps.append(s)
        pre_lbl = "pre" if "USD" not in str(s.get("sym", "")) and tk not in ("BTC-USD", "ETH-USD") else "8h"
        lineas.append(
            f"• {s['ticker']} ${s['precio']:,.2f}"
            f"  hoy {_fmt_var(s.get('var_hoy'))}"
            f"  ayer {_fmt_var(s.get('var_ayer'))}"
            f"  {pre_lbl} {_fmt_var(s.get('var_pre'))}"
        )
    if not snaps:
        lineas.append("• Sin datos de mercado (Yahoo).")
    propios = []
    tickers = _tickers_cartera_usuario(user_id)
    if es_finde:
        tickers = [t for t in tickers if t in CRIPTOS_COMUNES or str(t).endswith("-USD") or t in ("BTC", "ETH", "SOL")]
    for tk in tickers[:20]:
        s = snapshot_briefing_ticker(tk)
        if s.get("precio") is not None:
            propios.append(s)
    lineas.append("")
    lineas.append("Tus activos (hoy / ayer / pre|8h)")
    if not propios:
        lineas.append("• No hay posiciones para listar.")
    else:
        propios.sort(key=lambda x: abs(x.get("var_hoy") or x.get("var_ayer") or 0), reverse=True)
        for s in propios[:8]:
            vref = s.get("var_hoy") if s.get("var_hoy") is not None else s.get("var_ayer")
            mark = " ⚡" if vref is not None and abs(vref) >= UMBRAL_MOVIMIENTO_BRUSCO_PCT else ""
            pre_lbl = "8h" if str(s.get("sym", "")).endswith("-USD") or s["ticker"] in CRIPTOS_COMUNES else "pre"
            lineas.append(
                f"• {s['ticker']} ${s['precio']:,.2f}{mark}"
                f"  hoy {_fmt_var(s.get('var_hoy'))}"
                f"  ayer {_fmt_var(s.get('var_ayer'))}"
                f"  {pre_lbl} {_fmt_var(s.get('var_pre'))}"
            )
    lineas.append("")
    lineas.append("Qué puede mover el día")
    news = titulares_volatilidad()
    if news:
        for t in news[:4]:
            lineas.append(f"• {t}")
    else:
        lineas.append("• No llegaron titulares (Fed/regulación/macro) en Yahoo.")
    lineas.append("")
    lineas.append("Próximo briefing: pasado mañana 08:30 ART.")
    return "\n".join(lineas)

async def _enviar_bloque_alertas(app, uid, titulo, alertas):
    if not alertas:
        return
    mensajes = [titulo, ""]
    for a in alertas:
        mensajes.append(a["texto"])
        mensajes.append("")
        await asyncio.to_thread(registrar_alerta_enviada, uid, a["tipo"], a["clave"], a["hash"])
    try:
        await app.bot.send_message(chat_id=uid, text="\n".join(mensajes).strip())
    except Exception as e:
        logger.error(f"No se pudo enviar alerta a {uid}: {e}")

def _obtener_usuarios_alerta():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT user_id FROM (
                    SELECT user_id FROM portafolio_inversiones
                    UNION
                    SELECT user_id FROM movimientos
                    UNION
                    SELECT user_id FROM trades_cerrados
                ) u WHERE user_id IS NOT NULL;
            """)
            return [r[0] for r in cursor.fetchall()]

async def tarea_alertas_periodicas(app):
    await asyncio.sleep(45)
    ultimo_tech = 0.0
    while True:
        try:
            ahora = ahora_argentina()
            usuarios = await asyncio.to_thread(_obtener_usuarios_alerta)
            en_ventana_brief = ahora.hour == 8 and 25 <= ahora.minute <= 50
            dia_par = (ahora.toordinal() % 2 == 0)

            for uid in usuarios:
                if not alertas_activas(uid):
                    continue
                if en_horario_alertas():
                    bruscas = await asyncio.to_thread(generar_alertas_movimiento_brusco, uid)
                    if bruscas:
                        await _enviar_bloque_alertas(app, uid, "⚡ ALERTA DE MOVIMIENTO (fuera de señales técnicas)", bruscas)

                if en_ventana_brief and dia_par and es_dia_habil_arg():
                    h = _hash_alerta("briefing", ahora.strftime("%Y%m%d"), "")
                    ya = await asyncio.to_thread(alerta_ya_enviada, uid, h, 36)
                    if not ya:
                        texto = await asyncio.to_thread(texto_briefing_mananero, uid)
                        try:
                            await app.bot.send_message(chat_id=uid, text=texto)
                            await asyncio.to_thread(registrar_alerta_enviada, uid, "briefing", ahora.strftime("%Y%m%d"), h)
                        except Exception as e:
                            logger.error(f"Briefing {uid}: {e}")

            if en_horario_alertas() and (time.time() - ultimo_tech) >= ALERTA_INTERVALO_HORAS * 3600:
                for uid in usuarios:
                    if not alertas_activas(uid):
                        continue
                    alertas = await asyncio.to_thread(generar_alertas_para_usuario, uid)
                    tech = [a for a in alertas if a.get("tipo") != "brusco"]
                    if tech:
                        await _enviar_bloque_alertas(app, uid, "🔔 ALERTAS DE TU CARTERA", tech)
                ultimo_tech = time.time()
        except Exception as e:
            logger.error(f"Error en tarea de alertas: {e}", exc_info=True)

        await asyncio.sleep(15 * 60)

# ==================== SYSTEM INSTRUCTION PARA IA ====================
SYSTEM_INSTRUCTION = """Sos un router de comandos. Una sola línea, en español. Sin explicaciones. Sin inglés.
Si es una orden o reporte, devolvé exactamente:
COMANDO: /analisis GOOGL diario
El ticker va en mayúsculas. La temporalidad solo puede ser diario, semanal o 4h.
Ejemplos:
- "analiza googl en diario" -> COMANDO: /analisis GOOGL diario
- "fijate meli semanal" -> COMANDO: /analisis MELI semanal
- "a cuanto esta btc" -> COMANDO: /precio BTC
- "como vengo de gastos" -> COMANDO: /gastos
- "torta de la cartera" -> COMANDO: /torta cartera
- "vs el ciclo anterior" -> COMANDO: /vs
Comandos válidos: /gastos /vs /fijos /delivery /mes /resumen /riesgo /objetivos /mensual /spy /grafico /activos /analisis /precio /excel /briefing /torta
Si es un gasto o ingreso:
REGISTRO_ARS: GASTO|2125.4|Transporte|SUBE|2026-09-29
Si no entendés: una sola oración pidiendo /help.
"""

# ==================== FLUJO INTERACTIVO DE IMPORTACIÓN Y CONCILIACIÓN ====================
async def manejar_documento_extracto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    doc = update.message.document
    nombre = doc.file_name.lower()

    if not (nombre.endswith(".xlsx") or nombre.endswith(".xls") or nombre.endswith(".pdf")):
        await update.message.reply_text("Por favor enviame un archivo Excel (.xlsx / .xls) de tu banco o un PDF de Mercado Pago.")
        return

    await update.message.reply_text("📥 Leyendo y procesando documento...")
    archivo = await context.bot.get_file(doc.file_id)
    archivo_bytes = bytes(await archivo.download_as_bytearray())

    tipo_origen = "PDF_MP" if nombre.endswith(".pdf") else "EXCEL"
    if tipo_origen == "PDF_MP":
        movimientos = parsear_extracto_pdf_mercadopago(archivo_bytes)
    else:
        movimientos = parsear_extracto_bancario_excel(archivo_bytes)

    if not movimientos:
        await update.message.reply_text("⚠️ No se encontraron movimientos válidos en el archivo. Verificá que sea un extracto legible.")
        return

    importaciones_activas[user_id] = {
        "origen": tipo_origen,
        "items": movimientos,
        "idx": 0,
        "guardados": 0,
        "auto_aprobados": 0,
        "omitidos": 0,
        "reemplazados": 0,
        "esperando_reemplazo": False,
        "coincidencia_previa": None,
        "esperando_recordar": False,
        "patron_pendiente": None,
        "cat_pendiente": None,
        "desc_pendiente": None
    }

    await presentar_siguiente_movimiento(update, user_id)

async def presentar_siguiente_movimiento(update: Update, user_id: int):
    sesion = importaciones_activas.get(user_id)
    if not sesion:
        return

    items = sesion["items"]

    # Procesar de forma automática movimientos con alta recurrencia histórica
    while sesion["idx"] < len(items):
        item = items[sesion["idx"]]
        cat_sug, desc_sug, clave, usos, rechazos, auto_ok = buscar_clasificacion_previa(user_id, item["concepto"])

        # Auto-aprobar solo con historial 100% consistente (cero rechazos / cambios de rubro)
        if cat_sug and auto_ok:
            guardar_movimiento(user_id, item["tipo"], item["monto"], cat_sug, desc_sug, item["fecha"])
            guardar_aprendizaje_concepto(user_id, clave, cat_sug, desc_sug)
            sesion["guardados"] += 1
            sesion["auto_aprobados"] += 1
            sesion["idx"] += 1
            continue

        break

    if sesion["idx"] >= len(items):
        g = sesion["guardados"]
        auto = sesion["auto_aprobados"]
        o = sesion["omitidos"]
        r = sesion["reemplazados"]
        del importaciones_activas[user_id]
        
        reemp_txt = f"\n• Duplicados genéricos reemplazados: {r}" if r > 0 else ""
        auto_txt = f"\n• Auto-registrados (comercios habituales): {auto}" if auto > 0 else ""
        await update.message.reply_text(
            f"✅ ¡Importación completada!\n\n"
            f"• Registrados en tu historial: {g}{auto_txt}\n"
            f"• Descartados / omitidos: {o}{reemp_txt}\n\n"
            f"Podés ver tu nuevo estado con /gastos o /mes."
        )
        return

    idx = sesion["idx"]
    item = items[idx]
    em = "🔴 GASTO" if item["tipo"] == "GASTO" else "🟢 INGRESO"

    coincidencia = buscar_coincidencia_previa_db(user_id, item["monto"], item["fecha"], sesion.get("origen", "EXCEL"))
    if coincidencia and item["tipo"] == "GASTO":
        sesion["esperando_reemplazo"] = True
        sesion["coincidencia_previa"] = coincidencia

        msg = (
            f"🔍 POSIBLE DUPLICADO DETECTADO ({idx + 1}/{len(items)}):\n\n"
            f"Encontré un débito bancario genérico previo de ${coincidencia['monto']:,.2f} el {coincidencia['fecha']}:\n"
            f"• '{coincidencia['descripcion']}' ({coincidencia['categoria']})\n\n"
            f"En Mercado Pago figura la salida real:\n"
            f"• '{item['concepto']}' por ${item['monto']:,.2f} ARS el {item['fecha']}\n\n"
            f"¿Querés REEMPLAZAR el débito genérico por esta salida detallada?\n"
            f"• Respondé 'Si' para reemplazarlo (evita duplicar el gasto).\n"
            f"• Respondé 'No' para mantener ambos.\n"
            f"• Respondé 'Omitir' para saltear este movimiento."
        )
        await update.message.reply_text(msg)
        return

    cat_sug, desc_sug, clave, usos, rechazos, auto_ok = buscar_clasificacion_previa(user_id, item["concepto"])
    item["cat_sug"] = cat_sug
    item["desc_sug"] = desc_sug
    item["clave_patron"] = clave
    item["usos_sug"] = usos
    item["rechazos_sug"] = rechazos

    if cat_sug:
        extra_no = f" | {rechazos} rechazo(s)" if rechazos else ""
        opciones_txt = (
            f"🏷️ Clasificación sugerida ({usos}/{UMBRAL_AUTO_APROBAR} OK{extra_no}): {cat_sug} ({desc_sug})\n\n"
            f"¿Querés registrarlo con esta categoría?\n"
            f"• Respondé 'Si' u 'Ok' para guardarlo directo.\n"
            f"• O escribí otra categoría si preferís cambiarla.\n"
            f"• O 'No', 'Paso' u 'Omitir' para no guardarlo.\n"
            f"• O 'Cancelar' para frenar la importación."
        )
    else:
        opciones_txt = (
            f"¿Querés registrarlo? Respondeme por ejemplo:\n"
            f"• 'Panaderia, compra de pan'\n"
            f"• O simplemente 'No', 'Paso' u 'Omitir' para no guardarlo.\n"
            f"• O 'Cancelar' para frenar la importación."
        )

    msg = (
        f"📄 Movimiento {idx + 1} de {len(items)}:\n\n"
        f"• Fecha: {item['fecha']}\n"
        f"• Tipo: {em}\n"
        f"• Monto: ${item['monto']:,.2f} ARS\n"
        f"• Concepto: {item['concepto']}\n\n"
        f"{opciones_txt}"
    )
    await update.message.reply_text(msg)

async def procesar_respuesta_importacion(update: Update, user_id: int, user_text: str) -> bool:
    sesion = importaciones_activas.get(user_id)
    if not sesion:
        return False

    tlow = user_text.strip().lower()
    if tlow in ("cancelar", "abortar", "salir", "detener"):
        del importaciones_activas[user_id]
        await update.message.reply_text("🛑 Importación cancelada.")
        return True

    idx = sesion["idx"]
    item = sesion["items"][idx]

    # 1. Resolver reemplazo de duplicado
    if sesion.get("esperando_reemplazo"):
        coin = sesion["coincidencia_previa"]
        sesion["esperando_reemplazo"] = False
        sesion["coincidencia_previa"] = None

        if tlow in ("si", "sí", "s", "yes", "dale", "ok", "reemplazar"):
            borrar_movimiento_por_id_db(user_id, coin["id"])
            sesion["reemplazados"] += 1
            await update.message.reply_text(f"🗑️ Se eliminó el gasto previo genérico de ${coin['monto']:,.2f} (ID {coin['id']}).")
        elif tlow in ("omitir", "saltear", "descartar"):
            sesion["omitidos"] += 1
            sesion["idx"] += 1
            await presentar_siguiente_movimiento(update, user_id)
            return True
        else:
            await update.message.reply_text("👌 Se mantiene el registro previo. Procedemos a registrar este como adicional.")

    # 2. Confirmación de recordar patrón
    if sesion.get("esperando_recordar"):
        clave = sesion["patron_pendiente"]
        cat = sesion["cat_pendiente"]
        desc = sesion["desc_pendiente"]

        if tlow in ("si", "sí", "s", "yes", "dale", "ok", "guardalo", "recordar"):
            guardar_aprendizaje_concepto(user_id, clave, cat, desc)
            await update.message.reply_text(f"🧠 ¡Listo! Recordaré que '{clave}' suele ser {cat}.")
        else:
            await update.message.reply_text("👌 Perfecto, no lo guardo como regla fija.")

        sesion["esperando_recordar"] = False
        sesion["patron_pendiente"] = None
        sesion["cat_pendiente"] = None
        sesion["desc_pendiente"] = None
        sesion["idx"] += 1
        await presentar_siguiente_movimiento(update, user_id)
        return True

    # 3. Descarte del movimiento
    if tlow in ("no", "paso", "omitir", "saltear", "descartar", "nop"):
        clave_omit = item.get("clave_patron")
        if item.get("cat_sug") and clave_omit:
            registrar_rechazo_concepto(user_id, clave_omit)
        sesion["omitidos"] += 1
        sesion["idx"] += 1
        await presentar_siguiente_movimiento(update, user_id)
        return True

    cat_sug = item.get("cat_sug")
    desc_sug = item.get("desc_sug")
    clave = item.get("clave_patron")

    # Confirmó sugerencia aprendida previa (0 tokens)
    if cat_sug and tlow in ("si", "sí", "ok", "dale", "guardalo", "s", "yes", "confirmo"):
        cat_final = cat_sug
        desc_final = desc_sug
        guardar_movimiento(user_id, item["tipo"], item["monto"], cat_final, desc_final, item["fecha"])
        guardar_aprendizaje_concepto(user_id, clave, cat_final, desc_final)
        sesion["guardados"] += 1
        sesion["idx"] += 1
        await update.message.reply_text(f"✅ Guardado como {cat_final}: {desc_final}")
        await presentar_siguiente_movimiento(update, user_id)
        return True

    # Python primero (0 tokens). Gemini solo si no reconoce la categoría,
    # y únicamente con el texto que escribió el usuario.
    uso_gemini = False
    if categoria_reconocida_por_python(user_text) or match_regla_comercio(user_text):
        if match_regla_comercio(user_text):
            cat_final, desc_final, _, _ = clasificar_categoria_python(user_text)
        else:
            cat_final, desc_final = parsear_categoria_descripcion_usuario(user_text)
    else:
        try:
            cat_final, desc_final = emprolijar_categoria_con_gemini(user_text)
            uso_gemini = True
        except Exception:
            cat_final, desc_final = parsear_categoria_descripcion_usuario(user_text)

    guardar_movimiento(user_id, item["tipo"], item["monto"], cat_final, desc_final, item["fecha"])
    sesion["guardados"] += 1

    clave_aprender = clave or extraer_clave_comercio(item.get("concepto") or user_text) or extraer_clave_nl(user_text)
    if clave_aprender and len(clave_aprender) >= 3 and clave_aprender not in PALABRAS_PROHIBIDAS_PATRON:
        aprender_patron_clasificacion(user_id, item.get("concepto") or user_text, cat_final, desc_final, clave_aprender)
        tag = " · patrón de Gemini guardado" if uso_gemini else " · patrón guardado"
        sesion["idx"] += 1
        await update.message.reply_text(f"✅ Guardado como {cat_final}: {desc_final}{tag}")
        await presentar_siguiente_movimiento(update, user_id)
        return True

    sesion["idx"] += 1
    await update.message.reply_text(f"✅ Guardado como {cat_final}: {desc_final}")
    await presentar_siguiente_movimiento(update, user_id)
    return True

# ==================== COMANDOS RÁPIDOS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy tu copiloto financiero cuantitativo.\n\n"
        "📈 Trading e Inversiones (0 tokens)\n"
        "• /spy        →  Rendimiento % vs S&P 500\n"
        "• /spy todo   →  Rendimiento % vs SPY desde el inicio\n"
        "• /mensual    →  Rendimiento % mes a mes (con WR y vs SPY)\n"
        "• /grafico    →  Curva de PnL en USD\n"
        "• /activos    →  Comparativa relativa de activos\n"
        "• /analisis   →  Análisis técnico algorítmico\n"
        "• /riesgo     →  Drawdown, Win Rate, Expectancy\n"
        "• /resumen    →  Balance consolidado de cartera\n\n"
        "💵 Finanzas Personales (ARS)\n"
        "• /gastos     →  Costo de vida, Tasa de Ahorro y Gastos Hormiga\n"
        "• /vs         →  Este ciclo Havas vs el anterior\n"
        "• /fijos      →  Servicios y gastos que se repiten (aunque el monto cambie)\n"
        "• /delivery   →  Delivery neto (gasto − devoluciones) y tope\n"
        "• /torta      →  Torta de gastos del ciclo (o /torta cartera)\n"
        "• /mes        →  Presupuestos y control del mes\n"
        "• /objetivos  →  Progreso de metas financieras\n"
        "• /excel      →  Exportar planilla completa\n"
        "• Envía un archivo Excel (.xlsx) de tu banco o un PDF de Mercado Pago para procesar y conciliar gastos uno por uno.\n\n"
        "También podés hablarme en lenguaje natural."
    )

def extraer_periodo(texto: str) -> str:
    tlow = (texto or "").lower()
    if re.search(r"\b(todo|max|historico|histórico|desde el inicio|desde siempre|total)\b", tlow):
        return "todo"
    for key in ["ytd", "mtd", "wtd", "1y", "1a", "3m", "6m", "1m", "2m", "2y", "3y", "5y"]:
        if re.search(r"\b" + re.escape(key) + r"\b", tlow):
            return key
    m = re.search(r"(\d+)\s*(mes|meses|dia|dias|año|anos|ano|años)", tlow)
    if m:
        return m.group(0)
    if "este año" in tlow or "este ano" in tlow:
        return "ytd"
    return ""

async def enviar_grafico_cartera(update: Update, user_id: int, periodo: str = "", modo: str = "usd"):
    buf = generar_grafico_evolucion_cartera_consolidada(user_id, periodo, modo=modo)
    if buf:
        if modo in ("pct", "percent", "%", "spy"):
            cap = f"📊 Cartera vs SPY en % ({periodo or 'histórico'})"
        else:
            cap = f"📈 Cartera en USD ({periodo or 'histórico'}) — PnL del período"
        await update.message.reply_photo(photo=buf, caption=cap)
    else:
        await update.message.reply_text("No hay datos suficientes para la curva.")

async def enviar_grafico_activos(update: Update, user_id: int, periodo: str = "", tickers=None):
    buf = generar_grafico_evolucion_por_activos(user_id, periodo, tickers)
    if buf:
        extra = f" ({', '.join(tickers)})" if tickers else ""
        await update.message.reply_photo(photo=buf, caption=f"📊 Rendimiento relativo base 100{extra}")
    else:
        await update.message.reply_text("No hay suficientes activos que coincidan.")

MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

def cotizacion_usd_ars() -> float:
    for sym in ("USDARS=X", "ARS=X"):
        try:
            h = yf_history(sym, period="5d")
            if h is not None and not h.empty:
                px = float(h["Close"].dropna().iloc[-1])
                if 100 < px < 20000:
                    return px
                if 0 < px < 1:
                    return 1.0 / px
        except Exception:
            continue
    return 1400.0

def snapshot_capacidad(user_id: int) -> dict:
    with get_db_connection() as conn:
        df = pd.read_sql(
            "SELECT fecha, tipo, monto, categoria, descripcion FROM movimientos WHERE user_id = %s;",
            conn, params=(user_id,),
        )
    ingreso = consumo = ahorro = deuda = 0.0
    ciclos = 1
    if not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"])
        df["categoria"] = df["categoria"].apply(normalizar_categoria)
        sueldos = obtener_fechas_sueldo(user_id)
        df = asignar_ciclo_havas(df, sueldos)
        ciclos = max(1, df["ciclo_id"].nunique())
        for _, r in df.iterrows():
            blob = f"{r['categoria']} {r['descripcion']}".lower()
            mon = float(r["monto"] or 0)
            if r["tipo"] == "INGRESO":
                if "delfina" in blob and mon >= 100000:
                    continue
                if any(t in blob for t in TAGS_AHORRO):
                    continue
                ingreso += mon
            else:
                if any(t in blob for t in TAGS_AHORRO) or "fima" in blob:
                    ahorro += mon
                elif normalizar_categoria(str(r["categoria"])) == "Tarjeta de crédito" or any(t in blob for t in TAGS_DEUDA):
                    deuda += mon
                else:
                    consumo += mon
    # ingresos counted all non-capital; subtract was messy with "or True"
    superavit = (ingreso - consumo) / ciclos
    cartera = 0.0
    try:
        res = obtener_resumen_portafolio(user_id)
        if res:
            cartera = float(res.get("total_actual") or 0)
    except Exception:
        pass
    fx = cotizacion_usd_ars()
    return {
        "ingreso_mes": ingreso / ciclos,
        "consumo_mes": consumo / ciclos,
        "ahorro_mes": ahorro / ciclos,
        "deuda_mes": deuda / ciclos,
        "superavit_mes": superavit,
        "cartera_usd": cartera,
        "fx": fx,
        "ciclos": ciclos,
    }

def parsear_pedido_proyeccion(texto: str):
    low = (texto or "").lower()
    if not re.search(r"\b(vacaciones|viaje|viajar|puedo|podr[eé]|alcanza|me da|meta|quiero hacer|me voy)\b", low):
        return None
    if not re.search(r"\b(vacaciones|viaje|viajar|usd|d[oó]lar|plata|plata|presupuesto de viaje)\b", low) and not re.search(r"\d", low):
        return None
    # monto
    monto_usd = None
    monto_ars = None
    m = re.search(r"(\d[\d\.]*)\s*(k|mil)?\s*(usd|u\$d|u\$s|d[oó]lares?)", low)
    if m:
        n = float(m.group(1).replace(".", "").replace(",", ".")) if "," in m.group(1) else float(m.group(1).replace(".", "") if m.group(1).count(".")>1 else m.group(1).replace(",", ""))
        try:
            n = float(re.sub(r"[^\d.]", "", m.group(1).replace(",", ".")))
        except Exception:
            n = None
        if n is not None:
            if (m.group(2) or "") in ("k", "mil") and n < 10000:
                n *= 1000
            monto_usd = n
    if monto_usd is None:
        m2 = re.search(r"(usd|u\$d|u\$s|d[oó]lares?)\s*(\d[\d\.]*)\s*(k|mil)?", low)
        if m2:
            try:
                n = float(m2.group(2).replace(",", "."))
                if (m2.group(3) or "") in ("k", "mil") and n < 10000:
                    n *= 1000
                monto_usd = n
            except Exception:
                pass
    if monto_usd is None:
        m3 = re.search(r"(\d[\d\.]*)\s*(k|mil)?\s*(ars|pesos)?", low)
        # only if explicitly pesos or large number without usd already
        if "usd" not in low and "dólar" not in low and "dolar" not in low and m3:
            try:
                n = float(m3.group(1).replace(".", "").replace(",", "")) if m3.group(1).count(".") >= 1 and "usd" not in low else float(m3.group(1).replace(",", "."))
            except Exception:
                n = None
            if n and n >= 100000:
                monto_ars = n
    mes_n = None
    anio_n = None
    for nom, num in MESES_ES.items():
        if re.search(rf"\b{nom}\b", low):
            mes_n = num
            break
    m_an = re.search(r"\b(202[6-9]|203[0-9])\b", low)
    if m_an:
        anio_n = int(m_an.group(1))
    m_en = re.search(r"en\s+(\d+)\s+mes", low)
    meses_plazo = int(m_en.group(1)) if m_en else None
    if monto_usd is None and monto_ars is None and mes_n is None and meses_plazo is None:
        if re.search(r"\bvacaciones\b|\bviaje\b", low):
            return {"tipo": "vacaciones", "monto_usd": None, "monto_ars": None, "mes": None, "anio": None, "meses_plazo": None, "incompleto": True}
        return None
    return {
        "tipo": "vacaciones" if re.search(r"vacaciones|viaje", low) else "meta",
        "monto_usd": monto_usd,
        "monto_ars": monto_ars,
        "mes": mes_n,
        "anio": anio_n,
        "meses_plazo": meses_plazo,
        "incompleto": False,
    }

def texto_analisis_proyeccion(user_id: int, texto_orig: str) -> str:
    pedido = parsear_pedido_proyeccion(texto_orig)
    if not pedido:
        return ""
    if pedido.get("incompleto") or (not pedido.get("monto_usd") and not pedido.get("monto_ars")):
        return (
            "Para proyectar necesito el monto. Ejemplo:\n"
            "• 'Vacaciones en marzo de 3000 USD, ¿puedo?'\n"
            "• 'Viaje en 5 meses de 2k usd'"
        )
    snap = snapshot_capacidad(user_id)
    fx = snap["fx"]
    if pedido["monto_usd"]:
        need_usd = float(pedido["monto_usd"])
        need_ars = need_usd * fx
    else:
        need_ars = float(pedido["monto_ars"])
        need_usd = need_ars / fx if fx else 0
    ahora = ahora_argentina()
    if pedido.get("meses_plazo"):
        meses = max(1, int(pedido["meses_plazo"]))
        fecha_obj = ahora + timedelta(days=30 * meses)
    elif pedido.get("mes"):
        anio = pedido.get("anio") or ahora.year
        if pedido["mes"] < ahora.month or (pedido["mes"] == ahora.month and ahora.day > 15):
            if not pedido.get("anio"):
                anio = ahora.year + 1
        fecha_obj = datetime(anio, pedido["mes"], 1)
        meses = max(1, int(round((fecha_obj - ahora).days / 30.0)))
    else:
        meses = 6
        fecha_obj = ahora + timedelta(days=180)
    ahorro_acum_ars = snap["superavit_mes"] * meses
    ahorro_acum_usd = ahorro_acum_ars / fx if fx else 0
    cartera = snap["cartera_usd"]
    disponible_usd = cartera + max(0.0, ahorro_acum_usd)
    cubre_flujo = ahorro_acum_usd >= need_usd
    cubre_cartera = cartera >= need_usd
    cubre_mixto = disponible_usd >= need_usd
    if cubre_flujo:
        veredicto = "🟢 Sí, con el ritmo actual de ahorro (sin tocar la cartera)."
    elif cubre_mixto:
        veredicto = "🟡 Sí, si usás parte de la cartera + lo que ahorres hasta esa fecha."
    elif cubre_cartera:
        veredicto = "🟡 La cartera cubre el viaje, pero no lo financiás solo con el excedente mensual."
    else:
        falta = need_usd - disponible_usd
        veredicto = f"🔴 Justo no cierra. Te faltarían ~${falta:,.0f} USD a este ritmo."
    mes_txt = fecha_obj.strftime("%m/%Y")
    lineas = [
        f"🧳 Proyección · {pedido['tipo']} {mes_txt}",
        f"• Objetivo: ${need_usd:,.0f} USD  (~${need_ars:,.0f} ARS @ {fx:,.0f})",
        f"• Plazo: {meses} mes(es)",
        "",
        "Tu capacidad (Python, ciclo Havas):",
        f"• Ingresos habit. ${snap['ingreso_mes']:,.0f} ARS/mes",
        f"• Consumo vida    ${snap['consumo_mes']:,.0f} ARS/mes",
        f"• Excedente       ${snap['superavit_mes']:+,.0f} ARS/mes ({snap['superavit_mes']/fx:+,.0f} USD/mes)",
        f"• Cartera USD     ${cartera:,.0f}",
        "",
        f"• Ahorro estimado al plazo: ${ahorro_acum_usd:,.0f} USD",
        f"• Cartera + ahorro:         ${disponible_usd:,.0f} USD",
        "",
        veredicto,
        "No toca FIMA como si fuera plata líquida del viaje; si querés usarla, decilo.",
    ]
    return "\n".join(lineas)

async def intentar_comando_local(update: Update, user_id: int, user_msg: str) -> bool:
    raw = (user_msg or "").strip()
    low = raw.lower().strip()
    if low.startswith("/"):
        low = low[1:]
        raw = raw[1:] if raw.startswith("/") else raw

    if parsear_pedido_proyeccion(raw):
        await update.message.reply_text(texto_analisis_proyeccion(user_id, raw))
        return True
    reg = parsear_registro_manual(raw, user_id=user_id, usar_gemini=False)
    if reg and not reg.get("necesita_ia"):
        guardar_movimiento(user_id, reg["tipo"], reg["monto"], reg["categoria"], reg["descripcion"], reg.get("fecha"))
        if reg.get("clave"):
            aprender_patron_clasificacion(user_id, raw, reg["categoria"], reg["descripcion"], reg.get("clave"))
        ftxt = f" ({reg['fecha']})" if reg.get("fecha") else ""
        await update.message.reply_text(
            f"✅ {reg['tipo'].title()} {reg['categoria']}: ${reg['monto']:,.2f} ARS — {reg['descripcion']}{ftxt}"
        )
        return True

    aprendido = buscar_comando_por_frase(user_id, raw)
    if aprendido:
        raw = aprendido
        low = aprendido.lower()

    pedido_at = extraer_pedido_analisis(raw)
    if pedido_at:
        tk_at, tf_at = pedido_at
        await update.message.reply_text(f"📈 Armando {tk_at} {tf_at}…")
        buf_img, info_at = generar_grafico_analisis_tecnico(tk_at, tf_at, "fibo" in low, "fibo" in low)
        if buf_img and info_at and not isinstance(info_at, str):
            await update.message.reply_photo(photo=buf_img, caption=f"📈 {tk_at} ({tf_at}) POC + Pivots + RSI")
            await update.message.reply_text(formatear_reporte_tecnico(info_at))
        else:
            await update.message.reply_text(f"No pude analizar {tk_at} en {tf_at}.")
        return True

    m_px = re.search(r"\b(?:precio|coti|cotizaci[oó]n|a cu[aá]nto (?:esta|está|cotiza))\s+(?:de\s+|el\s+|la\s+)?([a-z0-9]{1,8})\b", low)
    if m_px:
        tk_px = m_px.group(1).upper()
        if tk_px not in {"DE", "EL", "LA", "HOY", "MES"} and not tk_px.isdigit():
            raw = f"precio {tk_px}"
            low = raw.lower()

    partes = raw.split()
    cmd = partes[0].lower() if partes else ""
    args = " ".join(partes[1:]) if len(partes) > 1 else ""
    periodo = extraer_periodo(raw)

    if not re.match(r"presupuesto\s+\S+.+\d", low):
        intent = resolver_intencion(low)
        if intent:
            partes_i = intent.split()
            cmd = partes_i[0]
            if len(partes_i) > 1 and not args:
                args = " ".join(partes_i[1:])
            low = (intent + (" " + args if args else "")).strip()

    if cmd in ("help", "ayuda", "comandos"):
        await start(update, None)
        return True
    if (cmd == "vs" and "spy" not in low) or re.search(r"\b(ciclo anterior|contra el ciclo|vs el mes|compar(a|ame|ar) ciclos?)\b", low):
        await update.message.reply_text(texto_comparar_ciclos(user_id))
        return True
    if cmd in ("fijos", "fijo", "recurrentes") or re.search(r"\b(gastos? fijos?|servicios recurrentes)\b", low):
        await update.message.reply_text(texto_gastos_fijos(user_id))
        return True
    if cmd in ("delivery", "pedidosya", "pedidos") or re.search(r"\b(como va(n)? delivery|gastos? de delivery)\b", low):
        await update.message.reply_text(texto_alerta_delivery(user_id))
        return True
    m_tf = re.match(r"^([a-zA-Z0-9.-]{2,12})\s+(4h|4hs|diario|semanal|1w|1d)$", low)
    if m_tf:
        tk_at = m_tf.group(1).upper().replace("-USD", "")
        if tk_at not in {"MES", "VS", "OK", "NO", "SI", "SÍ"}:
            tf_at = "4h" if m_tf.group(2) in ("4h", "4hs") else ("semanal" if m_tf.group(2) in ("semanal", "1w") else "diario")
            await update.message.reply_text(f"📈 Armando {tk_at} {tf_at}…")
            buf_img, info_at = generar_grafico_analisis_tecnico(tk_at, tf_at, False, False)
            if buf_img and info_at and not isinstance(info_at, str):
                await update.message.reply_photo(photo=buf_img, caption=f"📈 {tk_at} ({tf_at}) POC + Pivots + RSI")
                await update.message.reply_text(formatear_reporte_tecnico(info_at))
            else:
                await update.message.reply_text(f"No pude analizar {tk_at} en {tf_at}.")
            return True

    if cmd in ("briefing", "premarket"):
        if not es_dia_habil_arg():
            await update.message.reply_text("El briefing automático es solo días hábiles. El lunes a las 08:30 ART.")
            return True
        await update.message.reply_text(texto_briefing_mananero(user_id))
        return True
    if re.search(r"desactivar\s+alertas|apagar\s+alertas|alertas\s+off", low) or cmd in ("silencio",):
        set_pref(user_id, "alertas", "off")
        await update.message.reply_text("🔕 Alertas desactivadas (bruscas, técnicas, briefing). Decí 'activar alertas' para prenderlas.")
        return True
    if re.search(r"activar\s+alertas|prender\s+alertas|alertas\s+on", low):
        set_pref(user_id, "alertas", "on")
        await update.message.reply_text("🔔 Alertas activadas. Briefing días hábiles 08:30 ART (día por medio).")
        return True
    m_pres = re.search(
        r"presupuesto\s+([a-záéíóúüñ ]+?)\s+\$?\s*(\d[\d\.]*)\s*(k|mil)?",
        low,
    )
    if m_pres:
        cat_p = normalizar_categoria(m_pres.group(1).strip())
        bruto = m_pres.group(2).replace(".", "").replace(",", "")
        mon_p = float(bruto)
        suf = (m_pres.group(3) or "").strip()
        if suf in ("k", "mil") or low.rstrip().endswith("k"):
            if mon_p < 10000:
                mon_p *= 1000
        set_presupuesto(user_id, cat_p, mon_p)
        await update.message.reply_text(f"✅ Presupuesto {cat_p}: ${mon_p:,.0f} ARS este ciclo.")
        return True
    if cmd in ("mensual", "meses", "mesames") or re.search(r"\b(rendimiento mensual|como me fue cada mes|mes a mes|tasa mensual)\b", low):
        anio_filtro = None
        m_anio = re.search(r"\b(202\d)\b", low)
        if m_anio:
            anio_filtro = int(m_anio.group(1))
        elif re.search(r"\b(este a[ñn]o|ytd|actual)\b", low):
            anio_filtro = datetime.now().year
            
        await update.message.reply_text(calcular_rendimiento_por_meses(user_id, anio_filtro))
        return True
    if cmd in ("mes", "presupuesto", "presupuestos"):
        await cmd_mes(update, None)
        return True
    if cmd in ("objetivos", "metas"):
        await cmd_objetivos(update, None)
        return True
    if cmd in ("riesgo", "risk"):
        await cmd_riesgo(update, None)
        return True
    if cmd in ("resumen", "cartera", "balance"):
        await cmd_resumen(update, None)
        return True
    if cmd in ("gastos", "finanzas", "hormiga", "promedios") or re.search(r"\b(gasto(s)? hormiga|promedio(s)? de gasto(s)?|radiograf[ií]a)\b", low):
        await update.message.reply_text(calcular_metricas_finanzas_completas(user_id))
        return True
    if cmd in ("ytd", "rendimiento"):
        await update.message.reply_text(calcular_rendimiento_periodo(user_id, periodo or "ytd"))
        return True
    if cmd in ("cagr", "alpha"):
        per = periodo or "ytd"
        await update.message.reply_text(calcular_rendimiento_periodo(user_id, per))
        return True
    if cmd in ("spy", "benchmark"):
        await enviar_grafico_cartera(update, user_id, periodo or "ytd", modo="pct")
        await update.message.reply_text(calcular_rendimiento_periodo(user_id, periodo or "ytd"))
        return True
    if cmd in ("grafico", "gráfico", "curva", "evolucion", "evolución"):
        if re.search(r"activo", low):
            tks = [x.strip().upper() for x in re.split(r"[\s,]+", args) if x.strip() and x.lower() not in ("ytd","mtd","1y","3m","6m","1m","todo")]
            tks = [x for x in tks if re.match(r"^[A-Z0-9]{1,12}$", x)]
            await enviar_grafico_activos(update, user_id, periodo, tks or None)
        elif re.search(r"spy|%|porcent", low):
            await enviar_grafico_cartera(update, user_id, periodo or "ytd", modo="pct")
        else:
            await enviar_grafico_cartera(update, user_id, periodo, modo="usd")
        return True
    if cmd in ("activos", "comparar"):
        tks = [x.strip().upper() for x in re.split(r"[\s,]+", args) if x.strip()]
        tks = [x for x in tks if re.match(r"^[A-Z0-9]{1,12}$", x) and x.lower() not in ("YTD", "TODO")]
        await enviar_grafico_activos(update, user_id, periodo, tks or None)
        return True
    if cmd in ("precio", "coti", "cotizacion", "cotización"):
        tk = args.split()[0].upper() if args else ""
        if not tk:
            await update.message.reply_text("Usá: /precio BTC")
            return True
        datos = consultar_datos_mercado(tk)
        if not datos:
            await update.message.reply_text(f"No encontré precio para {tk}.")
            return True
        signo = "+" if datos["var_pct"] >= 0 else ""
        em = "🟢" if datos["var_pct"] >= 0 else "🔴"
        await update.message.reply_text(
            f"📈 {datos['ticker']}\n• ${datos['precio']:,.2f}  {em} {signo}{datos['var_pct']:.2f}%"
        )
        return True
    if cmd == "excel":
        excel_buf = generar_excel_completo(user_id)
        if excel_buf:
            await update.message.reply_document(document=excel_buf, filename="Finanzas_Consolidadas.xlsx")
        else:
            await update.message.reply_text("No hay datos para exportar.")
        return True
    if cmd in ("torta", "pie"):
        if re.search(r"cartera|invers|usd|activos", low):
            buf = generar_grafico_distribucion_inversiones(user_id)
            if buf:
                await update.message.reply_photo(photo=buf, caption="Distribución de la cartera (USD)")
            else:
                await update.message.reply_text("No hay posiciones abiertas para armar la torta.")
        else:
            buf, etq = generar_grafico_torta_gastos(user_id)
            if buf:
                await update.message.reply_photo(photo=buf, caption=f"Torta de gastos de vida — {etq}")
            else:
                await update.message.reply_text("No hay gastos de vida en este ciclo para armar la torta.")
        return True
    if cmd in ("analisis", "análisis", "at", "tecnico", "técnico"):
        tk = ""
        tf_at = "diario"
        if "4h" in low:
            tf_at = "4h"
        elif "sem" in low:
            tf_at = "semanal"
        for tok in args.split():
            u = tok.upper().replace(",", "")
            if u in ("DIARIO", "SEMANAL", "4H", "FIBO", "FIBONACCI"):
                continue
            if re.match(r"^[A-Z0-9]{1,12}$", u):
                tk = u
                break
        if not tk:
            await update.message.reply_text("Usá: /analisis BTC  o  /analisis MELI semanal")
            return True
        con_fibo = "fibo" in low
        buf_img, info_at = generar_grafico_analisis_tecnico(tk, tf_at, con_fibo, con_fibo)
        if buf_img and info_at and not isinstance(info_at, str):
            await update.message.reply_photo(photo=buf_img, caption=f"📈 {tk} ({tf_at}) POC + Pivots + RSI")
            await update.message.reply_text(formatear_reporte_tecnico(info_at))
        else:
            await update.message.reply_text(f"No pude analizar {tk}.")
        return True

    m_an = re.search(
        r"\b(?:analiz\w*|an[aá]lisis|c[oó]mo ves)\s+([a-z0-9]{2,10})(?:\s+(?:en\s+)?)?(diario|semanal|4h|4hs)?\b",
        low,
    )
    if m_an:
        posible_tk = m_an.group(1).upper()
        palabras_comunes = {"HOLA", "BUENAS", "GRACIAS", "OK", "RESET", "AYUDA", "MES", "GASTOS", "RESUMEN", "CARTERA", "OBJETIVOS", "RIESGO", "FINANZAS", "HORMIGA", "MENSUAL", "EN", "DE"}
        if posible_tk not in palabras_comunes and not posible_tk.isdigit():
            tk = posible_tk
            tf_raw = (m_an.group(2) or "").lower()
            tf_at = "4h" if tf_raw in ("4h", "4hs") else ("semanal" if tf_raw == "semanal" else "diario")
            if "sem" in low:
                tf_at = "semanal"
            if "4h" in low:
                tf_at = "4h"
            con_fibo = "fibo" in low
            buf_img, info_at = generar_grafico_analisis_tecnico(tk, tf_at, con_fibo, con_fibo)
            if buf_img and info_at and not isinstance(info_at, str):
                await update.message.reply_photo(photo=buf_img, caption=f"📈 {tk} ({tf_at}) POC + Pivots + RSI")
                await update.message.reply_text(formatear_reporte_tecnico(info_at))
            else:
                await update.message.reply_text(f"No pude analizar {tk}.")
            return True

    if re.search(r"\b(ytd|year to date|este año|este ano)\b", low) and not re.search(r"registr|anot|guarde|gan[eé]|gast[eé]", low):
        if re.search(r"graf|curva|evoluc|vs|spy", low):
            await enviar_grafico_cartera(update, user_id, "ytd", modo="pct")
        await update.message.reply_text(calcular_rendimiento_periodo(user_id, "ytd"))
        return True
    if re.search(r"\b(cagr|alpha)\b", low):
        await update.message.reply_text(calcular_rendimiento_periodo(user_id, periodo or "ytd"))
        return True
    if re.search(r"(vs\s*spy|contra el spy|compar(a|ame|ar).*spy|benchmark)", low):
        await enviar_grafico_cartera(update, user_id, periodo or "ytd", modo="pct")
        await update.message.reply_text(calcular_rendimiento_periodo(user_id, periodo or "ytd"))
        return True
    if re.search(r"(grafico|gráfico|curva|evoluci[oó]n).*(cartera|consolidat|total)", low) or low in ("grafico", "gráfico", "curva cartera"):
        await enviar_grafico_cartera(update, user_id, periodo)
        return True
    if re.search(r"(grafico|gráfico|curva|evoluci[oó]n|compar).*(activo|activos|meli|nvda|ggal)", low):
        tks = re.findall(r"\b(MELI|NU|GGAL|SUPV|NVDA|BTC|SOL|ETH|MSFT|META|YPF|VIST|AAPL|AMD|TSLA|NEXO)\b", raw, re.I)
        await enviar_grafico_activos(update, user_id, periodo, [x.upper() for x in tks] or None)
        return True
    if re.search(r"^(c[oó]mo viene(n)? mi(s)? (cartera|posiciones)|estado de (la )?cartera)$", low):
        await cmd_resumen(update, None)
        return True

    return False

async def cmd_borrar_todo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM movimientos WHERE user_id = %s;", (user_id,))
            cursor.execute("DELETE FROM portafolio_inversiones WHERE user_id = %s;", (user_id,))
            cursor.execute("DELETE FROM trades_cerrados WHERE user_id = %s;", (user_id,))
            cursor.execute("DELETE FROM mapeo_conceptos WHERE user_id = %s;", (user_id,))
            conn.commit()
    await update.message.reply_text("🗑️ Tu base de datos, cartera, trades cerrados y patrones aprendidos han sido reseteados.")

async def cmd_resumen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    resumen = obtener_resumen_portafolio(user_id)
    
    with get_db_connection() as conn:
        df_tc = pd.read_sql("SELECT SUM(pnl_usd) as pnl_tot, COUNT(id) as total_c FROM trades_cerrados WHERE user_id = %s;", conn, params=(user_id,))
    pnl_realizado = float(df_tc['pnl_tot'].iloc[0]) if not df_tc.empty and pd.notnull(df_tc['pnl_tot'].iloc[0]) else 0.0
    cant_c = int(df_tc['total_c'].iloc[0]) if not df_tc.empty and pd.notnull(df_tc['total_c'].iloc[0]) else 0
    
    if not resumen and cant_c == 0:
        await update.message.reply_text("📉 No tienes activos ni trades cargados todavía.")
        return

    total_inv = resumen['total_invertido'] if resumen else 0.0
    total_act = resumen['total_actual'] if resumen else 0.0
    pnl_flot = resumen['pnl_total_usd'] if resumen else 0.0
    pnl_global = pnl_flot + pnl_realizado

    em_flot = "🟢" if pnl_flot >= 0 else "🔴"
    em_real = "🟢" if pnl_realizado >= 0 else "🔴"
    em_glob = "🟢" if pnl_global >= 0 else "🔴"

    msg = (
        f"💼 ESTADO DE TU CARTERA\n\n"
        f"• Capital abierto: ${total_inv:,.2f} USD\n"
        f"• Valor actual: ${total_act:,.2f} USD\n"
        f"• PnL flotante: {em_flot} {pnl_flot:+,.2f} USD\n"
        f"• PnL realizado: {em_real} {pnl_realizado:+,.2f} USD ({cant_c} ops)\n"
        f"• RESULTADO NETO GLOBAL: {em_glob} {pnl_global:+,.2f} USD\n"
    )

    if resumen and resumen["posiciones"]:
        msg += "\n📊 Posiciones abiertas:\n"
        for pos in resumen["posiciones"]:
            pnl_s = "+" if pos["pnl_usd"] >= 0 else ""
            em = "🟢" if pos["pnl_usd"] >= 0 else "🔴"
            lev_tag = f"[{pos['tipo_pos']} {pos['lev']:.0f}x]" if pos['tipo_pos'] != "SPOT" else "[SPOT]"
            dist = f" | Dist. liq: {pos['dist_liq_pct']:.1f}%" if pos.get("dist_liq_pct") is not None else ""
            msg += (
                f"\n▪️ ID {pos['id']}  {pos['ticker']} {lev_tag}\n"
                f"   Margen ${pos['costo_margen']:,.0f}  |  PPC ${pos['ppc']:,.2f}  |  Spot ${pos['spot']:,.2f}\n"
                f"   PnL {em} {pnl_s}${pos['pnl_usd']:,.2f} ({pnl_s}{pos['pnl_pct']:.1f}%){dist}"
            )
    await update.message.reply_text(msg)

async def cmd_riesgo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    metricas = calcular_metricas_riesgo_completas(user_id)
    await update.message.reply_text(metricas["texto"])

async def cmd_mes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    inicio, fin, etq = resolver_ciclo_havas(user_id)
    texto, err = obtener_progreso_presupuestos(user_id)
    if err:
        with get_db_connection() as conn:
            df = pd.read_sql(
                """SELECT categoria, SUM(monto) as total FROM movimientos 
                   WHERE user_id = %s AND tipo = 'GASTO' 
                   AND fecha >= %s AND fecha < %s
                   GROUP BY categoria ORDER BY total DESC;""",
                conn, params=(user_id, inicio.to_pydatetime(), fin.to_pydatetime())
            )
        if df.empty:
            await update.message.reply_text(f"No hay gastos registrados en el {etq} ni presupuestos cargados.")
        else:
            lineas = [f"📅 GASTOS {etq}", ""]
            total_vida = 0.0
            total_ahorro = 0.0
            lineas.append("🛒 Costo de vida")
            for _, r in df.iterrows():
                cat_clean = normalizar_categoria(r['categoria'])
                blob = cat_clean.lower()
                es_ah = any(t in blob for t in TAGS_AHORRO)
                if es_ah:
                    continue
                lineas.append(f"• {cat_clean}: ${float(r['total']):,.0f}")
                total_vida += float(r['total'])
            lineas.append(f"Subtotal vida: ${total_vida:,.0f} ARS")
            lineas.append("")
            lineas.append("💼 Ahorro / inversión")
            hay_ah = False
            for _, r in df.iterrows():
                cat_clean = normalizar_categoria(r['categoria'])
                blob = cat_clean.lower()
                if not any(t in blob for t in TAGS_AHORRO):
                    continue
                hay_ah = True
                lineas.append(f"• {cat_clean}: ${float(r['total']):,.0f}")
                total_ahorro += float(r['total'])
            if not hay_ah:
                lineas.append("• (sin movimientos de ahorro en el ciclo)")
            lineas.append(f"Subtotal ahorro: ${total_ahorro:,.0f} ARS")
            lineas.append(f"\nTotal salidas: ${total_vida + total_ahorro:,.0f} ARS")
            lineas.append("\n💡 Tip: definí presupuestos diciendo 'Presupuesto Comida 180000'")
            await update.message.reply_text("\n".join(lineas))
    else:
        await update.message.reply_text(f"{etq}\n\n{texto}")

async def cmd_objetivos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    texto = obtener_progreso_objetivos(user_id)
    await update.message.reply_text(texto)

async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = update.message.text

    if user_id in importaciones_activas:
        if await procesar_respuesta_importacion(update, user_id, user_msg):
            return

    try:
        if await intentar_comando_local(update, user_id, user_msg):
            return
    except Exception as e:
        logger.error(f"Error comando local: {e}", exc_info=True)

    tlow = (user_msg or "").strip().lower()
    if tlow in ("hola", "buenas", "ok", "oka", "gracias", "thx", "dale", "si", "sí"):
        await update.message.reply_text("Decime /gastos, /mes, /torta, /vs o /help.")
        return

    reg_directo = parsear_registro_manual(user_msg, user_id=user_id, usar_gemini=True)
    if reg_directo and not reg_directo.get("necesita_ia"):
        guardar_movimiento(user_id, reg_directo["tipo"], reg_directo["monto"], reg_directo["categoria"], reg_directo["descripcion"], reg_directo.get("fecha"))
        if reg_directo.get("clave"):
            aprender_patron_clasificacion(user_id, user_msg, reg_directo["categoria"], reg_directo["descripcion"], reg_directo.get("clave"))
        ftxt = f" ({reg_directo['fecha']})" if reg_directo.get("fecha") else ""
        extra = " · patrón guardado" if reg_directo.get("origen") == "gemini" else ""
        await update.message.reply_text(
            f"✅ {reg_directo['tipo'].title()} {reg_directo['categoria']}: ${reg_directo['monto']:,.2f} ARS — {reg_directo['descripcion']}{ftxt}{extra}"
        )
        return

    try:
        prompt = f'Texto del usuario:\n"{user_msg[:240]}"'
        reply = llamar_gemini(prompt, SYSTEM_INSTRUCTION) or ""
        cmd_para_ejecutar = extraer_comando_de_respuesta(reply)
        if cmd_para_ejecutar:
            guardar_frase_comando(user_id, user_msg, cmd_para_ejecutar)
            await intentar_comando_local(update, user_id, cmd_para_ejecutar)
            return

        pedido_at = extraer_pedido_analisis(user_msg)
        if pedido_at:
            await intentar_comando_local(update, user_id, user_msg)
            return

        # Si Gemini recortó REGISTRO_*, no muestres basura: Python reintenta.
        if re.search(r"regist", reply, re.I) and "REGISTRO_ARS:" not in reply and "REGISTRO_INV:" not in reply:
            reg = parsear_registro_manual(user_msg, user_id=user_id, usar_gemini=True)
            if reg and not reg.get("necesita_ia"):
                guardar_movimiento(user_id, reg["tipo"], reg["monto"], reg["categoria"], reg["descripcion"], reg.get("fecha"))
                if reg.get("clave"):
                    aprender_patron_clasificacion(user_id, user_msg, reg["categoria"], reg["descripcion"], reg.get("clave"))
                ftxt = f" ({reg['fecha']})" if reg.get("fecha") else ""
                await update.message.reply_text(
                    f"✅ {reg['tipo'].title()} {reg['categoria']}: ${reg['monto']:,.2f} ARS — {reg['descripcion']}{ftxt}"
                )
                return
            reply = "No pude registrar eso. Probá: 'Registra gasto Transporte SUBE 2125,4 el 29/9/2026'"

        m_cmd = re.search(r"COMANDO:\s*/?(\S+)(?:[^\S\r\n]+([^\r\n]+))?", reply)
        cmd_para_ejecutar = extraer_comando_de_respuesta(reply)
        if m_cmd and not cmd_para_ejecutar:
            cmd_name = m_cmd.group(1).strip()
            cmd_args = m_cmd.group(2).strip() if m_cmd.group(2) else ""
            cmd_para_ejecutar = sanitizar_comando_derivado(f"{cmd_name} {cmd_args}")
        if m_cmd:
            reply = reply.replace(m_cmd.group(0), "").strip()

        match_tc = re.search(r"REGISTRO_TRADE_CERRADO:\s*([^\n\r]+)", reply)
        if match_tc:
            linea_tc = match_tc.group(1).strip()
            reply = reply.replace(match_tc.group(0), "").strip()
            partes = [p.strip() for p in linea_tc.split("|")]
            tk_c = partes[0].upper()
            pnl_c = float(partes[1])
            tipo_c = partes[2].upper() if len(partes) > 2 and partes[2] not in ["", "None"] else "SPOT"
            roi_c = float(partes[3]) if len(partes) > 3 and partes[3] not in ["", "None"] else None
            monto_c = float(partes[4]) if len(partes) > 4 and partes[4] not in ["", "None"] else None
            desc_c = partes[5] if len(partes) > 5 else "Trade cerrado"
            f_c = partes[6].split()[0] if len(partes) > 6 and partes[6] not in ["", "None"] else None

            tid, t, p, tp, r, f = registrar_trade_cerrado(user_id, tk_c, pnl_c, tipo_c, roi_c, monto_c, desc_c, f_c)
            signo_p = "+" if p >= 0 else ""
            roi_s = f" ({r:+.2f}%)" if r else ""
            f_txt = f" — {f}" if f else ""
            reply = f"{reply}\n\n🏆 Trade cerrado registrado: {t} [{tp}] | PnL {signo_p}${p:,.2f} USD{roi_s}{f_txt} (ID {tid})".strip()

        match_inv = re.search(r"REGISTRO_INV:\s*([^\n\r]+)", reply)
        if match_inv:
            linea_inv = match_inv.group(1).strip()
            reply = reply.replace(match_inv.group(0), "").strip()
            partes = [p.strip() for p in linea_inv.split("|")]
            ticker = partes[0].upper()
            monto = float(partes[1]) if len(partes) > 1 and partes[1] not in ["0", "", "None"] else None
            p_compra = float(partes[2]) if len(partes) > 2 and partes[2] not in ["0", "", "None"] else None
            cant = float(partes[3]) if len(partes) > 3 and partes[3] not in ["0", "", "None"] else None
            f_compra = partes[4].split()[0] if len(partes) > 4 and partes[4] not in ["0", "", "None"] else None
            tipo_pos = partes[5].upper() if len(partes) > 5 and partes[5] not in ["", "None"] else "SPOT"
            lev = float(partes[6]) if len(partes) > 6 and partes[6] not in ["0", "", "None"] else 1.0
            p_liq = float(partes[7]) if len(partes) > 7 and partes[7] not in ["0", "", "None"] else None
            
            t, c, p, m, f_reg, pos_t, lev_t, liq_t = registrar_operacion_inversion(
                user_id, ticker, monto, p_compra, cant, f_compra, tipo_pos, lev, p_liq
            )
            fecha_str = f" — {f_reg}" if f_reg else ""
            lev_str = f" [{pos_t} {lev_t:.0f}x]" if pos_t != "SPOT" else " [SPOT]"
            liq_str = f" | Liq est: ${liq_t:,.2f}" if liq_t else ""
            reply = f"{reply}\n\n💼 Guardado como abierto: {t}{lev_str} | Margen ${m:,.2f} | PPC ${p:,.2f} | Cant {c:,.4f}{liq_str}{fecha_str}".strip()

        match_ars = re.search(r"REGISTRO_ARS:\s*([^\n\r]+)", reply)
        if match_ars:
            linea_ars = match_ars.group(1).strip()
            reply = reply.replace(match_ars.group(0), "").strip()
            partes = [p.strip() for p in linea_ars.split("|")]
            try:
                if len(partes) < 4:
                    raise ValueError("linea incompleta")
                tipo = partes[0]
                monto = float(partes[1].replace(",", "."))
                categoria = normalizar_categoria(partes[2])
                descripcion = partes[3]
                f_gasto = partes[4].split()[0] if len(partes) > 4 and partes[4] not in ["0", "", "None"] else None
                guardar_movimiento(user_id, tipo, monto, categoria, descripcion, f_gasto)
                aprender_patron_clasificacion(user_id, user_msg, categoria, descripcion)
                fecha_str = f" — {f_gasto}" if f_gasto else ""
                reply = f"{reply}\n\n✅ Guardado: {tipo} de ${monto:,.2f} ARS en {categoria} — {descripcion}{fecha_str}\n🧠 Patrón aprendido para la próxima.".strip()
            except Exception:
                reg = parsear_registro_manual(user_msg, user_id=user_id, usar_gemini=True)
                if reg and not reg.get("necesita_ia"):
                    guardar_movimiento(user_id, reg["tipo"], reg["monto"], reg["categoria"], reg["descripcion"], reg.get("fecha"))
                    if reg.get("clave"):
                        aprender_patron_clasificacion(user_id, user_msg, reg["categoria"], reg["descripcion"], reg.get("clave"))
                    reply = f"✅ {reg['tipo'].title()} {reg['categoria']}: ${reg['monto']:,.2f} ARS — {reg['descripcion']}"
                else:
                    reply = "No pude registrar el movimiento. Escribí: registra gasto CATEGORIA desc MONTO el DD/MM/AAAA"

        m_close_pos = re.search(r"ACCION: CERRAR_POSICION\|(\d+)(?:\|([^|\n\r]*))?(?:\|([^\n\r]*))?", reply)
        if m_close_pos:
            c_inv_id = int(m_close_pos.group(1))
            c_pnl = float(m_close_pos.group(2).strip()) if m_close_pos.group(2) and m_close_pos.group(2).strip() not in ["", "None"] else None
            c_spot = float(m_close_pos.group(3).strip()) if m_close_pos.group(3) and m_close_pos.group(3).strip() not in ["", "None"] else None
            reply = reply.replace(m_close_pos.group(0), "")
            
            res_cierre, msg_cierre = cerrar_posicion_abierta_por_id(user_id, c_inv_id, c_pnl, c_spot)
            if res_cierre:
                signo_pnl = "+" if res_cierre["pnl_usd"] >= 0 else ""
                roi_str = f" ({res_cierre['roi_pct']:+.2f}%)" if res_cierre['roi_pct'] else ""
                reply += f"\n\n🎯 Trade cerrado: {res_cierre['ticker']} [{res_cierre['tipo_pos']}] | PnL {signo_pnl}${res_cierre['pnl_usd']:,.2f} USD{roi_str} → ID histórico {res_cierre['tc_id']}"
            else:
                reply += f"\n\n⚠️ No se pudo cerrar: {msg_cierre}"

        texto_limpio = limpiar_estilo_telegram(reply)
        charla = bool(re.search(r"let's check|vamos a ver si|checking if", texto_limpio or "", re.I))
        if texto_limpio and not charla and not cmd_para_ejecutar and not re.match(r"^regist\b", texto_limpio.strip(), re.I):
            await update.message.reply_text(texto_limpio)

        if cmd_para_ejecutar:
            guardar_frase_comando(user_id, user_msg, cmd_para_ejecutar)
            await intentar_comando_local(update, user_id, cmd_para_ejecutar)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text(f"Hubo un error: {e}")

async def cmd_ytd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    per = extraer_periodo(update.message.text or "ytd") or "ytd"
    await update.message.reply_text(calcular_rendimiento_periodo(user_id, per))

async def cmd_cagr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    per = extraer_periodo(update.message.text or "ytd") or "ytd"
    await update.message.reply_text(calcular_rendimiento_periodo(user_id, per))

async def cmd_mensual_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, update.message.text or "/mensual")

async def cmd_spy_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, update.message.text or "/spy")

async def cmd_grafico_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, update.message.text or "/grafico")

async def cmd_activos_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, update.message.text or "/activos")

async def cmd_precio_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, update.message.text or "/precio")

async def cmd_excel_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, "/excel")

async def cmd_analisis_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, update.message.text or "/analisis")

async def cmd_gastos_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, "/gastos")

async def cmd_vs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(texto_comparar_ciclos(update.effective_user.id))

async def cmd_fijos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(texto_gastos_fijos(update.effective_user.id))

async def cmd_delivery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(texto_alerta_delivery(update.effective_user.id))

async def cmd_briefing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_dia_habil_arg():
        await update.message.reply_text("El briefing automático es solo días hábiles.")
        return
    await update.message.reply_text(texto_briefing_mananero(update.effective_user.id))

async def cmd_torta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await intentar_comando_local(update, update.effective_user.id, update.message.text or "/torta")

async def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", cmd_borrar_todo))
    app.add_handler(CommandHandler("resumen", cmd_resumen))
    app.add_handler(CommandHandler("riesgo", cmd_riesgo))
    app.add_handler(CommandHandler("mes", cmd_mes))
    app.add_handler(CommandHandler("mensual", cmd_mensual_alias))
    app.add_handler(CommandHandler("objetivos", cmd_objetivos))
    app.add_handler(CommandHandler("gastos", cmd_gastos_alias))
    app.add_handler(CommandHandler("finanzas", cmd_gastos_alias))
    app.add_handler(CommandHandler("hormiga", cmd_gastos_alias))
    app.add_handler(CommandHandler("vs", cmd_vs))
    app.add_handler(CommandHandler("fijos", cmd_fijos))
    app.add_handler(CommandHandler("delivery", cmd_delivery))
    app.add_handler(CommandHandler("briefing", cmd_briefing))
    app.add_handler(CommandHandler("torta", cmd_torta))
    app.add_handler(CommandHandler("pie", cmd_torta))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("ayuda", start))
    app.add_handler(CommandHandler("cartera", cmd_resumen))
    app.add_handler(CommandHandler("ytd", cmd_ytd))
    app.add_handler(CommandHandler("cagr", cmd_cagr))
    app.add_handler(CommandHandler("spy", cmd_spy_alias))
    app.add_handler(CommandHandler("grafico", cmd_grafico_alias))
    app.add_handler(CommandHandler("activos", cmd_activos_alias))
    app.add_handler(CommandHandler("precio", cmd_precio_alias))
    app.add_handler(CommandHandler("excel", cmd_excel_alias))
    app.add_handler(CommandHandler("analisis", cmd_analisis_alias))
    
    app.add_handler(MessageHandler(filters.Document.ALL, manejar_documento_extracto))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, responder))
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    
    asyncio.create_task(tarea_alertas_periodicas(app))
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
