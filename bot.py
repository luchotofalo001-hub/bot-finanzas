import os
import io
import re
import json
import asyncio
import logging
import threading
import hashlib
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import requests
from google import genai
import psycopg2
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ==================== LOGS ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")
LUCHO_USER_ID = 8429535344
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")

WHATSAPP_TOKEN = os.environ.get("WHATSAPP_TOKEN")
WHATSAPP_PHONE_ID = os.environ.get("WHATSAPP_PHONE_ID", "1330411630157040")
WHATSAPP_VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN", "secreto_finanzas_123")
MI_NUMERO_WHATSAPP = os.environ.get("MI_NUMERO_WHATSAPP")

ALERTA_HORA_INICIO = 7
ALERTA_HORA_FIN = 22
ALERTA_INTERVALO_HORAS = 3
UMBRAL_LIQUIDACION_PCT = 15.0
UMBRAL_GASTO_INUSUAL = 2.2
UMBRAL_GASTO_HORMIGA_ARS = 15000.0

ai_client = genai.Client(api_key=GEMINI_API_KEY)
loop_principal = None

# ==================== MOTOR DE ENVÍO WHATSAPP ROBUSTO ====================
def enviar_mensaje_whatsapp(to_number: str, texto: str):
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_ID or not to_number:
        return
    url = f"https://graph.facebook.com/v21.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "text",
        "text": {"body": texto}
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        if r.status_code >= 400:
            logger.error(f"Error WhatsApp send message: {r.text}")
    except Exception as e:
        logger.error(f"Error enviando mensaje a WhatsApp: {e}")

def subir_media_whatsapp(buf: io.BytesIO, filename: str = "grafico.png", mime_type: str = "image/png") -> str:
    url = f"https://graph.facebook.com/v21.0/{WHATSAPP_PHONE_ID}/media"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}
    buf.seek(0)
    files = {"file": (filename, buf.read(), mime_type)}
    data = {"messaging_product": "whatsapp", "type": mime_type}
    try:
        r = requests.post(url, headers=headers, files=files, data=data, timeout=25)
        if r.status_code == 200:
            return r.json().get("id")
        else:
            logger.error(f"Error subiendo media: {r.text}")
            return None
    except Exception as e:
        logger.error(f"Excepción subiendo media a WhatsApp: {e}")
        return None

def enviar_imagen_whatsapp(to_number: str, buf: io.BytesIO, caption: str = ""):
    media_id = subir_media_whatsapp(buf, "grafico.png", "image/png")
    if not media_id:
        if caption:
            enviar_mensaje_whatsapp(to_number, f"{caption}\n(No se pudo procesar la imagen)")
        return

    url = f"https://graph.facebook.com/v21.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "image",
        "image": {"id": media_id, "caption": caption}
    }
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
    except Exception as e:
        logger.error(f"Error enviando imagen a WhatsApp: {e}")

def enviar_documento_whatsapp(to_number: str, buf: io.BytesIO, filename: str = "archivo.xlsx", caption: str = ""):
    media_id = subir_media_whatsapp(buf, filename, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    if not media_id:
        enviar_mensaje_whatsapp(to_number, "No se pudo subir el archivo Excel a WhatsApp.")
        return

    url = f"https://graph.facebook.com/v21.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "document",
        "document": {"id": media_id, "caption": caption, "filename": filename}
    }
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
    except Exception as e:
        logger.error(f"Error enviando documento a WhatsApp: {e}")

# ==================== SERVIDOR WEB PARA RENDER Y META ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/webhook":
            query = urllib.parse.parse_qs(parsed.query)
            mode = query.get("hub.mode", [None])[0]
            token = query.get("hub.verify_token", [None])[0]
            challenge = query.get("hub.challenge", [None])[0]
            
            if mode == "subscribe" and token == WHATSAPP_VERIFY_TOKEN:
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(challenge.encode("utf-8"))
                logger.info("Webhook de WhatsApp verificado correctamente con Meta.")
                return
            else:
                self.send_response(403)
                self.end_headers()
                return

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot WhatsApp activo")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/webhook":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"EVENT_RECEIVED")
            
            try:
                data = json.loads(body)
                entries = data.get("entry", [])
                if entries:
                    changes = entries[0].get("changes", [])
                    if changes:
                        value = changes[0].get("value", {})
                        messages = value.get("messages", [])
                        if messages:
                            msg = messages[0]
                            if msg.get("type") == "text":
                                sender = msg.get("from")
                                texto = msg.get("text", {}).get("body", "")
                                if loop_principal and loop_principal.is_running():
                                    asyncio.run_coroutine_threadsafe(
                                        procesar_mensaje_whatsapp(sender, texto),
                                        loop_principal
                                    )
            except Exception as e:
                logger.error(f"Error procesando payload de WhatsApp: {e}")
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        pass

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info(f"Servidor HTTP escuchando en el puerto {port}")
    server.serve_forever()

# ==================== SUPABASE DB ====================
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

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

                cursor.execute("ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS tipo_posicion VARCHAR(10) DEFAULT 'SPOT';")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS apalancamiento NUMERIC DEFAULT 1;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS precio_liquidacion NUMERIC DEFAULT NULL;")
                cursor.execute("ALTER TABLE trades_cerrados ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE trades_cerrados ADD COLUMN IF NOT EXISTS fecha_apertura TIMESTAMP;")

                cursor.execute("UPDATE movimientos SET user_id = %s WHERE user_id IS NULL;", (LUCHO_USER_ID,))
                cursor.execute("UPDATE portafolio_inversiones SET user_id = %s WHERE user_id IS NULL;", (LUCHO_USER_ID,))
                cursor.execute("UPDATE trades_cerrados SET user_id = %s WHERE user_id IS NULL;", (LUCHO_USER_ID,))

                cursor.execute("UPDATE portafolio_inversiones SET tipo_posicion = 'SPOT' WHERE tipo_posicion IS NULL OR TRIM(tipo_posicion) = '';")
                cursor.execute("UPDATE portafolio_inversiones SET apalancamiento = 1 WHERE apalancamiento IS NULL OR apalancamiento <= 0;")
                cursor.execute("UPDATE portafolio_inversiones SET precio_compra = monto_total_usd / cantidad WHERE (precio_compra IS NULL OR precio_compra <= 0) AND cantidad > 0;")
                conn.commit()
        logger.info("Tablas inicializadas y normalizadas en Supabase.")
    except Exception as e:
        logger.error(f"Error en init_db: {e}")

init_db()

# ==================== UTILIDADES DE TIEMPO ====================
def ahora_argentina():
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=3)

def en_horario_alertas():
    h = ahora_argentina().hour
    return ALERTA_HORA_INICIO <= h < ALERTA_HORA_FIN

def limpiar_estilo_whatsapp(texto: str) -> str:
    if not texto:
        return ""
    texto = re.sub(r"#{2,6}\s*", "", texto)
    texto = re.sub(r"::\s*", ": ", texto)
    texto = re.sub(r"\n\s*---\s*\n", "\n\n", texto)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    return texto.strip()

# ==================== MERCADO Y COTIZACIONES ====================
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
            df_hist = t.history(period="5d")
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

    df_mov['fecha'] = pd.to_datetime(df_mov['fecha'])
    df_mov['mes_periodo'] = df_mov['fecha'].dt.to_period('M')
    
    df_gastos = df_mov[df_mov['tipo'] == 'GASTO'].copy()
    df_ingresos = df_mov[df_mov['tipo'] == 'INGRESO'].copy()

    if df_gastos.empty and df_ingresos.empty:
        return "No hay registros suficientes de ingresos o gastos."

    cant_meses_reales = max(1, df_mov['mes_periodo'].nunique())
    
    tot_gastos = float(df_gastos['monto'].sum()) if not df_gastos.empty else 0.0
    tot_ingresos = float(df_ingresos['monto'].sum()) if not df_ingresos.empty else 0.0
    
    prom_gasto_mensual = tot_gastos / cant_meses_reales
    prom_ingreso_mensual = tot_ingresos / cant_meses_reales
    superavit_mensual_prom = prom_ingreso_mensual - prom_gasto_mensual
    tasa_ahorro_pct = ((tot_ingresos - tot_gastos) / tot_ingresos * 100.0) if tot_ingresos > 0 else 0.0

    df_hormiga = df_gastos[df_gastos['monto'] <= UMBRAL_GASTO_HORMIGA_ARS]
    tot_hormiga = float(df_hormiga['monto'].sum()) if not df_hormiga.empty else 0.0
    prom_hormiga_mes = tot_hormiga / cant_meses_reales
    pct_hormiga_sobre_gastos = (tot_hormiga / tot_gastos * 100.0) if tot_gastos > 0 else 0.0
    compras_hormiga_por_mes = len(df_hormiga) / cant_meses_reales

    cat_totales = df_gastos.groupby('categoria')['monto'].sum().sort_values(ascending=False) if not df_gastos.empty else pd.Series()
    
    periodo_actual = pd.Period(ahora_argentina(), freq='M')
    df_mes_actual = df_gastos[df_gastos['mes_periodo'] == periodo_actual]
    gasto_mes_actual = float(df_mes_actual['monto'].sum()) if not df_mes_actual.empty else 0.0
    dia_del_mes = ahora_argentina().day
    dias_en_mes = 30
    proyeccion_mes_actual = (gasto_mes_actual / dia_del_mes * dias_en_mes) if dia_del_mes > 0 else gasto_mes_actual
    desvio_pct_vs_prom = ((proyeccion_mes_actual - prom_gasto_mensual) / prom_gasto_mensual * 100.0) if prom_gasto_mensual > 0 else 0.0

    resumen_cartera = obtener_resumen_portafolio(user_id)
    patrimonio_usd = float(resumen_cartera['total_actual']) if resumen_cartera else 0.0
    patrimonio_ars_aprox = patrimonio_usd * 1300.0
    runway_meses = (patrimonio_ars_aprox / prom_gasto_mensual) if prom_gasto_mensual > 0 else 0.0

    lineas = [
        f"📊 *RADIOGRAFÍA FINANCIERA* (Últimos {cant_meses_reales} meses)",
        "",
        "💵 *Flujo de Caja y Ahorro*",
        f"• Ingresos promedio: ${prom_ingreso_mensual:,.0f} ARS/mes",
        f"• Gastos promedio:   ${prom_gasto_mensual:,.0f} ARS/mes",
        f"• Superávit neto:    ${superavit_mensual_prom:+,.0f} ARS/mes",
        f"• Tasa de ahorro:    {tasa_ahorro_pct:.1f}% del ingreso"
    ]

    em_ahorro = "🟢 Excelente capacidad de capitalización" if tasa_ahorro_pct >= 30 else ("🟡 Ahorro moderado" if tasa_ahorro_pct >= 10 else "🔴 Margen de ahorro muy ajustado")
    lineas.append(f"• Diagnóstico: {em_ahorro}")

    lineas.append("")
    lineas.append("🐜 *Análisis de Gastos Hormiga (≤ $15.000 ARS)*")
    lineas.append(f"• Fuga total acumulada: ${tot_hormiga:,.0f} ARS ({len(df_hormiga)} compras)")
    lineas.append(f"• Impacto mensual:      ${prom_hormiga_mes:,.0f} ARS/mes ({pct_hormiga_sobre_gastos:.1f}% del total)")
    lineas.append(f"• Frecuencia:           ~{compras_hormiga_por_mes:.0f} micro-compras al mes")
    
    if not df_hormiga.empty:
        top_h_cat = df_hormiga.groupby('categoria')['monto'].sum().sort_values(ascending=False).head(3)
        top_str = ", ".join([f"{c} (${v:,.0f})" for c, v in top_h_cat.items()])
        lineas.append(f"• Principales focos:    {top_str}")

    lineas.append("")
    lineas.append("🏆 *Salidas Principales (Ley de Pareto)*")
    acum = 0.0
    for cat, val in cat_totales.head(4).items():
        pct = (val / tot_gastos * 100.0) if tot_gastos > 0 else 0.0
        prom_cat = val / cant_meses_reales
        lineas.append(f"• {cat:<14} → ${prom_cat:,.0f} ARS/mes ({pct:.1f}%)")
        acum += pct
    lineas.append(f"   (Estas categorías explican el {acum:.0f}% de tus gastos totales)")

    lineas.append("")
    lineas.append("⏱️ *Control del Mes en Curso y Runway*")
    signo_d = "+" if desvio_pct_vs_prom >= 0 else ""
    em_d = "🔴" if desvio_pct_vs_prom > 15 else ("🟢" if desvio_pct_vs_prom < -5 else "🟡")
    lineas.append(f"• Gastado este mes:     ${gasto_mes_actual:,.0f} ARS (Día {dia_del_mes})")
    lineas.append(f"• Ritmo proyectado:     {em_d} {signo_d}{desvio_pct_vs_prom:.1f}% vs promedio histórico")
    if runway_meses > 0:
        lineas.append(f"• Runway de respaldo:   {runway_meses:.1f} meses de gastos cubiertos con tu cartera")

    return "\n".join(lineas)

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
                h = yf.Ticker(sym).history(start=fecha_start)
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
            hspy = yf.Ticker("SPY").history(start=fecha_start, auto_adjust=True)
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
                h = yf.Ticker(sym).history(start=fecha_start)
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
        t = yf.Ticker(simbolo)
        df_hist = t.history(start=fecha_start)
        if df_hist.empty and not simbolo.endswith("-USD"):
            simbolo = f"{simbolo}-USD"
            t = yf.Ticker(simbolo)
            df_hist = t.history(start=fecha_start)

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
    
    if len(ult_highs) >= 2 and len(ult_lows) >= 2:
        if ult_highs[-1] > ult_highs[-2] and ult_lows[-1] > ult_lows[-2]:
            estructura_txt = "Estructura ALCISTA (Máximos y mínimos en subida)"
            estructura_bias = "alcista"
        elif ult_highs[-1] < ult_highs[-2] and ult_lows[-1] < ult_lows[-2]:
            estructura_txt = "Estructura BAJISTA (Máximos y mínimos en caída)"
            estructura_bias = "bajista"
        elif ult_highs[-1] > ult_highs[-2]:
            estructura_txt = "Se rompió un techo/máximo importante (Posible giro alcista)"
            estructura_bias = "choch_alcista"
        elif ult_lows[-1] < ult_lows[-2]:
            estructura_txt = "Se rompió un piso/mínimo importante (Posible giro bajista)"
            estructura_bias = "choch_bajista"

    fibo_niveles = {}
    if pivots_h and pivots_l:
        last_ph_idx, last_ph = pivots_h[-1]
        last_pl_idx, last_pl = pivots_l[-1]
        diff = abs(last_ph - last_pl)
        if diff > 0:
            if last_ph_idx > last_pl_idx:
                fibo_niveles["0.382"] = last_ph - 0.382 * diff
                fibo_niveles["0.500"] = last_ph - 0.500 * diff
                fibo_niveles["Golden Pocket 0.618"] = last_ph - 0.618 * diff
                fibo_niveles["Ext 1.618"] = last_ph + 0.618 * diff
            else:
                fibo_niveles["0.382"] = last_pl + 0.382 * diff
                fibo_niveles["0.500"] = last_pl + 0.500 * diff
                fibo_niveles["Golden Pocket 0.618"] = last_pl + 0.618 * diff
                fibo_niveles["Ext 1.618"] = last_pl - 0.618 * diff

    return {
        "res_inmediata": res_inmediata,
        "res_segunda": res_segunda,
        "sop_inmediato": sop_inmediato,
        "sop_segundo": sop_segundo,
        "estructura_txt": estructura_txt,
        "estructura_bias": estructura_bias,
        "fibo_niveles": fibo_niveles
    }

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

def backtest_comportamiento_historico(df, condicion="sobreventa_rsi"):
    if len(df) < 250:
        return None
    
    rsi = df['RSI'].values
    close = df['Close'].values
    n = len(close)

    eventos_idx = []

    if condicion == "sobreventa_rsi":
        for i in range(15, n - 15):
            if rsi[i] <= 40 and rsi[i - 1] > 40:
                eventos_idx.append(i)
        label = "RSI en zona baja / sobreventa (<= 40)"

    elif condicion == "sobrecompra_rsi":
        for i in range(15, n - 15):
            if rsi[i] >= 60 and rsi[i - 1] < 60:
                eventos_idx.append(i)
        label = "RSI en zona alta / sobrecompra (>= 60)"

    elif condicion == "cruce_alcista_ema":
        ema20 = df['EMA20'].values
        ema50 = df['EMA50'].values
        for i in range(15, n - 15):
            if ema20[i] > ema50[i] and ema20[i - 1] <= ema50[i - 1]:
                eventos_idx.append(i)
        label = "Cruce alcista (EMA20 > EMA50)"

    elif condicion == "cruce_bajista_ema":
        ema20 = df['EMA20'].values
        ema50 = df['EMA50'].values
        for i in range(15, n - 15):
            if ema20[i] < ema50[i] and ema20[i - 1] >= ema50[i - 1]:
                eventos_idx.append(i)
        label = "Presión bajista (EMA20 < EMA50)"
    else:
        return None

    if not eventos_idx:
        return None

    horizontes = [
        ("15 días", 11),
        ("1 mes", 21),
        ("3 meses", 63),
        ("6 meses", 126),
        ("1 año", 252)
    ]

    desglose = []
    for nombre_h, barras in horizontes:
        rets = []
        for idx in eventos_idx:
            if idx + barras < n:
                r = (close[idx + barras] / close[idx] - 1.0) * 100.0
                if np.isfinite(r):
                    rets.append(r)
        if rets:
            wr = (len([r for r in rets if r > 0]) / len(rets)) * 100.0
            avg = float(np.mean(rets))
            desglose.append({
                "horizonte": nombre_h,
                "win_rate": wr,
                "avg_ret": avg,
                "muestras": len(rets)
            })

    if not desglose:
        return None

    return {
        "condicion": label,
        "total_eventos": len(eventos_idx),
        "desglose": desglose
    }

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
    fibo = info.get('fibo_niveles', {})
    poc = info.get('poc')
    hist_stat = info.get('hist_stat')
    
    lineas = [f"📊 *REPORTE TÉCNICO: {tk} ({tf})*", f"• Precio actual: ${p:,.2f} USD", ""]

    lineas.append("📈 *Estructura de Mercado y Pivots*")
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
    lineas.append("🌊 *Medias Móviles*")
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
    lineas.append("⚡ *Momentum*")
    lineas.append(f"• RSI 14: {rsi:.1f} — {diag_rsi}")
    macd = info.get("macd")
    macd_hist = info.get("macd_hist")
    if macd is not None:
        cruce = "histograma verde (+)" if (macd_hist or 0) >= 0 else "histograma rojo (-)"
        lineas.append(f"• MACD: {cruce} (Hist: {macd_hist:+.4f})")

    if fibo:
        lineas.append("")
        lineas.append("🎯 *Fibonacci del Último Impulso*")
        if "Golden Pocket 0.618" in fibo and not np.isnan(fibo['Golden Pocket 0.618']):
            lineas.append(f"• Golden Pocket 0.618: ${fibo['Golden Pocket 0.618']:,.2f}")
        if "0.500" in fibo and not np.isnan(fibo['0.500']):
            lineas.append(f"• 50% Retroceso: ${fibo['0.500']:,.2f}")
        if "Ext 1.618" in fibo and not np.isnan(fibo['Ext 1.618']):
            lineas.append(f"• Objetivo Extensión 1.618: ${fibo['Ext 1.618']:,.2f}")

    if hist_stat:
        lineas.append("")
        lineas.append(f"🧠 *Comportamiento Histórico ante: {hist_stat['condicion']}*")
        lineas.append(f"• Eventos detectados en su historia: {hist_stat['total_eventos']}")
        lineas.append("• Desglose por horizonte temporal:")
        for item in hist_stat['desglose']:
            em = "🟢" if item['win_rate'] >= 60 else ("🟡" if item['win_rate'] >= 45 else "🔴")
            signo = "+" if item['avg_ret'] >= 0 else ""
            lineas.append(
                f"   {em} {item['horizonte']:<8} → WR: {item['win_rate']:>5.1f}% | Retorno prom: {signo}{item['avg_ret']:>6.2f}% ({item['muestras']} casos)"
            )

    lineas.append("")
    lineas.append("💡 *Conclusión Operativa*")
    bias = info.get("estructura_bias") or "lateral"
    if "DIVERGENCIA ALCISTA" in diag_rsi.upper() or rsi <= 32:
        lineas.append("• Probabilidad alta de rebote técnico. Buscar confirmación sobre EMA20.")
    elif "DIVERGENCIA BAJISTA" in diag_rsi.upper() or rsi >= 68:
        lineas.append("• Zona de agotamiento de compras. Riesgo alto de pullback a soporte o POC.")
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
        t = yf.Ticker(simbolo)
        df = t.history(period=periodo, interval=intervalo)
        if df.empty and not simbolo.endswith("-USD"):
            simbolo = f"{simbolo}-USD"
            t = yf.Ticker(simbolo)
            df = t.history(period=periodo, interval=intervalo)
            
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

        df.index = pd.to_datetime(df.index).tz_localize(None)
        df['Close'] = df['Close'].ffill()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        
        df['RSI'] = calcular_rsi_serie(df['Close'], period=14)
        df['MACD'], df['MACDs'], df['MACDh'] = calcular_macd(df['Close'])
        df['ATR'] = calcular_atr(df)

        niveles_dict = calcular_pivots_y_niveles(df, ventana=4)
        poc_price = calcular_poc_volumen(df, barras_lookback=90)
        
        hist_stat = None
        rsi_act = float(df['RSI'].dropna().iloc[-1]) if 'RSI' in df and not df['RSI'].dropna().empty else 50.0
        
        if rsi_act <= 40:
            hist_stat = backtest_comportamiento_historico(df, "sobreventa_rsi")
        elif rsi_act >= 60:
            hist_stat = backtest_comportamiento_historico(df, "sobrecompra_rsi")
        elif df['EMA20'].iloc[-1] > df['EMA50'].iloc[-1]:
            hist_stat = backtest_comportamiento_historico(df, "cruce_alcista_ema")
        else:
            hist_stat = backtest_comportamiento_historico(df, "cruce_bajista_ema")

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
        if (con_fibo or con_ext) and fibo_niveles:
            colores_fibo = {"0.382": "#ab47bc", "0.500": "#26a69a", "Golden Pocket 0.618": "#ffca28", "Ext 1.618": "#ff7043"}
            for k, v in fibo_niveles.items():
                if k in colores_fibo and not np.isnan(v):
                    ax1.axhline(v, color=colores_fibo[k], linestyle="--", linewidth=1.1, alpha=0.75, label=f"{k} (${v:,.2f})")

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

        diag_texto = [f"📌 *{tk}* (${precio:,.2f} USD)"]
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

    resumen_final = "🔍 *ESCÁNER DE CARTERA (Diario + Semanal)*\n\n" + "\n\n".join(diagnosticos)
    if not imagenes_senales:
        resumen_final += "\n\nℹ️ No se detectaron divergencias ni extremos de RSI críticos."
    else:
        resumen_final += f"\n\n📊 Se detectaron {len(imagenes_senales)} señales claras. Gráficos a continuación."

    return resumen_final, imagenes_senales

# ==================== OPERACIONES Y BASE DE DATOS ====================
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
            else:
                cursor.execute(f"UPDATE movimientos SET {col} = %s WHERE id = %s AND user_id = %s;", (val.capitalize(), mov_id, user_id))
            conn.commit()
            return prev, f"Campo {col} actualizado a {val}"

def guardar_movimiento(user_id: int, tipo: str, monto: float, categoria: str, descripcion: str, fecha_str: str = None):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_str:
                cursor.execute(
                    "INSERT INTO movimientos (user_id, fecha, tipo, monto, categoria, descripcion) VALUES (%s, %s, %s, %s, %s, %s);",
                    (user_id, fecha_str, tipo.upper(), float(monto), categoria.capitalize(), descripcion)
                )
            else:
                cursor.execute(
                    "INSERT INTO movimientos (user_id, fecha, tipo, monto, categoria, descripcion) VALUES (%s, NOW(), %s, %s, %s, %s);",
                    (user_id, tipo.upper(), float(monto), categoria.capitalize(), descripcion)
                )
            conn.commit()

def borrar_inversion_por_id(user_id: int, inv_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, ticker, monto_total_usd FROM portafolio_inversiones WHERE id = %s AND user_id = %s;", (inv_id, user_id))
            reg = cursor.fetchone()
            if reg:
                cursor.execute("DELETE FROM portafolio_inversiones WHERE id = %s AND user_id = %s;", (inv_id, user_id))
                conn.commit()
                return reg
            return None

def borrar_trade_cerrado_por_id(user_id: int, tc_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, ticker, pnl_usd FROM trades_cerrados WHERE id = %s AND user_id = %s;", (tc_id, user_id))
            reg = cursor.fetchone()
            if reg:
                cursor.execute("DELETE FROM trades_cerrados WHERE id = %s AND user_id = %s;", (tc_id, user_id))
                conn.commit()
                return reg
            return None

def borrar_movimiento_por_id(user_id: int, mov_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, tipo, monto, categoria, descripcion FROM movimientos WHERE id = %s AND user_id = %s;", (mov_id, user_id))
            reg = cursor.fetchone()
            if reg:
                cursor.execute("DELETE FROM movimientos WHERE id = %s AND user_id = %s;", (mov_id, user_id))
                conn.commit()
                return reg
            return None

def borrar_ultimo_registro_general(user_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 'MOV' as origen, id, fecha, tipo, monto FROM movimientos WHERE user_id = %s ORDER BY id DESC LIMIT 1;", (user_id,))
            ultimo_mov = cursor.fetchone()
            cursor.execute("SELECT 'INV' as origen, id, fecha, ticker, monto_total_usd FROM portafolio_inversiones WHERE user_id = %s ORDER BY id DESC LIMIT 1;", (user_id,))
            ultimo_inv = cursor.fetchone()
            cursor.execute("SELECT 'TC' as origen, id, fecha, ticker, pnl_usd FROM trades_cerrados WHERE user_id = %s ORDER BY id DESC LIMIT 1;", (user_id,))
            ultimo_tc = cursor.fetchone()
            
            candidatos = []
            if ultimo_mov: candidatos.append((ultimo_mov[2], 'MOV', ultimo_mov[1], f"Gasto/Ingreso {ultimo_mov[3]} ${ultimo_mov[4]:,.2f} ARS"))
            if ultimo_inv: candidatos.append((ultimo_inv[2], 'INV', ultimo_inv[1], f"Posición abierta {ultimo_inv[3]} ${ultimo_inv[4]:,.2f} USD"))
            if ultimo_tc: candidatos.append((ultimo_tc[2], 'TC', ultimo_tc[1], f"Trade cerrado {ultimo_tc[3]} PnL ${ultimo_tc[4]:+,.2f} USD"))

            if not candidatos:
                return None
            
            candidatos.sort(key=lambda x: x[0], reverse=True)
            sel = candidatos[0]
            if sel[1] == 'MOV':
                cursor.execute("DELETE FROM movimientos WHERE id = %s AND user_id = %s;", (sel[2], user_id))
            elif sel[1] == 'INV':
                cursor.execute("DELETE FROM portafolio_inversiones WHERE id = %s AND user_id = %s;", (sel[2], user_id))
            else:
                cursor.execute("DELETE FROM trades_cerrados WHERE id = %s AND user_id = %s;", (sel[2], user_id))
            conn.commit()
            return sel[3]

def borrar_todos_los_movimientos(user_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM movimientos WHERE user_id = %s;", (user_id,))
            cursor.execute("DELETE FROM portafolio_inversiones WHERE user_id = %s;", (user_id,))
            cursor.execute("DELETE FROM trades_cerrados WHERE user_id = %s;", (user_id,))
            conn.commit()

def set_presupuesto(user_id: int, categoria: str, monto_limite: float, mes: int = None, anio: int = None):
    ahora = ahora_argentina()
    mes = mes or ahora.month
    anio = anio or ahora.year
    categoria = categoria.strip().capitalize()
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

# ==================== GRÁFICOS DE TORTA (INVERSIONES Y GASTOS) ====================
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

def generar_grafico_gastos(user_id: int):
    with get_db_connection() as conn:
        df = pd.read_sql("SELECT monto, categoria FROM movimientos WHERE user_id = %s AND tipo = 'GASTO';", conn, params=(user_id,))
    if df.empty:
        return None
    gastos_por_cat = df.groupby('categoria')['monto'].sum().sort_values(ascending=False)
    plt.figure(figsize=(8, 6))
    colores = ['#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f', '#edc948', '#b07aa1']
    plt.pie(
        gastos_por_cat, 
        labels=gastos_por_cat.index, 
        autopct='%1.1f%%', 
        startangle=140, 
        colors=colores[:len(gastos_por_cat)],
        wedgeprops=dict(width=0.6, edgecolor='w')
    )
    plt.title('Distribución de Gastos por Categoría (ARS)', fontsize=14, pad=20)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=200)
    buf.seek(0)
    plt.close()
    return buf

# ==================== SISTEMA DE ALERTAS ROBUSTO ====================
def _hash_alerta(tipo: str, clave: str, detalle: str = "") -> str:
    raw = f"{tipo}|{clave}|{detalle}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def alerta_ya_enviada(user_id: int, hash_a: str, horas_ventana: int = 12) -> bool:
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
    resumen = obtener_resumen_portafolio(user_id)
    if resumen:
        for p in resumen["posiciones"]:
            dist = p.get("dist_liq_pct")
            if dist is not None and dist < UMBRAL_LIQUIDACION_PCT:
                clave = f"{p['ticker']}_{p['id']}"
                h = _hash_alerta("liquidacion", clave, "")
                if not alerta_ya_enviada(user_id, h, horas_ventana=12):
                    alertas.append({
                        "texto": f"⚠️ *LIQUIDACIÓN CERCANA*\n{p['ticker']} [{p['tipo_pos']} {p['lev']:.0f}x]\nDistancia actual: {dist:.1f}%\nPrecio liq: ${p['precio_liq']:,.2f} | Spot: ${p['spot']:,.2f}",
                        "tipo": "liquidacion",
                        "clave": clave,
                        "hash": h
                    })

    if resumen:
        tickers = list({p['ticker'] for p in resumen["posiciones"] if p['ticker'] not in ["USDT", "USDC", "DAI", "USD"]})
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
                        if not alerta_ya_enviada(user_id, h, horas_ventana=24):
                            alertas.append({
                                "texto": f"🎯 *TESTEO DE POC INSTITUCIONAL*\n{tk} está testeando su POC de volumen en ${poc:,.2f} (Precio: ${p_act:,.2f}). Zona de alta reacción.",
                                "tipo": "poc_test",
                                "clave": clave,
                                "hash": h
                            })

                    if bias == "choch_alcista":
                        clave = f"{tk}_choch_up"
                        h = _hash_alerta("choch", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=24):
                            alertas.append({
                                "texto": f"🟢 *CAMBIO DE ESTRUCTURA*\n{tk} rompió un techo/máximo importante en ${p_act:,.2f}. Es posible un cambio a tendencia alcista.",
                                "tipo": "choch",
                                "clave": clave,
                                "hash": h
                            })
                    elif bias == "choch_bajista":
                        clave = f"{tk}_choch_down"
                        h = _hash_alerta("choch", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=24):
                            alertas.append({
                                "texto": f"🔴 *CAMBIO DE ESTRUCTURA*\n{tk} rompió un piso/mínimo importante en ${p_act:,.2f}. Es posible un cambio a tendencia bajista.",
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
                        if not alerta_ya_enviada(user_id, h, horas_ventana=18):
                            extra_stat = ""
                            if hist and hist.get('desglose'):
                                d = {x['horizonte']: x for x in hist['desglose']}
                                h15 = d.get('15 días')
                                h3m = d.get('3 meses')
                                h1y = d.get('1 año')
                                lineas_stat = []
                                if h15: lineas_stat.append(f"15d: {h15['win_rate']:.0f}% WR ({h15['avg_ret']:+.1f}%)")
                                if h3m: lineas_stat.append(f"3m: {h3m['win_rate']:.0f}% WR ({h3m['avg_ret']:+.1f}%)")
                                if h1y: lineas_stat.append(f"1a: {h1y['win_rate']:.0f}% WR ({h1y['avg_ret']:+.1f}%)")
                                extra_stat = "\n📊 Histórico: " + " | ".join(lineas_stat)

                            alertas.append({
                                "texto": f"📡 *SEÑAL TÉCNICA (Diario)*\n{tk} — RSI {rsi:.1f}\n{diag}{extra_stat}",
                                "tipo": "rsi_senal",
                                "clave": clave,
                                "hash": h
                            })
            except Exception:
                pass

    try:
        with get_db_connection() as conn:
            df_hoy = pd.read_sql("SELECT SUM(monto) as total FROM movimientos WHERE user_id = %s AND tipo = 'GASTO' AND fecha::date = CURRENT_DATE;", conn, params=(user_id,))
            df_prom = pd.read_sql("""SELECT AVG(diario) as promedio FROM (SELECT fecha::date as d, SUM(monto) as diario FROM movimientos WHERE user_id = %s AND tipo = 'GASTO' AND fecha > NOW() - INTERVAL '30 days' GROUP BY fecha::date) t;""", conn, params=(user_id,))
        total_hoy = float(df_hoy['total'].iloc[0]) if not df_hoy.empty and pd.notnull(df_hoy['total'].iloc[0]) else 0.0
        prom = float(df_prom['promedio'].iloc[0]) if not df_prom.empty and pd.notnull(df_prom['promedio'].iloc[0]) else 0.0
        if prom > 0 and total_hoy > prom * UMBRAL_GASTO_INUSUAL:
            clave = f"gasto_{ahora_argentina().strftime('%Y%m%d')}"
            h = _hash_alerta("gasto_inusual", clave, "")
            if not alerta_ya_enviada(user_id, h, horas_ventana=20):
                alertas.append({
                    "texto": f"💸 *GASTO INUSUAL HOY*\nGastaste ${total_hoy:,.0f} ARS\nPromedio diario (30d): ${prom:,.0f} ARS\n({total_hoy/prom:.1f}x el promedio)",
                    "tipo": "gasto_inusual",
                    "clave": clave,
                    "hash": h
                })
    except Exception as e:
        logger.error(f"Error alerta gasto: {e}")

    return alertas

async def tarea_alertas_whatsapp():
    await asyncio.sleep(60)
    while True:
        try:
            if en_horario_alertas() and MI_NUMERO_WHATSAPP:
                alertas = await asyncio.to_thread(generar_alertas_para_usuario, LUCHO_USER_ID)
                if alertas:
                    mensajes = ["🔔 *ALERTAS DE TU CARTERA*\n"]
                    for a in alertas:
                        mensajes.append(a["texto"])
                        mensajes.append("")
                        await asyncio.to_thread(registrar_alerta_enviada, LUCHO_USER_ID, a["tipo"], a["clave"], a["hash"])
                    texto_final = "\n".join(mensajes).strip()
                    enviar_mensaje_whatsapp(MI_NUMERO_WHATSAPP, texto_final)
        except Exception as e:
            logger.error(f"Error en tarea de alertas WhatsApp: {e}")
        await asyncio.sleep(ALERTA_INTERVALO_HORAS * 3600)

# ==================== ENRUTADOR PRINCIPAL ====================
async def procesar_mensaje_whatsapp(sender_phone: str, user_msg: str):
    user_id = LUCHO_USER_ID

    # 1. Filtro local ultra rápido (0 tokens)
    try:
        if await intentar_comando_local_whatsapp(sender_phone, user_id, user_msg):
            return
    except Exception as e:
        logger.error(f"Error comando local WhatsApp: {e}", exc_info=True)

    # 2. Copiloto Cognitivo con Gemini
    try:
        contexto = resumen_compacto_para_ia(user_id)
        prompt = f"""CONTEXTO DEL USUARIO:
{contexto}

MENSAJE DEL USUARIO:
"{user_msg}"

        reply = llamar_gemini(prompt, SYSTEM_INSTRUCTION)

        m_cmd = re.search(r"COMANDO:\s*(\S+)(?:[^\S\r\n]+([^\r\n]+))?", reply)
        cmd_para_ejecutar = None
        if m_cmd:
            cmd_name = m_cmd.group(1).strip()
            cmd_args = m_cmd.group(2).strip() if m_cmd.group(2) else ""
            cmd_para_ejecutar = f"{cmd_name} {cmd_args}".strip()
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
            reply = f"{reply}\n\n🏆 *Trade cerrado registrado*: {t} [{tp}] | PnL {signo_p}${p:,.2f} USD{roi_s}{f_txt} (ID {tid})".strip()

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
            reply = f"{reply}\n\n💼 *Guardado como abierto*: {t}{lev_str} | Margen ${m:,.2f} | PPC ${p:,.2f} | Cant {c:,.4f}{liq_str}{fecha_str}".strip()

        match_ars = re.search(r"REGISTRO_ARS:\s*([^\n\r]+)", reply)
        if match_ars:
            linea_ars = match_ars.group(1).strip()
            reply = reply.replace(match_ars.group(0), "").strip()
            partes = [p.strip() for p in linea_ars.split("|")]
            tipo = partes[0]
            monto = float(partes[1])
            categoria = partes[2]
            descripcion = partes[3]
            f_gasto = partes[4].split()[0] if len(partes) > 4 and partes[4] not in ["0", "", "None"] else None
            
            guardar_movimiento(user_id, tipo, monto, categoria, descripcion, f_gasto)
            fecha_str = f" — {f_gasto}" if f_gasto else ""
            reply = f"{reply}\n\n✅ *Guardado*: {tipo} de ${monto:,.2f} ARS en {categoria}{fecha_str}".strip()

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
                reply += f"\n\n🎯 *Trade cerrado*: {res_cierre['ticker']} [{res_cierre['tipo_pos']}] | PnL {signo_pnl}${res_cierre['pnl_usd']:,.2f} USD{roi_str} → ID {res_cierre['tc_id']}"
            else:
                reply += f"\n\n⚠️ No se pudo cerrar: {msg_cierre}"

        texto_limpio = limpiar_estilo_whatsapp(reply)
        if texto_limpio:
            enviar_mensaje_whatsapp(sender_phone, texto_limpio)

        if cmd_para_ejecutar:
            await intentar_comando_local_whatsapp(sender_phone, user_id, cmd_para_ejecutar)

    except Exception as e:
        logger.error(f"Error procesando solicitud WhatsApp: {e}", exc_info=True)
        enviar_mensaje_whatsapp(sender_phone, f"⚠️ Error: {e}")

# ==================== MAIN ====================
async def main():
    global loop_principal
    loop_principal = asyncio.get_running_loop()
    
    threading.Thread(target=run_web_server, daemon=True).start()
    logger.info("Servidor web iniciado en Render.")

    asyncio.create_task(tarea_alertas_whatsapp())

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
