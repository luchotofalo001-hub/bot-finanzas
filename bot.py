import os
import io
import re
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

# Horario de alertas (hora local Argentina ≈ UTC-3)
ALERTA_HORA_INICIO = 7
ALERTA_HORA_FIN = 22
ALERTA_INTERVALO_HORAS = 3
# Umbral de distancia a liquidación para alerta (%)
UMBRAL_LIQUIDACION_PCT = 15.0
# Umbral de gasto inusual (múltiplo del promedio)
UMBRAL_GASTO_INUSUAL = 2.2

ai_client = genai.Client(api_key=GEMINI_API_KEY)

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

                cursor.execute("UPDATE movimientos SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))
                cursor.execute("UPDATE portafolio_inversiones SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))
                cursor.execute("UPDATE trades_cerrados SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))

                cursor.execute("UPDATE portafolio_inversiones SET tipo_posicion = 'SPOT' WHERE tipo_posicion IS NULL OR TRIM(tipo_posicion) = '';")
                cursor.execute("UPDATE portafolio_inversiones SET apalancamiento = 1 WHERE apalancamiento IS NULL OR apalancamiento <= 0;")
                cursor.execute("UPDATE portafolio_inversiones SET precio_compra = monto_total_usd / cantidad WHERE (precio_compra IS NULL OR precio_compra <= 0) AND cantidad > 0;")
                conn.commit()
        logger.info("Tablas inicializadas y normalizadas.")
    except Exception as e:
        logger.error(f"Error en init_db: {e}")

init_db()

# ==================== UTILIDADES DE TIEMPO (Argentina UTC-3) ====================
def ahora_argentina():
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=3)

def en_horario_alertas():
    h = ahora_argentina().hour
    return ALERTA_HORA_INICIO <= h < ALERTA_HORA_FIN

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
            df_hist = t.history(period="5d")
            if df_hist is not None and not df_hist.empty:
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

                if last_price:
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

# ==================== GRÁFICO CONSOLIDADO: EVOLUCIÓN REAL Y CONTINUA ====================
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
                    s = h['Close']
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

        # 1. PnL de trades cerrados prorrateado de forma continua en su vida útil
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

        # 2. Posiciones abiertas actuales (PnL flotante en tiempo real)
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

        # Normalización: el rendimiento del período específico siempre inicia en $0 / 0.0%
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
                    s = h['Close']
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
        t = yf.Ticker(simbolo)
        df_hist = t.history(start=fecha_start)
        if df_hist.empty and not simbolo.endswith("-USD"):
            simbolo = f"{simbolo}-USD"
            t = yf.Ticker(simbolo)
            df_hist = t.history(start=fecha_start)

        if df_hist.empty:
            return None

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

# ==================== MOTOR CUANTITATIVO AVANZADO (SIN IA) ====================
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
    """Encuentra soportes y resistencias reales por fractales y detecta quiebres estructurales."""
    highs = df['High'].values
    lows = df['Low'].values
    precio_actual = float(df['Close'].iloc[-1])
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
    """Calcula el Point of Control (POC) de volumen."""
    if "Volume" not in df.columns or df["Volume"].sum() == 0:
        return None
    sub = df.iloc[-barras_lookback:] if len(df) >= barras_lookback else df
    min_p = sub["Low"].min()
    max_p = sub["High"].max()
    if min_p == max_p:
        return None

    bins = np.linspace(min_p, max_p, bins_count + 1)
    vol_per_bin = np.zeros(bins_count)
    typical_price = (sub["High"] + sub["Low"] + sub["Close"]) / 3.0
    vol = sub["Volume"].values

    for tp, v in zip(typical_price.values, vol):
        idx = np.digitize(tp, bins) - 1
        if 0 <= idx < bins_count:
            vol_per_bin[idx] += v

    poc_idx = np.argmax(vol_per_bin)
    poc_price = (bins[poc_idx] + bins[poc_idx + 1]) / 2.0
    return float(poc_price)

def backtest_comportamiento_historico(df, condicion="sobreventa_rsi"):
    """Analiza estadísticamente cómo reaccionó este activo en su historia a 15 días, 1 mes, 3 meses, 6 meses y 1 año."""
    if len(df) < 260:
        return None
    
    rsi = df['RSI'].values
    close = df['Close'].values
    n = len(close)

    eventos_idx = []

    if condicion == "sobreventa_rsi":
        for i in range(15, n - 15):
            if rsi[i] <= 35 and rsi[i - 1] > 35:
                eventos_idx.append(i)
        label = "RSI en sobreventa (<= 35)"

    elif condicion == "sobrecompra_rsi":
        for i in range(15, n - 15):
            if rsi[i] >= 68 and rsi[i - 1] < 68:
                eventos_idx.append(i)
        label = "RSI en sobrecompra (>= 68)"

    elif condicion == "cruce_alcista_ema":
        ema20 = df['EMA20'].values
        ema50 = df['EMA50'].values
        for i in range(15, n - 15):
            if ema20[i] > ema50[i] and ema20[i - 1] <= ema50[i - 1]:
                eventos_idx.append(i)
        label = "Cruce alcista (EMA20 > EMA50)"
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
    
    lineas = [f"📊 REPORTE TÉCNICO: {tk} ({tf})", f"• Precio actual: ${p:,.2f} USD", ""]

    lineas.append("📈 Estructura de Mercado y Pivots")
    est = info.get("estructura_txt") or "En desarrollo"
    lineas.append(f"• Estructura: {est}")
    
    sop1 = info.get("sop_inmediato")
    sop2 = info.get("sop_segundo")
    res1 = info.get("res_inmediata")
    res2 = info.get("res_segunda")
    
    if sop1:
        dist_s = ((p - sop1) / p) * 100.0
        lineas.append(f"• Soporte clave: ${sop1:,.2f} (-{dist_s:.1f}%)" + (f" | S2: ${sop2:,.2f}" if sop2 else ""))
    if res1:
        dist_r = ((res1 - p) / p) * 100.0
        lineas.append(f"• Resistencia clave: ${res1:,.2f} (+{dist_r:.1f}%)" + (f" | R2: ${res2:,.2f}" if res2 else ""))

    if poc:
        dist_poc = ((p - poc) / p) * 100.0
        lado_poc = "soporte de volumen institucional" if p >= poc else "resistencia magnética de volumen"
        signo = "+" if dist_poc >= 0 else ""
        lineas.append(f"• POC (Mayor volumen): ${poc:,.2f} ({signo}{dist_poc:.1f}% → {lado_poc})")

    lineas.append("")
    lineas.append("🌊 Medias Móviles")
    if p > e20 and e20 > e50:
        lineas.append(f"• Sesgo dinámico: Alcista sólido (Precio > EMA20 ${e20:,.2f} > EMA50 ${e50:,.2f})")
    elif p < e20 and e20 < e50:
        lineas.append(f"• Sesgo dinámico: Bajista bajo presión (Precio < EMA20 ${e20:,.2f} < EMA50 ${e50:,.2f})")
    else:
        lineas.append(f"• Sesgo dinámico: Mixto / compresión (EMA20 ${e20:,.2f} | EMA50 ${e50:,.2f})")
    if e200:
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

    if fibo:
        lineas.append("")
        lineas.append("🎯 Fibonacci del Último Impulso")
        if "Golden Pocket 0.618" in fibo:
            lineas.append(f"• Golden Pocket 0.618: ${fibo['Golden Pocket 0.618']:,.2f}")
        if "0.500" in fibo:
            lineas.append(f"• 50% Retroceso: ${fibo['0.500']:,.2f}")
        if "Ext 1.618" in fibo:
            lineas.append(f"• Objetivo Extensión 1.618: ${fibo['Ext 1.618']:,.2f}")

    if hist_stat:
        lineas.append("")
        lineas.append(f"🧠 Comportamiento Histórico ante: {hist_stat['condicion']}")
        lineas.append(f"• Eventos detectados en su historia: {hist_stat['total_eventos']}")
        lineas.append("• Desglose por horizonte temporal:")
        for item in hist_stat['desglose']:
            em = "🟢" if item['win_rate'] >= 60 else ("🟡" if item['win_rate'] >= 45 else "🔴")
            signo = "+" if item['avg_ret'] >= 0 else ""
            lineas.append(
                f"   {em} {item['horizonte']:<8} → WR: {item['win_rate']:>5.1f}% | Retorno prom: {signo}{item['avg_ret']:>6.2f}% ({item['muestras']} casos)"
            )

    lineas.append("")
    lineas.append("💡 Conclusión Operativa")
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
        periodo = "3y"
        intervalo = "1wk"
        tf_label = "Semanal"
    else:
        periodo = "1y"
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

        if tf_label == "4 Horas":
            df = df.resample('4h').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum'
            }).dropna()

        df.index = pd.to_datetime(df.index).tz_localize(None)
        
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        
        df['RSI'] = calcular_rsi_serie(df['Close'], period=14)
        df['MACD'], df['MACDs'], df['MACDh'] = calcular_macd(df['Close'])
        df['ATR'] = calcular_atr(df)

        niveles_dict = calcular_pivots_y_niveles(df, ventana=4)
        poc_price = calcular_poc_volumen(df, barras_lookback=90)
        
        hist_stat = None
        rsi_act = float(df['RSI'].iloc[-1])
        if rsi_act <= 36:
            hist_stat = backtest_comportamiento_historico(df, "sobreventa_rsi")
        elif rsi_act >= 65:
            hist_stat = backtest_comportamiento_historico(df, "sobrecompra_rsi")
        elif df['EMA20'].iloc[-1] > df['EMA50'].iloc[-1]:
            hist_stat = backtest_comportamiento_historico(df, "cruce_alcista_ema")

        vol_txt = None
        if "Volume" in df.columns and df["Volume"].fillna(0).sum() > 0:
            v_now = float(df["Volume"].iloc[-1])
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

        fig, (ax1, axm, ax2) = plt.subplots(3, 1, figsize=(11, 8.2), gridspec_kw={'height_ratios': [3.0, 1.0, 1.1]}, sharex=True)
        
        ax1.plot(df.index, df['Close'], label="Precio", color="#ffffff", linewidth=1.5, alpha=0.9)
        ax1.plot(df.index, df['EMA20'], label="EMA 20", color="#29b6f6", linewidth=1.4)
        ax1.plot(df.index, df['EMA50'], label="EMA 50", color="#ffa726", linewidth=1.4)
        if len(df) >= 150:
            ax1.plot(df.index, df['EMA200'], label="EMA 200", color="#ef5350", linewidth=1.8)

        if poc_price:
            ax1.axhline(poc_price, color="#00e676", linestyle="-.", linewidth=1.3, alpha=0.9, label=f"POC Vol (${poc_price:,.2f})")
            
        if niveles_dict["sop_inmediato"]:
            ax1.axhline(niveles_dict["sop_inmediato"], color="#29b6f6", linestyle=":", linewidth=1.2, label=f"Soporte (${niveles_dict['sop_inmediato']:,.2f})")
            
        if niveles_dict["res_inmediata"]:
            ax1.axhline(niveles_dict["res_inmediata"], color="#ff5252", linestyle=":", linewidth=1.2, label=f"Resistencia (${niveles_dict['res_inmediata']:,.2f})")

        fibo_niveles = niveles_dict["fibo_niveles"]
        if (con_fibo or con_ext) and fibo_niveles:
            colores_fibo = {"0.382": "#ab47bc", "0.500": "#26a69a", "Golden Pocket 0.618": "#ffca28", "Ext 1.618": "#ff7043"}
            for k, v in fibo_niveles.items():
                if k in colores_fibo:
                    ax1.axhline(v, color=colores_fibo[k], linestyle="--", linewidth=1.1, alpha=0.75, label=f"{k} (${v:,.2f})")

        ax1.set_facecolor("#131722")
        fig.patch.set_facecolor("#131722")
        ax1.grid(True, linestyle="--", alpha=0.15, color="#787b86")
        ax1.set_title(f"{ticker} | Analisis Tecnico Cuantitativo ({tf_label})\nEMA 20/50/200 + POC + Pivots + RSI", color="#ffffff", fontsize=12, fontweight='bold', pad=10)
        ax1.tick_params(colors="#787b86")
        ax1.legend(loc="upper left", facecolor="#1e222d", edgecolor="#2a2e39", labelcolor="#d1d4dc", fontsize=8)

        axm.set_facecolor("#131722")
        hist_colors = np.where(df["MACDh"] >= 0, "#26a69a", "#ef5350")
        axm.bar(df.index, df["MACDh"], color=hist_colors, width=1.0, alpha=0.7, label="Hist")
        axm.plot(df.index, df["MACD"], color="#29b6f6", linewidth=1.2, label="MACD")
        axm.plot(df.index, df["MACDs"], color="#ffa726", linewidth=1.1, label="Signal")
        axm.axhline(0, color="#787b86", linewidth=0.7, alpha=0.6)
        axm.grid(True, linestyle="--", alpha=0.15, color="#787b86")
        axm.tick_params(colors="#787b86")
        axm.legend(loc="upper left", facecolor="#1e222d", edgecolor="#2a2e39", labelcolor="#d1d4dc", fontsize=7)

        ax2.set_facecolor("#131722")
        ax2.plot(df.index, df['RSI'], color="#ba68c8", linewidth=1.6, label="RSI 14")
        ax2.axhline(70, color="#ef5350", linestyle=":", linewidth=1.1, alpha=0.8)
        ax2.axhline(30, color="#26a69a", linestyle=":", linewidth=1.1, alpha=0.8)
        ax2.axhline(50, color="#787b86", linestyle="--", linewidth=0.8, alpha=0.5)
        ax2.fill_between(df.index, df['RSI'], 70, where=(df['RSI'] >= 70), color="#ef5350", alpha=0.25)
        ax2.fill_between(df.index, df['RSI'], 30, where=(df['RSI'] <= 30), color="#26a69a", alpha=0.25)
        ax2.set_ylim(10, 90)
        ax2.grid(True, linestyle="--", alpha=0.15, color="#787b86")
        ax2.tick_params(colors="#787b86")
        ax2.legend(loc="upper left", facecolor="#1e222d", edgecolor="#2a2e39", labelcolor="#d1d4dc", fontsize=8)
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200, facecolor=fig.get_facecolor())
        buf.seek(0)
        plt.close()

        precio_actual = float(df['Close'].iloc[-1])
        ema20_val = float(df['EMA20'].iloc[-1])
        ema50_val = float(df['EMA50'].iloc[-1])
        ema200_val = float(df['EMA200'].iloc[-1]) if 'EMA200' in df else None
        rsi_val = float(df['RSI'].iloc[-1])
        atr_val = float(df['ATR'].iloc[-1]) if pd.notnull(df['ATR'].iloc[-1]) else None
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
            "macd": float(df['MACD'].iloc[-1]),
            "macd_signal": float(df['MACDs'].iloc[-1]),
            "macd_hist": float(df['MACDh'].iloc[-1]),
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

# ==================== MÉTRICAS DE RIESGO Y PERFORMANCE ====================
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

        lineas = []
        lineas.append("📊 MÉTRICAS DE RIESGO Y PERFORMANCE")
        lineas.append("")
        lineas.append("🏆 Trades Cerrados")
        lineas.append(f"• PnL Realizado: ${resultado['pnl_realizado']:+,.2f} USD")
        lineas.append(f"• Operaciones: {resultado['cant_trades']}  |  Win Rate: {resultado['win_rate']:.1f}%")
        lineas.append(f"• Profit Factor: {resultado['profit_factor']:.2f}")
        lineas.append(f"• Expectancy: ${resultado['expectancy']:+,.2f} por trade")
        lineas.append(f"• Promedio ganancia: ${resultado['avg_win']:+,.2f}  |  Promedio pérdida: ${resultado['avg_loss']:+,.2f}")
        lineas.append("")
        lineas.append("📉 Riesgo")
        lineas.append(f"• Max Drawdown: ${resultado['max_drawdown_usd']:+,.2f} USD ({resultado['max_drawdown_pct']:.1f}%)")
        lineas.append(f"• Sharpe aproximado (anualizado): {resultado['sharpe_aprox']:.2f}")
        lineas.append(f"• Tiempo promedio en posición: {resultado['tiempo_promedio_dias']:.0f} días")
        
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
            h = yf.Ticker(sym).history(start=fecha_start, auto_adjust=True)
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
                f"ID {p['id']} {p['ticker']} {p['tipo_pos']} {p['lev']:.0f}x margen ${p['costo_margen']:,.0f} PnL {p['pnl_usd']:+.0f}"
            )
    elif not df_ids.empty:
        for _, r in df_ids.iterrows():
            lineas.append(f"ID {int(r['id'])} {r['ticker']} {r['tipo_posicion']} ${float(r['monto_total_usd']):,.0f}")
    if not df_mov.empty:
        for _, r in df_mov.iterrows():
            lineas.append(f"Movimientos {r['tipo']}: {int(r['n'])} / ${float(r['tot']):,.0f} ARS")
    return "\n".join(lineas)

def llamar_gemini(prompt: str, system_instruction: str) -> str:
    response = ai_client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config={"system_instruction": system_instruction},
    )
    return response.text or ""

# ==================== PRESUPUESTOS ====================
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

def obtener_progreso_presupuestos(user_id: int, mes: int = None, anio: int = None):
    ahora = ahora_argentina()
    mes = mes or ahora.month
    anio = anio or ahora.year

    with get_db_connection() as conn:
        df_pres = pd.read_sql(
            "SELECT categoria, monto_limite FROM presupuestos WHERE user_id = %s AND mes = %s AND anio = %s;",
            conn, params=(user_id, mes, anio)
        )
        df_gastos = pd.read_sql(
            """SELECT categoria, SUM(monto) as gastado 
               FROM movimientos 
               WHERE user_id = %s AND tipo = 'GASTO' 
               AND EXTRACT(MONTH FROM fecha) = %s AND EXTRACT(YEAR FROM fecha) = %s
               GROUP BY categoria;""",
            conn, params=(user_id, mes, anio)
        )

    if df_pres.empty:
        return None, "No tenés presupuestos cargados para este mes. Decime por ejemplo: 'Presupuesto Comida 180000'"

    gastos_dict = dict(zip(df_gastos['categoria'], df_gastos['gastado'])) if not df_gastos.empty else {}
    lineas = [f"📅 PRESUPUESTOS — {mes:02d}/{anio}"]
    lineas.append("")
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
                        "texto": f"⚠️ LIQUIDACIÓN CERCANA\n{p['ticker']} [{p['tipo_pos']} {p['lev']:.0f}x]\nDistancia actual: {dist:.1f}%\nPrecio liq: ${p['precio_liq']:,.2f} | Spot: ${p['spot']:,.2f}",
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

                    # Testeo de POC
                    if poc and abs(p_act - poc) / p_act <= 0.008:
                        clave = f"{tk}_test_poc"
                        h = _hash_alerta("poc_test", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=24):
                            alertas.append({
                                "texto": f"🎯 TESTEO DE POC INSTITUCIONAL\n{tk} está testeando su POC de volumen en ${poc:,.2f} (Precio: ${p_act:,.2f}). Zona de alta reacción.",
                                "tipo": "poc_test",
                                "clave": clave,
                                "hash": h
                            })

                    # Quiebre de Estructura en lenguaje informal
                    if bias == "choch_alcista":
                        clave = f"{tk}_choch_up"
                        h = _hash_alerta("choch", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=24):
                            alertas.append({
                                "texto": f"🟢 CAMBIO DE ESTRUCTURA\n{tk} rompió un techo/máximo importante en ${p_act:,.2f}. Es posible un cambio a tendencia alcista.",
                                "tipo": "choch",
                                "clave": clave,
                                "hash": h
                            })
                    elif bias == "choch_bajista":
                        clave = f"{tk}_choch_down"
                        h = _hash_alerta("choch", clave, "")
                        if not alerta_ya_enviada(user_id, h, horas_ventana=24):
                            alertas.append({
                                "texto": f"🔴 CAMBIO DE ESTRUCTURA\n{tk} rompió un piso/mínimo importante en ${p_act:,.2f}. Es posible un cambio a tendencia bajista.",
                                "tipo": "choch",
                                "clave": clave,
                                "hash": h
                            })

                    # Señal RSI con Backtest multitemporal
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

async def tarea_alertas_periodicas(app):
    await asyncio.sleep(60)
    while True:
        try:
            if en_horario_alertas():
                def _obtener_usuarios():
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                SELECT DISTINCT user_id FROM (
                                    SELECT user_id FROM portafolio_inversIONES
                                    UNION
                                    SELECT user_id FROM movimientos
                                    UNION
                                    SELECT user_id FROM trades_cerrados
                                ) u WHERE user_id IS NOT NULL;
                            """)
                            return [r[0] for r in cursor.fetchall()]

                usuarios = await asyncio.to_thread(_obtener_usuarios)

                for uid in usuarios:
                    alertas = await asyncio.to_thread(generar_alertas_para_usuario, uid)
                    if alertas:
                        mensajes = ["🔔 ALERTAS DE TU CARTERA\n"]
                        for a in alertas:
                            mensajes.append(a["texto"])
                            mensajes.append("")
                            await asyncio.to_thread(registrar_alerta_enviada, uid, a["tipo"], a["clave"], a["hash"])
                        texto_final = "\n".join(mensajes).strip()
                        try:
                            await app.bot.send_message(chat_id=uid, text=texto_final)
                            logger.info(f"Alertas enviadas a user {uid}: {len(alertas)}")
                        except Exception as e:
                            logger.error(f"No se pudo enviar alerta a {uid}: {e}")
            else:
                logger.info("Fuera de horario de alertas, se omite ciclo.")
        except Exception as e:
            logger.error(f"Error en tarea de alertas: {e}", exc_info=True)

        await asyncio.sleep(ALERTA_INTERVALO_HORAS * 3600)

# ==================== MOTOR DE MÉTRICAS ANALÍTICAS ====================
def calcular_super_metricas_totales(user_id: int):
    metricas = []
    pnl_realizado_total = 0.0
    cant_trades_cerrados = 0
    try:
        with get_db_connection() as conn:
            df_tc = pd.read_sql("SELECT id, fecha, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion FROM trades_cerrados WHERE user_id = %s ORDER BY fecha ASC;", conn, params=(user_id,))
        if not df_tc.empty:
            pnl_realizado_total = float(df_tc['pnl_usd'].sum())
            cant_trades_cerrados = len(df_tc)
            trades_ganadores = len(df_tc[df_tc['pnl_usd'] > 0])
            win_rate = (trades_ganadores / cant_trades_cerrados * 100) if cant_trades_cerrados > 0 else 0.0
            
            metricas.append("=== TRADES CERRADOS (GANANCIAS REALIZADAS) ===")
            metricas.append(f"• PnL Realizado Total: ${pnl_realizado_total:+,.2f} USD")
            metricas.append(f"• Operaciones cerradas: {cant_trades_cerrados} (Win Rate: {win_rate:.1f}%)")
            metricas.append("• Desglose:")
            for _, tc in df_tc.iterrows():
                roi_txt = f" ({tc['roi_pct']:+.2f}%)" if pd.notnull(tc['roi_pct']) else ""
                metricas.append(f"   - [{tc['fecha'].strftime('%Y-%m-%d')}] {tc['ticker']} ({tc['tipo_posicion']}): PnL ${tc['pnl_usd']:+,.2f} USD{roi_txt}")
        else:
            metricas.append("=== TRADES CERRADOS: Sin historial ===")
    except Exception as e:
        logger.error(f"Error métricas trades cerrados: {e}")

    try:
        resumen = obtener_resumen_portafolio(user_id)
        if resumen and resumen["posiciones"]:
            pnl_no_realizado = resumen['pnl_total_usd']
            pnl_neto_global_combinado = pnl_realizado_total + pnl_no_realizado
            
            metricas.append("\n=== CARTERA ACTUAL (POSICIONES ABIERTAS) ===")
            metricas.append(f"• Capital/Margen Abierto: ${resumen['total_invertido']:,.2f} USD")
            metricas.append(f"• Valor de Mercado Actual: ${resumen['total_actual']:,.2f} USD")
            metricas.append(f"• PnL Flotante: ${pnl_no_realizado:+,.2f} USD ({resumen['pnl_total_pct']:+.2f}%)")
            metricas.append(f"• PnL TOTAL HISTÓRICO: ${pnl_neto_global_combinado:+,.2f} USD")
            
            pos_ordenadas = sorted(resumen["posiciones"], key=lambda x: x['pnl_pct'], reverse=True)
            top = pos_ordenadas[0]
            worst = pos_ordenadas[-1]
            metricas.append(f"• Mejor posición: {top['ticker']} ({top['tipo_pos']} {top['lev']:.0f}x) {top['pnl_pct']:+.2f}%")
            metricas.append(f"• Peor posición: {worst['ticker']} ({worst['tipo_pos']} {worst['lev']:.0f}x) {worst['pnl_pct']:+.2f}%")
            
            metricas.append("• Detalle:")
            for p in resumen["posiciones"]:
                liq_txt = f" | Liq: ${p['precio_liq']:,.2f}" if p['precio_liq'] else ""
                lev_txt = f" [{p['tipo_pos']} {p['lev']:.0f}x]" if p['tipo_pos'] != "SPOT" else " [SPOT]"
                metricas.append(
                    f"   - ID {p['id']}: {p['ticker']}{lev_txt} | Margen: ${p['costo_margen']:,.2f} | PPC: ${p['ppc']:,.2f} | Spot: ${p['spot']:,.2f} | PnL: ${p['pnl_usd']:,.2f} ({p['pnl_pct']:+.2f}%){liq_txt}"
                )
        else:
            metricas.append("\n=== CARTERA ACTUAL: Sin posiciones abiertas ===")
    except Exception as e:
        logger.error(f"Error métricas de inversión: {e}")

    try:
        with get_db_connection() as conn:
            df_mov = pd.read_sql("SELECT id, fecha, tipo, monto, categoria, descripcion FROM movimientos WHERE user_id = %s ORDER BY fecha ASC;", conn, params=(user_id,))
        if not df_mov.empty:
            df_mov['fecha'] = pd.to_datetime(df_mov['fecha'])
            df_gastos = df_mov[df_mov['tipo'] == 'GASTO']
            df_ingresos = df_mov[df_mov['tipo'] == 'INGRESO']
            tot_g = float(df_gastos['monto'].sum()) if not df_gastos.empty else 0.0
            tot_i = float(df_ingresos['monto'].sum()) if not df_ingresos.empty else 0.0
            ahorro = tot_i - tot_g
            tasa_ahorro = (ahorro / tot_i * 100) if tot_i > 0 else 0.0
            
            metricas.append("\n=== FLUJO DE CAJA (ARS) ===")
            metricas.append(f"• Ingresos Totales: ${tot_i:,.2f} ARS")
            metricas.append(f"• Gastos Totales: ${tot_g:,.2f} ARS")
            metricas.append(f"• Superávit/Ahorro Neto: ${ahorro:,.2f} ARS (Tasa: {tasa_ahorro:.2f}%)")
    except Exception as e:
        logger.error(f"Error métricas ARS: {e}")

    return "\n".join(metricas)

# ==================== HISTORIAL Y BORRADO ====================
def obtener_historial_completo_texto(user_id: int):
    lineas = []
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, fecha, tipo, monto, categoria, descripcion FROM movimientos WHERE user_id = %s ORDER BY fecha DESC LIMIT 30;", (user_id,))
            movs = cursor.fetchall()
            if movs:
                lineas.append("📋 GASTOS E INGRESOS (ARS):")
                for m in movs:
                    lineas.append(f"- ID {m[0]} | Fecha: {m[1].strftime('%Y-%m-%d')} | {m[2]}: ${m[3]:,.2f} ARS | {m[4]} | {m[5]}")

            cursor.execute(
                "SELECT id, fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion FROM portafolio_inversiones WHERE user_id = %s ORDER BY fecha DESC LIMIT 30;",
                (user_id,)
            )
            invs = cursor.fetchall()
            lineas.append("\n💼 POSICIONES / TRADES ABIERTOS (USD):")
            if invs:
                for inv in invs:
                    tipo_p = inv[6] if inv[6] else "SPOT"
                    lev_p = f" x{inv[7]:.0f}" if inv[7] and inv[7] > 1 else ""
                    liq_p = f" | Liq: ${inv[8]:,.2f}" if inv[8] else ""
                    lineas.append(f"- ID {inv[0]} (INV ABIERTA) | Fecha: {inv[1].strftime('%Y-%m-%d')} | {inv[2]} [{tipo_p}{lev_p}] | Margen: ${inv[5]:,.2f} USD | PPC: ${inv[4]:,.2f}{liq_p}")

            cursor.execute("SELECT id, fecha, ticker, tipo_posicion, pnl_usd, roi_pct, descripcion FROM trades_cerrados WHERE user_id = %s ORDER BY fecha DESC LIMIT 30;", (user_id,))
            tcs = cursor.fetchall()
            lineas.append("\n🏆 TRADES CERRADOS / GANANCIAS REALIZADAS (USD):")
            if tcs:
                for tc in tcs:
                    roi_s = f" ({tc[5]:+.2f}%)" if tc[5] else ""
                    lineas.append(f"- ID {tc[0]} (CERRADO) | Fecha: {tc[1].strftime('%Y-%m-%d')} | {tc[2]} ({tc[3]}): PnL ${tc[4]:+,.2f} USD{roi_s} | {tc[6]}")
    return "\n".join(lineas)

def borrar_inversion_por_ticker(user_id: int, ticker: str):
    ticker = ticker.strip().upper()
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM portafolio_inversiones WHERE user_id = %s AND UPPER(ticker) = %s RETURNING id;", (user_id, ticker))
            filas = cursor.fetchall()
            conn.commit()
            return len(filas)

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

# ==================== GASTOS (ARS) ====================
def guardar_movimiento(user_id: int, tipo: str, monto: float, categoria: str, descripcion: str, fecha_str: str = None):
    fecha_limpia = None
    if fecha_str:
        f_cand = fecha_str.strip().split()[0]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f_cand):
            fecha_limpia = f_cand

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_limpia:
                cursor.execute(
                    "INSERT INTO movimientos (user_id, fecha, tipo, monto, categoria, descripcion) VALUES (%s, %s, %s, %s, %s, %s)",
                    (user_id, fecha_limpia, tipo.upper(), float(monto), categoria.capitalize(), descripcion)
                )
            else:
                cursor.execute(
                    "INSERT INTO movimientos (user_id, fecha, tipo, monto, categoria, descripcion) VALUES (%s, NOW(), %s, %s, %s, %s)",
                    (user_id, tipo.upper(), float(monto), categoria.capitalize(), descripcion)
                )
            conn.commit()

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

def generar_excel_completo(user_id: int):
    with get_db_connection() as conn:
        df_mov = pd.read_sql("SELECT id, fecha, tipo, monto, categoria, descripcion FROM movimientos WHERE user_id = %s ORDER BY fecha ASC;", conn, params=(user_id,))
        df_inv = pd.read_sql("SELECT id, fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion FROM portafolio_inversiones WHERE user_id = %s ORDER BY fecha ASC;", conn, params=(user_id,))
        df_tc = pd.read_sql("SELECT id, fecha, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion FROM trades_cerrados WHERE user_id = %s ORDER BY fecha ASC;", conn, params=(user_id,))
    if df_mov.empty and df_inv.empty and df_tc.empty:
        return None
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        if not df_mov.empty:
            df_mov.to_excel(writer, sheet_name='Gastos_Ingresos_ARS', index=False)
        if not df_inv.empty:
            df_inv.to_excel(writer, sheet_name='Posiciones_Abiertas_USD', index=False)
        if not df_tc.empty:
            df_tc.to_excel(writer, sheet_name='Trades_Cerrados_Ganancias', index=False)
    buf.seek(0)
    return buf

# ==================== SYSTEM INSTRUCTION ====================
SYSTEM_INSTRUCTION = """
Eres un analista y asesor financiero cuantitativo institucional. Manejas tres mundos:
1. GASTOS E INGRESOS: Flujo cotidiano en Pesos Argentinos (ARS $).
2. CARTERA ACTUAL: Trades o posiciones que ESTÁN ABIERTOS TODAVÍA en Dólares (USD $), incluyendo SPOT y FUTUROS (LONG / SHORT).
3. TRADES CERRADOS / GANANCIAS REALIZADAS: Operaciones que YA SE CERRARON y cuyo dinero ya está en el bolsillo.

TODOS LOS REGISTROS QUE APARECEN EN "POSICIONES / TRADES ABIERTOS" REPRESENTAN OPERACIONES QUE EL USUARIO TIENE ABIERTAS HOY EN DÍA.

REGLAS DE FORMATO Y ESTILO VISUAL (OBLIGATORIO Y ESTRICTO):
- PROHIBIDO usar hashtags para títulos como '###' o '##'. Usa emojis temáticos al inicio de cada sección.
- PROHIBIDO usar viñetas como '* **Texto:**'. Usa siempre viñetas limpias con '• ' seguidas del concepto.
- PROHIBIDO escribir textos excesivamente largos. Sé conciso, directo, cuantitativo y estructurado.
- Evita líneas divisorias '---' innecesarias.
- Diseña respuestas prolijas, elegantes y scannables para la pantalla de un celular.

REGLAS DE ANÁLISIS TÉCNICO PERSONALIZADO (MUY IMPORTANTE):
- Si el usuario pide escanear o analizar todos sus activos:
  DEBES EMITIR OBLIGATORIAMENTE: ACCION: ESCANEAR_CARTERA

- Si el usuario pide analizar técnicamente un activo:
  DEBES EMITIR: ACCION: ANALIZAR_ACTIVO|[TICKER]|[TIMEFRAME_DETECTADO]
  Donde TIMEFRAME_DETECTADO puede ser: "diario", "semanal" o "4h" (por defecto "diario").

REGLAS DE GRÁFICOS (MUY ESTRICTAS Y OBLIGATORIAS):
1. COMPARATIVA DE DOS O MÁS ACTIVOS:
   ACCION: GRAFICO_EVOLUCION_POR_ACTIVOS|[PERIODO_DETECTADO]|[TICKERS_SEPARADOS_POR_COMA]

2. TODOS LOS ACTIVOS DE LA CARTERA:
   ACCION: GRAFICO_EVOLUCION_POR_ACTIVOS|[PERIODO_DETECTADO]

3. EVOLUCIÓN CONSOLIDADA DE CARTERA (UNA SOLA LÍNEA):
   ACCION: GRAFICO_EVOLUCION_CARTERA_CONSOLIDADA|[PERIODO_DETECTADO]

4. UN SOLO ACTIVO INDIVIDUAL:
   ACCION: GRAFICO_EVOLUCION_ACTIVO|[TICKER]|[PERIODO_DETECTADO]

REGLAS DE CIERRE DE POSICIÓN ABIERTA:
- ACCION: CERRAR_POSICION|[ID]|[PNL_USD_MANUAL_O_VACIO]|[PRECIO_SALIDA_O_VACIO]

REGLAS DE REGISTRO DE TRADES CERRADOS PASADOS:
- REGISTRO_TRADE_CERRADO: [TICKER]|[PNL_USD]|[TIPO_POS]|[ROI_PCT]|[MONTO_INVERTIDO]|[DESCRIPCION]|[FECHA_YYYY-MM-DD]

REGLAS DE REGISTRO DE POSICIONES ABIERTAS (SPOT / FUTUROS):
- REGISTRO_INV: [TICKER]|[MARGEN_USD]|[PPC]|[CANTIDAD]|[FECHA_YYYY-MM-DD]|[TIPO_POS]|[APALANCAMIENTO]|[PRECIO_LIQ]

REGLAS DE MODIFICACIÓN Y AGREGADO DE MARGEN:
- Agregar margen: ACCION: AGREGAR_MARGEN|[ID]|[MONTO_EXTRA_USD]
- Modificar posición abierta: ACCION: MODIFICAR_INVERSION|[ID]|[CAMPO]|[NUEVO_VALOR]
- Modificar gasto/ingreso: ACCION: MODIFICAR_MOVIMIENTO|[ID]|[CAMPO]|[NUEVO_VALOR]

REGLAS DE BORRADO:
- Borrar por ticker: ACCION: BORRAR_INVERSION_TICKER|[TICKER]
- Borrar posición abierta por ID: ACCION: BORRAR_INVERSION_ID|[ID]
- Borrar trade cerrado por ID: ACCION: BORRAR_TRADE_CERRADO_ID|[ID]
- Borrar movimiento ARS por ID: ACCION: BORRAR_MOVIMIENTO_ID|[ID]
- Borrar último registro: ACCION: BORRAR_ULTIMO
- Resetear todo: ACCION: BORRAR_TODO

REGLAS DE PRESUPUESTOS:
- Definir presupuesto: ACCION: SET_PRESUPUESTO|[CATEGORIA]|[MONTO]
- Ver presupuestos: ACCION: VER_PRESUPUESTOS

REGLAS DE OBJETIVOS:
- Definir objetivo: ACCION: CREAR_OBJETIVO|[DESCRIPCION]|[TIPO]|[MONTO]|[FECHA_YYYY-MM-DD_O_VACIO]
- Ver objetivos: ACCION: VER_OBJETIVOS

REGLAS DE MÉTRICAS DE RIESGO:
- ACCION: VER_RIESGO

REGLAS GENERALES:
- Registro ARS: REGISTRO_ARS: [TIPO]|[MONTO]|[CATEGORIA]|[DESCRIPCION]|[FECHA_YYYY-MM-DD]
- Ver cartera: ACCION: VER_CARTERA
- Cotización en vivo: ACCION: CONSULTA_PRECIO|[TICKER]
- Gráfico torta inversiones: ACCION: GRAFICO_INVERSIONES
- Gráfico torta gastos: ACCION: GRAFICO_GASTOS
- Excel: ACCION: EXCEL
"""

# ==================== COMANDOS RÁPIDOS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy tu asistente financiero cuantitativo.\n\n"
        "📈 Gráficos\n"
        "• /spy        →  Rendimiento % vs S&P 500 (año actual)\n"
        "• /spy todo   →  Rendimiento % vs S&P 500 (desde tu inicio)\n"
        "• /grafico    →  Curva de PnL en USD\n"
        "• /activos    →  Comparativa relativa de activos\n\n"
        "🟢 Posiciones abiertas\n"
        "• ¿Cómo vienen mis posiciones?\n"
        "• Cerré la posición ID 2 con 150 usd de ganancia\n\n"
        "🏆 Trades cerrados\n"
        "• Gané 450 usd en un trade de SOL\n\n"
        "💼 Resumen rápido\n"
        "/resumen   →  balance consolidado\n"
        "/ytd       →  rendimiento acumulado + Alpha SPY\n"
        "/riesgo    →  métricas de riesgo + liquidaciones\n"
        "/analisis  →  análisis técnico cuantitativo (sin IA)\n"
        "/mes       →  gastos del mes + presupuestos\n"
        "/objetivos →  progreso de metas\n\n"
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

async def intentar_comando_local(update: Update, user_id: int, user_msg: str) -> bool:
    raw = (user_msg or "").strip()
    low = raw.lower().strip()
    if low.startswith("/"):
        low = low[1:]
        raw = raw[1:] if raw.startswith("/") else raw

    partes = raw.split()
    cmd = partes[0].lower() if partes else ""
    args = " ".join(partes[1:]) if len(partes) > 1 else ""
    periodo = extraer_periodo(raw)

    if cmd in ("help", "ayuda", "comandos"):
        await start(update, None)
        return True
    if cmd in ("resumen", "cartera", "balance"):
        await cmd_resumen(update, None)
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
    if cmd == "torta":
        buf = generar_grafico_distribucion_inversiones(user_id)
        if buf:
            await update.message.reply_photo(photo=buf, caption="Distribución abierta")
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

    # 100% LOCAL: Captura lenguaje ultra informal sin consumir tokens de IA
    m_an = re.match(r"^(?:(?:analiza(?:me)?|an[aá]lisis(?:\s+t[eé]cnico)?|c[oó]mo\s+ves|at)\s+)?([a-zA-Z0-9]{2,10})(?:\s+(diario|semanal|4h))?$", low)
    if m_an:
        posible_tk = m_an.group(1).upper()
        palabras_comunes = {"HOLA", "BUENAS", "GRACIAS", "OK", "RESET", "AYUDA", "MES", "GASTOS", "RESUMEN", "CARTERA", "OBJETIVOS", "RIESGO"}
        if posible_tk not in palabras_comunes and not posible_tk.isdigit():
            tk = posible_tk
            tf_at = m_an.group(2) or "diario"
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
    borrar_todos_los_movimientos(user_id)
    await update.message.reply_text("🗑️ Tu base de datos, cartera y trades cerrados han sido reseteados.")

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
    texto, err = obtener_progreso_presupuestos(user_id)
    if err:
        ahora = ahora_argentina()
        with get_db_connection() as conn:
            df = pd.read_sql(
                """SELECT categoria, SUM(monto) as total FROM movimientos 
                   WHERE user_id = %s AND tipo = 'GASTO' 
                   AND EXTRACT(MONTH FROM fecha) = %s AND EXTRACT(YEAR FROM fecha) = %s
                   GROUP BY categoria ORDER BY total DESC;""",
                conn, params=(user_id, ahora.month, ahora.year)
            )
        if df.empty:
            await update.message.reply_text("No hay gastos registrados este mes ni presupuestos cargados.")
        else:
            lineas = [f"📅 GASTOS DEL MES {ahora.month:02d}/{ahora.year}", ""]
            total = 0.0
            for _, r in df.iterrows():
                lineas.append(f"• {r['categoria']}: ${float(r['total']):,.0f}")
                total += float(r['total'])
            lineas.append(f"\nTotal: ${total:,.0f} ARS")
            lineas.append("\n💡 Tip: definí presupuestos diciendo 'Presupuesto Comida 180000'")
            await update.message.reply_text("\n".join(lineas))
    else:
        await update.message.reply_text(texto)

async def cmd_objetivos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    texto = obtener_progreso_objetivos(user_id)
    await update.message.reply_text(texto)

async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = update.message.text

    try:
        if await intentar_comando_local(update, user_id, user_msg):
            return
    except Exception as e:
        logger.error(f"Error comando local: {e}", exc_info=True)

    try:
        prompt = (
            f"CONTEXTO COMPACTO:\n{resumen_compacto_para_ia(user_id)}\n\n"
            f"MENSAJE: {user_msg}\n\n"
            "Si es un pedido de gráfico, rendimiento, YTD, CAGR o vs SPY, NO inventes números: "
            "emití solo el tag ACCION correspondiente. Sé breve."
        )
        reply = llamar_gemini(prompt, SYSTEM_INSTRUCTION)
        
        necesita_cartera = "ACCION: VER_CARTERA" in reply
        necesita_grafico_inv = "ACCION: GRAFICO_INVERSIONES" in reply
        necesita_grafico_gastos = "ACCION: GRAFICO_GASTOS" in reply
        necesita_excel = "ACCION: EXCEL" in reply
        necesita_borrar_todo = "ACCION: BORRAR_TODO" in reply
        necesita_borrar_ultimo = "ACCION: BORRAR_ULTIMO" in reply
        necesita_ver_riesgo = "ACCION: VER_RIESGO" in reply
        necesita_ver_presupuestos = "ACCION: VER_PRESUPUESTOS" in reply
        necesita_ver_objetivos = "ACCION: VER_OBJETIVOS" in reply
        
        texto_limpio = reply
        for tag in ["ACCION: VER_CARTERA", "ACCION: GRAFICO_INVERSIONES", "ACCION: GRAFICO_GASTOS", 
                    "ACCION: EXCEL", "ACCION: BORRAR_TODO", "ACCION: BORRAR_ULTIMO",
                    "ACCION: VER_RIESGO", "ACCION: VER_PRESUPUESTOS", "ACCION: VER_OBJETIVOS"]:
            texto_limpio = texto_limpio.replace(tag, "")

        necesita_escanear_cartera = "ACCION: ESCANEAR_CARTERA" in reply
        if not necesita_escanear_cartera and re.search(r"(?:analiza(?:me)?\s+todos?\s+(?:mis\s+)?activos?|escanear?\s+(?:mi\s+)?cartera|revisa(?:me)?\s+mis\s+activos)", user_msg, re.IGNORECASE):
            necesita_escanear_cartera = True
        texto_limpio = texto_limpio.replace("ACCION: ESCANEAR_CARTERA", "")

        ticker_at = None
        tf_at = "diario"
        m_at = re.search(r"ACCION:\s*ANALIZAR_ACTIVO\|([^\n\r|]+)(?:\|([^\n\r]+))?", texto_limpio)
        if m_at:
            ticker_at = m_at.group(1).strip()
            tf_at = m_at.group(2).strip() if m_at.group(2) else "diario"
            texto_limpio = texto_limpio.replace(m_at.group(0), "")

        if not ticker_at and re.search(r"(?:analiza|analizame|analisis\s+tecnico)\s+([a-zA-Z0-9]+)", user_msg, re.IGNORECASE):
            m_fall = re.search(r"(?:analiza|analizame|analisis\s+tecnico)\s+([a-zA-Z0-9]+)", user_msg, re.IGNORECASE)
            ticker_at = m_fall.group(1).strip().upper()
            if "4h" in user_msg.lower():
                tf_at = "4h"
            elif "sem" in user_msg.lower():
                tf_at = "semanal"
            else:
                tf_at = "diario"

        ticker_a_cotizar = None
        m_precio = re.search(r"ACCION: CONSULTA_PRECIO\|([^\n\r]+)", texto_limpio)
        if m_precio:
            ticker_a_cotizar = m_precio.group(1).strip()
            texto_limpio = texto_limpio.replace(m_precio.group(0), "")

        necesita_grafico_por_activos = False
        periodo_por_activos = ""
        tickers_filtro_activos = []
        m_gpa = re.search(r"ACCION: GRAFICO_EVOLUCION_POR_ACTIVOS(?:\|([^|\n\r]*))?(?:\|([^\n\r]*))?", texto_limpio)
        if m_gpa:
            necesita_grafico_por_activos = True
            periodo_por_activos = m_gpa.group(1).strip() if m_gpa.group(1) else ""
            raw_tks = m_gpa.group(2).strip() if m_gpa.group(2) else ""
            if raw_tks:
                tickers_filtro_activos = [t.strip().upper() for t in raw_tks.split(",") if t.strip()]
            texto_limpio = texto_limpio.replace(m_gpa.group(0), "")

        necesita_grafico_consolidado = False
        periodo_consolidado = ""
        m_gc = re.search(r"ACCION: GRAFICO_EVOLUCION_CARTERA_CONSOLIDADA(?:\|([^\n\r]+))?", texto_limpio)
        if m_gc:
            necesita_grafico_consolidado = True
            periodo_consolidado = m_gc.group(1).strip() if m_gc.group(1) else ""
            texto_limpio = texto_limpio.replace(m_gc.group(0), "")

        if not necesita_grafico_consolidado:
            m_gc_old = re.search(r"ACCION: GRAFICO_EVOLUCION_CARTERA(?:\|([^\n\r]+))?", texto_limpio)
            if m_gc_old:
                necesita_grafico_consolidado = True
                periodo_consolidado = m_gc_old.group(1).strip() if m_gc_old.group(1) else ""
                texto_limpio = texto_limpio.replace(m_gc_old.group(0), "")

        ticker_grafico_evol = None
        periodo_activo = ""
        m_ga = re.search(r"ACCION: GRAFICO_EVOLUCION_ACTIVO\|([^\n\r|]+)(?:\|([^\n\r]+))?", texto_limpio)
        if m_ga:
            ticker_grafico_evol = m_ga.group(1).strip()
            periodo_activo = m_ga.group(2).strip() if m_ga.group(2) else ""
            texto_limpio = texto_limpio.replace(m_ga.group(0), "")

        es_pedido_grafico_o_comparativa = bool(re.search(r"(?:grafico|grafica|evolucion|compara|comparame|comparar|vs|versus)", user_msg, re.IGNORECASE))
        if es_pedido_grafico_o_comparativa and not necesita_grafico_consolidado:
            tickers_posibles = ["MELI", "NU", "GGAL", "SUPV", "NVDA", "BTC", "SOL", "YPF", "VIST", "MSFT", "META", "AMD", "TSLA", "GOOGL", "LOMA", "ETH", "BNB", "NEXO"]
            tickers_mencionados = []
            for tk in tickers_posibles:
                if re.search(r'\b' + re.escape(tk) + r'\b', user_msg, re.IGNORECASE):
                    tickers_mencionados.append(tk)
            
            if len(tickers_mencionados) >= 2:
                ticker_grafico_evol = None
                necesita_grafico_por_activos = True
                tickers_filtro_activos = tickers_mencionados
                if not periodo_por_activos:
                    periodo_por_activos = periodo_activo

        m_close_pos = re.search(r"ACCION: CERRAR_POSICION\|(\d+)(?:\|([^|\n\r]*))?(?:\|([^\n\r]*))?", texto_limpio)
        if m_close_pos:
            c_inv_id = int(m_close_pos.group(1))
            c_pnl = float(m_close_pos.group(2).strip()) if m_close_pos.group(2) and m_close_pos.group(2).strip() not in ["", "None"] else None
            c_spot = float(m_close_pos.group(3).strip()) if m_close_pos.group(3) and m_close_pos.group(3).strip() not in ["", "None"] else None
            texto_limpio = texto_limpio.replace(m_close_pos.group(0), "")
            
            res_cierre, msg_cierre = cerrar_posicion_abierta_por_id(user_id, c_inv_id, c_pnl, c_spot)
            if res_cierre:
                signo_pnl = "+" if res_cierre["pnl_usd"] >= 0 else ""
                roi_str = f" ({res_cierre['roi_pct']:+.2f}%)" if res_cierre['roi_pct'] else ""
                texto_limpio += f"\n\n🎯 Trade cerrado: {res_cierre['ticker']} [{res_cierre['tipo_pos']}] | PnL {signo_pnl}${res_cierre['pnl_usd']:,.2f} USD{roi_str} → ID histórico {res_cierre['tc_id']}"
            else:
                texto_limpio += f"\n\n⚠️ No se pudo cerrar: {msg_cierre}"

        m_add_m = re.search(r"ACCION: AGREGAR_MARGEN\|(\d+)\|([^\n\r]+)", texto_limpio)
        if m_add_m:
            inv_id_m = int(m_add_m.group(1))
            m_extra = float(m_add_m.group(2).strip())
            texto_limpio = texto_limpio.replace(m_add_m.group(0), "")
            reg, info = agregar_margen_a_posicion(user_id, inv_id_m, m_extra)
            if reg:
                texto_limpio += f"\n\n🛡️ Margen extra en {reg[1]}: +${m_extra:,.2f} USD | {info}"
            else:
                texto_limpio += f"\n\n⚠️ No se encontró la posición ID {inv_id_m}."

        m_mod_inv = re.search(r"ACCION: MODIFICAR_INVERSION\|(\d+)\|([^|\n\r]+)\|([^\n\r]+)", texto_limpio)
        if m_mod_inv:
            inv_id = int(m_mod_inv.group(1))
            campo = m_mod_inv.group(2).strip()
            val = m_mod_inv.group(3).strip()
            texto_limpio = texto_limpio.replace(m_mod_inv.group(0), "")
            prev, info = modificar_inversion_por_id(user_id, inv_id, campo, val)
            texto_limpio += f"\n\n✏️ Inversión ID {inv_id}: {info}"

        m_mod_mov = re.search(r"ACCION: MODIFICAR_MOVIMIENTO\|(\d+)\|([^|\n\r]+)\|([^\n\r]+)", texto_limpio)
        if m_mod_mov:
            mov_id = int(m_mod_mov.group(1))
            campo = m_mod_mov.group(2).strip()
            val = m_mod_mov.group(3).strip()
            texto_limpio = texto_limpio.replace(m_mod_mov.group(0), "")
            prev, info = modificar_movimiento_por_id(user_id, mov_id, campo, val)
            texto_limpio += f"\n\n✏️ Movimiento ID {mov_id}: {info}"

        m_set_pres = re.search(r"ACCION: SET_PRESUPUESTO\|([^|\n\r]+)\|([^\n\r]+)", texto_limpio)
        if m_set_pres:
            cat = m_set_pres.group(1).strip()
            monto = float(m_set_pres.group(2).strip().replace(".", "").replace(",", "."))
            texto_limpio = texto_limpio.replace(m_set_pres.group(0), "")
            c, m, mes, anio = set_presupuesto(user_id, cat, monto)
            texto_limpio += f"\n\n📅 Presupuesto guardado: {c} → ${m:,.0f} ARS ({mes:02d}/{anio})"

        m_obj = re.search(r"ACCION: CREAR_OBJETIVO\|([^|\n\r]+)\|([^|\n\r]+)\|([^|\n\r]+)(?:\|([^\n\r]*))?", texto_limpio)
        if m_obj:
            desc = m_obj.group(1).strip()
            tipo = m_obj.group(2).strip()
            monto = float(m_obj.group(3).strip().replace(".", "").replace(",", "."))
            fecha = m_obj.group(4).strip() if m_obj.group(4) and m_obj.group(4).strip() not in ["", "None", "VACIO"] else None
            texto_limpio = texto_limpio.replace(m_obj.group(0), "")
            oid = crear_objetivo(user_id, desc, tipo, monto, fecha)
            texto_limpio += f"\n\n🎯 Objetivo creado (ID {oid}): {desc} → ${monto:,.0f}"

        m_btk = re.search(r"ACCION: BORRAR_INVERSION_TICKER\|([^\n\r]+)", texto_limpio)
        if m_btk:
            tk_b = m_btk.group(1).strip()
            texto_limpio = texto_limpio.replace(m_btk.group(0), "")
            c_del = borrar_inversion_por_ticker(user_id, tk_b)
            texto_limpio += f"\n\n🗑️ Se eliminaron {c_del} registros de {tk_b}"

        m_bid = re.search(r"ACCION: BORRAR_INVERSION_ID\|(\d+)", texto_limpio)
        if m_bid:
            iid_b = int(m_bid.group(1))
            texto_limpio = texto_limpio.replace(m_bid.group(0), "")
            r_del = borrar_inversion_por_id(user_id, iid_b)
            if r_del:
                texto_limpio += f"\n\n🗑️ Eliminada posición abierta ID {iid_b}: {r_del[1]}"

        m_btc_id = re.search(r"ACCION: BORRAR_TRADE_CERRADO_ID\|(\d+)", texto_limpio)
        if m_btc_id:
            tcid_b = int(m_btc_id.group(1))
            texto_limpio = texto_limpio.replace(m_btc_id.group(0), "")
            r_del = borrar_trade_cerrado_por_id(user_id, tcid_b)
            if r_del:
                texto_limpio += f"\n\n🗑️ Eliminado trade cerrado ID {tcid_b}: {r_del[1]}"

        m_bmid = re.search(r"ACCION: BORRAR_MOVIMIENTO_ID\|(\d+)", texto_limpio)
        if m_bmid:
            mid_b = int(m_bmid.group(1))
            texto_limpio = texto_limpio.replace(m_bmid.group(0), "")
            r_del = borrar_movimiento_por_id(user_id, mid_b)
            if r_del:
                texto_limpio += f"\n\n🗑️ Eliminado gasto/ingreso ID {mid_b}"

        match_tc = re.search(r"REGISTRO_TRADE_CERRADO:\s*([^\n\r]+)", texto_limpio)
        if match_tc:
            linea_tc = match_tc.group(1).strip()
            texto_limpio = texto_limpio.replace(match_tc.group(0), "").strip()
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
            texto_limpio = f"{texto_limpio}\n\n🏆 Trade cerrado registrado: {t} [{tp}] | PnL {signo_p}${p:,.2f} USD{roi_s}{f_txt} (ID {tid})".strip()

        match_inv = re.search(r"REGISTRO_INV:\s*([^\n\r]+)", texto_limpio)
        if match_inv:
            linea_inv = match_inv.group(1).strip()
            texto_limpio = texto_limpio.replace(match_inv.group(0), "").strip()
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
            texto_limpio = f"{texto_limpio}\n\n💼 Guardado como abierto: {t}{lev_str} | Margen ${m:,.2f} | PPC ${p:,.2f} | Cant {c:,.4f}{liq_str}{fecha_str}".strip()

        match_ars = re.search(r"REGISTRO_ARS:\s*([^\n\r]+)", texto_limpio)
        if match_ars:
            linea_ars = match_ars.group(1).strip()
            texto_limpio = texto_limpio.replace(match_ars.group(0), "").strip()
            partes = [p.strip() for p in linea_ars.split("|")]
            tipo = partes[0]
            monto = float(partes[1])
            categoria = partes[2]
            descripcion = partes[3]
            f_gasto = partes[4].split()[0] if len(partes) > 4 and partes[4] not in ["0", "", "None"] else None
            
            guardar_movimiento(user_id, tipo, monto, categoria, descripcion, f_gasto)
            fecha_str = f" — {f_gasto}" if f_gasto else ""
            texto_limpio = f"{texto_limpio}\n\n✅ Guardado: {tipo} de ${monto:,.2f} ARS en {categoria}{fecha_str}".strip()

        if necesita_borrar_todo:
            borrar_todos_los_movimientos(user_id)
            texto_limpio += "\n\n🗑️ Base de datos, cartera y trades reseteados."

        if necesita_borrar_ultimo:
            res_ultimo = borrar_ultimo_registro_general(user_id)
            if res_ultimo:
                texto_limpio += f"\n\n🗑️ Eliminado: {res_ultimo}"
            else:
                texto_limpio += "\n\n⚠️ No había registros para borrar."

        texto_limpio = re.sub(r"ACCION:\s*[^\n\r]+", "", texto_limpio)
        texto_limpio = re.sub(r"REGISTRO_[^\n\r]+", "", texto_limpio)
        texto_limpio = limpiar_estilo_telegram(texto_limpio)

        if ticker_a_cotizar:
            datos_mkt = consultar_datos_mercado(ticker_a_cotizar)
            if datos_mkt:
                signo = "+" if datos_mkt["var_pct"] >= 0 else ""
                emoji = "🟢" if datos_mkt["var_pct"] >= 0 else "🔴"
                rango_txt = f"\n• Rango del día: ${datos_mkt['day_low']:,.2f} – ${datos_mkt['day_high']:,.2f}" if datos_mkt["day_high"] else ""
                msg_mkt = (
                    f"📈 {datos_mkt['ticker']} en vivo\n\n"
                    f"• Precio: ${datos_mkt['precio']:,.2f} USD\n"
                    f"• Variación: {emoji} {signo}${datos_mkt['var_usd']:,.2f} ({signo}{datos_mkt['var_pct']:.2f}%)\n"
                    f"• Cierre anterior: ${datos_mkt['prev_close']:,.2f}"
                    f"{rango_txt}"
                )
                texto_limpio = f"{texto_limpio}\n\n{msg_mkt}".strip()

        if texto_limpio:
            await update.message.reply_text(texto_limpio)

        if necesita_ver_riesgo:
            metricas = calcular_metricas_riesgo_completas(user_id)
            await update.message.reply_text(metricas["texto"])

        if necesita_ver_presupuestos:
            texto, err = obtener_progreso_presupuestos(user_id)
            await update.message.reply_text(texto if texto else err)

        if necesita_ver_objetivos:
            await update.message.reply_text(obtener_progreso_objetivos(user_id))

        if necesita_escanear_cartera:
            resumen_escaner, fotos_senales = escanear_cartera_senales(user_id)
            await update.message.reply_text(resumen_escaner)
            for buf_foto, cap_foto in fotos_senales:
                await update.message.reply_photo(photo=buf_foto, caption=cap_foto)

        if ticker_at:
            con_fibo = bool(re.search(r'\bfibo\b|\bfibonacci\b|\bretroceso\b', user_msg, re.IGNORECASE))
            con_ext = bool(re.search(r'\bextensi[oó]n\b|\bext\b', user_msg, re.IGNORECASE))
            buf_img, info_at = generar_grafico_analisis_tecnico(ticker_at, tf_at, con_fibo, con_ext)
            if buf_img and info_at and not isinstance(info_at, str):
                tags = "POC + Pivots + RSI"
                if con_fibo or con_ext:
                    tags += " + Fibo"
                cap_txt = f"📈 {ticker_at} ({tf_at.capitalize()}) | {tags}"
                await update.message.reply_photo(photo=buf_img, caption=cap_txt)
                reporte_txt = formatear_reporte_tecnico(info_at)
                await update.message.reply_text(reporte_txt)
            else:
                await update.message.reply_text(f"⚠️ No se pudo generar el análisis técnico de {ticker_at}.")

        if necesita_grafico_por_activos:
            buf_img = generar_grafico_evolucion_por_activos(user_id, periodo_por_activos, tickers_filtro_activos)
            if buf_img:
                filtro_txt = f" ({', '.join(tickers_filtro_activos)})" if tickers_filtro_activos else ""
                await update.message.reply_photo(photo=buf_img, caption=f"📊 Rendimiento relativo (Base 100){filtro_txt}")
            else:
                await update.message.reply_text("No hay suficientes activos que coincidan.")

        elif necesita_grafico_consolidado:
            buf_img = generar_grafico_evolucion_cartera_consolidada(user_id, periodo_consolidado)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📈 Evolución consolidada de cartera (PnL del período)")
            else:
                await update.message.reply_text("No hay datos suficientes para la curva consolidada.")

        elif ticker_grafico_evol:
            buf_img = generar_grafico_evolucion_activo(user_id, ticker_grafico_evol, periodo_activo)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption=f"📈 Evolución de {ticker_grafico_evol}")
            else:
                await update.message.reply_text(f"⚠️ No pude generar la curva de {ticker_grafico_evol}.")

        if necesita_cartera:
            resumen = obtener_resumen_portafolio(user_id)
            with get_db_connection() as conn:
                df_tc = pd.read_sql("SELECT SUM(pnl_usd) as pnl_tot, COUNT(id) as total_c FROM trades_cerrados WHERE user_id = %s;", conn, params=(user_id,))
            pnl_realizado = float(df_tc['pnl_tot'].iloc[0]) if not df_tc.empty and pd.notnull(df_tc['pnl_tot'].iloc[0]) else 0.0
            cant_c = int(df_tc['total_c'].iloc[0]) if not df_tc.empty and pd.notnull(df_tc['total_c'].iloc[0]) else 0
            
            if not resumen and cant_c == 0:
                await update.message.reply_text("📉 No tienes activos ni trades cargados todavía.")
            else:
                total_inv = resumen['total_invertido'] if resumen else 0.0
                total_act = resumen['total_actual'] if resumen else 0.0
                pnl_flot = resumen['pnl_total_usd'] if resumen else 0.0
                pnl_global = pnl_flot + pnl_realizado

                em_flot = "🟢" if pnl_flot >= 0 else "🔴"
                em_real = "🟢" if pnl_realizado >= 0 else "🔴"
                em_glob = "🟢" if pnl_global >= 0 else "🔴"

                msg_rep = (
                    f"💼 ESTADO DE TU CARTERA\n\n"
                    f"• Capital abierto: ${total_inv:,.2f} USD\n"
                    f"• Valor actual: ${total_act:,.2f} USD\n"
                    f"• PnL flotante: {em_flot} {pnl_flot:+,.2f} USD\n"
                    f"• PnL realizado: {em_real} {pnl_realizado:+,.2f} USD ({cant_c} ops)\n"
                    f"• RESULTADO NETO GLOBAL: {em_glob} {pnl_global:+,.2f} USD\n"
                )

                if resumen and resumen["posiciones"]:
                    msg_rep += "\n📊 Posiciones abiertas:\n"
                    for pos in resumen["posiciones"]:
                        pnl_s = "+" if pos["pnl_usd"] >= 0 else ""
                        em = "🟢" if pos["pnl_usd"] >= 0 else "🔴"
                        lev_tag = f"[{pos['tipo_pos']} {pos['lev']:.0f}x]" if pos['tipo_pos'] != "SPOT" else "[SPOT]"
                        dist = f"\n   Dist. liquidación: {pos['dist_liq_pct']:.1f}%" if pos.get("dist_liq_pct") is not None else ""
                        msg_rep += (
                            f"\n▪️ ID {pos['id']}  {pos['ticker']} {lev_tag}\n"
                            f"   Margen ${pos['costo_margen']:,.0f}  |  PPC ${pos['ppc']:,.2f}  |  Spot ${pos['spot']:,.2f}\n"
                            f"   PnL {em} {pnl_s}${pos['pnl_usd']:,.2f} ({pnl_s}{pos['pnl_pct']:.1f}%){dist}"
                        )
                await update.message.reply_text(msg_rep)

        if necesita_grafico_inv:
            buf_img = generar_grafico_distribucion_inversiones(user_id)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📊 Distribución de posiciones abiertas")

        if necesita_grafico_gastos:
            buf_img = generar_grafico_gastos(user_id)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📊 Distribución de gastos por categoría (ARS)")

        if necesita_excel:
            excel_buf = generar_excel_completo(user_id)
            if excel_buf:
                await update.message.reply_document(
                    document=excel_buf, 
                    filename="Finanzas_Consolidadas.xlsx", 
                    caption="📁 Planilla completa: Gastos (ARS) + Posiciones abiertas + Trades cerrados"
                )

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

async def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", cmd_borrar_todo))
    app.add_handler(CommandHandler("resumen", cmd_resumen))
    app.add_handler(CommandHandler("riesgo", cmd_riesgo))
    app.add_handler(CommandHandler("mes", cmd_mes))
    app.add_handler(CommandHandler("objetivos", cmd_objetivos))
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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, responder))
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    
    asyncio.create_task(tarea_alertas_periodicas(app))
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

