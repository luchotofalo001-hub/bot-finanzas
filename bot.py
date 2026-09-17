import os
import io
import re
import asyncio
import logging
import threading
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from google import genai
import psycopg2
import pandas as pd
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
                        descripcion TEXT
                    );
                """)
                # Migraciones y normalización de columnas
                cursor.execute("ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS tipo_posicion VARCHAR(10) DEFAULT 'SPOT';")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS apalancamiento NUMERIC DEFAULT 1;")
                cursor.execute("ALTER TABLE portafolio_inversiones ADD COLUMN IF NOT EXISTS precio_liquidacion NUMERIC DEFAULT NULL;")
                cursor.execute("ALTER TABLE trades_cerrados ADD COLUMN IF NOT EXISTS user_id BIGINT;")

                # Asignar todo lo previo huérfano a Lucho
                cursor.execute("UPDATE movimientos SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))
                cursor.execute("UPDATE portafolio_inversiones SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))
                cursor.execute("UPDATE trades_cerrados SET user_id = %s WHERE user_id IS NULL;", (LUCHO_TELEGRAM_ID,))

                # NORMALIZACIÓN AUTOMÁTICA DE POSICIONES ABIERTAS
                cursor.execute("""
                    UPDATE portafolio_inversiones 
                    SET tipo_posicion = 'SPOT' 
                    WHERE tipo_posicion IS NULL OR TRIM(tipo_posicion) = '';
                """)
                cursor.execute("""
                    UPDATE portafolio_inversiones 
                    SET apalancamiento = 1 
                    WHERE apalancamiento IS NULL OR apalancamiento <= 0;
                """)
                cursor.execute("""
                    UPDATE portafolio_inversiones 
                    SET precio_compra = monto_total_usd / cantidad 
                    WHERE (precio_compra IS NULL OR precio_compra <= 0) AND cantidad > 0;
                """)
                conn.commit()
        logger.info("Tablas inicializadas y posiciones abiertas normalizadas con éxito.")
    except Exception as e:
        logger.error(f"Error en init_db: {e}")

init_db()

# ==================== CONSULTAS DE MERCADO EN VIVO ====================
def normalizar_ticker_yf(ticker: str):
    ticker = ticker.strip().upper()
    if ticker in ["BTC", "ETH", "SOL", "BNB", "ADA", "XRP"]:
        return f"{ticker}-USD"
    return ticker

def consultar_datos_mercado(ticker: str):
    ticker = ticker.strip().upper()
    simbolos_a_probar = [normalizar_ticker_yf(ticker)]
    if not ticker.endswith("-USD") and ticker not in ["BTC", "ETH", "SOL", "BNB", "ADA", "XRP"]:
        simbolos_a_probar.append(f"{ticker}-USD")

    for sym in simbolos_a_probar:
        try:
            t = yf.Ticker(sym)
            df_hist = t.history(period="5d")
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
            fi = t.fast_info
            last_price = getattr(fi, "last_price", None) or fi.get("last_price", None)
            if last_price:
                prev_close = getattr(fi, "previous_close", None) or fi.get("previous_close", last_price)
                var_usd = (last_price - prev_close) if prev_close else 0.0
                var_pct = (var_usd / prev_close * 100) if prev_close else 0.0
                return {
                    "ticker": sym,
                    "precio": float(last_price),
                    "prev_close": float(prev_close) if prev_close else None,
                    "var_usd": float(var_usd),
                    "var_pct": float(var_pct),
                    "day_high": getattr(fi, "day_high", None),
                    "day_low": getattr(fi, "day_low", None)
                }
        except Exception as e:
            logger.error(f"Error consultando ticker {sym}: {e}")
            pass
    return None

def obtener_precio_actual(ticker: str):
    datos = consultar_datos_mercado(ticker)
    if datos:
        return datos["precio"], datos["ticker"]
    return None, ticker

# ==================== CÁLCULO DE FECHAS SEGÚN PERIODO ====================
def resolver_fecha_inicio(periodo_str: str, fecha_compra_db: str = None):
    p = periodo_str.strip().lower() if periodo_str else ""
    hoy = datetime.now()
    
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
    
    if fecha_compra_db:
        return fecha_compra_db, f"Desde tu compra ({fecha_compra_db})"
    
    return (hoy - timedelta(days=180)).strftime('%Y-%m-%d'), "Últimos 6 meses"

# ==================== GRÁFICOS DE EVOLUCIÓN HISTÓRICA ====================
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

def generar_grafico_evolucion_cartera(user_id: int, periodo_solicitado: str = ""):
    try:
        with get_db_connection() as conn:
            df = pd.read_sql(
                "SELECT ticker, MIN(fecha) as primera_compra FROM portafolio_inversiones WHERE user_id = %s GROUP BY ticker;",
                conn, params=(user_id,)
            )
        
        if df.empty:
            return None
        
        primera_fecha_db = df['primera_compra'].min().strftime('%Y-%m-%d')
        fecha_start, desc_periodo = resolver_fecha_inicio(periodo_solicitado, primera_fecha_db)
        
        series_dict = {}
        for _, row in df.iterrows():
            tk = row['ticker']
            sym = normalizar_ticker_yf(tk)
            try:
                h = yf.Ticker(sym).history(start=fecha_start)
                if not h.empty:
                    s = h['Close']
                    s.index = pd.to_datetime(s.index).tz_localize(None)
                    series_dict[tk] = s
            except Exception:
                pass

        if not series_dict:
            return None

        df_precios = pd.DataFrame(series_dict)
        df_precios = df_precios.resample('D').last().ffill().bfill()
        
        df_norm = pd.DataFrame()
        for col in df_precios.columns:
            primer_val = df_precios[col].iloc[0]
            if primer_val > 0:
                df_norm[col] = (df_precios[col] / primer_val) * 100

        fig, ax = plt.subplots(figsize=(10, 5.2))
        colores = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2']
        
        for idx, col in enumerate(df_norm.columns):
            c = colores[idx % len(colores)]
            ax.plot(df_norm.index, df_norm[col], label=f"{col} ({df_norm[col].iloc[-1]:.1f}%)", linewidth=2.2, color=c)

        ax.axhline(y=100, color="gray", linestyle=":", linewidth=1.3, alpha=0.75, label="Base 100")
        ax.set_title(f"Rendimiento Relativo de Cartera (Base 100)\n{desc_periodo}", fontsize=12, fontweight='bold', pad=12)
        ax.set_xlabel("Fecha")
        ax.set_ylabel("Rendimiento Relativo (%)")
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
        logger.error(f"Error generando evolución cartera: {e}")
        return None

# ==================== INVERSIONES SPOT Y FUTUROS (USD) ====================
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
                    """INSERT INTO portafolio_inversIONES (user_id, fecha, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento, precio_liquidacion)
                       VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s);""",
                    (user_id, ticker, float(cantidad), float(precio_compra), float(monto_usd), tipo_pos, float(lev), float(precio_liq) if precio_liq else None)
                )
            conn.commit()
    return ticker, cantidad, precio_compra, monto_usd, fecha_limpia, tipo_pos, lev, precio_liq

# ==================== TRADES CERRADOS Y CIERRE DE POSICIONES ABIERTAS ====================
def registrar_trade_cerrado(user_id: int, ticker: str, pnl_usd: float, tipo_posicion: str = "SPOT", roi_pct: float = None, monto_invertido: float = None, descripcion: str = "", fecha_str: str = None):
    ticker = ticker.strip().upper()
    tipo_pos = tipo_posicion.strip().upper() if tipo_posicion else "SPOT"
    
    fecha_limpia = None
    if fecha_str:
        f_cand = fecha_str.strip().split()[0]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f_cand):
            fecha_limpia = f_cand

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_limpia:
                cursor.execute(
                    """INSERT INTO trades_cerrados (user_id, fecha, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;""",
                    (user_id, fecha_limpia, ticker, tipo_pos, float(pnl_usd), float(roi_pct) if roi_pct else None, float(monto_invertido) if monto_invertido else None, descripcion)
                )
            else:
                cursor.execute(
                    """INSERT INTO trades_cerrados (user_id, fecha, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion)
                       VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s) RETURNING id;""",
                    (user_id, ticker, tipo_pos, float(pnl_usd), float(roi_pct) if roi_pct else None, float(monto_invertido) if monto_invertido else None, descripcion)
                )
            new_id = cursor.fetchone()[0]
            conn.commit()
            return new_id, ticker, pnl_usd, tipo_pos, roi_pct, fecha_limpia

def cerrar_posicion_abierta_por_id(user_id: int, inv_id: int, pnl_manual: float = None, precio_salida_manual: float = None):
    """Cierra una posición que estaba en portafolio_inversiones y la pasa a trades_cerrados"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, ticker, cantidad, precio_compra, monto_total_usd, tipo_posicion, apalancamiento FROM portafolio_inversiones WHERE id = %s AND user_id = %s;",
                (inv_id, user_id)
            )
            reg = cursor.fetchone()
            if not reg:
                return None, "Posición abierta no encontrada"
            
            _, ticker, cant, ppc, margen, tipo_pos, lev = reg
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
                else: # SPOT
                    pnl_final = (cant * spot) - margen
            
            roi_final = (pnl_final / margen * 100) if margen > 0 else 0.0
            desc = f"Posición cerrada (Entrada: ${ppc:,.2f})"

            cursor.execute(
                """INSERT INTO trades_cerrados (user_id, fecha, ticker, tipo_posicion, pnl_usd, roi_pct, monto_invertido, descripcion)
                   VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s) RETURNING id;""",
                (user_id, ticker, tipo_pos, pnl_final, roi_final, margen, desc)
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
        "ticker": "ticker",
        "activo": "ticker",
        "cantidad": "cantidad",
        "cant": "cantidad",
        "precio": "precio_compra",
        "ppc": "precio_compra",
        "precio_compra": "precio_compra",
        "monto": "monto_total_usd",
        "monto_total_usd": "monto_total_usd",
        "fecha": "fecha",
        "tipo": "tipo_posicion",
        "tipo_posicion": "tipo_posicion",
        "apalancamiento": "apalancamiento",
        "leverage": "apalancamiento",
        "liq": "precio_liquidacion",
        "precio_liquidacion": "precio_liquidacion"
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
        "monto": "monto",
        "categoria": "categoria",
        "descripcion": "descripcion",
        "desc": "descripcion",
        "fecha": "fecha",
        "tipo": "tipo"
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
        ticker = fila['ticker']
        cant = float(fila['cantidad'])
        costo_margen = float(fila['monto_total_usd'])
        ppc = float(fila['precio_compra']) if fila['precio_compra'] else (costo_margen / cant if cant > 0 else 0.0)
        tipo_pos = str(fila['tipo_posicion']).upper() if fila['tipo_posicion'] else "SPOT"
        lev = float(fila['apalancamiento']) if fila['apalancamiento'] else 1.0
        p_liq = float(fila['precio_liquidacion']) if fila['precio_liquidacion'] else None

        spot, _ = obtener_precio_actual(ticker)
        if spot is None:
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

        total_margen_invertido += costo_margen
        total_valor_actual += valor_actual

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
            "precio_liq": p_liq
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

# ==================== MOTOR DE MÉTRICAS ANALÍTICAS AVANZADAS ====================
def calcular_super_metricas_totales(user_id: int):
    metricas = []
    
    # 1. PnL Realizado de Trades Cerrados
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
            
            metricas.append("=== TRADES CERRADOS (GANANCIAS REALIZADAS EN BOLSILLO) ===")
            metricas.append(f"• PnL Realizado Total: ${pnl_realizado_total:+,.2f} USD")
            metricas.append(f"• Operaciones cerradas registradas: {cant_trades_cerrados} (Win Rate: {win_rate:.1f}%)")
            metricas.append("• Desglose de Trades Cerrados:")
            for _, tc in df_tc.iterrows():
                roi_txt = f" ({tc['roi_pct']:+.2f}%)" if pd.notnull(tc['roi_pct']) else ""
                metricas.append(f"   - [{tc['fecha'].strftime('%Y-%m-%d')}] {tc['ticker']} ({tc['tipo_posicion']}): PnL ${tc['pnl_usd']:+,.2f} USD{roi_txt} - {tc['descripcion']}")
        else:
            metricas.append("=== TRADES CERRADOS: Sin historial de ganancias realizadas cargado ===")
    except Exception as e:
        logger.error(f"Error métricas trades cerrados: {e}")

    # 2. Posiciones Abiertas
    try:
        resumen = obtener_resumen_portafolio(user_id)
        if resumen and resumen["posiciones"]:
            pnl_no_realizado = resumen['pnl_total_usd']
            pnl_neto_global_combinado = pnl_realizado_total + pnl_no_realizado
            
            metricas.append("\n=== CARTERA ACTUAL (TRADES / POSICIONES ABIERTAS) ===")
            metricas.append(f"• Capital/Margen Total Abierto: ${resumen['total_invertido']:,.2f} USD")
            metricas.append(f"• Valor de Mercado Actual: ${resumen['total_actual']:,.2f} USD")
            metricas.append(f"• PnL Flotante (No Realizado): ${pnl_no_realizado:+,.2f} USD ({resumen['pnl_total_pct']:+.2f}%)")
            metricas.append(f"• PnL TOTAL HISTÓRICO CONSOLIDADO (Realizado + Flotante): ${pnl_neto_global_combinado:+,.2f} USD")
            
            pos_ordenadas = sorted(resumen["posiciones"], key=lambda x: x['pnl_pct'], reverse=True)
            top = pos_ordenadas[0]
            worst = pos_ordenadas[-1]
            metricas.append(f"• Posición abierta más rentable: {top['ticker']} ({top['tipo_pos']} {top['lev']:.0f}x) con {top['pnl_pct']:+.2f}% (${top['pnl_usd']:,.2f} USD)")
            metricas.append(f"• Posición abierta más rezagada: {worst['ticker']} ({worst['tipo_pos']} {worst['lev']:.0f}x) con {worst['pnl_pct']:+.2f}% (${worst['pnl_usd']:,.2f} USD)")
            
            metricas.append("• Detalle de Posiciones Abiertas:")
            for p in resumen["posiciones"]:
                liq_txt = f" | Liq: ${p['precio_liq']:,.2f}" if p['precio_liq'] else ""
                lev_txt = f" [{p['tipo_pos']} {p['lev']:.0f}x]" if p['tipo_pos'] != "SPOT" else " [SPOT]"
                metricas.append(
                    f"   - ID {p['id']}: {p['ticker']}{lev_txt} (ABIERTO) | Margen: ${p['costo_margen']:,.2f} USD | PPC: ${p['ppc']:,.2f} | Spot: ${p['spot']:,.2f} | PnL: ${p['pnl_usd']:,.2f} ({p['pnl_pct']:+.2f}%){liq_txt}"
                )
        else:
            metricas.append("\n=== CARTERA ACTUAL: Sin trades o posiciones abiertas ===")
    except Exception as e:
        logger.error(f"Error métricas de inversión: {e}")

    # 3. Flujo de caja ARS
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

# ==================== HISTORIAL Y BORRADO POR USUARIO ====================
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

REGLAS DE CIERRE DE POSICIÓN ABIERTA:
- Si el usuario dice que cerró una posición que tenía abierta (ej: "cerré el trade de btc", "cerré la posición ID 2 con 150 usd de ganancia", "cerré el long de SOL a precio actual"):
  Identifica el ID de la posición abierta (o el ticker) y el PnL obtenido si lo indica.
  Escribe: ACCION: CERRAR_POSICION|[ID]|[PNL_USD_MANUAL_O_VACIO]|[PRECIO_SALIDA_O_VACIO]

REGLAS DE REGISTRO DE TRADES CERRADOS PASADOS:
- Si el usuario menciona una ganancia o trade pasado que ya cerró hace tiempo (ej: "gané 450 usd en un trade de sol el mes pasado"):
  REGISTRO_TRADE_CERRADO: [TICKER]|[PNL_USD]|[TIPO_POS]|[ROI_PCT]|[MONTO_INVERTIDO]|[DESCRIPCION]|[FECHA_YYYY-MM-DD]

REGLAS DE REGISTRO DE POSICIONES ABIERTAS (SPOT / FUTUROS):
- Si el usuario abre un trade o compra activa:
  REGISTRO_INV: [TICKER]|[MARGEN_USD]|[PPC]|[CANTIDAD]|[FECHA_YYYY-MM-DD]|[TIPO_POS]|[APALANCAMIENTO]|[PRECIO_LIQ]

REGLAS DE MODIFICACIÓN Y AGREGADO DE MARGEN:
- Agregar margen: ACCION: AGREGAR_MARGEN|[ID]|[MONTO_EXTRA_USD]
- Modificar posición abierta: ACCION: MODIFICAR_INVERSION|[ID]|[CAMPO]|[NUEVO_VALOR]
- Modificar gasto/ingreso: ACCION: MODIFICAR_MOVIMIENTO|[ID]|[CAMPO]|[NUEVO_VALOR]

REGLAS DE GRÁFICOS:
- Activo específico con periodo: ACCION: GRAFICO_EVOLUCION_ACTIVO|[TICKER]|[PERIODO_DETECTADO]
- Cartera general con periodo: ACCION: GRAFICO_EVOLUCION_CARTERA|[PERIODO_DETECTADO]

REGLAS DE BORRADO:
- Borrar por ticker: ACCION: BORRAR_INVERSION_TICKER|[TICKER]
- Borrar posición abierta por ID: ACCION: BORRAR_INVERSION_ID|[ID]
- Borrar trade cerrado por ID: ACCION: BORRAR_TRADE_CERRADO_ID|[ID]
- Borrar movimiento ARS por ID: ACCION: BORRAR_MOVIMIENTO_ID|[ID]
- Borrar último registro: ACCION: BORRAR_ULTIMO
- Resetear todo: ACCION: BORRAR_TODO

REGLAS GENERALES:
- Registro ARS: REGISTRO_ARS: [TIPO]|[MONTO]|[CATEGORIA]|[DESCRIPCION]|[FECHA_YYYY-MM-DD]
- Ver cartera y balance: ACCION: VER_CARTERA
- Cotización en vivo: ACCION: CONSULTA_PRECIO|[TICKER]
- Gráfico torta inversiones: ACCION: GRAFICO_INVERSIONES
- Gráfico torta gastos: ACCION: GRAFICO_GASTOS
- Excel: ACCION: EXCEL
"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy tu asistente financiero y analista cuantitativo institucional.\n\n"
        "🟢 Trades Abiertos (En Vivo):\n"
        "• '¿Cómo vienen mis trades abiertos?'\n"
        "• 'Cerré la posición ID 2 con 150 usd de ganancia'\n\n"
        "🏆 Trades Cerrados y Ganancias Realizadas:\n"
        "• 'Gané 450 usd en un trade de SOL el mes pasado'\n\n"
        "⚡ Futuros y Apalancamiento:\n"
        "• 'Abrí un Long en BTC x10 con 500 usd a 60.000'\n"
        "• 'Agregué 150 usd de margen a la posición ID 1'\n\n"
        "💼 Cartera Consolidada:\n"
        "• '¿Cuál es mi balance total?' / 'Mandame un excel'"
    )

async def cmd_borrar_todo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    borrar_todos_los_movimientos(user_id)
    await update.message.reply_text("🗑️ Tu base de datos, cartera y trades cerrados han sido reseteados.")

async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = update.message.text
    
    historial_unificado = obtener_historial_completo_texto(user_id)
    metricas_cuantitativas = calcular_super_metricas_totales(user_id)
    
    prompt = (
        f"MÉTRICAS CUANTITATIVAS REALES DEL USUARIO:\n{metricas_cuantitativas}\n\n"
        f"HISTORIAL DETALLADO REGISTRADO DEL USUARIO (con IDs y Fechas):\n{historial_unificado}\n\n"
        f"MENSAJE DEL USUARIO: {user_msg}"
    )

    try:
        response = ai_client.models.generate_content(
            model="gemini-3.6-flash",
            contents=prompt,
            config={"system_instruction": SYSTEM_INSTRUCTION}
        )
        reply = response.text
        
        necesita_cartera = "ACCION: VER_CARTERA" in reply
        necesita_grafico_inv = "ACCION: GRAFICO_INVERSIONES" in reply
        necesita_grafico_gastos = "ACCION: GRAFICO_GASTOS" in reply
        necesita_excel = "ACCION: EXCEL" in reply
        necesita_borrar_todo = "ACCION: BORRAR_TODO" in reply
        necesita_borrar_ultimo = "ACCION: BORRAR_ULTIMO" in reply
        
        texto_limpio = reply
        for tag in ["ACCION: VER_CARTERA", "ACCION: GRAFICO_INVERSIONES", "ACCION: GRAFICO_GASTOS", "ACCION: EXCEL", "ACCION: BORRAR_TODO", "ACCION: BORRAR_ULTIMO"]:
            texto_limpio = texto_limpio.replace(tag, "")

        # Cotización
        ticker_a_cotizar = None
        m_precio = re.search(r"ACCION: CONSULTA_PRECIO\|([^\n\r]+)", texto_limpio)
        if m_precio:
            ticker_a_cotizar = m_precio.group(1).strip()
            texto_limpio = texto_limpio.replace(m_precio.group(0), "")

        # Gráficos con periodos
        ticker_grafico_evol = None
        periodo_activo = ""
        m_ga = re.search(r"ACCION: GRAFICO_EVOLUCION_ACTIVO\|([^\n\r|]+)(?:\|([^\n\r]+))?", texto_limpio)
        if m_ga:
            ticker_grafico_evol = m_ga.group(1).strip()
            periodo_activo = m_ga.group(2).strip() if m_ga.group(2) else ""
            texto_limpio = texto_limpio.replace(m_ga.group(0), "")

        necesita_grafico_evol_cartera = False
        periodo_cartera = ""
        m_gc = re.search(r"ACCION: GRAFICO_EVOLUCION_CARTERA(?:\|([^\n\r]+))?", texto_limpio)
        if m_gc:
            necesita_grafico_evol_cartera = True
            periodo_cartera = m_gc.group(1).strip() if m_gc.group(1) else ""
            texto_limpio = texto_limpio.replace(m_gc.group(0), "")

        # Cierre de posición abierta
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
                texto_limpio += f"\n\n🎯 *(Trade Cerrado con éxito: {res_cierre['ticker']} [{res_cierre['tipo_pos']}] | PnL Realizado: {signo_pnl}${res_cierre['pnl_usd']:,.2f} USD{roi_str} - Pasado a histórico ID {res_cierre['tc_id']})*"
            else:
                texto_limpio += f"\n\n⚠️ No se pudo cerrar la posición: {msg_cierre}"

        # Agregar margen
        m_add_m = re.search(r"ACCION: AGREGAR_MARGEN\|(\d+)\|([^\n\r]+)", texto_limpio)
        if m_add_m:
            inv_id_m = int(m_add_m.group(1))
            m_extra = float(m_add_m.group(2).strip())
            texto_limpio = texto_limpio.replace(m_add_m.group(0), "")
            reg, info = agregar_margen_a_posicion(user_id, inv_id_m, m_extra)
            if reg:
                texto_limpio += f"\n\n🛡️ *(Margen extra agregado a {reg[1]}: +${m_extra:,.2f} USD | {info})*"
            else:
                texto_limpio += f"\n\n⚠️ No se encontró la posición con ID {inv_id_m}."

        # Modificaciones
        m_mod_inv = re.search(r"ACCION: MODIFICAR_INVERSION\|(\d+)\|([^|\n\r]+)\|([^\n\r]+)", texto_limpio)
        if m_mod_inv:
            inv_id = int(m_mod_inv.group(1))
            campo = m_mod_inv.group(2).strip()
            val = m_mod_inv.group(3).strip()
            texto_limpio = texto_limpio.replace(m_mod_inv.group(0), "")
            prev, info = modificar_inversion_por_id(user_id, inv_id, campo, val)
            texto_limpio += f"\n\n✏️ *(Inversión ID {inv_id}: {info})*"

        m_mod_mov = re.search(r"ACCION: MODIFICAR_MOVIMIENTO\|(\d+)\|([^|\n\r]+)\|([^\n\r]+)", texto_limpio)
        if m_mod_mov:
            mov_id = int(m_mod_mov.group(1))
            campo = m_mod_mov.group(2).strip()
            val = m_mod_mov.group(3).strip()
            texto_limpio = texto_limpio.replace(m_mod_mov.group(0), "")
            prev, info = modificar_movimiento_por_id(user_id, mov_id, campo, val)
            texto_limpio += f"\n\n✏️ *(Movimiento ID {mov_id}: {info})*"

        # Borrados
        m_btk = re.search(r"ACCION: BORRAR_INVERSION_TICKER\|([^\n\r]+)", texto_limpio)
        if m_btk:
            tk_b = m_btk.group(1).strip()
            texto_limpio = texto_limpio.replace(m_btk.group(0), "")
            c_del = borrar_inversion_por_ticker(user_id, tk_b)
            texto_limpio += f"\n\n🗑️ *(Se eliminaron {c_del} registros de {tk_b} de tu cartera)*"

        m_bid = re.search(r"ACCION: BORRAR_INVERSION_ID\|(\d+)", texto_limpio)
        if m_bid:
            iid_b = int(m_bid.group(1))
            texto_limpio = texto_limpio.replace(m_bid.group(0), "")
            r_del = borrar_inversion_por_id(user_id, iid_b)
            if r_del:
                texto_limpio += f"\n\n🗑️ *(Eliminada posición abierta ID {iid_b}: {r_del[1]})*"

        m_btc_id = re.search(r"ACCION: BORRAR_TRADE_CERRADO_ID\|(\d+)", texto_limpio)
        if m_btc_id:
            tcid_b = int(m_btc_id.group(1))
            texto_limpio = texto_limpio.replace(m_btc_id.group(0), "")
            r_del = borrar_trade_cerrado_por_id(user_id, tcid_b)
            if r_del:
                texto_limpio += f"\n\n🗑️ *(Eliminado trade cerrado ID {tcid_b}: {r_del[1]})*"

        m_bmid = re.search(r"ACCION: BORRAR_MOVIMIENTO_ID\|(\d+)", texto_limpio)
        if m_bmid:
            mid_b = int(m_bmid.group(1))
            texto_limpio = texto_limpio.replace(m_bmid.group(0), "")
            r_del = borrar_movimiento_por_id(user_id, mid_b)
            if r_del:
                texto_limpio += f"\n\n🗑️ *(Eliminado gasto/ingreso ID {mid_b})*"

        # Registro Trade Cerrado Pasado
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
            f_txt = f" - Fecha: {f}" if f else ""
            texto_limpio = f"{texto_limpio}\n\n🏆 *(Trade Cerrado Registrado: {t} [{tp}] | PnL Realizado: {signo_p}${p:,.2f} USD{roi_s}{f_txt} - ID {tid})*".strip()

        # Registro Inversión Abierta
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
            fecha_str = f" - Fecha: {f_reg}" if f_reg else ""
            lev_str = f" [{pos_t} {lev_t:.0f}x]" if pos_t != "SPOT" else " [SPOT]"
            liq_str = f" | Liq est: ${liq_t:,.2f}" if liq_t else ""
            texto_limpio = f"{texto_limpio}\n\n💼 *(Guardado como Abierto: {t}{lev_str} | Margen: ${m:,.2f} USD | PPC: ${p:,.2f} | Cant: {c:,.4f}{liq_str}{fecha_str})*".strip()

        # Registro ARS
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
            fecha_str = f" - Fecha: {f_gasto}" if f_gasto else ""
            texto_limpio = f"{texto_limpio}\n\n✅ *(Guardado: {tipo} de ${monto:,.2f} ARS en {categoria}{fecha_str})*".strip()

        texto_limpio = texto_limpio.strip()

        if necesita_borrar_todo:
            borrar_todos_los_movimientos(user_id)
            texto_limpio += "\n\n🗑️ *(Tu base de datos, cartera y trades cerrados han sido reseteados)*"

        if necesita_borrar_ultimo:
            res_ultimo = borrar_ultimo_registro_general(user_id)
            if res_ultimo:
                texto_limpio += f"\n\n🗑️ *(Eliminado: {res_ultimo})*"
            else:
                texto_limpio += "\n\n⚠️ No había registros para borrar."

        # Cotización puntual
        if ticker_a_cotizar:
            datos_mkt = consultar_datos_mercado(ticker_a_cotizar)
            if datos_mkt:
                signo = "+" if datos_mkt["var_pct"] >= 0 else ""
                emoji = "🟢" if datos_mkt["var_pct"] >= 0 else "🔴"
                rango_txt = f"\n• Rango del día: ${datos_mkt['day_low']:,.2f} - ${datos_mkt['day_high']:,.2f} USD" if datos_mkt["day_high"] else ""
                msg_mkt = (
                    f"📈 {datos_mkt['ticker']} en vivo:\n\n"
                    f"• Precio actual: ${datos_mkt['precio']:,.2f} USD\n"
                    f"• Variación del día: {emoji} {signo}${datos_mkt['var_usd']:,.2f} USD ({signo}{datos_mkt['var_pct']:.2f}%)\n"
                    f"• Cierre anterior: ${datos_mkt['prev_close']:,.2f} USD"
                    f"{rango_txt}"
                )
                texto_limpio = f"{texto_limpio}\n\n{msg_mkt}".strip()

        # Envío seguro
        if texto_limpio:
            try:
                await update.message.reply_text(texto_limpio, parse_mode="Markdown")
            except Exception:
                await update.message.reply_text(texto_limpio)

        # Gráfico activo puntual
        if ticker_grafico_evol:
            buf_img = generar_grafico_evolucion_activo(user_id, ticker_grafico_evol, periodo_activo)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption=f"📈 Evolución de {ticker_grafico_evol}.")
            else:
                await update.message.reply_text(f"⚠️ No pude generar la curva de evolución para {ticker_grafico_evol}.")

        # Gráfico evolución cartera
        if necesita_grafico_evol_cartera:
            buf_img = generar_grafico_evolucion_cartera(user_id, periodo_cartera)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📈 Evolución relativa de tus activos (Base 100).")
            else:
                await update.message.reply_text("No hay suficientes activos registrados en tu cartera para armar el gráfico comparativo.")

        # Reporte de cartera
        if necesita_cartera:
            resumen = obtener_resumen_portafolio(user_id)
            
            # Obtener PnL Realizado
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
                    f"💼 ESTADO DE TU CARTERA CONSOLIDADA\n\n"
                    f"• Capital Abierto en Cartera: ${total_inv:,.2f} USD\n"
                    f"• Valor Actual de Posiciones: ${total_act:,.2f} USD\n"
                    f"• PnL Flotante (No Realizado): {em_flot} {pnl_flot:+,.2f} USD\n"
                    f"• PnL Realizado (Trades Cerrados): {em_real} {pnl_realizado:+,.2f} USD ({cant_c} ops)\n"
                    f"• RESULTADO NETO HISTÓRICO GLOBAL: {em_glob} {pnl_global:+,.2f} USD\n\n"
                )

                if resumen and resumen["posiciones"]:
                    msg_rep += "📊 Posiciones / Trades Abiertos Actualmente:\n"
                    for pos in resumen["posiciones"]:
                        pnl_s = "+" if pos["pnl_usd"] >= 0 else ""
                        em = "🟢" if pos["pnl_usd"] >= 0 else "🔴"
                        lev_tag = f"[{pos['tipo_pos']} {pos['lev']:.0f}x]" if pos['tipo_pos'] != "SPOT" else "[SPOT]"
                        liq_tag = f"\n   - Liquidación est: ${pos['precio_liq']:,.2f} USD" if pos['precio_liq'] else ""
                        msg_rep += (
                            f"▪️ ID {pos['id']} | *{pos['ticker']}* {lev_tag} (ABIERTO):\n"
                            f"   - Margen: ${pos['costo_margen']:,.2f} USD | Cant: {pos['cantidad']:,.4f}\n"
                            f"   - Entrada: ${pos['ppc']:,.2f} | Spot: ${pos['spot']:,.2f} USD\n"
                            f"   - PnL Flotante: {em} {pnl_s}${pos['pnl_usd']:,.2f} USD ({pnl_s}{pos['pnl_pct']:.2f}%){liq_tag}\n\n"
                        )
                try:
                    await update.message.reply_text(msg_rep, parse_mode="Markdown")
                except Exception:
                    await update.message.reply_text(msg_rep)

        if necesita_grafico_inv:
            buf_img = generar_grafico_distribucion_inversiones(user_id)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📊 Asset Allocation: Distribución de posiciones abiertas.")

        if necesita_grafico_gastos:
            buf_img = generar_grafico_gastos(user_id)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📊 Distribución de tus gastos por categoría (ARS).")

        if necesita_excel:
            excel_buf = generar_excel_completo(user_id)
            if excel_buf:
                await update.message.reply_document(
                    document=excel_buf, 
                    filename="Finanzas_Consolidadas.xlsx", 
                    caption="📁 Planilla completa con Gastos (ARS), Posiciones Abiertas (USD) y Trades Cerrados con Ganancias."
                )

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text(f"Hubo un error: {e}")

async def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", cmd_borrar_todo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, responder))
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

