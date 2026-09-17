import os
import io
import re
import asyncio
import logging
import threading
from datetime import datetime
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
                        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ticker VARCHAR(20),
                        cantidad NUMERIC,
                        precio_compra NUMERIC,
                        monto_total_usd NUMERIC
                    );
                """)
                conn.commit()
        logger.info("Tablas inicializadas.")
    except Exception as e:
        logger.error(f"Error en init_db: {e}")

init_db()

# ==================== CONSULTAS DE MERCADO EN VIVO ====================
def consultar_datos_mercado(ticker: str):
    ticker = ticker.strip().upper()
    simbolos_a_probar = [ticker]
    
    if ticker in ["BTC", "ETH", "SOL", "BNB", "ADA", "XRP"]:
        simbolos_a_probar = [f"{ticker}-USD"]
    elif not ticker.endswith("-USD"):
        simbolos_a_probar.append(f"{ticker}-USD")

    for sym in simbolos_a_probar:
        try:
            t = yf.Ticker(sym)
            df_hist = t.history(period="5d")
            if not df_hist.empty:
                last_price = float(df_hist['Close'].iloc[-1])
                day_high = float(df_hist['High'].iloc[-1])
                day_low = float(df_hist['Low'].iloc[-1])
                
                if len(df_hist) > 1:
                    prev_close = float(df_hist['Close'].iloc[-2])
                else:
                    prev_close = float(df_hist['Open'].iloc[-1])
                    
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

# ==================== GRÁFICOS DE EVOLUCIÓN ====================
def generar_grafico_evolucion_activo(ticker: str, periodo_default: str = "6mo"):
    ticker = ticker.strip().upper()
    simbolo = f"{ticker}-USD" if ticker in ["BTC", "ETH", "SOL", "BNB", "ADA", "XRP"] else ticker
    
    fecha_inicio = None
    ppc_referencia = None
    
    try:
        with get_db_connection() as conn:
            df_inv = pd.read_sql(
                "SELECT fecha, cantidad, monto_total_usd FROM portafolio_inversiones WHERE UPPER(ticker) = %s ORDER BY fecha ASC;",
                conn, params=(ticker,)
            )
            if not df_inv.empty and df_inv['cantidad'].sum() > 0:
                fecha_inicio = df_inv['fecha'].iloc[0].strftime('%Y-%m-%d')
                ppc_referencia = df_inv['monto_total_usd'].sum() / df_inv['cantidad'].sum()
    except Exception as e:
        logger.error(f"Error consultando fecha de compra para {ticker}: {e}")

    try:
        t = yf.Ticker(simbolo)
        if fecha_inicio:
            df_hist = t.history(start=fecha_inicio)
        else:
            df_hist = t.history(period=periodo_default)

        if df_hist.empty and not simbolo.endswith("-USD"):
            simbolo = f"{simbolo}-USD"
            t = yf.Ticker(simbolo)
            if fecha_inicio:
                df_hist = t.history(start=fecha_inicio)
            else:
                df_hist = t.history(period=periodo_default)

        if df_hist.empty:
            return None

        plt.figure(figsize=(9, 5))
        color_linea = "#2b5c8f"
        plt.plot(df_hist.index, df_hist['Close'], label=f"{ticker} (USD)", color=color_linea, linewidth=2.2)
        plt.fill_between(df_hist.index, df_hist['Close'], alpha=0.15, color=color_linea)
        
        if ppc_referencia:
            plt.axhline(y=ppc_referencia, color="#d62728", linestyle="--", linewidth=1.5, label=f"Tu PPC (${ppc_referencia:,.2f})")
            subtitulo = f"Desde tu primera compra ({fecha_inicio})"
        else:
            subtitulo = "Últimos 6 meses"

        plt.title(f"Evolución Histórica: {ticker}\n{subtitulo}", fontsize=12, fontweight='bold', pad=12)
        plt.xlabel("Fecha")
        plt.ylabel("Precio USD")
        plt.grid(True, linestyle="--", alpha=0.4)
        plt.legend(loc="upper left")
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200)
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Error generando evolución para {ticker}: {e}")
        return None

def generar_grafico_evolucion_cartera():
    try:
        with get_db_connection() as conn:
            df = pd.read_sql("SELECT ticker, MIN(fecha) as primera_compra FROM portafolio_inversiones GROUP BY ticker;", conn)
        
        if df.empty:
            return None
        
        primera_fecha_global = df['primera_compra'].min().strftime('%Y-%m-%d')
        
        datos_cierre = {}
        for _, row in df.iterrows():
            tk = row['ticker']
            sym = f"{tk}-USD" if tk in ["BTC", "ETH", "SOL", "BNB", "ADA", "XRP"] else tk
            try:
                h = yf.Ticker(sym).history(start=primera_fecha_global)
                if not h.empty and h['Close'].iloc[0] > 0:
                    datos_cierre[tk] = (h['Close'] / h['Close'].iloc[0]) * 100
            except Exception:
                pass
                
        if not datos_cierre:
            return None

        df_comp = pd.DataFrame(datos_cierre)
        plt.figure(figsize=(9, 5))
        for col in df_comp.columns:
            plt.plot(df_comp.index, df_comp[col], label=col, linewidth=2)
            
        plt.axhline(y=100, color="gray", linestyle=":", alpha=0.7)
        plt.title(f"Rendimiento de Cartera (Base 100)\nDesde tu primera inversión ({primera_fecha_global})", fontsize=12, fontweight='bold', pad=12)
        plt.xlabel("Fecha")
        plt.ylabel("Rendimiento Relativo (%)")
        plt.grid(True, linestyle="--", alpha=0.4)
        plt.legend(loc="upper left")
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200)
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Error generando evolución de cartera: {e}")
        return None

# ==================== INVERSIONES (USD) ====================
def registrar_operacion_inversion(ticker: str, monto_usd: float, precio_compra: float = None, cantidad: float = None, fecha_compra: str = None):
    ticker = ticker.strip().upper()
    if (not precio_compra or precio_compra <= 0) and (not cantidad or cantidad <= 0):
        precio_mercado, _ = obtener_precio_actual(ticker)
        precio_compra = precio_mercado if precio_mercado else 1.0
    
    if cantidad is None or cantidad <= 0:
        cantidad = monto_usd / precio_compra if precio_compra > 0 else 0
    elif monto_usd is None or monto_usd <= 0:
        monto_usd = cantidad * precio_compra

    # Limpiar y validar fecha
    fecha_limpia = None
    if fecha_compra:
        f_cand = fecha_compra.strip().split()[0]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f_cand):
            fecha_limpia = f_cand

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_limpia:
                cursor.execute(
                    """INSERT INTO portafolio_inversiones (fecha, ticker, cantidad, precio_compra, monto_total_usd)
                       VALUES (%s, %s, %s, %s, %s);""",
                    (fecha_limpia, ticker, float(cantidad), float(precio_compra), float(monto_usd))
                )
            else:
                cursor.execute(
                    """INSERT INTO portafolio_inversiones (fecha, ticker, cantidad, precio_compra, monto_total_usd)
                       VALUES (NOW(), %s, %s, %s, %s);""",
                    (ticker, float(cantidad), float(precio_compra), float(monto_usd))
                )
            conn.commit()
    return ticker, cantidad, precio_compra, monto_usd, fecha_limpia

def obtener_resumen_portafolio():
    with get_db_connection() as conn:
        df = pd.read_sql("SELECT ticker, cantidad, monto_total_usd FROM portafolio_inversiones;", conn)
    
    if df.empty:
        return None

    posiciones = []
    total_invertido = 0.0
    total_actual = 0.0

    agrupado = df.groupby('ticker').agg({'cantidad': 'sum', 'monto_total_usd': 'sum'}).reset_index()

    for _, fila in agrupado.iterrows():
        ticker = fila['ticker']
        cant = float(fila['cantidad'])
        costo_total = float(fila['monto_total_usd'])
        ppc = costo_total / cant if cant > 0 else 0.0
        
        precio_actual, _ = obtener_precio_actual(ticker)
        if precio_actual is None:
            precio_actual = ppc
            
        valor_mercado = cant * precio_actual
        pnl_usd = valor_mercado - costo_total
        pnl_pct = (pnl_usd / costo_total * 100) if costo_total > 0 else 0.0
        
        total_invertido += costo_total
        total_actual += valor_mercado
        
        posiciones.append({
            "ticker": ticker,
            "cantidad": cant,
            "ppc": ppc,
            "precio_actual": precio_actual,
            "costo_total": costo_total,
            "valor_mercado": valor_mercado,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct
        })

    pnl_total_usd = total_actual - total_invertido
    pnl_total_pct = (pnl_total_usd / total_invertido * 100) if total_invertido > 0 else 0.0

    return {
        "posiciones": posiciones,
        "total_invertido": total_invertido,
        "total_actual": total_actual,
        "pnl_total_usd": pnl_total_usd,
        "pnl_total_pct": pnl_total_pct
    }

def generar_grafico_distribucion_inversiones():
    resumen = obtener_resumen_portafolio()
    if not resumen or not resumen["posiciones"]:
        return None
    
    df = pd.DataFrame(resumen["posiciones"])
    plt.figure(figsize=(7, 6))
    colores = ['#2ca02c', '#1f77b4', '#ff7f0e', '#d62728', '#9467bd', '#8c564b', '#e377c2']
    
    plt.pie(
        df['valor_mercado'],
        labels=[f"{row['ticker']} ({row['valor_mercado']:,.0f} USD)" for _, row in df.iterrows()],
        autopct='%1.1f%%',
        startangle=140,
        colors=colores[:len(df)],
        wedgeprops=dict(width=0.6, edgecolor='w')
    )
    plt.title('Distribución de Cartera de Inversiones (USD)', fontsize=14, pad=20)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=200)
    buf.seek(0)
    plt.close()
    return buf

# ==================== CONSULTAS Y BORRADO UNIVERSAL ====================
def obtener_historial_completo_texto():
    lineas = []
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, fecha, tipo, monto, categoria, descripcion FROM movimientos ORDER BY fecha DESC LIMIT 50;")
            movs = cursor.fetchall()
            if movs:
                lineas.append("📋 GASTOS E INGRESOS (ARS):")
                for m in movs:
                    lineas.append(f"- ID {m[0]} | Fecha: {m[1].strftime('%Y-%m-%d %H:%M')} | {m[2]}: ${m[3]:,.2f} ARS | Cat: {m[4]} | Desc: {m[5]}")
            else:
                lineas.append("📋 GASTOS E INGRESOS: Sin registros.")

            cursor.execute("SELECT id, fecha, ticker, cantidad, precio_compra, monto_total_usd FROM portafolio_inversiones ORDER BY fecha DESC LIMIT 50;")
            invs = cursor.fetchall()
            lineas.append("\n💼 INVERSIONES (USD):")
            if invs:
                for inv in invs:
                    lineas.append(f"- ID {inv[0]} (INV) | Fecha: {inv[1].strftime('%Y-%m-%d %H:%M')} | Ticker: {inv[2]} | Cant: {inv[3]:,.4f} | PPC: ${inv[4]:,.2f} USD | Total: ${inv[5]:,.2f} USD")
            else:
                lineas.append("Sin inversiones registradas.")
    return "\n".join(lineas)

def borrar_inversion_por_ticker(ticker: str):
    ticker = ticker.strip().upper()
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM portafolio_inversiones WHERE UPPER(ticker) = %s RETURNING id;", (ticker,))
            filas = cursor.fetchall()
            conn.commit()
            return len(filas)

def borrar_inversion_por_id(inv_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, ticker, monto_total_usd FROM portafolio_inversiones WHERE id = %s;", (inv_id,))
            reg = cursor.fetchone()
            if reg:
                cursor.execute("DELETE FROM portafolio_inversiones WHERE id = %s;", (inv_id,))
                conn.commit()
                return reg
            return None

def borrar_movimiento_por_id(mov_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, tipo, monto, categoria, descripcion FROM movimientos WHERE id = %s;", (mov_id,))
            reg = cursor.fetchone()
            if reg:
                cursor.execute("DELETE FROM movimientos WHERE id = %s;", (mov_id,))
                conn.commit()
                return reg
            return None

def borrar_ultimo_registro_general():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 'MOV' as origen, id, fecha, tipo, monto FROM movimientos ORDER BY id DESC LIMIT 1;")
            ultimo_mov = cursor.fetchone()
            cursor.execute("SELECT 'INV' as origen, id, fecha, ticker, monto_total_usd FROM portafolio_inversiones ORDER BY id DESC LIMIT 1;")
            ultimo_inv = cursor.fetchone()
            
            if not ultimo_mov and not ultimo_inv:
                return None
            
            if ultimo_mov and not ultimo_inv:
                cursor.execute("DELETE FROM movimientos WHERE id = %s;", (ultimo_mov[1],))
                conn.commit()
                return f"Gasto/Ingreso {ultimo_mov[3]} de ${ultimo_mov[4]:,.2f} ARS (ID {ultimo_mov[1]})"
            
            if ultimo_inv and not ultimo_mov:
                cursor.execute("DELETE FROM portafolio_inversiones WHERE id = %s;", (ultimo_inv[1],))
                conn.commit()
                return f"Inversión de {ultimo_inv[3]} de ${ultimo_inv[4]:,.2f} USD (ID {ultimo_inv[1]})"
            
            if ultimo_mov[2] >= ultimo_inv[2]:
                cursor.execute("DELETE FROM movimientos WHERE id = %s;", (ultimo_mov[1],))
                conn.commit()
                return f"Gasto/Ingreso {ultimo_mov[3]} de ${ultimo_mov[4]:,.2f} ARS (ID {ultimo_mov[1]})"
            else:
                cursor.execute("DELETE FROM portafolio_inversiones WHERE id = %s;", (ultimo_inv[1],))
                conn.commit()
                return f"Inversión de {ultimo_inv[3]} de ${ultimo_inv[4]:,.2f} USD (ID {ultimo_inv[1]})"

def borrar_todos_los_movimientos():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE movimientos RESTART IDENTITY;")
            cursor.execute("TRUNCATE TABLE portafolio_inversiones RESTART IDENTITY;")
            conn.commit()

# ==================== GASTOS / INGRESOS (ARS) ====================
def obtener_metricas_analisis_gastos():
    with get_db_connection() as conn:
        df = pd.read_sql("SELECT fecha, monto, categoria, descripcion FROM movimientos WHERE tipo = 'GASTO' ORDER BY fecha ASC;", conn)
    
    if df.empty:
        return "No hay suficientes gastos registrados en ARS."
    
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['mes_ano'] = df['fecha'].dt.to_period('M').astype(str)
    
    por_mes = df.groupby('mes_ano')['monto'].sum()
    promedio_mensual = por_mes.mean()
    cat_mes = df.groupby(['mes_ano', 'categoria'])['monto'].sum().unstack(fill_value=0)
    cat_total = df.groupby('categoria')['monto'].sum().sort_values(ascending=False)
    
    top_gastos = df.sort_values(by='monto', ascending=False).head(5)[['fecha', 'monto', 'categoria', 'descripcion']]
    top_gastos_txt = "\n".join([f"- [{r['fecha'].strftime('%Y-%m-%d')}] ${r['monto']:,.2f} ARS en {r['categoria']} ({r['descripcion']})" for _, r in top_gastos.iterrows()])
    
    return (
        f"MÉTRICAS ESTADÍSTICAS REALES:\n"
        f"- Gasto total histórico: ${df['monto'].sum():,.2f} ARS\n"
        f"- Promedio mensual: ${promedio_mensual:,.2f} ARS\n"
        f"- Totales por mes:\n{por_mes.to_string()}\n\n"
        f"- Gastos por categoría y mes:\n{cat_mes.to_string()}\n\n"
        f"- Ranking categorías:\n{cat_total.to_string()}\n\n"
        f"- Picos / Anomalías individuales:\n{top_gastos_txt}"
    )

def guardar_movimiento(tipo, monto, categoria, descripcion, fecha_str=None):
    fecha_limpia = None
    if fecha_str:
        f_cand = fecha_str.strip().split()[0]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f_cand):
            fecha_limpia = f_cand

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if fecha_limpia:
                cursor.execute(
                    "INSERT INTO movimientos (fecha, tipo, monto, categoria, descripcion) VALUES (%s, %s, %s, %s, %s)",
                    (fecha_limpia, tipo.upper(), float(monto), categoria.capitalize(), descripcion)
                )
            else:
                cursor.execute(
                    "INSERT INTO movimientos (fecha, tipo, monto, categoria, descripcion) VALUES (NOW(), %s, %s, %s, %s)",
                    (tipo.upper(), float(monto), categoria.capitalize(), descripcion)
                )
            conn.commit()

def generar_grafico_gastos():
    with get_db_connection() as conn:
        df = pd.read_sql("SELECT monto, categoria FROM movimientos WHERE tipo = 'GASTO';", conn)
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

def generar_excel_completo():
    with get_db_connection() as conn:
        df_mov = pd.read_sql("SELECT * FROM movimientos ORDER BY fecha ASC;", conn)
        df_inv = pd.read_sql("SELECT * FROM portafolio_inversiones ORDER BY fecha ASC;", conn)
    
    if df_mov.empty and df_inv.empty:
        return None
    
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        if not df_mov.empty:
            df_mov.to_excel(writer, sheet_name='Gastos_Ingresos_ARS', index=False)
        if not df_inv.empty:
            df_inv.to_excel(writer, sheet_name='Inversiones_USD', index=False)
    buf.seek(0)
    return buf

# ==================== SYSTEM INSTRUCTION ====================
SYSTEM_INSTRUCTION = """
Eres un analista y asesor financiero personal de alto nivel.
Tienes acceso al historial unificado con IDs y fechas exactas de GASTOS, INGRESOS e INVERSIONES.

REGLAS DE CONSULTA DE FECHAS / HISTORIAL:
- Si el usuario pregunta cuándo compró o registró algo (ej: "¿cuándo compré BTC?", "¿qué compré el mes pasado?", "¿en qué fecha registré el gasto de farmacia?", "mostrame las fechas de mis compras"):
  Responde con precisión humana usando los datos exactos del historial proporcionado. No agregues etiquetas REGISTRO a consultas pasadas.

REGLAS DE BORRADO:
- Borrar por ticker de inversión (ej: "borrá NVDA", "eliminá las compras de BTC", "borrá nvda de mi cartera"):
  ACCION: BORRAR_INVERSION_TICKER|[TICKER]
- Borrar una inversión específica por ID (ej: "borrá la inversión ID 3"):
  ACCION: BORRAR_INVERSION_ID|[ID]
- Borrar un gasto o ingreso por ID (ej: "borrá el gasto ID 5", "borrá el movimiento 4"):
  ACCION: BORRAR_MOVIMIENTO_ID|[ID]
- Borrar lo último que se registró (sea gasto o inversión):
  ACCION: BORRAR_ULTIMO
- Borrar todo el historial y resetear:
  ACCION: BORRAR_TODO

REGLAS DE REGISTRO DE COMPRA / APORTE (USD):
- Si el usuario indica compra de activos (ej: "el 29 de mayo compre 288 usd de nvda a 208.0079 de ppc", "compré 1000 usd de MELI"):
  Escribe obligatoriamente en UNA SOLA LÍNEA SEPARADA:
  REGISTRO_INV: [TICKER]|[MONTO_USD]|[PRECIO_COMPRA]|[CANTIDAD]|[FECHA_YYYY-MM-DD]
  (Si no dice fecha, deja el último campo vacío. Ejemplo: REGISTRO_INV: NVDA|288.0|208.01|1.3845|2026-05-29)

REGLAS DE GASTOS / INGRESOS (ARS):
- Registro de gasto o ingreso:
  REGISTRO_ARS: [TIPO]|[MONTO]|[CATEGORIA]|[DESCRIPCION]|[FECHA_YYYY-MM-DD]

REGLAS DE MERCADO Y GRÁFICOS:
- Consulta precio en vivo: ACCION: CONSULTA_PRECIO|[TICKER]
- Gráfico evolución activo: ACCION: GRAFICO_EVOLUCION_ACTIVO|[TICKER]
- Gráfico evolución toda la cartera: ACCION: GRAFICO_EVOLUCION_CARTERA
- Ver estado actual cartera: ACCION: VER_CARTERA
- Gráfico de torta inversiones: ACCION: GRAFICO_INVERSIONES
- Gráfico de torta gastos: ACCION: GRAFICO_GASTOS
- Excel: ACCION: EXCEL
"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy tu asistente financiero integral.\n\n"
        "📅 Consultas y Fechas:\n"
        "• '¿Cuándo compré BTC y a qué precio?'\n"
        "• 'Mostrame las fechas de mis compras de NVDA'\n\n"
        "🗑️ Borrado Universal:\n"
        "• 'Borrá NVDA de mi cartera'\n"
        "• 'Borrá el gasto ID 4' / 'Borrá la inversión ID 2'\n"
        "• 'Borrá lo último que cargué'\n\n"
        "📈 Inversiones y Mercado:\n"
        "• 'El 29 de mayo compré 288 usd de NVDA a 208 de ppc'\n"
        "• 'Evolución de mi cartera' / 'Precio de MELI'\n\n"
        "💸 Gastos (ARS):\n"
        "• 'Almuerzo 12k' / 'Mandame un excel'"
    )

async def cmd_borrar_todo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    borrar_todos_los_movimientos()
    await update.message.reply_text("🗑️ Base de datos y cartera reseteadas por completo.")

async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = update.message.text
    historial_unificado = obtener_historial_completo_texto()
    
    msg_lower = user_msg.lower()
    es_pregunta_analisis = any(w in msg_lower for w in ["promedio", "analisis", "analizá", "analiza", "aumento", "subió", "subio", "por qué", "por que", "desvío", "desvio", "en qué gasté"])
    
    if es_pregunta_analisis:
        metricas = obtener_metricas_analisis_gastos()
        prompt = f"DATOS ESTADÍSTICOS EXACTOS:\n{metricas}\n\nHISTORIAL UNIFICADO CON FECHAS:\n{historial_unificado}\n\nPREGUNTA DEL USUARIO: {user_msg}"
    else:
        prompt = f"HISTORIAL UNIFICADO REGISTRADO (con IDs y Fechas):\n{historial_unificado}\n\nMensaje del usuario: {user_msg}"

    try:
        response = ai_client.models.generate_content(
            model="gemini-3.6-flash",
            contents=prompt,
            config={"system_instruction": SYSTEM_INSTRUCTION}
        )
        reply = response.text
        
        necesita_cartera = "ACCION: VER_CARTERA" in reply
        necesita_grafico_inv = "ACCION: GRAFICO_INVERSIONES" in reply
        necesita_grafico_evol_cartera = "ACCION: GRAFICO_EVOLUCION_CARTERA" in reply
        necesita_grafico_gastos = "ACCION: GRAFICO_GASTOS" in reply
        necesita_excel = "ACCION: EXCEL" in reply
        necesita_borrar_todo = "ACCION: BORRAR_TODO" in reply
        necesita_borrar_ultimo = "ACCION: BORRAR_ULTIMO" in reply
        necesita_precio = "ACCION: CONSULTA_PRECIO|" in reply
        necesita_grafico_evol_activo = "ACCION: GRAFICO_EVOLUCION_ACTIVO|" in reply
        
        necesita_borrar_inv_tk = "ACCION: BORRAR_INVERSION_TICKER|" in reply
        necesita_borrar_inv_id = "ACCION: BORRAR_INVERSION_ID|" in reply
        necesita_borrar_mov_id = "ACCION: BORRAR_MOVIMIENTO_ID|" in reply
        
        # Extracción segura de tags
        texto_limpio = reply
        for tag in ["ACCION: VER_CARTERA", "ACCION: GRAFICO_INVERSIONES", "ACCION: GRAFICO_EVOLUCION_CARTERA", "ACCION: GRAFICO_GASTOS", "ACCION: EXCEL", "ACCION: BORRAR_TODO", "ACCION: BORRAR_ULTIMO"]:
            texto_limpio = texto_limpio.replace(tag, "")
            
        ticker_a_cotizar = None
        ticker_grafico_evol = None
        ticker_a_borrar = None
        inv_id_a_borrar = None
        mov_id_a_borrar = None

        if necesita_precio:
            m = re.search(r"ACCION: CONSULTA_PRECIO\|([^\n\r]+)", texto_limpio)
            if m:
                ticker_a_cotizar = m.group(1).strip()
                texto_limpio = texto_limpio.replace(m.group(0), "")

        if necesita_grafico_evol_activo:
            m = re.search(r"ACCION: GRAFICO_EVOLUCION_ACTIVO\|([^\n\r]+)", texto_limpio)
            if m:
                ticker_grafico_evol = m.group(1).strip()
                texto_limpio = texto_limpio.replace(m.group(0), "")

        if necesita_borrar_inv_tk:
            m = re.search(r"ACCION: BORRAR_INVERSION_TICKER\|([^\n\r]+)", texto_limpio)
            if m:
                ticker_a_borrar = m.group(1).strip()
                texto_limpio = texto_limpio.replace(m.group(0), "")

        if necesita_borrar_inv_id:
            m = re.search(r"ACCION: BORRAR_INVERSION_ID\|([^\n\r]+)", texto_limpio)
            if m:
                try:
                    inv_id_a_borrar = int(m.group(1).strip())
                except Exception:
                    pass
                texto_limpio = texto_limpio.replace(m.group(0), "")

        if necesita_borrar_mov_id:
            m = re.search(r"ACCION: BORRAR_MOVIMIENTO_ID\|([^\n\r]+)", texto_limpio)
            if m:
                try:
                    mov_id_a_borrar = int(m.group(1).strip())
                except Exception:
                    pass
                texto_limpio = texto_limpio.replace(m.group(0), "")

        # Parseo exacto de REGISTRO_INV con regex de una sola línea
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
            
            t, c, p, m, f_reg = registrar_operacion_inversion(ticker, monto, p_compra, cant, f_compra)
            fecha_str = f" - Fecha: {f_reg}" if f_reg else ""
            texto_limpio = f"{texto_limpio}\n\n💼 *(Guardado en Cartera: {c:,.4f} {t} a PPC ${p:,.2f} USD - Total: ${m:,.2f} USD{fecha_str})*".strip()

        # Parseo exacto de REGISTRO_ARS con regex
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
            
            guardar_movimiento(tipo, monto, categoria, descripcion, f_gasto)
            fecha_str = f" - Fecha: {f_gasto}" if f_gasto else ""
            texto_limpio = f"{texto_limpio}\n\n✅ *(Guardado: {tipo} de ${monto:,.2f} ARS en {categoria}{fecha_str})*".strip()

        texto_limpio = texto_limpio.strip()

        # Borrados
        if necesita_borrar_todo:
            borrar_todos_los_movimientos()
            texto_limpio += "\n\n🗑️ *(Base de datos completa reseteada)*"

        if necesita_borrar_ultimo:
            res_ultimo = borrar_ultimo_registro_general()
            if res_ultimo:
                texto_limpio += f"\n\n🗑️ *(Eliminado: {res_ultimo})*"
            else:
                texto_limpio += "\n\n⚠️ No había registros para borrar."

        if ticker_a_borrar:
            cant = borrar_inversion_por_ticker(ticker_a_borrar)
            if cant > 0:
                texto_limpio += f"\n\n🗑️ *(Se eliminaron {cant} compra(s) de {ticker_a_borrar} de tu cartera)*"
            else:
                texto_limpio += f"\n\n⚠️ No se encontraron compras de `{ticker_a_borrar}`."

        if inv_id_a_borrar:
            reg = borrar_inversion_por_id(inv_id_a_borrar)
            if reg:
                texto_limpio += f"\n\n🗑️ *(Eliminada inversión ID {reg[0]}: {reg[1]} por ${reg[2]:,.2f} USD)*"
            else:
                texto_limpio += f"\n\n⚠️ No se encontró la inversión con ID {inv_id_a_borrar}."

        if mov_id_a_borrar:
            reg = borrar_movimiento_por_id(mov_id_a_borrar)
            if reg:
                texto_limpio += f"\n\n🗑️ *(Eliminado {reg[1]} de ${float(reg[2]):,.2f} ARS en {reg[3]} - ID {reg[0]})*"
            else:
                texto_limpio += f"\n\n⚠️ No se encontró el movimiento con ID {mov_id_a_borrar}."

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
            else:
                texto_limpio += f"\n\n⚠️ No pude obtener la cotización de `{ticker_a_cotizar}` en este momento."

        # Envío seguro de texto
        if texto_limpio:
            try:
                await update.message.reply_text(texto_limpio, parse_mode="Markdown")
            except Exception:
                await update.message.reply_text(texto_limpio)

        # Gráfico evolución activo puntual
        if ticker_grafico_evol:
            buf_img = generar_grafico_evolucion_activo(ticker_grafico_evol)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption=f"📈 Evolución de {ticker_grafico_evol} desde tu fecha de compra.")
            else:
                await update.message.reply_text(f"⚠️ No pude generar la curva de evolución para {ticker_grafico_evol}.")

        # Gráfico evolución cartera
        if necesita_grafico_evol_cartera:
            buf_img = generar_grafico_evolucion_cartera()
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📈 Evolución de rendimiento de tu cartera desde tu primera inversión (Base 100).")
            else:
                await update.message.reply_text("No hay suficientes activos registrados en tu cartera para armar el gráfico comparativo.")

        # Reporte de cartera
        if necesita_cartera:
            resumen = obtener_resumen_portafolio()
            if not resumen:
                await update.message.reply_text("📉 No tienes activos cargados en tu cartera todavía.")
            else:
                signo = "+" if resumen["pnl_total_usd"] >= 0 else ""
                emoji_rend = "🟢" if resumen["pnl_total_usd"] >= 0 else "🔴"
                
                msg_rep = (
                    f"💼 ESTADO DE TU CARTERA EN VIVO\n\n"
                    f"• Capital Invertido: ${resumen['total_invertido']:,.2f} USD\n"
                    f"• Valor de Mercado Actual: ${resumen['total_actual']:,.2f} USD\n"
                    f"• Resultado Total (PnL): {emoji_rend} {signo}${resumen['pnl_total_usd']:,.2f} USD ({signo}{resumen['pnl_total_pct']:.2f}%)\n\n"
                    f"📊 Detalle por Activo:\n"
                )
                
                for pos in resumen["posiciones"]:
                    pnl_s = "+" if pos["pnl_usd"] >= 0 else ""
                    em = "🟢" if pos["pnl_usd"] >= 0 else "🔴"
                    peso = (pos['valor_mercado'] / resumen['total_actual'] * 100) if resumen['total_actual'] > 0 else 0
                    msg_rep += (
                        f"▪️ {pos['ticker']} ({peso:.1f}% de cartera):\n"
                        f"   - Tenencia: {pos['cantidad']:,.4f} acc/tokens\n"
                        f"   - PPC: ${pos['ppc']:,.2f} - Precio hoy: ${pos['precio_actual']:,.2f} USD\n"
                        f"   - PnL: {em} {pnl_s}${pos['pnl_usd']:,.2f} USD ({pnl_s}{pos['pnl_pct']:.2f}%)\n\n"
                    )
                try:
                    await update.message.reply_text(msg_rep, parse_mode="Markdown")
                except Exception:
                    await update.message.reply_text(msg_rep)

        if necesita_grafico_inv:
            buf_img = generar_grafico_distribucion_inversiones()
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📊 Asset Allocation: Distribución actual de tu cartera.")
            else:
                await update.message.reply_text("No tienes inversiones registradas para graficar.")

        if necesita_grafico_gastos:
            buf_img = generar_grafico_gastos()
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📊 Distribución de tus gastos por categoría (ARS).")
            else:
                await update.message.reply_text("No tienes gastos en ARS registrados para graficar.")

        if necesita_excel:
            excel_buf = generar_excel_completo()
            if excel_buf:
                await update.message.reply_document(
                    document=excel_buf, 
                    filename="Finanzas_e_Inversiones.xlsx", 
                    caption="📁 Planilla completa con hojas de Gastos (ARS) e Inversiones (USD)."
                )
            else:
                await update.message.reply_text("No hay datos para generar el reporte.")

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

