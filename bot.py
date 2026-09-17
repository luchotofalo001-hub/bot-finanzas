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

# ==================== GRÁFICOS DE EVOLUCIÓN CON ALINEACIÓN PERFECTA ====================
def generar_grafico_evolucion_activo(ticker: str, periodo_default: str = "6mo"):
    ticker = ticker.strip().upper()
    simbolo = normalizar_ticker_yf(ticker)
    
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
        logger.error(f"Error consultando fecha para {ticker}: {e}")

    try:
        t = yf.Ticker(simbolo)
        df_hist = t.history(start=fecha_inicio) if fecha_inicio else t.history(period=periodo_default)
        if df_hist.empty and not simbolo.endswith("-USD"):
            simbolo = f"{simbolo}-USD"
            t = yf.Ticker(simbolo)
            df_hist = t.history(start=fecha_inicio) if fecha_inicio else t.history(period=periodo_default)

        if df_hist.empty:
            return None

        # Descartar zona horaria y rellenar días no bursátiles para línea continua
        df_hist.index = pd.to_datetime(df_hist.index).tz_localize(None)
        serie = df_hist['Close'].ffill().bfill()

        plt.figure(figsize=(9.5, 5))
        color_linea = "#1f77b4"
        plt.plot(serie.index, serie, label=f"Precio {ticker} (USD)", color=color_linea, linewidth=2.2)
        plt.fill_between(serie.index, serie, alpha=0.12, color=color_linea)
        
        if ppc_referencia:
            plt.axhline(y=ppc_referencia, color="#d62728", linestyle="--", linewidth=1.6, label=f"Tu PPC (${ppc_referencia:,.2f})")
            subtitulo = f"Desde tu primera compra ({fecha_inicio})"
        else:
            subtitulo = "Últimos 6 meses"

        plt.title(f"Evolución Histórica: {ticker}\n{subtitulo}", fontsize=12, fontweight='bold', pad=12)
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
        logger.error(f"Error graficando evolución activo {ticker}: {e}")
        return None

def generar_grafico_evolucion_cartera():
    try:
        with get_db_connection() as conn:
            df = pd.read_sql("SELECT ticker, MIN(fecha) as primera_compra, SUM(cantidad) as cantidad, SUM(monto_total_usd) as costo_total FROM portafolio_inversiones GROUP BY ticker;", conn)
        
        if df.empty:
            return None
        
        primera_fecha_global = df['primera_compra'].min().strftime('%Y-%m-%d')
        series_dict = {}

        for _, row in df.iterrows():
            tk = row['ticker']
            sym = normalizar_ticker_yf(tk)
            try:
                h = yf.Ticker(sym).history(start=primera_fecha_global)
                if not h.empty:
                    s = h['Close']
                    s.index = pd.to_datetime(s.index).tz_localize(None)
                    series_dict[tk] = s
            except Exception:
                pass

        if not series_dict:
            return None

        # Alineación temporal completa (resample diario continuo + ffill para unir cripto y acciones)
        df_precios = pd.DataFrame(series_dict)
        df_precios = df_precios.resample('D').last().ffill().bfill()
        
        # Normalizar cada activo a Base 100 desde el inicio de la cartera
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

        ax.axhline(y=100, color="gray", linestyle=":", linewidth=1.3, alpha=0.75, label="Punto de partida (100)")
        ax.set_title(f"Rendimiento Relativo de tu Cartera (Base 100)\nDesde tu primera inversión ({primera_fecha_global})", fontsize=12, fontweight='bold', pad=12)
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

# ==================== MOTOR DE MÉTRICAS ANALÍTICAS AVANZADAS ====================
def calcular_super_metricas_totales():
    """Genera un reporte analítico de alto nivel con todas las métricas de Inversiones y Gastos/Ingresos"""
    metricas = []
    
    # 1. MÉTRICAS DE INVERSIONES (USD)
    try:
        with get_db_connection() as conn:
            df_inv = pd.read_sql("SELECT id, fecha, ticker, cantidad, precio_compra, monto_total_usd FROM portafolio_inversiones ORDER BY fecha ASC;", conn)
        
        if not df_inv.empty:
            total_invertido = float(df_inv['monto_total_usd'].sum())
            total_actual = 0.0
            detalle_activos = []
            
            agrupado = df_inv.groupby('ticker').agg({'cantidad': 'sum', 'monto_total_usd': 'sum'}).reset_index()
            for _, r in agrupado.iterrows():
                tk = r['ticker']
                cant = float(r['cantidad'])
                costo = float(r['monto_total_usd'])
                ppc = costo / cant if cant > 0 else 0.0
                spot, _ = obtener_precio_actual(tk)
                if spot is None:
                    spot = ppc
                val_mercado = cant * spot
                pnl_usd = val_mercado - costo
                pnl_pct = (pnl_usd / costo * 100) if costo > 0 else 0.0
                total_actual += val_mercado
                
                detalle_activos.append({
                    "ticker": tk,
                    "cant": cant,
                    "ppc": ppc,
                    "spot": spot,
                    "costo": costo,
                    "val_mercado": val_mercado,
                    "pnl_usd": pnl_usd,
                    "pnl_pct": pnl_pct
                })
            
            pnl_global_usd = total_actual - total_invertido
            pnl_global_pct = (pnl_global_usd / total_invertido * 100) if total_invertido > 0 else 0.0
            
            # Ordenar por rendimiento
            detalle_activos.sort(key=lambda x: x['pnl_pct'], reverse=True)
            top_asset = detalle_activos[0]
            worst_asset = detalle_activos[-1]
            
            metricas.append("=== MÉTRICAS DE CARTERA DE INVERSIÓN (USD) ===")
            metricas.append(f"• Total Capital Invertido: ${total_invertido:,.2f} USD")
            metricas.append(f"• Valor Actual de Cartera: ${total_actual:,.2f} USD")
            metricas.append(f"• Ganancia/Pérdida Neta Total (PnL): ${pnl_global_usd:,.2f} USD ({pnl_global_pct:+.2f}%)")
            metricas.append(f"• Mejor Activo (Top Gainer): {top_asset['ticker']} ({top_asset['pnl_pct']:+.2f}% / ${top_asset['pnl_usd']:,.2f} USD)")
            metricas.append(f"• Activo más rezagado: {worst_asset['ticker']} ({worst_asset['pnl_pct']:+.2f}% / ${worst_asset['pnl_usd']:,.2f} USD)")
            metricas.append(f"• Primera fecha de inversión: {df_inv['fecha'].iloc[0].strftime('%Y-%m-%d')}")
            metricas.append(f"• Última fecha de inversión: {df_inv['fecha'].iloc[-1].strftime('%Y-%m-%d')}")
            metricas.append(f"• Cantidad de operaciones de compra registradas: {len(df_inv)}")
            metricas.append("• Desglose por Activo:")
            for a in detalle_activos:
                peso = (a['val_mercado'] / total_actual * 100) if total_actual > 0 else 0
                metricas.append(f"   - {a['ticker']}: Tenencia {a['cant']:,.4f} | PPC ${a['ppc']:,.2f} | Spot ${a['spot']:,.2f} | PnL ${a['pnl_usd']:,.2f} ({a['pnl_pct']:+.2f}%) | Peso en cartera: {peso:.1f}%")
        else:
            metricas.append("=== CARTERA DE INVERSIÓN: Sin compras registradas todavía ===")
    except Exception as e:
        logger.error(f"Error métricas inversión: {e}")

    # 2. MÉTRICAS DE GASTOS E INGRESOS (ARS)
    try:
        with get_db_connection() as conn:
            df_mov = pd.read_sql("SELECT id, fecha, tipo, monto, categoria, descripcion FROM movimientos ORDER BY fecha ASC;", conn)
        
        if not df_mov.empty:
            df_mov['fecha'] = pd.to_datetime(df_mov['fecha'])
            df_gastos = df_mov[df_mov['tipo'] == 'GASTO']
            df_ingresos = df_mov[df_mov['tipo'] == 'INGRESO']
            
            tot_gastos = float(df_gastos['monto'].sum()) if not df_gastos.empty else 0.0
            tot_ingresos = float(df_ingresos['monto'].sum()) if not df_ingresos.empty else 0.0
            ahorro_neto = tot_ingresos - tot_gastos
            tasa_ahorro = (ahorro_neto / tot_ingresos * 100) if tot_ingresos > 0 else 0.0
            
            metricas.append("\n=== MÉTRICAS DE FLUJO DE CAJA (ARS) ===")
            metricas.append(f"• Ingresos Totales Históricos: ${tot_ingresos:,.2f} ARS")
            metricas.append(f"• Gastos Totales Históricos: ${tot_gastos:,.2f} ARS")
            metricas.append(f"• Superávit/Déficit Neto (Ahorro): ${ahorro_neto:,.2f} ARS")
            metricas.append(f"• Tasa de Ahorro Histórica: {tasa_ahorro:.2f}%")
            
            if not df_gastos.empty:
                df_gastos['mes_ano'] = df_gastos['fecha'].dt.to_period('M').astype(str)
                prom_mes = df_gastos.groupby('mes_ano')['monto'].sum().mean()
                max_gasto = df_gastos.sort_values(by='monto', ascending=False).iloc[0]
                top_cat = df_gastos.groupby('categoria')['monto'].sum().sort_values(ascending=False)
                
                metricas.append(f"• Promedio mensual de gastos: ${prom_mes:,.2f} ARS")
                metricas.append(f"• Mayor gasto individual registrado: ${max_gasto['monto']:,.2f} ARS ({max_gasto['categoria']} - {max_gasto['descripcion']} el {max_gasto['fecha'].strftime('%Y-%m-%d')})")
                metricas.append("• Top categorías de gasto:")
                for cat, val in top_cat.head(4).items():
                    pct_cat = (val / tot_gastos * 100) if tot_gastos > 0 else 0
                    metricas.append(f"   - {cat}: ${val:,.2f} ARS ({pct_cat:.1f}%)")
        else:
            metricas.append("\n=== FLUJO DE CAJA (ARS): Sin movimientos registrados ===")
    except Exception as e:
        logger.error(f"Error métricas ARS: {e}")

    return "\n".join(metricas)

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
Eres un analista y asesor financiero cuantitativo de nivel institucional.
Dispones del MOTOR DE MÉTRICAS ANALÍTICAS EXACTAS de Inversiones (USD) y Flujo de Caja (ARS), además del historial unificado con IDs y fechas.

REGLAS DE RESPUESTA A MÉTRICAS Y CONSULTAS:
- Si el usuario pregunta por cualquier métrica, cálculo o dato de su cartera o gastos (ej: "¿cuál es mi activo más rentable?", "¿cuánto vengo ganando en USD y en %?", "¿cuánto invertí en total?", "¿cuál es mi tasa de ahorro?", "¿en qué mes gasté más?", "¿cuándo compré NVDA?", "haceme un balance"):
  Utiliza siempre los números exactos calculados del bloque MÉTRICAS CUANTITATIVAS REALES. Da respuestas claras, directas, con números precisos y emojis profesionales. No inventes números.

REGLAS DE BORRADO:
- Borrar por ticker de inversión: ACCION: BORRAR_INVERSION_TICKER|[TICKER]
- Borrar inversión por ID: ACCION: BORRAR_INVERSION_ID|[ID]
- Borrar movimiento por ID: ACCION: BORRAR_MOVIMIENTO_ID|[ID]
- Borrar lo último registrado: ACCION: BORRAR_ULTIMO
- Resetear todo: ACCION: BORRAR_TODO

REGLAS DE REGISTRO (EN UNA SOLA LÍNEA):
- Inversión USD: REGISTRO_INV: [TICKER]|[MONTO_USD]|[PRECIO_COMPRA]|[CANTIDAD]|[FECHA_YYYY-MM-DD]
- Gasto/Ingreso ARS: REGISTRO_ARS: [TIPO]|[MONTO]|[CATEGORIA]|[DESCRIPCION]|[FECHA_YYYY-MM-DD]

REGLAS DE GRÁFICOS Y MERCADO:
- Cotización en vivo: ACCION: CONSULTA_PRECIO|[TICKER]
- Gráfico evolución activo puntual: ACCION: GRAFICO_EVOLUCION_ACTIVO|[TICKER]
- Gráfico evolución comparativa de cartera (Base 100): ACCION: GRAFICO_EVOLUCION_CARTERA
- Ver estado actual cartera: ACCION: VER_CARTERA
- Gráfico de torta inversiones: ACCION: GRAFICO_INVERSIONES
- Gráfico de torta gastos: ACCION: GRAFICO_GASTOS
- Excel: ACCION: EXCEL
"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy tu asistente financiero y analista cuantitativo.\n\n"
        "📊 Métricas y Análisis de Cartera:\n"
        "• '¿Cuál es mi rendimiento total?'\n"
        "• '¿Cuál es mi activo más rentable?'\n"
        "• 'Evolución del rendimiento de mi cartera' (Líneas comparativas)\n\n"
        "📅 Fechas y Consultas:\n"
        "• '¿Cuándo compré NVDA?' / 'Detalle de compras de SOL'\n\n"
        "🗑️ Borrado:\n"
        "• 'Borrá NVDA' / 'Borrá la inversión ID 2'\n"
        "• 'Borrá lo último que cargué'\n\n"
        "💸 Gastos (ARS):\n"
        "• '¿Cuál es mi tasa de ahorro?' / 'Mandame un excel'"
    )

async def cmd_borrar_todo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    borrar_todos_los_movimientos()
    await update.message.reply_text("🗑️ Base de datos y cartera reseteadas por completo.")

async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = update.message.text
    historial_unificado = obtener_historial_completo_texto()
    metricas_cuantitativas = calcular_super_metricas_totales()
    
    prompt = (
        f"MÉTRICAS CUANTITATIVAS REALES CALCULADAS EN VIVO:\n{metricas_cuantitativas}\n\n"
        f"HISTORIAL DETALLADO REGISTRADO (con IDs y Fechas):\n{historial_unificado}\n\n"
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

        # Procesar Registro Inversión
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

        # Procesar Registro Gasto/Ingreso
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

        # Ejecución de Borrados
        if necesita_borrar_todo:
            borrar_todos_los_movimientos()
            texto_limpio += "\n\n🗑️ *(Base de datos y cartera reseteadas)*"

        if necesita_borrar_ultimo:
            res_ultimo = borrar_ultimo_registro_general()
            if res_ultimo:
                texto_limpio += f"\n\n🗑️ *(Eliminado: {res_ultimo})*"
            else:
                texto_limpio += "\n\n⚠️ No había registros para borrar."

        if ticker_a_borrar:
            cant = borrar_inversion_por_ticker(ticker_a_borrar)
            if cant > 0:
                texto_limpio += f"\n\n🗑️ *(Se eliminaron {cant} compras de {ticker_a_borrar} de tu cartera)*"
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

        # Envío de texto seguro
        if texto_limpio:
            try:
                await update.message.reply_text(texto_limpio, parse_mode="Markdown")
            except Exception:
                await update.message.reply_text(texto_limpio)

        # Gráfico evolución activo puntual
        if ticker_grafico_evol:
            buf_img = generar_grafico_evolucion_activo(ticker_grafico_evol)
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption=f"📈 Evolución continua de {ticker_grafico_evol} desde tu fecha de compra.")
            else:
                await update.message.reply_text(f"⚠️ No pude generar la curva de evolución para {ticker_grafico_evol}.")

        # Gráfico evolución comparativa de cartera
        if necesita_grafico_evol_cartera:
            buf_img = generar_grafico_evolucion_cartera()
            if buf_img:
                await update.message.reply_photo(photo=buf_img, caption="📈 Evolución relativa de tus activos (Base 100) desde tu primera inversión.")
            else:
                await update.message.reply_text("No hay suficientes activos registrados para armar el gráfico comparativo.")

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

