import os
import io
import asyncio
import logging
import threading
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
            fi = t.fast_info
            last_price = fi.get("last_price")
            prev_close = fi.get("previous_close")
            day_high = fi.get("day_high")
            day_low = fi.get("day_low")

            if last_price is not None and last_price > 0:
                var_usd = (last_price - prev_close) if prev_close else 0.0
                var_pct = (var_usd / prev_close * 100) if prev_close else 0.0
                
                return {
                    "ticker": sym,
                    "precio": float(last_price),
                    "prev_close": float(prev_close) if prev_close else None,
                    "var_usd": float(var_usd),
                    "var_pct": float(var_pct),
                    "day_high": float(day_high) if day_high else None,
                    "day_low": float(day_low) if day_low else None
                }
        except Exception:
            pass
    return None

def obtener_precio_actual(ticker: str):
    datos = consultar_datos_mercado(ticker)
    if datos:
        return datos["precio"], datos["ticker"]
    return None, ticker

# ==================== INVERSIONES (USD) ====================
def registrar_operacion_inversion(ticker: str, monto_usd: float, precio_compra: float = None, cantidad: float = None):
    ticker = ticker.strip().upper()
    if (not precio_compra or precio_compra <= 0) and (not cantidad or cantidad <= 0):
        precio_mercado, _ = obtener_precio_actual(ticker)
        precio_compra = precio_mercado if precio_mercado else 1.0
    
    if cantidad is None or cantidad <= 0:
        cantidad = monto_usd / precio_compra if precio_compra > 0 else 0
    elif monto_usd is None or monto_usd <= 0:
        monto_usd = cantidad * precio_compra

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO portafolio_inversiones (fecha, ticker, cantidad, precio_compra, monto_total_usd)
                   VALUES (NOW(), %s, %s, %s, %s);""",
                (ticker, float(cantidad), float(precio_compra), float(monto_usd))
            )
            conn.commit()
    return ticker, cantidad, precio_compra, monto_usd

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

def guardar_movimiento(tipo, monto, categoria, descripcion):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO movimientos (fecha, tipo, monto, categoria, descripcion) VALUES (NOW(), %s, %s, %s, %s)",
                (tipo.upper(), float(monto), categoria.capitalize(), descripcion)
            )
            conn.commit()

def borrar_todos_los_movimientos():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE movimientos RESTART IDENTITY;")
            cursor.execute("TRUNCATE TABLE portafolio_inversiones RESTART IDENTITY;")
            conn.commit()

def borrar_por_id(movimiento_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, tipo, monto, categoria, descripcion FROM movimientos WHERE id = %s;", (int(movimiento_id),))
            fila = cursor.fetchone()
            if fila:
                cursor.execute("DELETE FROM movimientos WHERE id = %s;", (int(movimiento_id),))
                conn.commit()
                return fila
            return None

def borrar_ultimo_movimiento():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, tipo, monto, categoria, descripcion FROM movimientos ORDER BY id DESC LIMIT 1;")
            ultimo = cursor.fetchone()
            if ultimo:
                cursor.execute("DELETE FROM movimientos WHERE id = %s;", (ultimo[0],))
                conn.commit()
                return ultimo
            return None

def obtener_historial_texto(limite=40):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, fecha, tipo, monto, categoria, descripcion FROM movimientos ORDER BY id DESC LIMIT %s;", (limite,))
                filas = cursor.fetchall()
                if not filas:
                    return "Sin transacciones en ARS."
                lineas = [f"- ID {f[0]} | [{f[1].strftime('%Y-%m-%d %H:%M')}] {f[2]}: ${f[3]:,.2f} ARS | {f[4]} | {f[5]}" for f in filas]
                return "\n".join(lineas)
    except Exception as e:
        return "Sin historial ARS."

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
Distingues estrictamente dos mundos:
1. GASTOS E INGRESOS: Flujo cotidiano en Pesos Argentinos (ARS $).
2. MERCADO E INVERSIONES: Activos financieros en Dólares (USD $), identificados por TICKERS (acciones, CEDEARs, ETFs, Cripto).

REGLAS DE PRECIO Y MERCADO:
- Si el usuario te pregunta por la cotización, precio o variación de un activo o acción (ej: "¿a cuánto está MELI?", "precio de BTC", "cómo viene AAPL hoy"):
  Identifica el ticker y responde OBLIGATORIAMENTE agregando al final una única línea:
  ACCION: CONSULTA_PRECIO|[TICKER]
  (No inventes números; el sistema consultará el precio exacto y la variación del día).

REGLAS DE COMPRA / APORTE A CARTERA:
- Si el usuario indica compra de activos (ej: "compre 1000 usd de MELI", "compre 0.5 BTC a 62000"):
  Agrega al final:
  REGISTRO_INV: [TICKER]|[MONTO_USD]|[PRECIO_COMPRA]|[CANTIDAD]

REGLAS DE SEGUIMIENTO DE CARTERA:
- Si pregunta por el rendimiento o evolución de su cartera propia:
  ACCION: VER_CARTERA
- Si pide gráfico de cartera de inversión:
  ACCION: GRAFICO_INVERSIONES

REGLAS DE GASTOS ARS:
- Registro diario: REGISTRO: [TIPO]|[MONTO]|[CATEGORIA]|[DESCRIPCION]
- Gráfico de gastos: ACCION: GRAFICO_GASTOS
- Borrados: ACCION: BORRAR_ID|[ID] / ACCION: BORRAR_ULTIMO / ACCION: BORRAR_TODO
- Excel: ACCION: EXCEL
"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy tu asistente financiero y de mercado en vivo.\n\n"
        "📊 **Mercado en vivo (Precios y Variación)**:\n"
        "• '¿a cuánto está MELI?'\n"
        "• '¿cómo viene AAPL hoy?'\n"
        "• 'precio de BTC / SPY / TSLA'\n\n"
        "📈 **Cartera (USD)**:\n"
        "• 'compré 1000 usd de MELI'\n"
        "• '¿cómo viene evolucionando mi cartera?'\n"
        "• 'mostrame la distribución de mis activos'\n\n"
        "💸 **Gastos y Análisis (ARS)**:\n"
        "• 'uber 8.5k' / 'sueldo 950 lucas'\n"
        "• '¿cuál es mi gasto promedio?'\n"
        "• 'mandame un excel'"
    )

async def cmd_borrar_todo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    borrar_todos_los_movimientos()
    await update.message.reply_text("🗑️ Base de datos y cartera reseteadas por completo.")

async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = update.message.text
    historial_ars = obtener_historial_texto()
    
    msg_lower = user_msg.lower()
    es_pregunta_analisis = any(w in msg_lower for w in ["promedio", "analisis", "analizá", "analiza", "aumento", "subió", "subio", "por qué", "por que", "desvío", "desvio", "en qué gasté"])
    
    if es_pregunta_analisis:
        metricas = obtener_metricas_analisis_gastos()
        prompt = f"DATOS ESTADÍSTICOS EXACTOS:\n{metricas}\n\nHISTORIAL DETALLADO:\n{historial_ars}\n\nPREGUNTA DEL USUARIO: {user_msg}"
    else:
        prompt = f"Movimientos ARS:\n{historial_ars}\n\nMensaje del usuario: {user_msg}"

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
        necesita_borrar_id = "ACCION: BORRAR_ID|" in reply
        necesita_precio = "ACCION: CONSULTA_PRECIO|" in reply
        registro_inv = "REGISTRO_INV:" in reply
        registro_ars = "REGISTRO:" in reply
        
        texto_limpio = reply
        for tag in ["ACCION: VER_CARTERA", "ACCION: GRAFICO_INVERSIONES", "ACCION: GRAFICO_GASTOS", "ACCION: EXCEL", "ACCION: BORRAR_TODO", "ACCION: BORRAR_ULTIMO"]:
            texto_limpio = texto_limpio.replace(tag, "")
            
        id_a_borrar = None
        ticker_a_cotizar = None

        if necesita_borrar_id:
            for linea in texto_limpio.splitlines():
                if "ACCION: BORRAR_ID|" in linea:
                    try:
                        id_a_borrar = int(linea.split("ACCION: BORRAR_ID|")[1].strip())
                    except:
                        pass
                    texto_limpio = texto_limpio.replace(linea, "")

        if necesita_precio:
            for linea in texto_limpio.splitlines():
                if "ACCION: CONSULTA_PRECIO|" in linea:
                    ticker_a_cotizar = linea.split("ACCION: CONSULTA_PRECIO|")[1].strip()
                    texto_limpio = texto_limpio.replace(linea, "")

        texto_limpio = texto_limpio.strip()

        # Registro Inversión USD
        if registro_inv:
            partes = texto_limpio.split("REGISTRO_INV:")
            texto_usuario = partes[0].strip()
            datos = [d.strip() for d in partes[1].strip().split("|")]
            ticker = datos[0].upper()
            monto = float(datos[1]) if len(datos) > 1 and datos[1] not in ["0", ""] else None
            p_compra = float(datos[2]) if len(datos) > 2 and datos[2] not in ["0", ""] else None
            cant = float(datos[3]) if len(datos) > 3 and datos[3] not in ["0", ""] else None
            
            t, c, p, m = registrar_operacion_inversion(ticker, monto, p_compra, cant)
            texto_limpio = f"{texto_usuario}\n\n💼 *(Guardado en Cartera: {c:,.4f} {t} a PPC ${p:,.2f} USD | Total: ${m:,.2f} USD)*"

        # Registro Gasto/Ingreso ARS
        if registro_ars and not registro_inv and not necesita_precio:
            partes = texto_limpio.split("REGISTRO:")
            texto_usuario = partes[0].strip()
            datos = [d.strip() for d in partes[1].strip().split("|")]
            if len(datos) == 4:
                guardar_movimiento(datos[0], datos[1], datos[2], datos[3])
                texto_limpio = f"{texto_usuario}\n\n✅ *(Guardado: {datos[0]} de ${float(datos[1]):,.2f} ARS en {datos[2]})*"

        if necesita_borrar_todo:
            borrar_todos_los_movimientos()
            texto_limpio += "\n\n🗑️ *(Base de datos y cartera reseteadas)*"

        if necesita_borrar_ultimo:
            eliminado = borrar_ultimo_movimiento()
            if eliminado:
                texto_limpio += f"\n\n🗑️ *(Eliminado último ARS: {eliminado[1]} de ${float(eliminado[2]):,.2f})*"

        if id_a_borrar is not None:
            eliminado = borrar_por_id(id_a_borrar)
            if eliminado:
                texto_limpio += f"\n\n🗑️ *(Eliminado ID {eliminado[0]})*"

        # Si consultó precio en vivo de una acción / cripto
        if ticker_a_cotizar:
            datos_mkt = consultar_datos_mercado(ticker_a_cotizar)
            if datos_mkt:
                signo = "+" if datos_mkt["var_pct"] >= 0 else ""
                emoji = "🟢" if datos_mkt["var_pct"] >= 0 else "🔴"
                rango_txt = f"\n• *Rango del día:* ${datos_mkt['day_low']:,.2f} - ${datos_mkt['day_high']:,.2f} USD" if datos_mkt["day_high"] else ""
                
                msg_mkt = (
                    f"📈 *{datos_mkt['ticker']} en vivo:*\n\n"
                    f"• *Precio actual:* ${datos_mkt['precio']:,.2f} USD\n"
                    f"• *Variación del día:* {emoji} {signo}${datos_mkt['var_usd']:,.2f} USD ({signo}{datos_mkt['var_pct']:.2f}%)\n"
                    f"• *Cierre anterior:* ${datos_mkt['prev_close']:,.2f} USD"
                    f"{rango_txt}"
                )
                texto_limpio = f"{texto_limpio}\n\n{msg_mkt}".strip()
            else:
                texto_limpio += f"\n\n⚠️ No pude obtener la cotización de `{ticker_a_cotizar}` en este momento."

        await update.message.reply_text(texto_limpio, parse_mode="Markdown")

        # Reporte de cartera
        if necesita_cartera:
            resumen = obtener_resumen_portafolio()
            if not resumen:
                await update.message.reply_text("📉 No tienes activos cargados en tu cartera todavía.")
            else:
                signo = "+" if resumen["pnl_total_usd"] >= 0 else ""
                emoji_rend = "🟢" if resumen["pnl_total_usd"] >= 0 else "🔴"
                
                msg_rep = (
                    f"💼 *ESTADO DE TU CARTERA EN VIVO*\n\n"
                    f"• *Capital Invertido:* ${resumen['total_invertido']:,.2f} USD\n"
                    f"• *Valor de Mercado Actual:* ${resumen['total_actual']:,.2f} USD\n"
                    f"• *Resultado Total (PnL):* {emoji_rend} {signo}${resumen['pnl_total_usd']:,.2f} USD ({signo}{resumen['pnl_total_pct']:.2f}%)\n\n"
                    f"📊 *Detalle por Activo:*\n"
                )
                
                for pos in resumen["posiciones"]:
                    pnl_s = "+" if pos["pnl_usd"] >= 0 else ""
                    em = "🟢" if pos["pnl_usd"] >= 0 else "🔴"
                    peso = (pos['valor_mercado'] / resumen['total_actual'] * 100) if resumen['total_actual'] > 0 else 0
                    msg_rep += (
                        f"▪️ *{pos['ticker']}* ({peso:.1f}% de cartera):\n"
                        f"   - Tenencia: {pos['cantidad']:,.4f} acc/tokens\n"
                        f"   - PPC: ${pos['ppc']:,.2f} | Precio hoy: ${pos['precio_actual']:,.2f} USD\n"
                        f"   - PnL: {em} {pnl_s}${pos['pnl_usd']:,.2f} USD ({pnl_s}{pos['pnl_pct']:.2f}%)\n\n"
                    )
                await update.message.reply_text(msg_rep, parse_mode="Markdown")

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
