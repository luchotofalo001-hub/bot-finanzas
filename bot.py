import os
import io
import datetime
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from google import genai
import psycopg2
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ==================== MINI SERVIDOR WEB PARA RENDER ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot activo y funcionando.")

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()

# Iniciar servidor web en segundo plano
threading.Thread(target=run_web_server, daemon=True).start()

# ==================== VARIABLES DE ENTORNO ====================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

ai_client = genai.Client(api_key=GEMINI_API_KEY)

# ==================== BASE DE DATOS (POSTGRESQL) ====================
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
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
            conn.commit()

init_db()

def guardar_movimiento(tipo, monto, categoria, descripcion):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO movimientos (fecha, tipo, monto, categoria, descripcion) VALUES (NOW(), %s, %s, %s, %s)",
                (tipo.upper(), float(monto), categoria.capitalize(), descripcion)
            )
            conn.commit()

def obtener_dataframe():
    with get_db_connection() as conn:
        query = "SELECT fecha, tipo, monto, categoria, descripcion FROM movimientos ORDER BY fecha ASC;"
        df = pd.read_sql(query, conn)
        return df

def obtener_historial_texto(limite=40):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT fecha, tipo, monto, categoria, descripcion FROM movimientos ORDER BY id DESC LIMIT %s;", (limite,))
            filas = cursor.fetchall()
            if not filas:
                return "No hay transacciones registradas todavía."
            lineas = [f"- [{f[0].strftime('%Y-%m-%d %H:%M')}] {f[1]}: ${f[2]:,.2f} | {f[3]} | {f[4]}" for f in filas]
            return "\n".join(lineas)

# ==================== PROMPT ====================
SYSTEM_INSTRUCTION = """
Eres un asesor financiero personal analítico, práctico y ágil.
El usuario te hablará de sus gastos, ingresos o inversiones, o te pedirá análisis, comparaciones y consejos.
Entiende expresiones cotidianas como "10k", "5 lucas", "15 mil", "2.5k", etc.

REGLAS DE REGISTRO:
Si el mensaje del usuario indica un gasto, ingreso o inversión (incluso en mensajes ultra cortos como "uber 10k", "alquiler 300 lucas", "sueldo 800k"):
1. Clasifica el TIPO (GASTO, INGRESO o INVERSION).
2. Asigna una CATEGORIA clara (ej: Comida, Transporte, Vivienda, Sueldo, Crypto, Salidas, Salud, etc.).
3. Convierte SIEMPRE los montos a números puros (ej: "10k" -> 10000, "2.5 lucas" -> 2500).
4. Agrega OBLIGATORIAMENTE al final de tu respuesta una única línea con este formato:
REGISTRO: [TIPO]|[MONTO]|[CATEGORIA]|[DESCRIPCION]

REGLAS PARA GRÁFICOS Y EXCEL:
- Si el usuario te pide un gráfico (de torta, de barras, etc.), confirma y añade al final:
ACCION: GRAFICO
- Si el usuario te pide una planilla o un archivo Excel, confirma y añade al final:
ACCION: EXCEL

Para balances, consultas y consejos, responde claro usando el historial provisto.
"""

def generar_grafico_gastos():
    df = obtener_dataframe()
    if df.empty:
        return None
    gastos = df[df['tipo'] == 'GASTO']
    if gastos.empty:
        return None
    gastos_por_cat = gastos.groupby('categoria')['monto'].sum().sort_values(ascending=False)
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
    plt.title('Distribución de Gastos por Categoría', fontsize=14, pad=20)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=200)
    buf.seek(0)
    plt.close()
    return buf

def generar_excel():
    df = obtener_dataframe()
    if df.empty:
        return None
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Movimientos', index=False)
    buf.seek(0)
    return buf

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy tu asistente financiero personal con memoria permanente.\n\n"
        "Puedes decirme cosas como:\n"
        "• 'uber 8k'\n"
        "• 'anota un sueldo de 850 lucas'\n"
        "• '¿en qué gasté más este mes?'\n"
        "• 'haceme un gráfico de mis gastos'\n"
        "• 'mandame un excel con mis finanzas'"
    )

async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = update.message.text
    historial = obtener_historial_texto()
    prompt = f"Historial registrado en la base de datos:\n{historial}\n\nMensaje del usuario: {user_msg}"

    try:
        response = ai_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={"system_instruction": SYSTEM_INSTRUCTION}
        )
        reply = response.text
        necesita_grafico = "ACCION: GRAFICO" in reply
        necesita_excel = "ACCION: EXCEL" in reply
        registro_detectado = "REGISTRO:" in reply
        
        texto_limpio = reply.replace("ACCION: GRAFICO", "").replace("ACCION: EXCEL", "").strip()
        
        if registro_detectado:
            partes = texto_limpio.split("REGISTRO:")
            texto_usuario = partes[0].strip()
            datos = [d.strip() for d in partes[1].strip().split("|")]
            if len(datos) == 4:
                guardar_movimiento(datos[0], datos[1], datos[2], datos[3])
                texto_limpio = f"{texto_usuario}\n\n✅ *(Guardado: {datos[0]} de ${float(datos[1]):,.2f} en {datos[2]})*"
        
        await update.message.reply_text(texto_limpio, parse_mode="Markdown")
        
        if necesita_grafico:
            grafico_buf = generar_grafico_gastos()
            if grafico_buf:
                await update.message.reply_photo(photo=grafico_buf, caption="📊 Aquí tienes el gráfico de tus gastos.")
            else:
                await update.message.reply_text("Aún no tienes gastos registrados para armar un gráfico.")
                
        if necesita_excel:
            excel_buf = generar_excel()
            if excel_buf:
                await update.message.reply_document(
                    document=excel_buf, 
                    filename="Reporte_Finanzas.xlsx", 
                    caption="📁 Aquí tienes la planilla Excel completa con todos tus movimientos."
                )
            else:
                await update.message.reply_text("Aún no hay transacciones para generar la planilla.")
                
    except Exception as e:
        await update.message.reply_text(f"Hubo un error: {e}")

if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, responder))
    app.run_polling()
