from flask import Flask, render_template, jsonify, send_from_directory
from flask_socketio import SocketIO
import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import threading
import time
import git
import json
from datetime import datetime, timedelta

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ruta local de tu repositorio
REPO_PATH = "C:/Users/mafierro/3D Objects/grafica_alcaldes"
CSV_PATH = os.path.join(REPO_PATH, "static", "tanque_alcaldes.csv")
REPORTE_PATH = os.path.join(REPO_PATH, "reporte_tanque.json")

def build_query():
    # Obtener datos de las últimas 24 horas
    fecha_fin = datetime.now()
    fecha_inicio = fecha_fin - timedelta(days=1)
    query = f"""
        SELECT Nivel_1, t_stamp
        FROM datos.tanque_alcaldes
        WHERE t_stamp >= '{fecha_inicio}' AND t_stamp <= '{fecha_fin}'
        ORDER BY t_stamp ASC;
    """
    return query

def push_to_github(repo_path, file_path):
    try:
        repo = git.Repo(repo_path)
        repo.git.add(file_path)
        commit_message = f"Update {os.path.basename(file_path)} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        repo.index.commit(commit_message)
        origin = repo.remote(name='origin')
        push_info = origin.push()
        if push_info[0].flags & push_info[0].ERROR:
            logging.error(f"Error al subir a GitHub: {push_info[0].summary}")
        else:
            logging.info(f"{file_path} subido a GitHub exitosamente ✅")
    except git.exc.GitCommandError as e:
        logging.error(f"Error al subir a GitHub: {e}")
        logging.error("Asegúrate de que no haya conflictos y de que tengas permisos para hacer push.")
    except Exception as e:
        logging.error(f"Error inesperado al subir a GitHub: {e}")

def analizar_comportamiento(df):
    NIVEL_MAX = 3.0  # metros
    CAPACIDAD = 5000  # m³
    AREA_BASE_TANQUE = 1720.82  # m²
    CAPACIDAD_PIPA = 7.57  # m³ (7,570 litros)

    df['t_stamp'] = pd.to_datetime(df['t_stamp'])

    # Filtrar solo desde el primer día del mes actual
    now = datetime.now()
    fecha_inicio = pd.Timestamp(year=now.year, month=now.month, day=1)
    df = df[df['t_stamp'] >= fecha_inicio]

    # Filtrar registros con Nivel_1 = 0 (falsos negativos)
    df = df[df['Nivel_1'] > 0]

    # Agregar columna de hora
    df['hora'] = df['t_stamp'].dt.hour

    # Separar en día y noche
    dia = df[(df['hora'] >= 6) & (df['hora'] < 16)]
    noche = df[(df['hora'] >= 16) | (df['hora'] < 6)]

    def calcular_metricas(subset, es_noche=False):
        if subset.empty:
            return {}

        tirante_inicio = subset.iloc[0]['Nivel_1']
        tirante_fin = subset.iloc[-1]['Nivel_1']
        volumen_inicio = (tirante_inicio / NIVEL_MAX) * CAPACIDAD
        volumen_fin = (tirante_fin / NIVEL_MAX) * CAPACIDAD
        volumen_rebombeado = max(volumen_fin - volumen_inicio, 0)

        # Estimación de horas de operación
        delta_t = subset['t_stamp'].diff().median().seconds / 3600 if len(subset) > 1 else 1
        horas_operacion = ((subset['Nivel_1'].diff() > 0).sum()) * delta_t
        gasto_promedio = (volumen_rebombeado * 1000) / (horas_operacion * 3600) if horas_operacion > 0 else 0

        # Análisis de seguridad
        alertas = []
        if es_noche:
            # Calcular la tendencia previa
            subset['tendencia'] = subset['Nivel_1'].diff().rolling(window=3).mean()

            # Ajustar el umbral para detectar cambios significativos
            UMBRAL_CAMBIO_SIGNIFICATIVO = -0.05  # Cambio mínimo de 5 cm en el nivel
            cambios_significativos = subset[(subset['Nivel_1'].diff() < UMBRAL_CAMBIO_SIGNIFICATIVO) & (subset['tendencia'] >= 0)]

            if not cambios_significativos.empty:
                volumen_extraido_total = abs((cambios_significativos['Nivel_1'].diff()).sum() * AREA_BASE_TANQUE)
                VOLUMEN_MINIMO_ALERTA = 1.0  # 1 m³ como mínimo para generar alerta
                if volumen_extraido_total > VOLUMEN_MINIMO_ALERTA:
                    alertas.append(f"Descargas nocturnas no autorizadas: {len(cambios_significativos)} eventos")
                    alertas.append(f"Volumen total extraído: {volumen_extraido_total:.2f} m³ ({volumen_extraido_total * 1000:.0f} litros)")
                    num_pipas_equivalente = volumen_extraido_total / CAPACIDAD_PIPA
                    alertas.append(f"Equivalente a {num_pipas_equivalente:.1f} pipas")

        return {
            "tirante_inicio": round(tirante_inicio, 2),
            "tirante_fin": round(tirante_fin, 2),
            "volumen_inicio": round(volumen_inicio, 0),
            "volumen_fin": round(volumen_fin, 0),
            "volumen_rebombeado": round(volumen_rebombeado, 0),
            "horas_operacion": round(horas_operacion, 1),
            "gasto_lps": round(gasto_promedio, 1),
            "alertas": alertas
        }

    return {
        "dia": calcular_metricas(dia),
        "noche": calcular_metricas(noche, es_noche=True)
    }

def extract_and_update_data():
    db_connection = None
    while True:  # Bucle infinito para ejecutar periódicamente
        try:
            logging.info("Conectando a la base de datos...")
            db_connection_str = 'mysql+pymysql://admin:Password0@192.168.103.2/datos'
            db_connection = create_engine(db_connection_str)
            logging.info("Conexión a la base de datos exitosa.")
            logging.info("Extrayendo datos históricos para tanque_alcaldes...")
            query = build_query()
            df = pd.read_sql(query, con=db_connection)
            if df.empty:
                logging.warning("No se encontraron datos para tanque_alcaldes.")
            else:
                df['nombre_sitio'] = 'tanque_alcaldes'
                df['t_stamp'] = pd.to_datetime(df['t_stamp'])
                df['fecha_hora'] = df['t_stamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                # Guardar CSV en la carpeta static
                df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
                logging.info(f"Historial guardado en {CSV_PATH}")
                # Analizar comportamiento
                reporte = analizar_comportamiento(df)
                with open(REPORTE_PATH, "w", encoding="utf-8") as f:
                    json.dump(reporte, f, indent=4, ensure_ascii=False)
                logging.info(f"Reporte guardado en {REPORTE_PATH}")
                # Subir ambos archivos a GitHub
                push_to_github(REPO_PATH, CSV_PATH)
                push_to_github(REPO_PATH, REPORTE_PATH)
                # Emitir datos y reporte al frontend
                last_data = df.drop_duplicates(subset=['nombre_sitio'], keep='last').tail(1)
                socketio.emit('update_data', {
                    "ultimos_datos": last_data.to_dict(orient='records'),
                    "reporte": reporte
                })
                logging.info(f"Último dato extraído: {last_data.iloc[0]}")
        except Exception as e:
            logging.error(f"Ocurrió un error: {e}")
        finally:
            if db_connection:
                db_connection.dispose()
                logging.info("Conexión a la base de datos cerrada.")
        time.sleep(300)  # Esperar 5 minutos antes de la siguiente ejecución

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/seguridad')
def seguridad():
    return render_template('seguridad.html')

@app.route('/reporte')
def reporte():
    return render_template('reporte.html')

@app.route('/detalle-alertas')
def detalle_alertas():
    return render_template('detalle_alertas.html')

if __name__ == "__main__":
    threading.Thread(target=extract_and_update_data, args=(), daemon=True).start()
    socketio.run(app, debug=True)