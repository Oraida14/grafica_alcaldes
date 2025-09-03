from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import threading
import time
import git
import json
from datetime import datetime

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ruta local de tu repositorio
REPO_PATH = "C:/Users/mafierro/3D Objects/grafica_alcaldes"  # 👈 ajusta a tu ruta local
CSV_PATH = os.path.join(REPO_PATH, "tanque_alcaldes_historial_30dias.csv")
REPORTE_PATH = os.path.join(REPO_PATH, "reporte_tanque.json")

def build_query():
    return """
        SELECT Nivel_1, t_stamp
        FROM datos.tanque_alcaldes
        WHERE t_stamp >= NOW() - INTERVAL 30 DAY
        ORDER BY t_stamp ASC;
    """

def push_to_github(repo_path, file_path):
    try:
        repo = git.Repo(repo_path)
        repo.git.add(file_path)

        commit_message = f"Update {os.path.basename(file_path)} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        repo.index.commit(commit_message)

        origin = repo.remote(name='origin')
        origin.push()
        logging.info(f"{file_path} subido a GitHub exitosamente ✅")

    except Exception as e:
        logging.error(f"Error al subir a GitHub: {e}")

def analizar_comportamiento(df):
    """
    Analiza comportamiento día/noche del tanque Alcaldes
    """
    NIVEL_MAX = 3.0  # metros
    CAPACIDAD = 5000  # m3

    df['t_stamp'] = pd.to_datetime(df['t_stamp'])
    df['hora'] = df['t_stamp'].dt.hour

    dia = df[(df['hora'] >= 6) & (df['hora'] < 16)]
    noche = df[(df['hora'] >= 16) | (df['hora'] < 6)]

    def calcular_metricas(subset):
        if subset.empty:
            return {}
        tirante_inicio = subset.iloc[0]['Nivel_1']
        tirante_fin = subset.iloc[-1]['Nivel_1']
        volumen_inicio = (tirante_inicio / NIVEL_MAX) * CAPACIDAD
        volumen_fin = (tirante_fin / NIVEL_MAX) * CAPACIDAD
        volumen_rebombeado = max(volumen_fin - volumen_inicio, 0)

        # Estimación de horas de operación
        delta_t = df['t_stamp'].diff().median().seconds / 3600 if len(df) > 1 else 1
        horas_operacion = ((subset['Nivel_1'].diff() > 0).sum()) * delta_t

        gasto_promedio = (volumen_rebombeado * 1000) / (horas_operacion * 3600) if horas_operacion > 0 else 0

        return {
            "tirante_inicio": round(tirante_inicio, 2),
            "tirante_fin": round(tirante_fin, 2),
            "volumen_inicio": round(volumen_inicio, 0),
            "volumen_fin": round(volumen_fin, 0),
            "volumen_rebombeado": round(volumen_rebombeado, 0),
            "horas_operacion": round(horas_operacion, 1),
            "gasto_lps": round(gasto_promedio, 1)
        }

    return {
        "dia": calcular_metricas(dia),
        "noche": calcular_metricas(noche)
    }

def extract_and_update_data():
    db_connection = None
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
            return

        df['nombre_sitio'] = 'tanque_alcaldes'
        df['t_stamp'] = pd.to_datetime(df['t_stamp'])
        df['fecha_hora'] = df['t_stamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Guardar CSV
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
        last_data = df.sort_values('t_stamp').drop_duplicates(subset=['nombre_sitio'], keep='last')
        socketio.emit('update_data', {
            "ultimos_datos": last_data.to_dict(orient='records'),
            "reporte": reporte
        })

    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")
    finally:
        if db_connection:
            db_connection.dispose()
            logging.info("Conexión a la base de datos cerrada.")

@app.route('/')
def index():
    return render_template('mapa.html')

@app.route('/api/data')
def get_data():
    data = pd.read_csv(CSV_PATH)
    return jsonify(data.to_dict(orient='records'))

@app.route('/api/reporte')
def get_reporte():
    with open(REPORTE_PATH, "r", encoding="utf-8") as f:
        reporte = json.load(f)
    return jsonify(reporte)

def periodic_data_update(interval):
    while True:
        extract_and_update_data()
        time.sleep(interval)

if __name__ == "__main__":
    interval = 1800  # cada 30 min
    threading.Thread(target=periodic_data_update, args=(interval,), daemon=True).start()
    socketio.run(app, debug=True)
