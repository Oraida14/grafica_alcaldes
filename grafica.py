from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import threading
import time
import git
from datetime import datetime

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ruta local de tu repositorio (ajústala a tu máquina/servidor)
REPO_PATH = "/ruta/a/tu/repositorio"  # 👈 cambiar a donde tengas clonado el repo
# Ruta del archivo CSV dentro del repo
CSV_PATH = os.path.join(REPO_PATH, "tanque_alcaldes_historial_30dias.csv")

def build_query():
    return """
        SELECT Nivel_1, t_stamp
        FROM datos.tanque_alcaldes
        WHERE t_stamp >= NOW() - INTERVAL 30 DAY
        ORDER BY t_stamp DESC;
    """

def push_to_github(repo_path, file_path):
    try:
        repo = git.Repo(repo_path)

        # Agregar archivo al commit
        repo.git.add(file_path)

        # Mensaje con fecha y hora
        commit_message = f"Update datos tanque_alcaldes {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        repo.index.commit(commit_message)

        # Subir cambios
        origin = repo.remote(name='origin')
        origin.push()
        logging.info("Archivo subido a GitHub exitosamente ✅")

    except Exception as e:
        logging.error(f"Error al subir a GitHub: {e}")

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

        # Guardar CSV directamente en el repositorio
        df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
        logging.info(f"Historial guardado en {CSV_PATH}")

        # Subir a GitHub
        push_to_github(REPO_PATH, CSV_PATH)

        # Último dato para API/WebSocket
        last_data = df.sort_values('t_stamp').drop_duplicates(subset=['nombre_sitio'], keep='last')
        socketio.emit('update_data', last_data.to_dict(orient='records'))

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

def periodic_data_update(interval):
    while True:
        extract_and_update_data()
        time.sleep(interval)

if __name__ == "__main__":
    interval = 1800  # cada 30 minutos (puedes bajar a 300 = 5 min si lo quieres en vivo)
    threading.Thread(target=periodic_data_update, args=(interval,), daemon=True).start()
    socketio.run(app, debug=True)
