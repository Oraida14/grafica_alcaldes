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
import shutil
from datetime import datetime, timedelta

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REPO_PATH = "C:/Users/mafierro/3D Objects/grafica_alcaldes"
STATIC_PATH = os.path.join(REPO_PATH, "static")
DOCS_PATH = os.path.join(REPO_PATH, "docs")
CSV_PATH = os.path.join(STATIC_PATH, "dashboard_data.csv")
REPORTE_PATH = os.path.join(REPO_PATH, "reporte_nuevo.json")
DOCS_CSV_PATH = os.path.join(DOCS_PATH, "dashboard_data.csv")
DOCS_REPORTE_PATH = os.path.join(DOCS_PATH, "reporte_nuevo.json")

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
    except Exception as e:
        logging.error(f"Error al subir a GitHub: {e}")

def calcular_volumen(tirante):
    radio = 23.4
    pi = 3.1416
    return pi * (radio ** 2) * tirante

def analizar_comportamiento_nuevo(df):
    df['t_stamp'] = pd.to_datetime(df['t_stamp'])
    df = df[df['Nivel_1'] > 0]
    ahora = datetime.now()
    inicio = (ahora - timedelta(days=1)).replace(hour=5, minute=57, second=0, microsecond=0)
    fin = ahora.replace(hour=6, minute=3, second=0, microsecond=0)  # incluir hasta después de las 06:00
    df = df[(df['t_stamp'] >= inicio) & (df['t_stamp'] <= fin)].sort_values('t_stamp')

    if df.empty:
        return {}

    # Obtener el primer valor válido del día actual
    df_hoy = df[df['t_stamp'].dt.day == ahora.day]
    tirante_06_actual = df_hoy.iloc[0]['Nivel_1'] if not df_hoy.empty else None

    # Obtener el valor más cercano a las 16:00 y 06:00 del día anterior
    def valor_mas_cercano(hora_objetivo):
        df['diff_horas'] = abs(df['t_stamp'].dt.hour + df['t_stamp'].dt.minute/60 - hora_objetivo)
        return df.loc[df['diff_horas'].idxmin(), 'Nivel_1']

    tirante_16 = valor_mas_cercano(16)
    tirante_06_anterior = valor_mas_cercano(6)

    volumen_16 = calcular_volumen(tirante_16)
    volumen_06_anterior = calcular_volumen(tirante_06_anterior)
    volumen_06_actual = calcular_volumen(tirante_06_actual) if tirante_06_actual else None
    volumen_rebombeado = volumen_06_actual - volumen_16 if volumen_06_actual else None
    horas_operacion = 10
    gasto_promedio = (volumen_rebombeado * 1000) / (horas_operacion * 3.6) if volumen_rebombeado else None

    return {
        "tirante_termino_anterior_16": round(tirante_16, 2),
        "volumen_termino_anterior_16": round(volumen_16, 4),
        "volumen_acumulado_16_06": round(volumen_rebombeado, 4) if volumen_rebombeado else None,
        "tirante_inicio_anterior_06": round(tirante_06_anterior, 2),
        "volumen_inicio_anterior_06": round(volumen_06_anterior, 4),
        "tirante_inicio_06": round(tirante_06_actual, 2) if tirante_06_actual else None,
        "volumen_inicio_06": round(volumen_06_actual, 4) if volumen_06_actual else None,
        "volumen_rebombeado": round(volumen_rebombeado, 4) if volumen_rebombeado else None,
        "horas_operacion": horas_operacion,
        "gasto_promedio_lps": round(gasto_promedio, 2) if gasto_promedio else None,
        "volumen_total_24h": round(volumen_06_actual, 4) if volumen_06_actual else None
    }

def extract_and_update_data():
    db_connection = None
    while True:
        try:
            logging.info("Conectando a la base de datos...")
            db_connection = create_engine('mysql+pymysql://admin:Password0@192.168.103.2/datos')
            logging.info("Conexión exitosa")
            start_time = (datetime.now() - timedelta(days=1)).replace(hour=5, minute=57, second=0, microsecond=0)
            query = f"""
            SELECT Nivel_1, t_stamp
            FROM datos.tanque_alcaldes
            WHERE t_stamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY t_stamp ASC;
            """
            df = pd.read_sql(query, con=db_connection)
            if df.empty:
                logging.warning("No hay datos para tanque_alcaldes")
            else:
                df['nombre_sitio'] = 'tanque_alcaldes'
                df['t_stamp'] = pd.to_datetime(df['t_stamp'])
                df['fecha_hora'] = df['t_stamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
                logging.info(f"CSV guardado en {CSV_PATH}")
                shutil.copy(CSV_PATH, DOCS_CSV_PATH)
                logging.info(f"CSV copiado a {DOCS_CSV_PATH}")
                reporte = analizar_comportamiento_nuevo(df)
                with open(REPORTE_PATH, 'w', encoding='utf-8') as f:
                    json.dump(reporte, f, indent=4, ensure_ascii=False)
                logging.info(f"Reporte actualizado en {REPORTE_PATH}")
                shutil.copy(REPORTE_PATH, DOCS_REPORTE_PATH)
                logging.info(f"Reporte copiado a {DOCS_REPORTE_PATH}")
                push_to_github(REPO_PATH, DOCS_CSV_PATH)
                push_to_github(REPO_PATH, DOCS_REPORTE_PATH)
                socketio.emit('update_data', {
                    "ultimos_datos": df.tail(1).to_dict(orient='records'),
                    "reporte": reporte
                })
        except Exception as e:
            logging.error(f"Ocurrió un error: {e}")
        finally:
            if db_connection:
                db_connection.dispose()
                logging.info("Conexión cerrada")
        time.sleep(300)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/reporte_nuevo')
def reporte_nuevo():
    try:
        with open(REPORTE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    threading.Thread(target=extract_and_update_data, daemon=True).start()
    socketio.run(app, host="127.0.0.1", port=5502, debug=True)
