from flask import Flask, jsonify, send_file, send_from_directory
import threading
import mqtt_parser
import os

app = Flask(__name__, static_folder=".", static_url_path="")


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/data")
def data():
    import time
    import subprocess
    try:
        git_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.STDOUT).decode("utf-8").strip()
        git_date = subprocess.check_output(["git", "log", "-1", "--format=%cd", "--date=short"], stderr=subprocess.STDOUT).decode("utf-8").strip()
        version_str = f"{git_hash} - {git_date}"
    except Exception:
        version_str = "1.0.1 β" # Fallback

    current_calls = mqtt_parser.get_recent_calls(limit=40)
    return jsonify({"server_time": time.time(), "version": version_str, "calls": current_calls})


@app.route("/gateway_status")
def gateway_status():
    return jsonify({"gateways": mqtt_parser.get_gateway_status()})


@app.route("/clear", methods=["POST"])
def clear_history():
    import sqlite3

    conn = sqlite3.connect(mqtt_parser.DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM calls")
    conn.commit()
    conn.close()
    with mqtt_parser.calls_lock:
        mqtt_parser.calls.clear()
    return jsonify({"status": "ok"})


def start_mqtt():
    mqtt_parser.start_mqtt()


if __name__ == "__main__":

    t = threading.Thread(target=start_mqtt)
    t.daemon = True
    t.start()

    try:
        from waitress import serve

        print("Avvio del server WSGI (Waitress) sulla porta 7000...")
        serve(app, host="0.0.0.0", port=7000)
    except ImportError:
        print(
            "Libreria Waitress non trovata. Esecuzione server di sviluppo Flask sulla porta 958..."
        )
        print(
            "Consiglio: per un ambiente stabile in produzione installa 'pip install waitress'."
        )
        app.run(host="0.0.0.0", port=7000)
