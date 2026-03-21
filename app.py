from flask import Flask, jsonify, send_file, send_from_directory
import threading
import mqtt_parser
import os

app = Flask(__name__)


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/style.css")
def style():
    return send_file("style.css")


@app.route("/manifest.json")
def manifest():
    return send_file("manifest.json")


@app.route("/sw.js")
def sw():
    return send_file("sw.js")


@app.route("/icons/<path:path>")
def icons(path):
    return send_from_directory("icons", path)


@app.route("/data")
def data():
    import time

    with mqtt_parser.calls_lock:
        current_calls = list(mqtt_parser.calls)
    return jsonify({"server_time": time.time(), "calls": current_calls})


def start_mqtt():
    mqtt_parser.start_mqtt()


if __name__ == "__main__":

    t = threading.Thread(target=start_mqtt)
    t.daemon = True
    t.start()

    try:
        from waitress import serve

        print("Avvio del server WSGI (Waitress) sulla porta 958...")
        serve(app, host="0.0.0.0", port=7000)
    except ImportError:
        print(
            "Libreria Waitress non trovata. Esecuzione server di sviluppo Flask sulla porta 958..."
        )
        print(
            "Consiglio: per un ambiente stabile in produzione installa 'pip install waitress'."
        )
        app.run(host="0.0.0.0", port=7000)
