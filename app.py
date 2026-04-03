from flask import Flask, jsonify, send_file, Response, stream_with_context
import threading
import mqtt_parser
import os
import json
import queue
import time

app = Flask(__name__, static_folder=".", static_url_path="")


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/data")
def data():
    import time
    import subprocess
    try:
        git_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
        git_date = subprocess.check_output(["git", "log", "-1", "--format=%cd", "--date=short"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
        version_str = f"{git_hash} - {git_date}"
    except Exception:
        version_str = "1.0.1 β (Static)" # Fallback

    current_calls = mqtt_parser.get_recent_calls(limit=50)
    return jsonify({"server_time": time.time(), "version": version_str, "calls": current_calls})


@app.route("/gateway_status")
def gateway_status():
    return jsonify({"gateways": mqtt_parser.get_gateway_status()})


@app.route("/events")
def events():
    def event_stream():
        # Crea una coda per questo client
        q = queue.Queue(maxsize=100)
        with mqtt_parser.event_lock:
            mqtt_parser.event_subscribers.append(q)
            print(f"DEBUG SSE: Nuovo client connesso. Totale: {len(mqtt_parser.event_subscribers)}")

        try:
            # Opzionale: invia un evento 'connected'
            yield f"data: {json.dumps({'type': 'system', 'msg': 'connected'})}\n\n"
            
            while True:
                # Blocca finché non arriva un evento
                event = q.get()
                yield f"data: {json.dumps(event)}\n\n"
                q.task_done()
        except GeneratorExit:
            with mqtt_parser.event_lock:
                if q in mqtt_parser.event_subscribers:
                    mqtt_parser.event_subscribers.remove(q)
                    print(f"DEBUG SSE: Client disconnesso. Totale: {len(mqtt_parser.event_subscribers)}")
        except Exception as e:
            print(f"DEBUG SSE ERRORE: {e}")
            with mqtt_parser.event_lock:
                if q in mqtt_parser.event_subscribers:
                    mqtt_parser.event_subscribers.remove(q)

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")


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

        print(f"Avvio del server WSGI (Waitress) sulla porta {mqtt_parser.HTTP_PORT}...")
        serve(app, host="0.0.0.0", port=mqtt_parser.HTTP_PORT)
    except ImportError:
        print(
            f"Libreria Waitress non trovata. Esecuzione server di sviluppo Flask sulla porta {mqtt_parser.HTTP_PORT}..."
        )
        print(
            "Consiglio: per un ambiente stabile in produzione installa 'pip install waitress'."
        )
        app.run(host="0.0.0.0", port=mqtt_parser.HTTP_PORT)
