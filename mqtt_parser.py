import paho.mqtt.client as mqtt
import json
import time
import threading
import os
import urllib.request
import datetime
import sqlite3

# Caricamento manuale del file .env (evita dipendenze esterne)
if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                parts = line.strip().split("=", 1)
                if len(parts) == 2:
                    os.environ[parts[0].strip()] = parts[1].strip()

# --- CONFIG ---
DEFAULT_TOPICS = [
    "mmdvm/+/json",
    "p25-gateway/+/json",
    "nxdn-gateway/+/json",
    "dmr-gateway/+/json",
    "dstar-gateway/+/json",
    "ysf-gateway/+/json"
]

env_topics_str = os.environ.get("MQTT_TOPICS", "")
if env_topics_str:
    MQTT_TOPICS = [t.strip() for t in env_topics_str.split(",") if t.strip()]
else:
    env_topic = os.environ.get("MQTT_TOPIC", "")
    if env_topic:
        MQTT_TOPICS = [env_topic] + [t for t in DEFAULT_TOPICS if t != "mmdvm/+/json"]
    else:
        MQTT_TOPICS = DEFAULT_TOPICS

MQTT_CONFIG = {
    "broker": os.environ.get("MQTT_BROKER"),
    "port": int(os.environ.get("MQTT_PORT", 1883)),
    "topics": MQTT_TOPICS,
    "user": os.environ.get("MQTT_USER"),
    "pass": os.environ.get("MQTT_PASS")
}

HTTP_PORT = int(os.environ.get("HTTP_PORT", 7000))

from pathlib import Path
DB_PATH = str(Path(__file__).parent / "dashboard.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Tabella chiamate
    c.execute('''CREATE TABLE IF NOT EXISTS calls
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  from_type TEXT, id_raw TEXT, callsign TEXT, name TEXT,
                  city TEXT, country TEXT,
                  tg TEXT, mode TEXT, slot TEXT, nodo TEXT, ber TEXT,
                  data TEXT, orario TEXT, duration TEXT, start_ts REAL,
                  lat REAL, lon REAL)''')
    
    # Migrazione per database esistenti senza colonne city/country
    try:
        c.execute("ALTER TABLE calls ADD COLUMN city TEXT")
        c.execute("ALTER TABLE calls ADD COLUMN country TEXT")
        print("DEBUG: Colonne city/country aggiunte alla tabella calls.")
    except sqlite3.OperationalError:
        pass
    
    # Migrazione per source_type
    try:
        c.execute("ALTER TABLE calls ADD COLUMN source_type TEXT DEFAULT 'MMDVM'")
        print("DEBUG: Colonna source_type aggiunta alla tabella calls.")
    except sqlite3.OperationalError:
        pass
        pass

    try:
        c.execute("ALTER TABLE calls ADD COLUMN source_ext TEXT")
        print("DEBUG: Colonna source_ext aggiunta alla tabella calls.")
    except sqlite3.OperationalError:
        pass

    # Migrazione per database esistenti senza colonne lat/lon
    try:
        c.execute("ALTER TABLE calls ADD COLUMN lat REAL")
        c.execute("ALTER TABLE calls ADD COLUMN lon REAL")
        print("DEBUG: Colonne lat/lon aggiunte alla tabella calls.")
    except sqlite3.OperationalError:
        pass

    # Tabelle anagrafica
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (radio_id TEXT PRIMARY KEY, callsign TEXT, name TEXT, city TEXT, country TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS nxdn_users
                 (radio_id TEXT PRIMARY KEY, callsign TEXT, name TEXT, city TEXT, country TEXT)''')
    
    # Indici per velocità
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_id ON users(radio_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_nxdn_id ON nxdn_users(radio_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_call ON users(callsign)')
    
    conn.commit()
    conn.close()

init_db()

calls = []
calls_lock = threading.Lock()
gateway_status = {}  # {node_name: {type, action, reason, talkgroup, repeater, timestamp}}
gateway_lock = threading.Lock()
user_map, nxdn_map, callsign_map, tg_map = {}, {}, {}, {}

def format_ber(val):
    if val is None or val == "": return "0%"
    try:
        
        if isinstance(val, str) and "%" in val:
            val = val.replace("%", "")
        f_val = float(val)
        
        if f_val < 0.1 and f_val > 0:
            return f"{f_val:.2f}%"
        return f"{f_val:.1f}%"
    except:
        return str(val)

def download_databases():
    base_dir = "/opt/mmdvm_web"
    if not os.path.exists(base_dir):
        base_dir = "."
    
    urls = {
        "nxdn.csv": "https://radioid.net/static/nxdn.csv",
        "user.csv": "https://radioid.net/static/user.csv",
        "dmrid.dat": "https://radioid.net/static/dmrid.dat"
    }
    
    print(f"DEBUG: Avvio aggiornamento database in {base_dir}...")
    for filename, url in urls.items():
        try:
            path = os.path.join(base_dir, filename)
            urllib.request.urlretrieve(url, path)
            print(f"DEBUG: Scaricato {filename}")
        except Exception as e:
            print(f"Errore download {filename}: {e}")
    load_databases()

def db_scheduler():
    print("DEBUG: Scheduler database avviato (prossimo controllo tra 1 ora)")
    last_update_day = -1
    while True:
        now = datetime.datetime.now()
        
        if now.hour == 4 and now.day != last_update_day:
            download_databases()
            last_update_day = now.day
        time.sleep(3600)

def load_databases():
    global tg_map
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for db_type, filename in [("dmr", "user.csv"), ("nxdn", "nxdn.csv")]:
        paths = [f"/opt/mmdvm_web/{filename}", filename]
        found = False
        for path in paths:
            if os.path.exists(path):
                try:
                    # Controlla se il DB è già popolato
                    table = "users" if db_type == "dmr" else "nxdn_users"
                    c.execute(f"SELECT count(*) FROM {table}")
                    if c.fetchone()[0] > 0:
                        print(f"DEBUG: Tabella {table} già popolata.")
                        found = True
                        break

                    print(f"DEBUG: Migrazione {filename} su SQLite...")
                    loaded_count = 0
                    with open(path, encoding="utf-8", errors="ignore") as f:
                        f.seek(0)
                        for line in f:
                            p = line.strip().split(",")
                            if len(p) >= 3:
                                radio_id = p[0].strip()
                                if radio_id == "RADIO_ID" or not radio_id.isnumeric():
                                    continue
                                callsign = p[1].strip().upper()
                                name = p[2].strip()
                                city = p[4].strip() if len(p) > 4 else ""
                                country = p[6].strip() if len(p) > 6 else ""
                                c.execute(f"INSERT OR REPLACE INTO {table} (radio_id, callsign, name, city, country) VALUES (?, ?, ?, ?, ?)",
                                          (radio_id, callsign, name, city, country))
                                loaded_count += 1
                    conn.commit()
                    print(f"DEBUG: Migrati {loaded_count} record in {table}")
                    found = True
                    break
                except Exception as e:
                    print(f"Errore caricamento {path}: {e}")
        if not found:
            print(f"ATTENZIONE: Database {filename} non trovato.")
    
    conn.close()

    # Caricamento FreeDMR.csv per i TalkGroups
    tg_path = "FreeDMR.csv"
    if os.path.exists(tg_path):
        try:
            loaded_tg = 0
            with open(tg_path, encoding="utf-8", errors="ignore") as f:
                import csv
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 3:
                        tg_id = row[1].strip()
                        tg_name = row[2].strip()
                        tg_map[tg_id] = tg_name
                        loaded_tg += 1
            print(f"DEBUG: Caricati {loaded_tg} record da {tg_path} (TG)")
        except Exception as e:
            print(f"Errore caricamento {tg_path}: {e}")

def get_user_info(radio_id, mode):
    table = "nxdn_users" if mode == "NXDN" else "users"
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(f"SELECT callsign, name, city, country FROM {table} WHERE radio_id=?", (radio_id,))
        res = c.fetchone()
    return res if res else (radio_id, "Unknown", "", "")

def get_callsign_info(callsign):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT name, city, country FROM users WHERE callsign=?", (callsign,))
        res = c.fetchone()
    return res if res else (None, "", "")

def save_or_update_call(call_data):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        if call_data.get("TIME") != "":
            c.execute('''UPDATE calls SET duration=?, ber=? 
                         WHERE mode=? AND slot=? AND start_ts=?''',
                      (call_data["TIME"], call_data["BER"], 
                       call_data["MODE"], call_data["SLOT"], call_data["start_ts"]))
        else:
            c.execute("SELECT id FROM calls WHERE mode=? AND slot=? AND start_ts=?", 
                      (call_data["MODE"], call_data["SLOT"], call_data["start_ts"]))
            existing = c.fetchone()
            if existing:
                c.execute("UPDATE calls SET ber=? WHERE id=?", (call_data["BER"], existing[0]))
            else:
                c.execute('''INSERT INTO calls 
                             (from_type, id_raw, callsign, name, city, country, tg, mode, slot, nodo, ber, data, orario, start_ts, duration, source_ext, lat, lon, source_type)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (call_data["FROM"], call_data["id_raw"], call_data["ID"], call_data["NAME"], 
                            call_data.get("CITY", ""), call_data.get("COUNTRY", ""),
                            call_data["TG"], call_data["MODE"], call_data["SLOT"], call_data["NODO"],
                            call_data["BER"], call_data["DATA"], call_data["ORARIO"], call_data["start_ts"], "",
                            call_data.get("SOURCE_EXT", ""), call_data.get("LAT"), call_data.get("LON"),
                            call_data.get("SOURCE_TYPE", "MMDVM")))
        conn.commit()

def get_recent_calls(limit=40):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT from_type, id_raw, callsign, name, city, country, tg, mode, slot, nodo, ber, data, orario, duration, start_ts, source_ext, lat, lon, source_type 
                 FROM calls ORDER BY id DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    
    results = []
    for r in rows:
        results.append({
            "FROM": r[0], "id_raw": r[1], "ID": r[2], "NAME": r[3],
            "CITY": r[4], "COUNTRY": r[5],
            "TG": r[6], "MODE": r[7], "SLOT": r[8], "NODO": r[9],
            "BER": r[10], "DATA": r[11], "ORARIO": r[12], "TIME": r[13],
            "start_ts": r[14], "SOURCE_EXT": r[15] or "",
            "LAT": r[16], "LON": r[17],
            "SOURCE_TYPE": r[18] if len(r) > 18 else "MMDVM"
        })
    return results


def handle_link_status_message(topic, data, mode, now_ts):
    topic_parts = topic.split('/')
    if len(topic_parts) < 1: return
    
    topic_prefix = topic_parts[0] if topic_parts[0] else ""
    gw_types = {"dstar": "DSTAR", "nxdn": "NXDN", "p25": "P25", "ysf": "YSF", "dmr": "DMR"}
    gw_type = next((v for k, v in gw_types.items() if k in topic_prefix.lower()), 
                   ("MMDVM" if topic_prefix.startswith("mmdvm") else topic_prefix.upper().replace("-GATEWAY", "")))
    
    node_name = topic_parts[1] if len(topic_parts) >= 2 else "N/A"
    link_action = data.get("action", "unknown")
    link_reason = data.get("reason", "")
    link_tg = data.get("talkgroup") or data.get("reflector") or ""
    link_repeater = data.get("repeater") or ""
    link_timestamp = data.get("timestamp") or ""
    
    if mode == "status":
        link_action = "online"
        link_reason = data.get("message", "")
    
    with gateway_lock:
        gateway_status[node_name] = {
            "type": gw_type, "node": node_name, "action": link_action, "reason": link_reason,
            "talkgroup": str(link_tg) if link_tg else "", "repeater": link_repeater,
            "timestamp": link_timestamp, "updated": now_ts
        }

def handle_call_start(topic, data, mode, slot, now_ts):
    topic_parts = topic.split('/')
    node_name = topic_parts[1] if len(topic_parts) >= 2 else "N/A"
    source_type = "MMDVM" if topic.startswith("mmdvm/") else "GATEWAY"
    src_id_raw = str(data.get("source_id") or data.get("src_id") or data.get("radio_id") or "")
    src_call_raw = data.get("source_cs") or data.get("src_callsign") or data.get("callsign") or ""
    
    uid = src_call_raw if (mode.upper() in ["YSF", "D-STAR"] or not src_id_raw) else src_id_raw
    
    with calls_lock:
        if any(c["id_raw"] == uid and c["TIME"] == "" and (now_ts - c["start_ts"]) < 2 for c in calls):
            return

    callsign = src_call_raw.upper().strip()
    name, city, country = f"{mode} User", "", ""
    lookup_call = callsign
    for sfx in ["-RPT", "-G", "-L"]:
        if lookup_call.endswith(sfx):
            lookup_call = lookup_call[:-len(sfx)]; break
    
    if mode.upper() in ["YSF", "D-STAR"]:
        db_res = get_callsign_info(lookup_call)
        if db_res[0]: name, city, country = db_res
        elif src_id_raw:
            callsign_db, name_db, city_db, country_db = get_user_info(src_id_raw, mode)
            if callsign_db != src_id_raw: callsign, name, city, country = callsign_db, name_db, city_db, country_db
    else:
        callsign, name, city, country = get_user_info(src_id_raw, mode)

    tg_id = str(data.get("reflector") or data.get("destination_id") or data.get("dg-id") or 
                data.get("destination_cs") or data.get("tg") or data.get("talkgroup") or "N/A").strip()
    tg_label = tg_map.get(tg_id, "")
    target = f"{tg_id} ({tg_label})" if tg_label else tg_id
    source_ext = data.get("source_ext", "") if mode.upper() == "D-STAR" else ""

    new_call = {
        "FROM": (data.get("source") or data.get("from") or "NET").upper(),
        "id_raw": uid, "ID": callsign, "NAME": name, "CITY": city, "COUNTRY": country,
        "TG": target, "MODE": mode, "SLOT": slot, "NODO": node_name, 
        "BER": format_ber(data.get("ber") or data.get("BER")), "DATA": time.strftime("%d-%m-%Y"),
        "ORARIO": time.strftime("%H:%M:%S"), "TIME": "", "start_ts": now_ts,
        "SOURCE_EXT": source_ext, "LAT": data.get("lat") or data.get("latitude"),
        "LON": data.get("lon") or data.get("longitude") or data.get("lng"), "SOURCE_TYPE": source_type
    }
    
    with calls_lock:
        calls.append(new_call)
        if len(calls) > 40: calls.pop(0)
    save_or_update_call(new_call)

def handle_call_end_or_update(mode, slot, data, now_ts, action):
    with calls_lock:
        for c in reversed(calls):
            if c["MODE"] == mode and c["TIME"] == "" and (mode != "DMR" or c["SLOT"] == slot):
                if action in ["end", "lost", "watchdog", "timeout"]:
                    json_dur = data.get("duration")
                    try:
                        val_dur = round(float(json_dur), 1) if json_dur is not None else round(now_ts - c["start_ts"], 1)
                    except:
                        val_dur = round(now_ts - c["start_ts"], 1)
                    c["TIME"] = f"{val_dur}!" if action == "lost" else val_dur
                
                ber_val = data.get("ber") or data.get("BER")
                if ber_val is not None:
                    c["BER"] = format_ber(ber_val)
                save_or_update_call(c)
                break

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="ignore"))
        mode = list(payload.keys())[0]
        data = payload[mode]
        action = data.get("action")
        slot = data.get("slot", "-")
        now_ts = time.time()

        if mode in ["link", "status"]:
            handle_link_status_message(msg.topic, data, mode, now_ts)
        elif action == "start":
            handle_call_start(msg.topic, data, mode, slot, now_ts)
        else:
            handle_call_end_or_update(mode, slot, data, now_ts, action)
    except Exception as e:
        print(f"Errore parsing: {e}")
def get_gateway_status():
    with gateway_lock:
        return list(gateway_status.values())

def start_mqtt():
    load_databases()
    
    sched_t = threading.Thread(target=db_scheduler)
    sched_t.daemon = True
    sched_t.start()
    
    client = mqtt.Client()
    client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])
    def on_connect(c, u, f, rc):
        if rc == 0:
            print(f"Connesso con successo al broker MQTT. Iscrizione ai topic:")
            for topic in MQTT_CONFIG["topics"]:
                print(f"  - {topic}")
                c.subscribe(topic)
        else:
            print(f"Connessione MQTT fallita (codice rc={rc})")
            
    client.on_connect = on_connect
    client.on_message = on_message
    while True:
        try:
            client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], 60)
            break
        except ConnectionRefusedError:
            print("MQTT Broker non ancora pronto. Riprovo tra 5 secondi...")
            time.sleep(5)
        except Exception as e:
            print(f"Errore connessione MQTT: {e}")
            time.sleep(5)
    client.loop_forever()

if __name__ == "__main__":
    start_mqtt()
