import mysql.connector
import json
import asyncio
import websockets
import paho.mqtt.client as mqtt
from datetime import datetime

# ==========================================
# 1. DATABASE CONFIGURATION & CONNECTIVITY
# ==========================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',          
    'password': 'root',  # <-- CHANGE THIS TO YOUR MYSQL PASSWORD
    'database': 'smart_retail_shelf'
}

def log_to_mysql(node_id, mass_kg, height_cm, status, current_time):
    """Establishes a transactional connection to store data rows inside MySQL."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = """
            INSERT INTO shelf_telemetry (timestamp, node_id, mass_kg, height_cm, status)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (current_time, node_id, mass_kg, height_cm, status)
        
        cursor.execute(query, values)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[MYSQL LOGGED] Transaction committed: {mass_kg}kg at {current_time}")
    except mysql.connector.Error as err:
        print(f"[MYSQL ERROR] Database transaction failed: {err}")

def fetch_recent_history():
    """Queries MySQL to pull the last 15 records to pre-fill the web app graph upon reload."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT timestamp, node_id, CAST(mass_kg AS DOUBLE) as mass_kg, 
                   CAST(height_cm AS DOUBLE) as height_cm, status 
            FROM shelf_telemetry 
            ORDER BY id DESC LIMIT 15
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Chronological rendering flip (left-to-right timeline)
        rows.reverse()
        return rows
    except mysql.connector.Error as err:
        print(f"[MYSQL FETCH ERROR] Failed to load history: {err}")
        return []

# ==========================================
# 2. MQTT BROKER LISTENER (ESP32 PAYLOAD INTEGRATION)
# ==========================================
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "shelf/telemetry"

connected_web_clients = set()

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[MQTT] Connected to HiveMQ Broker with code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Safely convert arriving tokens with explicit safety typing
        node_id = payload.get('node_id', 'ESP32_NODE')
        mass_kg = float(payload.get('mass_kg', 0.0))
        height_cm = float(payload.get('height_cm', 0.0))
        status = payload.get('status', 'STANDBY')

        # Push to transactional log engine
        log_to_mysql(node_id, mass_kg, height_cm, status, current_time)

        # Assemble clean pipeline packet for active browser sockets
        broadcast_payload = {
            "type": "live_update",
            "timestamp": current_time,
            "node_id": node_id,
            "mass_kg": mass_kg,
            "height_cm": height_cm,
            "status": status
        }
        
        message_str = json.dumps(broadcast_payload)
        asyncio.run_coroutine_threadsafe(broadcast_to_webpages(message_str), main_loop)

    except Exception as e:
        print(f"[ERROR] Failed to process incoming telemetry stream: {e}")

# ==========================================
# 3. WEBSOCKETS REAL-TIME ENGINE
# ==========================================
async def broadcast_to_webpages(message):
    if connected_web_clients:
        clients = connected_web_clients.copy()
        await asyncio.gather(*[client.send(message) for client in clients], return_exceptions=True)

async def websocket_handler(websocket):
    connected_web_clients.add(websocket)
    print(f"[WEB DASHBOARD] Dashboard tab connected. Active sessions: {len(connected_web_clients)}")
    
    # Pre-populate chart on initial handshake connection
    history = fetch_recent_history()
    history_packet = {
        "type": "historical_data",
        "data": history
    }
    await websocket.send(json.dumps(history_packet))

    try:
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_web_clients.remove(websocket)
        print(f"[WEB DASHBOARD] Dashboard tab disconnected. Active sessions: {len(connected_web_clients)}")

# ==========================================
# 4. RUNTIME ENVIRONMENT INITIATION
# ==========================================
main_loop = None

async def main():
    global main_loop
    main_loop = asyncio.get_running_loop()

    print("[SERVER ENGINE] Verifying local MySQL parameters...")
    try:
        test_conn = mysql.connector.connect(**DB_CONFIG)
        test_conn.close()
        print("[MYSQL DATABASE] Secure connection verified successfully.")
    except mysql.connector.Error as err:
        print(f"[CRITICAL FAILURE] Cannot reach MySQL Server: {err}")
        return

    try:
        mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        mqtt_client = mqtt.Client()

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    print("[SERVER ENGINE] Starting server gateway on port 8765...")
    # Bind to 0.0.0.0 to break down local routing port blocks
    async with websockets.serve(websocket_handler, "0.0.0.0", 8765):
        await asyncio.Future()  

if __name__ == "__main__":
    asyncio.run(main())