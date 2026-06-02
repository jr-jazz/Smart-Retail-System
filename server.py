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
    'password': 'root', 
    'database': 'smart_retail_shelf'
}

def log_to_mysql(node_id, mass_kg, height_cm, status, current_time):
    """Establishes a connection and inserts a new row into the MySQL database."""
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

# ==========================================
# 2. MQTT BROKER LISTENER & PIPELINE
# ==========================================
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "shelf/telemetry"

connected_web_clients = set()

def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected to HiveMQ Broker with result code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        # Decode data coming from the physical/simulated ESP32
        payload = json.loads(msg.payload.decode('utf-8'))
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Pass variables to our transactional MySQL storage block
        log_to_mysql(
            node_id=payload['node_id'],
            mass_kg=payload['mass_kg'],
            height_cm=payload['height_cm'],
            status=payload['status'],
            current_time=current_time
        )

        # Inject runtime timestamp and push out to our open browser clients
        payload['timestamp'] = current_time
        broadcast_to_webpages(json.dumps(payload))

    except Exception as e:
        print(f"[ERROR] Failed to process incoming telemetry stream: {e}")

# ==========================================
# 3. WEBSOCKETS REAL-TIME ENGINE
# ==========================================
def broadcast_to_webpages(message):
    """Broadcasts telemetry changes to all open browser windows immediately."""
    if connected_web_clients:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = [client.send(message) for client in connected_web_clients]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

async def websocket_handler(websocket, path):
    connected_web_clients.add(websocket)
    print(f"[WEB DASHBOARD] Dashboard tab connected. Active sessions: {len(connected_web_clients)}")
    try:
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_web_clients.remove(websocket)
        print(f"[WEB DASHBOARD] Dashboard tab disconnected. Active sessions: {len(connected_web_clients)}")

# ==========================================
# 4. RUNTIME SYSTEM EXECUTION
# ==========================================
if __name__ == "__main__":
    print("[SERVER ENGINE] Verifying local MySQL parameters...")
    try:
        # Simple handshake check to confirm server credentials at initialization
        test_conn = mysql.connector.connect(**DB_CONFIG)
        test_conn.close()
        print("[MYSQL DATABASE] Secure connection verified successfully.")
    except mysql.connector.Error as err:
        print(f"[CRITICAL FAILURE] Cannot reach MySQL Server: {err}")
        exit(1)

    # Boot up background MQTT listener loop
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # Launch WebSocket server on port 8765
    print("[SERVER ENGINE] Starting server gateway on port 8765...")
    start_server = websockets.serve(websocket_handler, "localhost", 8765)

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()