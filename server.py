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
    'password': 'your_password_here',  # <-- PUT YOUR ACTUAL MYSQL PASSWORD HERE
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

def on_connect(client, userdata, flags, rc, properties=None):
    """Updated to support both Paho MQTT v1 and v2 callback signatures."""
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

        # Inject runtime timestamp
        payload['timestamp'] = current_time
        
        # Safe async broadcast task creation for running loop
        message_str = json.dumps(payload)
        asyncio.run_coroutine_threadsafe(broadcast_to_webpages(message_str), main_loop)

    except Exception as e:
        print(f"[ERROR] Failed to process incoming telemetry stream: {e}")

# ==========================================
# 3. WEBSOCKETS REAL-TIME ENGINE
# ==========================================
async def broadcast_to_webpages(message):
    """Broadcasts telemetry changes to all open browser windows immediately."""
    if connected_web_clients:
        # Create a copy of the set to avoid modification errors during iteration
        clients = connected_web_clients.copy()
        await asyncio.gather(*[client.send(message) for client in clients], return_exceptions=True)

async def websocket_handler(websocket):
    """Updated syntax for newer websockets library versions."""
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
# 4. RUNTIME SYSTEM EXECUTION (ASYNC ENTRY)
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

    # Boot up background MQTT listener loop using explicit Callback API version 2
    # This completely eliminates the DeprecationWarning
    try:
        mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        # Fallback for older Paho versions if version 2 is unavailable
        mqtt_client = mqtt.Client()

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # Launch WebSocket server on port 8765 using the modern async context manager syntax
    print("[SERVER ENGINE] Starting server gateway on port 8765...")
    async with websockets.serve(websocket_handler, "localhost", 8765):
        await asyncio.Future()  # This keeps the server running forever

if __name__ == "__main__":
    # Use modern asyncio.run() to properly establish the event loop at boot time
    asyncio.run(main())