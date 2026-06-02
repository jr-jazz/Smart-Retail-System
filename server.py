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
    'password': 'root',  # <-- MAKE SURE THIS PASSWORD IS 100% CORRECT
    'database': 'smart_retail_shelf'
}

def log_to_mysql(node_id, mass_kg, height_cm, status, current_time):
    """Establishes a transactional connection to store data rows inside MySQL."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # UPDATED: Matches the clean weight/distance columns
        query = """
            INSERT INTO shelf_telemetry (timestamp, node_id, weight, distance, status)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (current_time, node_id, mass_kg, height_cm, status)
        
        cursor.execute(query, values)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[MYSQL LOGGED] Transaction committed: {mass_kg}kg | {height_cm}cm")
    except mysql.connector.Error as err:
        print(f"[MYSQL ERROR] Database transaction failed: {err}")

def fetch_recent_history():
    """Queries MySQL to pull the last 15 records to pre-fill the web app graph upon reload."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # UPDATED: Pulls directly from weight and distance columns
        query = """
            SELECT timestamp, node_id, CAST(weight AS DOUBLE) as mass_kg, 
                   CAST(distance AS DOUBLE) as height_cm, status 
            FROM shelf_telemetry 
            ORDER BY id DESC LIMIT 15
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        rows.reverse()
        return rows
    except mysql.connector.Error as err:
        print(f"[MYSQL FETCH ERROR] Failed to load history: {err}")
        return []

# ==========================================
# 2. MQTT DIAGNOSTIC SUBSCRIBER PIPELINE
# ==========================================
# Double-check this matches your ESP32 publish topic perfectly (case-sensitive!)
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "smart_retail/analytics/retailshelf" 

connected_web_clients = set()

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"\n[MQTT GATEWAY] Connected to HiveMQ Broker! Result code: {rc}")
    print(f"[MQTT GATEWAY] Actively Subscribing to topic: '{MQTT_TOPIC}'...")
    
    # Force registration check
    result, mid = client.subscribe(MQTT_TOPIC)
    if result == mqtt.MQTT_ERR_SUCCESS:
        print("[MQTT GATEWAY] Subscription request acknowledged by broker.")
    else:
        print(f"[MQTT GATEWAY] CRITICAL: Subscription failed with error code: {result}")

def on_message(client, userdata, msg):
    try:
        raw_data = msg.payload.decode('utf-8')
        print(f"\n[ALERT - INBOUND PACKET RECEIVAL]: {raw_data}")
        
        payload = json.loads(raw_data)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Pull values exactly as your ESP32 string constructs them
        node_id = payload.get('meta', 'ESP32_NODE')  
        mass_kg = float(payload.get('weight', 0.0))   
        height_cm = float(payload.get('distance', 0.0)) 
        status = payload.get('status', 'STANDBY')     

        log_to_mysql(node_id, mass_kg, height_cm, status, current_time)

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
        print(f"[PARSING CRASH] Packet arrived but structure failed to compile: {e}")

# ==========================================
# 3. WEBSOCKETS ENGINE
# ==========================================
async def broadcast_to_webpages(message):
    if connected_web_clients:
        clients = connected_web_clients.copy()
        await asyncio.gather(*[client.send(message) for client in clients], return_exceptions=True)

async def websocket_handler(websocket):
    connected_web_clients.add(websocket)
    print(f"[FRONTEND DISPATCHER] Browser socket established. Active sessions: {len(connected_web_clients)}")
    
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
        print(f"[FRONTEND DISPATCHER] Browser socket dropped. Active sessions: {len(connected_web_clients)}")

# ==========================================
# 4. RUNTIME BOOTSTRAPPER
# ==========================================
main_loop = None

async def main():
    global main_loop
    main_loop = asyncio.get_running_loop()

    print("[BOOT] Starting system diagnostic boot sequence...")
    try:
        test_conn = mysql.connector.connect(**DB_CONFIG)
        test_conn.close()
        print("[BOOT] MySQL verification loop clear.")
    except mysql.connector.Error as err:
        print(f"[BOOT CRITICAL FAILURE] Could not access local MySQL instance daemon: {err}")
        return

    try:
        mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        mqtt_client = mqtt.Client()

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    print(f"[BOOT] Initializing HiveMQ gateway socket connection to port {MQTT_PORT}...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    print("[BOOT] Spawning local host server engine at ws://127.0.0.1:8765...")
    async with websockets.serve(websocket_handler, "0.0.0.0", 8765):
        print("[BOOT READY] Everything initialized. Awaiting hardware transmissions...")
        while True:
            await asyncio.sleep(0.1) 

if __name__ == "__main__":
    asyncio.run(main())