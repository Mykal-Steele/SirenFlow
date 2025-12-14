# backend/producer.py - Ambulance Simulator
import os
import time
import json
import random
from confluent_kafka import Producer

def load_env():
    """Load environment variables from .env file"""
    env_vars = {}
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                # Remove quotes if present
                value = value.strip().strip('"').strip("'")
                env_vars[key.strip()] = value
    return env_vars

# Load configuration
env = load_env()

BOOTSTRAP_SERVER = env.get('Bootstrap_server')
API_KEY = env.get('Confluent_API')
API_SECRET = env.get('Confluent_API_SECRET')

# Kafka producer configuration
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
}

# Initialize producer 
producer = Producer(conf)
print("Kafka Producer initialized successfully.")

# Topics
GPS_TOPIC = 'topic_ambulance_gps'
SENSOR_TOPIC = 'topic_sensors'
TOPIC_TRAFFIC_COMMANDS = 'topic_traffic_commands'

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_message(topic, key, value):
    """Produce a message to the specified topic"""
    producer.produce(topic, key=key, value=value, callback=delivery_report)
    producer.flush()

# --- 2. Markov Chain for Traffic Density ---
# Initial density 
current_density = 50.0

def get_next_density():
    """Simulates traffic density using a simple Markov Chain."""
    global current_density
    
    # N(0, sigma) represents small random fluctuations. 
    fluctuation = random.normalvariate(0, 5) 
    
    next_density = current_density + fluctuation
    
    # Constrain the density between 0% and 100%
    next_density = max(0, min(100, next_density))
    current_density = next_density
    
    # Density for the sensors topic 
    sensor_data = {
        "intersection_id": "ASOK",
        "timestamp": int(time.time()),
        "density_percent": round(current_density, 1),
        "queue_length_meters": round(current_density * 5, 0) # Simple proxy for queue length
    }
    
    # Send to SENSOR_TOPIC
    producer.produce(SENSOR_TOPIC, key="ASOK", value=json.dumps(sensor_data))
    
    return sensor_data

# --- 3. Ambulance Telemetry (GPS) ---
# Placeholder coordinates for the ambulance near the target intersection (ASOK)
AMBULANCE_LAT = 13.74000 
AMBULANCE_LON = 100.56000
AMBULANCE_ID = "AMB-TH-882"

def get_ambulance_telemetry(speed=45.0):
    """Generates a message matching the 'topic_ambulance_gps' schema."""
    # NOTE: We'll use static location for now
    telemetry = {
        "vehicle_id": AMBULANCE_ID,
        "timestamp": int(time.time()),
        "location": {
            "lat": AMBULANCE_LAT, 
            "lon": AMBULANCE_LON
        },
        "speed_kmh": speed,
        "status": "EMERGENCY_TRANSIT"
    }
    
    # Send to GPS_TOPIC
    producer.produce(GPS_TOPIC, key=AMBULANCE_ID, value=json.dumps(telemetry))
    
    return telemetry

# --- 4. Main Simulation Loop ---
def run_simulation():
    """Main simulation loop for Day 2"""
    i = 0
    while True:
        # Produce density data and GPS data every 1 second
        density = get_next_density()
        gps = get_ambulance_telemetry()
        
        # Log to console for debugging
        print(f"[{i:03}] -> Sent GPS (Lat: {gps['location']['lat']}, Speed: {gps['speed_kmh']})")
        print(f"      -> Sent Sensor (ASOK Density: {density['density_percent']}%)")
        
        producer.flush(timeout=1) # Ensure messages are sent
        time.sleep(1)
        i += 1

if __name__ == "__main__":
    print("Kafka producer configured and ready.")
    print(f"Bootstrap Server: {BOOTSTRAP_SERVER}")
    print("Ready to produce messages to topics:")
    print(f"  - {GPS_TOPIC}")
    print(f"  - {SENSOR_TOPIC}")
    print(f"  - {TOPIC_TRAFFIC_COMMANDS}")
    print("\nStarting simulation...")
    
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
        producer.flush()
