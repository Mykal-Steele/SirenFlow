# backend/producer.py - Ambulance Simulator
import os
import time
import json
import random
import math
from confluent_kafka import Producer

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points in kilometers"""
    R = 6371

    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    
    return R * c

def load_env():
    """Load environment variables from .env file"""
    env_vars = {}
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
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

# Route and ambulance configuration
ASOK_INTERSECTION = {"lat": 13.73702, "lon": 100.56041}

AMBULANCE_ROUTE_POLYLINE = [
    {"lat": 13.74600, "lon": 100.56700},
    {"lat": 13.74400, "lon": 100.56550},
    {"lat": 13.74200, "lon": 100.56380},
    {"lat": 13.74000, "lon": 100.56200},
    {"lat": 13.73702, "lon": 100.56041}
]

AMBULANCE_ID = "AMB-TH-882"
SPEED_KMH = 45.0
UPDATE_INTERVAL_S = 1.0
DISTANCE_STEP_M = (SPEED_KMH / 3600) * 1000 * UPDATE_INTERVAL_S

polyline_index = 0
current_lat = AMBULANCE_ROUTE_POLYLINE[0]["lat"]
current_lon = AMBULANCE_ROUTE_POLYLINE[0]["lon"]

# Markov Chain for Traffic Density
current_density = 50.0

def get_next_density():
    """Simulates traffic density using a simple Markov Chain."""
    global current_density
    
    fluctuation = random.normalvariate(0, 5) 
    
    next_density = current_density + fluctuation
    
    next_density = max(0, min(100, next_density))
    current_density = next_density
    
    sensor_data = {
        "intersection_id": "ASOK",
        "timestamp": int(time.time()),
        "density_percent": round(current_density, 1),
        "queue_length_meters": round(current_density * 5, 0)
    }
    
    producer.produce(SENSOR_TOPIC, key="ASOK", value=json.dumps(sensor_data))
    
    return sensor_data

def get_ambulance_telemetry():
    """Generates GPS telemetry by moving the ambulance along the polyline"""
    global polyline_index, current_lat, current_lon
    
    if polyline_index >= len(AMBULANCE_ROUTE_POLYLINE) - 1:
        print("\nAmbulance reached the intersection. Stopping simulation.")
        return None
    
    start_point = AMBULANCE_ROUTE_POLYLINE[polyline_index]
    end_point = AMBULANCE_ROUTE_POLYLINE[polyline_index + 1]

    segment_dist_km = haversine(start_point["lat"], start_point["lon"], 
                                end_point["lat"], end_point["lon"])
    segment_dist_m = segment_dist_km * 1000

    current_lat += (end_point["lat"] - start_point["lat"]) / (segment_dist_m / DISTANCE_STEP_M)
    current_lon += (end_point["lon"] - start_point["lon"]) / (segment_dist_m / DISTANCE_STEP_M)

    dist_to_asok_km = haversine(current_lat, current_lon, 
                                ASOK_INTERSECTION["lat"], ASOK_INTERSECTION["lon"])
                                
    if haversine(current_lat, current_lon, end_point["lat"], end_point["lon"]) < DISTANCE_STEP_M/1000:
        polyline_index += 1
        current_lat = end_point["lat"]
        current_lon = end_point["lon"]
        print(f"\n--- AMBULANCE CROSSED SEGMENT {polyline_index-1} ---\n")

    telemetry = {
        "vehicle_id": AMBULANCE_ID,
        "timestamp": int(time.time()),
        "location": {
            "lat": round(current_lat, 6),
            "lon": round(current_lon, 6)
        },
        "speed_kmh": SPEED_KMH,
        "status": "EMERGENCY_TRANSIT",
        "distance_to_asok_km": round(dist_to_asok_km, 3)
    }
    
    producer.produce(GPS_TOPIC, key=AMBULANCE_ID, value=json.dumps(telemetry))
    
    return telemetry

def run_simulation():
    """Main simulation loop"""
    i = 0
    while True:
        density = get_next_density()
        gps = get_ambulance_telemetry()
        
        if gps is None:
            break
            
        print(f"[{i:03}] -> Sent GPS (Dist to ASOK: {gps['distance_to_asok_km']} km)")
        print(f"      -> Sent Sensor (ASOK Density: {density['density_percent']}%)")
        
        producer.flush(timeout=1)
        time.sleep(UPDATE_INTERVAL_S)
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
