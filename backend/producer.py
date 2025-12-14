import time
import json
import random
import math
from confluent_kafka import Producer
from config import (
    get_kafka_producer_config,
    GPS_TOPIC,
    SENSOR_TOPIC,
    COMMAND_TOPIC,
    ASOK_INTERSECTION
)

# Simulation constants
AMBULANCE_ID = "AMB-TH-882"
BASE_SPEED_KMH = 60.0
MIN_SPEED_KMH = 20.0
MAX_SPEED_KMH = 80.0
UPDATE_INTERVAL_S = 1.0

AMBULANCE_ROUTE = [
    {"lat": 13.74600, "lon": 100.56700},
    {"lat": 13.74400, "lon": 100.56550},
    {"lat": 13.74200, "lon": 100.56380},
    {"lat": 13.74000, "lon": 100.56200},
    {"lat": 13.73702, "lon": 100.56041}
]

producer = Producer(get_kafka_producer_config())
print("Kafka Producer initialized")

def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance between two GPS coordinates in km"""
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

polyline_index = 0
current_lat = AMBULANCE_ROUTE[0]["lat"]
current_lon = AMBULANCE_ROUTE[0]["lon"]
current_speed = BASE_SPEED_KMH
current_density = 50.0

def get_next_density():
    global current_density
    fluctuation = random.normalvariate(0, 5)
    current_density = max(0, min(100, current_density + fluctuation))
    
    sensor_data = {
        "intersection_id": "ASOK",
        "timestamp": int(time.time()),
        "density_percent": round(current_density, 1),
        "queue_length_meters": round(current_density * 5, 0)
    }
    
    producer.produce(SENSOR_TOPIC, key="ASOK", value=json.dumps(sensor_data))
    return sensor_data

def get_ambulance_telemetry():
    global polyline_index, current_lat, current_lon, current_speed
    
    if polyline_index >= len(AMBULANCE_ROUTE) - 1:
        print("\nAmbulance reached intersection")
        return None
    
    start_point = AMBULANCE_ROUTE[polyline_index]
    end_point = AMBULANCE_ROUTE[polyline_index + 1]

    dist_to_asok_km = haversine(
        current_lat, current_lon, 
        ASOK_INTERSECTION["lat"], ASOK_INTERSECTION["lon"]
    )
    
    target_speed = BASE_SPEED_KMH
    
    if dist_to_asok_km < 0.3:
        target_speed = MIN_SPEED_KMH + (dist_to_asok_km / 0.3) * (BASE_SPEED_KMH - MIN_SPEED_KMH)
    elif dist_to_asok_km > 1.0:
        target_speed = BASE_SPEED_KMH + random.uniform(-5, 10)
    else:
        traffic_factor = max(0.3, 1.0 - (current_density / 200))
        target_speed = BASE_SPEED_KMH * traffic_factor + random.uniform(-8, 8)
    
    target_speed = max(MIN_SPEED_KMH, min(MAX_SPEED_KMH, target_speed))
    
    if abs(current_speed - target_speed) > 15:
        acceleration = 3.0 if target_speed > current_speed else -4.0
        current_speed += acceleration * UPDATE_INTERVAL_S
    else:
        current_speed += (target_speed - current_speed) * 0.3
    
    current_speed = max(MIN_SPEED_KMH, min(MAX_SPEED_KMH, current_speed))
    
    distance_step_m = (current_speed / 3600) * 1000 * UPDATE_INTERVAL_S

    segment_dist_km = haversine(
        start_point["lat"], start_point["lon"], 
        end_point["lat"], end_point["lon"]
    )
    segment_dist_m = segment_dist_km * 1000

    current_lat += (end_point["lat"] - start_point["lat"]) / (segment_dist_m / distance_step_m)
    current_lon += (end_point["lon"] - start_point["lon"]) / (segment_dist_m / distance_step_m)
                                
    if haversine(current_lat, current_lon, end_point["lat"], end_point["lon"]) < distance_step_m/1000:
        polyline_index += 1
        current_lat = end_point["lat"]
        current_lon = end_point["lon"]
        print(f"\nCrossed segment {polyline_index-1}\n")

    telemetry = {
        "vehicle_id": AMBULANCE_ID,
        "timestamp": int(time.time()),
        "location": {
            "lat": round(current_lat, 6),
            "lon": round(current_lon, 6)
        },
        "speed_kmh": round(current_speed, 1),
        "status": "EMERGENCY_TRANSIT",
        "distance_to_asok_km": round(dist_to_asok_km, 3)
    }
    
    producer.produce(GPS_TOPIC, key=AMBULANCE_ID, value=json.dumps(telemetry))
    return telemetry

def run_simulation():
    iteration = 0
    while True:
        density = get_next_density()
        gps = get_ambulance_telemetry()
        
        if gps is None:
            break
            
        print(f"[{iteration:03}] Distance: {gps['distance_to_asok_km']} km, Density: {density['density_percent']}%")
        
        producer.flush(timeout=1)
        time.sleep(UPDATE_INTERVAL_S)
        iteration += 1

if __name__ == "__main__":
    print("Producer initialized")
    print(f"Topics: {GPS_TOPIC}, {SENSOR_TOPIC}, {COMMAND_TOPIC}")
    print("Starting simulation\n")
    
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("\nStopped")
        producer.flush()
