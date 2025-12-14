import os
import json
import time
from google import genai
from google.genai import types
from confluent_kafka import Consumer, Producer
from config import (
    get_kafka_consumer_config,
    get_kafka_producer_config,
    PROJECT_ID,
    GOOGLE_CREDS,
    LOCATION,
    GPS_TOPIC,
    SENSOR_TOPIC,
    COMMAND_TOPIC
)

# Constants
AI_COOLDOWN_SECONDS = 3
MAX_DISTANCE_FOR_AI_KM = 2.0
MAX_POSITION_HISTORY = 5

# Initialize Gemini AI
print("Starting Gemini AI...")
try:
    if GOOGLE_CREDS:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_CREDS
    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location=LOCATION
    )
    print("Gemini connected")
except Exception as e:
    print(f"Failed to initialize Gemini: {e}")
    print("Check your .env file for GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_PROJECT_ID")
    exit(1)

producer = Producer(get_kafka_producer_config())

traffic_data = {
    "ASOK": {"density": 0, "queue": 0, "last_updated": 0}
}
ambulance_positions = {}
last_ai_call = 0

def ask_gemini(vehicle_data, traffic_context, history):
    vehicle_id = vehicle_data['vehicle_id']
    dist = vehicle_data['distance_to_asok_km']
    speed = vehicle_data.get('speed_kmh', 0)
    density = traffic_context['density']
    queue = traffic_context['queue']
    
    eta_minutes = (dist / speed * 60) if speed > 0 else 999
    prev_distances = [h['distance'] for h in history[-3:]] if history else []
    
    prompt = f"""Traffic light control for emergency vehicle:

Situation:
- Ambulance {vehicle_id}
- Distance: {dist:.2f} km, Speed: {speed:.1f} km/h, ETA: {eta_minutes:.1f} min
- Traffic density: {density:.1f}%, Queue: {queue:.0f}m
- Recent distances: {prev_distances}

YOUR OBJECTIVE:
Balance emergency vehicle priority with overall traffic efficiency. Consider:
- Urgency: How critical is immediate action based on distance + speed?
- Traffic Impact: Will forcing green cause massive backup elsewhere?
- Timing: Can we optimize light timing instead of forcing immediate change?
- Efficiency: Will vehicle arrive during natural green phase anyway?

AVAILABLE ACTIONS (choose the most appropriate):

NO_ACTION:
- Use when vehicle is far away or moving very slowly
- Use when light will naturally be green when ambulance arrives
- Use when traffic is light and timing will work out naturally
- Least disruptive option

PREEMPT_NEXT:
- Use when ambulance is approaching but not urgent yet
- Schedules green for next cycle (smart timing)
- Moderate disruption, good for planning ahead
- Balances preparation with minimal traffic impact

EXTEND_GREEN:
- Use when light is currently green and ambulance is approaching
- Keeps current green phase going longer
- Low disruption if already green
- Good for maintaining flow without full reset

FORCE_GREEN:
- Use ONLY when truly critical
- When ambulance is very close AND traffic is heavy
- Immediate green but causes maximum disruption
- Use sparingly - consider if other options can work

THINK STRATEGICALLY:
- Default to NO_ACTION or PREEMPT_NEXT unless truly urgent
- FORCE_GREEN should be rare - only when absolutely necessary
- Consider if natural traffic flow will clear by arrival time
- Account for queue length - forcing green with huge queue causes gridlock
- BE CONSISTENT: stick with your strategy unless conditions change significantly

Return ONLY valid JSON (pick ONE action):
{{
  "target_intersection": "ASOK",
  "action": "string (must be exactly one of: NO_ACTION, PREEMPT_NEXT, EXTEND_GREEN, FORCE_GREEN)",
  "reason": "Your strategic reasoning (one sentence)",
  "confidence": 0.0-1.0,
  "predicted_impact": "Brief impact assessment"
}}
"""
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.7,
                max_output_tokens=300
            )
        )
        raw_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(raw_text)
    except Exception as e:
        print(f"AI decision error: {e}")
        return None

def process_sensor_data(data):
    intersection = data.get('intersection_id')
    if intersection:
        traffic_data[intersection] = {
            "density": data.get('density_percent', 0),
            "queue": data.get('queue_length_meters', 0),
            "last_updated": data.get('timestamp')
        }

def process_ambulance_data(data):
    global last_ai_call
    
    vehicle_id = data.get('vehicle_id')
    dist_km = data.get('distance_to_asok_km', 999)
    speed_kmh = data.get('speed_kmh', 0)
    context = traffic_data.get("ASOK")
    
    if vehicle_id not in ambulance_positions:
        ambulance_positions[vehicle_id] = []
    
    ambulance_positions[vehicle_id].append({
        'distance': dist_km,
        'speed': speed_kmh,
        'timestamp': time.time()
    })
    ambulance_positions[vehicle_id] = ambulance_positions[vehicle_id][-MAX_POSITION_HISTORY:]
    
    print(f"\n{'='*60}")
    print(f"{vehicle_id} status:")
    print(f"  Distance: {dist_km:.3f} km | Speed: {speed_kmh:.1f} km/h")
    print(f"  Traffic: {context['density']:.1f}% density, {context['queue']:.0f}m queue")
    
    if dist_km < MAX_DISTANCE_FOR_AI_KM and (time.time() - last_ai_call > AI_COOLDOWN_SECONDS):
        print(f"  Requesting AI decision...")
        decision = ask_gemini(data, context, ambulance_positions[vehicle_id])
        
        if decision:
            action = decision['action']
            confidence = decision.get('confidence', 0)
            
            if action == "FORCE_GREEN":
                print(f"  >> FORCE GREEN (confidence: {confidence:.0%})")
            elif action == "EXTEND_GREEN":
                print(f"  >> Extend green (confidence: {confidence:.0%})")
            elif action == "PREEMPT_NEXT":
                print(f"  >> Preempt next cycle (confidence: {confidence:.0%})")
            else:
                print(f"  >> No action (confidence: {confidence:.0%})")
                
            print(f"  Reason: {decision['reason']}")
            print(f"  Impact: {decision.get('predicted_impact', 'unknown')}")
            
            if action != "NO_ACTION":
                producer.produce(COMMAND_TOPIC, key="ASOK", value=json.dumps(decision))
                producer.flush()
            
        last_ai_call = time.time()
    else:
        if dist_km >= MAX_DISTANCE_FOR_AI_KM:
            print(f"  Too far for AI decision")
        else:
            print(f"  Waiting for cooldown ({AI_COOLDOWN_SECONDS}s)")
    print('='*60)

def run_consumer():
    consumer = Consumer(get_kafka_consumer_config())
    consumer.subscribe([GPS_TOPIC, SENSOR_TOPIC])
    print("Consumer running, listening for messages...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                topic = msg.topic()

                if topic == SENSOR_TOPIC:
                    process_sensor_data(data)
                elif topic == GPS_TOPIC:
                    process_ambulance_data(data)

            except json.JSONDecodeError:
                print(f"Invalid JSON from {msg.topic()}")
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()
