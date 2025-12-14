# backend/consumer.py - Vertex AI "Brain"
import os
from confluent_kafka import Consumer

# Load environment variables from .env file (no terminal-based env)
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
VERTEX_AI_APIKEY = env.get('VERTEX_AI_APIKEY')

# Kafka consumer configuration with SASL_SSL/PLAIN mechanism
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'group.id': 'vertex-ai-consumer-group-1',
    'auto.offset.reset': 'earliest'
}

# Topics
TOPIC_AMBULANCE_GPS = 'topic_ambulance_gps'
TOPIC_TRAFFIC_COMMANDS = 'topic_traffic_commands'
TOPIC_SENSORS = 'topic_sensors'

def consume_messages(topics):
    """Consume messages from specified topics"""
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8') if msg.value() else None
            print(f"Consumed from {msg.topic()}: key={key}, value={value}")
            
    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    print("Kafka consumer configured. Ready for Day 2.")
    print(f"Bootstrap Server: {BOOTSTRAP_SERVER}")
    print(f"Consumer Group: vertex-ai-consumer-group-1")
    print("Subscribed topics will be:")
    print(f"  - {TOPIC_AMBULANCE_GPS}")
    print(f"  - {TOPIC_TRAFFIC_COMMANDS}")
    print(f"  - {TOPIC_SENSORS}")
