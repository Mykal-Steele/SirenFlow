# backend/producer.py - Ambulance Simulator
import os
from confluent_kafka import Producer

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

# Kafka producer configuration
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
}

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_message(topic, key, value):
    """Produce a message to the specified topic"""
    producer = Producer(conf)
    producer.produce(topic, key=key, value=value, callback=delivery_report)
    producer.flush()

# Topics
TOPIC_AMBULANCE_GPS = 'topic_ambulance_gps'
TOPIC_TRAFFIC_COMMANDS = 'topic_traffic_commands'
TOPIC_SENSORS = 'topic_sensors'

if __name__ == "__main__":
    print("Kafka producer configured and ready.")
    print(f"Bootstrap Server: {BOOTSTRAP_SERVER}")
    print("Ready to produce messages to topics:")
    print(f"  - {TOPIC_AMBULANCE_GPS}")
    print(f"  - {TOPIC_TRAFFIC_COMMANDS}")
    print(f"  - {TOPIC_SENSORS}")
