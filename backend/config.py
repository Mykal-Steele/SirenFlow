import os
from dotenv import load_dotenv

load_dotenv()

# Kafka settings
BOOTSTRAP_SERVER = os.getenv('Bootstrap_server')
API_KEY = os.getenv('Confluent_API')
API_SECRET = os.getenv('Confluent_API_SECRET')

# Google settings
PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID')
GOOGLE_CREDS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
LOCATION = "us-central1"

# Kafka topics
GPS_TOPIC = 'topic_ambulance_gps'
SENSOR_TOPIC = 'topic_sensors'
COMMAND_TOPIC = 'topic_traffic_commands'

# Simulation settings
ASOK_INTERSECTION = {"lat": 13.73702, "lon": 100.56041}

def get_kafka_consumer_config(group_id='sirenflow-brain-v2'):
    return {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET,
        'group.id': group_id,
        'auto.offset.reset': 'latest'
    }

def get_kafka_producer_config():
    return {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET
    }
