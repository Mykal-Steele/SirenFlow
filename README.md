# 🚑 SirenFlow: Intelligent Emergency Transit Optimization

> **Accelerating innovation through the Google Cloud partner ecosystem** > _Submitted for the AI Partner Catalyst Hackathon (Confluent Challenge)_

**SirenFlow** is a cloud-native, event-driven architecture designed to minimize ambulance travel time in high-density metropolitan areas like Bangkok. By leveraging **Confluent Cloud (Apache Kafka)** for real-time data ingestion and **Google Vertex AI (Gemini)** for probabilistic reasoning, SirenFlow dynamically orchestrates "Green Waves", clearing intersections _before_ an emergency vehicle arrives.

## 💡 The Problem & The Solution

Traditional Emergency Vehicle Preemption (EVP) systems are reactive, often triggering too late to clear long traffic queues. SirenFlow solves this by replacing deterministic timers with contextual, AI-driven prediction.

- **Predictive Preemption:** The system identifies a critical window (e.g., 2km to 100m geofence) where intervention is most effective.
- **Contextual Reasoning:** The **Gemini 2.5 Flash-Lite** model analyzes real-time data—ambulance distance, speed, and intersection traffic density—to make a nuanced decision (e.g., `FORCE_GREEN` to change traffic light to green by force.).

## 🏗️ System Architecture

The project follows a strictly decoupled Event-Driven Architecture (EDA) pattern .

- **Ingestion:** Real-time data streams are handled by a **Confluent Cloud Basic Cluster**.
- **Intelligence:** The Python Consumer (`consumer.py`) acts as the "Brain" using **Gemini 2.5 Flash-Lite**.
- **Visualization:** A **Next.js/Mapbox GL JS** frontend will display the ambulance's movement and the dynamically changing traffic light status.

### Data Flow Overview

1.  **Producer (`producer.py`):** Simulates ambulance telemetry and traffic density (Markov Chain).
    - **Topics:** `topic_ambulance_gps`, `topic_sensors`.
2.  **Consumer (`consumer.py`):** Subscribes to both topics, maintains a local state store of traffic density, and triggers the AI.
3.  **Vertex AI:** Receives a structured prompt and outputs a JSON command.
4.  **Command Topic:** The AI's decision is published back to `topic_traffic_commands`.

---

## 🚀 Getting Started

### Prerequisites

- Python 3.x
- Node.js 18+
- A Confluent Cloud Account
- A Google Cloud Project with the **Vertex AI API** enabled

### 1. Installation

```bash
# Clone the repository
git clone [https://github.com/Mykal-Steele/SirenFlow.git](https://github.com/Mykal-Steele/SirenFlow.git)
cd SirenFlow

# Install Python dependencies (using the modern Google Gen AI SDK)
pip install -r backend/requirements.txt
```

### 2\. Configuration (`.env` file)

Create a `.env` file in the root directory to store your credentials securely:

```env
# Google Cloud Authentication
# This file is critical for local development and should be git-ignored.
# Ensure you run 'gcloud auth application-default login' in your terminal
# to set this path, or provide the full path to your service account key.
GOOGLE_APPLICATION_CREDENTIALS=path/to/application_default_credentials.json

# Confluent Cloud Configuration
# Credentials for Kafka producer/consumer authentication (SASL_SSL/PLAIN)
CONFLUENT_BOOTSTRAP=pkc-xxx.cloud.confluent.cloud:9092
CONFLUENT_API_KEY=your-api-key
CONFLUENT_API_SECRET=your-api-secret
```

### 3\. Run the Backend System

You need two terminal windows for the simultaneous simulation.

**Terminal 1: Start the Intelligence Core (The Brain)**

```bash
# This service consumes GPS and Sensor data, calls Gemini, and publishes commands.
python backend/consumer.py
```

**Terminal 2: Start the City Simulator (Producer)**

```bash
# This service streams the moving ambulance GPS and fluctuating traffic data.
python backend/producer.py
```

---

## 📄 Schemas & Deliverables

### AI Command Schema (`topic_traffic_commands`)

The AI is instructed to output a predictable JSON object:

```json
{
  "target_intersection": "ASOK",
  "action": "FORCE_GREEN" | "EXTEND_GREEN" | "PREEMPT_NEXT" | "NO_ACTION",
  "reason": "Reason",
  "confidence": 0.0-1.0,
  "predicted_impact": "Brief impact assessment"
}
```

---

## 🏆 Hackathon Tracks

**SirenFlow** targets the **Confluent Challenge**: "Unleash the power of AI on data in motion."

---

## 📄 License

This project is open-source and available under the **MIT License**.
