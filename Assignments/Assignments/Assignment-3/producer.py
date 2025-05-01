from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Kafka Configuration
KAFKA_TOPIC = "traffic_data"
KAFKA_SERVER = "localhost:9092"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Simulate traffic data from 5 sensors
SENSOR_IDS = ["S101", "S102", "S103", "S104", "S105"]

# Generate traffic data every second
while True:
    for sensor_id in SENSOR_IDS:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().isoformat(),
            "vehicle_count": random.randint(0, 50),
            "average_speed": round(random.uniform(20.0, 80.0), 2),
            "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"]),
        }
        producer.send(KAFKA_TOPIC, value=data)
    time.sleep(1)
