import os
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
)

SENSOR_IDS = ["sensor-01", "sensor-02", "sensor-03", "sensor-04", "sensor-05"]

def generar_medicion():
    return {
        "sensor_id": random.choice(SENSOR_IDS),
        "temperature": round(random.uniform(10, 35), 1),
        "humidity": round(random.uniform(30, 90), 1),
        "air_quality_index": random.randint(0, 300),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

while True:
    medicion = generar_medicion()
    producer.send(KAFKA_TOPIC, key=medicion["sensor_id"], value=medicion)
    print(f"Enviado a {KAFKA_TOPIC}: {medicion}")
    time.sleep(1)