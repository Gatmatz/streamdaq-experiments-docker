from datetime import datetime, timedelta
import os
import time
import json
import random
import uuid
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'atlantis_input')

def json_serializer(v):
    return json.dumps(v).encode('utf-8')

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=json_serializer,
        linger_ms=0
    )

def generate_angle_data():
    """Generate 500 columns with angle_{number} and random float values"""
    angles = {}
    for i in range(500):
        angles[f"angle_{i}"] = round(random.uniform(0.0, 360.0), 6)
    return angles

def main():
    producer = None
    try:
        producer = create_producer()
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        return

    try:
        while True:
            timestamp = datetime.now()

            # Generate message with only angles and timestamp
            message = {
                "timestamp": int(timestamp.timestamp()), # In seconds
            }

            # Add 500 angle columns
            message.update(generate_angle_data())

            try:
                producer.send(KAFKA_TOPIC, value=message)
            except Exception as e:
                print(f"Send error: {e}")

            time.sleep(0.9)

    except KeyboardInterrupt:
        print("Interrupted, flushing and closing producer...")
    finally:
        if producer:
            try:
                producer.flush()
                producer.close()
            except Exception:
                pass
        print("Producer closed.")

if __name__ == "__main__":
    main()