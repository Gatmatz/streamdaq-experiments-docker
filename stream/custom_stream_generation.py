from datetime import datetime, timedelta
import os
import time
import json
import random
import uuid
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')

def json_serializer(v):
    return json.dumps(v).encode('utf-8')

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=json_serializer,
        linger_ms=0
    )

def main():
    producer = None
    try:
        producer = create_producer()
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        return
    i = 0
    try:
        while True:
            timestamp = datetime.now()

            message = {
                "user_id": f"user_{i % 10}",  # 10 different users
                "event_type": "purchase" if i % 3 == 0 else "view",
                "amount": round(10 + (i * 1.5) % 100, 2),
                "timestamp": int(timestamp.timestamp()),  # in seconds
                "session_id": f"session_{i // 5}",  # 5 events per session
            }

            try:
                producer.send(KAFKA_TOPIC, value=message)
            except Exception as e:
                print(f"Send error: {e}")

            i += 1

            print(f"Sent: {message}")
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