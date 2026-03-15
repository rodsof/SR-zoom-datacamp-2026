import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from kafka import KafkaConsumer
from models import ride_deserializer
TOPIC = 'green-trips-october-2025'
BOOTSTRAP_SERVERS = ['localhost:9092']

print("entra")
consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='rides-greentrips-consumer',
        value_deserializer=ride_deserializer
    )
print(f"Listening to {TOPIC}...")
count = 0
start_time = datetime.now()
for message in consumer:
        trip = message.value
        print(count)
        try:
            if float(trip.trip_distance) > 5: # miles to km
                count += 1

        except Exception as e:
            print(f"Error parsing message: {e}")
            continue
                                                                                               
print(f"Number of trips with trip_distance > 5.0 km: {count}")


consumer.close()
