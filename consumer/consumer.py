# consumer/consumer.py
from kafka import KafkaConsumer
from questdb.ingress import Sender, TimestampNanos
import json
import time
import os

KAFKA_TOPIC = "location"
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9093") 
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_ILP_PORT = int(os.getenv("QUESTDB_ILP_PORT", 9009))

def connect_to_kafka():
    print(f"Connecting to Kafka at {KAFKA_SERVER}...")
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='questdb-consumer-group'
            )
            print("Successfully connected to Kafka.")
            return consumer
        except Exception as e:
            print(f"Failed to connect to Kafka at {KAFKA_SERVER}, retrying in 5s... Error: {e}")
            time.sleep(5)

def main():
    consumer = connect_to_kafka()

    sender = None
    while sender is None:
        try:
            conf = f"tcp::addr={QUESTDB_HOST}:{QUESTDB_ILP_PORT};protocol_version=2;"
            sender = Sender.from_conf(conf)
            print(f"QuestDB Sender initiated at {QUESTDB_HOST}:{QUESTDB_ILP_PORT}.")
        except Exception as e:
            print(f"Failed to connect to QuestDB at {QUESTDB_HOST}:{QUESTDB_ILP_PORT}, retrying in 5s... Error: {e}")
            time.sleep(5)
    
    with sender:
        print("Waiting for messages from Kafka...")
        
        for message in consumer:
            try:
                data = message.value
                
                sender.row(
                    'locations',
                    symbols={'id': data['id']},
                    columns={'lat': data['lat'], 'lon': data['lon']},
                    at=TimestampNanos.now()
                )
                
            except Exception as e:
                print(f"Error processing message: {message.value}. Error: {e}")

if __name__ == "__main__":
    main()