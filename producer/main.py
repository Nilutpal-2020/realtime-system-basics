# producer/main.py
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import os
import time

class LocationData(BaseModel):
    id: str
    lat: float
    lon: float

KAFKA_TOPIC = "location"
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9093") 

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Successfully connected to Kafka at {KAFKA_SERVER}")
    except Exception as e:
        print(f"Failed to connect to Kafka at {KAFKA_SERVER}, retrying in 5s... Error: {e}")
        time.sleep(5)


app = FastAPI()

@app.post("/track")
async def track_location(data: LocationData):
    """
    Receives location data from a device and sends it to Kafka.
    """
    try:
        producer.send(
            KAFKA_TOPIC, 
            value=data.model_dump(),
            key=data.id.encode('utf-8')
        )
        producer.flush() 
        
        return {"status": "success", "data": data}
        
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        return {"status": "error", "message": str(e)}