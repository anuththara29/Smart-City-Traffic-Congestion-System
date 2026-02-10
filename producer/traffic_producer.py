
import json, time, random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

sensors = ["J1","J2","J3","J4"]

while True:
    data = {
        "sensor_id": random.choice(sensors),
        "timestamp": str(datetime.now()),
        "vehicle_count": random.randint(1,50),
        "avg_speed": random.randint(5,60)
    }
    producer.send("traffic-data", data)
    print(data)
    time.sleep(1)
