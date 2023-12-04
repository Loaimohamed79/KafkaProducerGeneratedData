from kafka import KafkaProducer
from faker import Faker
import json
import time
from datetime import datetime

# Initialize Faker
fake = Faker()

# Define Kafka parameters
bootstrap_servers = ['worker2:9092']
topic = 'Demo'

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def generate_random_data():
    name = fake.name()
    age = fake.random_int(min=18, max=80, step=1)
    gender = fake.random_element(elements=('Male', 'Female'))
    systime = datetime.now().strftime("%Y-%m-%d")
    return {'name': name, 'age': age, 'gender': gender, 'systime': systime}

try:
    while True:
        data = generate_random_data()
        producer.send(topic, value=data)
        print(data)
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    producer.close()

