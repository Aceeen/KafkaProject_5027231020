import json
import time
import random
from kafka import KafkaProducer

# Konfigurasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # Ganti localhost dengan localhost
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'sensor-suhu-gudang'
gudang_ids = ['G1', 'G2', 'G3']

print(f"Memulai producer suhu ke topik: {topic_name}")
try:
    while True:
        gudang_id = random.choice(gudang_ids)
        suhu = random.randint(75, 90)  # Suhu antara 75 dan 90
        
        message = {
            "gudang_id": gudang_id,
            "suhu": suhu,
            "timestamp": time.time() # Tambahkan timestamp untuk join nanti
        }
        
        producer.send(topic_name, value=message)
        print(f"Terkirim (Suhu): {message}")
        
        time.sleep(1) # Kirim data setiap detik
except KeyboardInterrupt:
    print("Producer suhu dihentikan.")
finally:
    producer.close()
