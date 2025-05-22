import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # Ganti localhost dengan localhost
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


topic_name = 'sensor-kelembaban-gudang'
gudang_ids = ['G1', 'G2', 'G3']

print(f"Memulai producer kelembaban ke topik: {topic_name}")

try:
    while True:
        gudang_id = random.choice(gudang_ids)
        kelembaban = random.randint(60, 80)  # Kelembaban antara 60 dan 80
        
        message = {
            "gudang_id": gudang_id,
            "kelembaban": kelembaban,
            "timestamp": time.time() # Tambahkan timestamp untuk join nanti
        }
        
        producer.send(topic_name, value=message)
        print(f"Terkirim (Kelembaban): {message}")
        
        time.sleep(1) # Kirim data setiap detik
except KeyboardInterrupt:
    print("Producer kelembaban dihentikan.")
finally:
    producer.close()
