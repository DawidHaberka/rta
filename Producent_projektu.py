import json
import pandas as pd
from time import sleep
from kafka import KafkaProducer

SERVER = "broker:9092"
TOPIC = "samoloty"

# Wczytanie i przygotowanie danych
df = pd.read_csv("8to10.csv")
columns = ['flight','alt_geom', 'gs', 'category', 'lat', 'lon', 'seen_pos', 'geom_rate', 'track_rate', 'distance_km', 'time']
df = df[columns].fillna("Brak danych")

# Grupowanie danych po czasie
grouped = df.groupby("time")

# Producent Kafka
producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

try:
    for time_value, group in grouped:
        records = group.to_dict(orient="records")
        for record in records:
            producer.send(TOPIC, value=record)
        print(f"📦 Wysłano {len(records)} rekordów dla time={time_value}")
        sleep(0.5)
except KeyboardInterrupt:
    print("⛔ Przerwano przez użytkownika.")
finally:
    producer.close()