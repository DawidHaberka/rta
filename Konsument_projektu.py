from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt
import os

SERVER = "broker:9092"
TOPIC = "samoloty"

# Konsument Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=SERVER,
    auto_offset_reset='latest',   #to sprawia, że konsument czyta dopiero to co się pojawiło po odpaleniu go, a nie wszystko ogólnie
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id="samoloty-grupa"
)

# Folder do zapisów
folder_path = "delay_analysis"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)
else:
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

print("📁 Folder delay_analysis gotowy.")

# Dane w pamięci
flights_data = {}

print("🎧 Konsument nasłuchuje na dane...")

try:
    while True:
        message = consumer.poll(timeout_ms=1000)
        if not message:
            continue

        latest_time = None  # do śledzenia ostatniego czasu z paczki

        for tp, messages in message.items():
            for m in messages:
                record = m.value
                flight = record['flight']
                time_value = record['time']
                distance = record['distance_km']

                if flight not in flights_data:
                    flights_data[flight] = []

                flights_data[flight].append((time_value, distance))

                print(f"✅ Aktualizacja danych lotu {flight}: {time_value}, {distance}")

                latest_time = time_value  # zapisz ostatni czas

        # Po przetworzeniu paczki - aktualizuje pliki i wykresy
        for flight, data in flights_data.items():
            df = pd.DataFrame(data, columns=['time', 'distance_km'])
            flight_csv_path = os.path.join(folder_path, f"{flight}.csv")
            df.to_csv(flight_csv_path, index=False)

            if not df.empty:
                fig, ax = plt.subplots(figsize=(8,6))
                x = pd.to_datetime(df['time'], format='%H%M%S', errors='coerce')
                y = df['distance_km'].astype(float)

                ax.plot(x, y, marker='o')
                ax.set_xlabel('Czas')
                ax.set_ylabel('Dystans od Okęcia (km)')
                ax.set_title(f'Przebieg lotu {flight}')
                ax.grid(True)
                fig.autofmt_xdate()

                flight_png_path = os.path.join(folder_path, f"{flight}_wykres.png")
                fig.savefig(flight_png_path)
                plt.close(fig)

        if latest_time:
            print(f"📈 Wygenerowano wykresy na time = {latest_time}")

except KeyboardInterrupt:
    print("⛔ Przerwano konsumenta.")
