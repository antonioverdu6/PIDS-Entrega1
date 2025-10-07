import os, time, random, socket
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer

# --- Config ---
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC     = os.getenv("TOPIC", "taxi_trips")
CSV_PATH  = os.getenv("CSV_PATH", "/app/prueba.csv")
MIN_INTERVAL = float(os.getenv("MIN_INTERVAL", "0.5"))
MAX_INTERVAL = float(os.getenv("MAX_INTERVAL", "3.0"))
MIN_BATCH    = int(os.getenv("MIN_BATCH", "1"))
MAX_BATCH    = int(os.getenv("MAX_BATCH", "10"))
SHUFFLE      = os.getenv("SHUFFLE", "true").lower() == "true"
DT_FMT = "%m/%d/%Y %I:%M:%S %p"

COLS = ["VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count",
        "trip_distance","RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID",
        "payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount",
        "improvement_surcharge","total_amount","congestion_surcharge"]

def wait_for(host_port):
    host, port = host_port.split(":")
    while True:
        try:
            with socket.create_connection((host, int(port)), timeout=2.0): return
        except OSError: time.sleep(0.5)

def load_rows(path):
    df = pd.read_csv(path, header=None, names=COLS, dtype=str).fillna("")
    if SHUFFLE: df = df.sample(frac=1.0).reset_index(drop=True)
    # duración por fila (si no se puede calcular, 5 min)
    def dur(row):
        try:
            a = datetime.strptime(row["tpep_pickup_datetime"], DT_FMT)
            b = datetime.strptime(row["tpep_dropoff_datetime"], DT_FMT)
            d = b - a
            if d.total_seconds() <= 0 or d > timedelta(hours=3): return timedelta(minutes=5)
            return d
        except Exception:
            return timedelta(minutes=5)
    durations = [dur(r) for _, r in df.iterrows()]
    return df, durations

def to_csv(row): return ",".join(str(row[c]) for c in COLS)

if __name__ == "__main__":
    print(f"🔌 Esperando broker {BOOTSTRAP}…"); wait_for(BOOTSTRAP)
    prod = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all",
                         value_serializer=lambda v: v.encode("utf-8"))
    df, durations = load_rows(CSV_PATH)
    i, n = 0, len(df)
    print(f"▶️ Enviando {n} filas de {CSV_PATH} a topic '{TOPIC}' (batches aleatorios)…")

    while True:
        batch = random.randint(MIN_BATCH, MAX_BATCH)
        for _ in range(batch):
            if i >= n:
                i = 0
                if SHUFFLE: df = df.sample(frac=1.0).reset_index(drop=True); durations = [durations[j] for j in df.index]
            row = df.iloc[i].copy(); i += 1
            now = datetime.now(); row["tpep_pickup_datetime"] = now.strftime(DT_FMT)
            row["tpep_dropoff_datetime"] = (now + durations[i-1]).strftime(DT_FMT)
            prod.send(TOPIC, to_csv(row))
        prod.flush()
        time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))

