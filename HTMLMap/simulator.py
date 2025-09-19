"""
simulator.py
Generates synthetic network events across continents / regions / countries / DeLand
Posts to server's /ingest endpoint to simulate live traffic
"""

import requests, time, random, threading
from datetime import datetime, timezone
import argparse
SERVER = "http://127.0.0.1:5000"
INGEST = SERVER + "/ingest"

# Define clusters: continents -> region -> country -> city lat/lon
CLUSTERS = {
    "North America": {
        "Southeast US": [
            ("United States", "DeLand, FL", 29.0283, -81.3031),
            ("United States", "Orlando, FL", 28.5383, -81.3792),
            ("United States", "Tampa, FL", 27.9506, -82.4572),
            ("United States", "Miami, FL", 25.7617, -80.1918),
        ],
        "Northeast US": [
            ("United States", "New York, NY", 40.7128, -74.0060),
            ("United States", "Boston, MA", 42.3601, -71.0589),
        ],
        "Central America": [
            ("Mexico", "Mexico City", 19.4326, -99.1332),
        ]
    },
    "Europe": {
        "Western Europe": [
            ("United Kingdom", "London", 51.5074, -0.1278),
            ("Germany", "Frankfurt", 50.1109, 8.6821),
        ],
        "Southern Europe": [
            ("Spain", "Madrid", 40.4168, -3.7038),
        ]
    },
    "Asia": {
        "South Asia": [
            ("India", "Bengaluru", 12.9716, 77.5946),
            ("India", "Mumbai", 19.0760, 72.8777),
        ],
        "East Asia": [
            ("Japan", "Tokyo", 35.6762, 139.6503),
            ("South Korea", "Seoul", 37.5665, 126.9780),
        ]
    },
    "South America": {
        "Brazil/Andes": [
            ("Brazil", "Sao Paulo", -23.55, -46.6333),
            ("Colombia", "Bogota", 4.7110, -74.0721),
        ]
    }
}

# probabilities for blocked vs allowed
STATUS_WEIGHTS = [("allowed", 0.7), ("blocked", 0.3)]

def pick_location():
    if random.random() < 0.5:
        return "North America", "Southeast US", "United States", "DeLand, FL", 29.0283 + random.uniform(-0.02,0.02), -81.3031 + random.uniform(-0.02,0.02)
    continent = random.choice(list(CLUSTERS.keys()))
    region = random.choice(list(CLUSTERS[continent].keys()))
    country, city, lat, lon = random.choice(CLUSTERS[continent][region])
    return continent, region, country, city, lat + random.uniform(-0.02,0.02), lon + random.uniform(-0.02,0.02)


def pick_status():
    r = random.random()
    acc = 0.0
    for s,w in STATUS_WEIGHTS:
        acc += w
        if r <= acc:
            return s
    return "blocked"

def gen_event():
    continent, region, country, city, lat, lon = pick_location()
    status = pick_status()
    # synthetic IP
    ip = ".".join(str(random.randint(1,254)) for _ in range(4))
    ev = {
        "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "lat": lat,
        "lon": lon,
        "continent": continent,
        "region": region,
        "country": country,
        "city": city,
        "ip": ip,
        "status": status
    }
    return ev
stop_event = threading.Event()
event_counts = [0, 0]

def burst_send(rate_per_sec=5, idx=0):
    while not stop_event.is_set():
        batch = [gen_event() for _ in range(max(1, int(rate_per_sec)))]
        event_counts[idx] += len(batch)
        try:
            resp = requests.post(INGEST, json=batch, timeout=3)
            print("posted", len(batch), resp.status_code)
        except Exception as e:
            print("post error:", e)
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Network event simulator")
    parser.add_argument("--rate1", type=int, default=6, help="Events/sec for thread 1")
    parser.add_argument("--rate2", type=int, default=2, help="Events/sec for thread 2")
    parser.add_argument("--duration", type=int, default=0, help="Run duration in seconds (0=forever)")
    args = parser.parse_args()

    print("Starting simulator - posting events to", INGEST)
    t1 = threading.Thread(target=burst_send, args=(args.rate1, 0), daemon=True)
    t2 = threading.Thread(target=burst_send, args=(args.rate2, 1), daemon=True)
    t1.start(); t2.start()
    start_time = time.time()
    try:
        while True:
            time.sleep(1)
            if args.duration > 0 and (time.time() - start_time) > args.duration:
                print("Simulator finished.")
                stop_event.set()
                break
    except KeyboardInterrupt:
        print("simulator stopped")
        stop_event.set()
        t1.join()
        t2.join()
    print(f"Thread 1 sent {event_counts[0]} events.")
    print(f"Thread 2 sent {event_counts[1]} events.")
