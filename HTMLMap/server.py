"""
server.py
Simple Flask backend for ingesting network events and serving aggregated GeoJSON
Supports hierarchical aggregation: continent -> region -> country -> city/point
"""

from flask import Flask, request, jsonify, send_from_directory
from collections import defaultdict, deque
from datetime import datetime, timedelta
import threading, time, json, math, os

app = Flask(__name__, static_folder='.')

# In-memory storage of raw events (for demo only). In production use a DB.
# We'll keep last N minutes of events for time-windowed queries.
EVENT_RETENTION_MINUTES = 60
_lock = threading.Lock()
events = deque()  # store (timestamp_iso, src_lat, src_lon, continent, region, country, city, ip, status)

# Repeat offender counter (increment per alert-worthy event)
repeat_counts = defaultdict(int)

# Pre-saved continents/regions mapping (for simulator consistency)
# For simplicity, continent/region/country strings come with the simulated events.
PERSIST_FILE = "events_persist.json"


CAMPUS_COLORS = {
    "DeLand, FL": "#377eb8",
    "Gulfport, FL": "#4daf4a",
    "Tampa, FL": "#984ea3",
    "Celebration, FL": "#ff7f00",
    "Unknown": "#999999"
}

#  campus mapping 
CITY_TO_CAMPUS = {
    "DeLand, FL": "DeLand, FL",
    "Orlando, FL": "DeLand, FL",
    "Miami, FL": "DeLand, FL",
    "Tampa, FL": "Tampa, FL",
    "Gulfport, FL": "Gulfport, FL",
    "St. Petersburg, FL": "Gulfport, FL",
    "Celebration, FL": "Celebration, FL"
}

def persist_periodically():
    """Every 10s: write recent events to disk (for crash recovery)."""
    while True:
        time.sleep(10)
        with _lock:
            try:
                dump = list(events)
                with open(PERSIST_FILE, "w") as f:
                    json.dump(dump, f)
            except Exception as e:
                print("persist error:", e)

def cleanup_old_events():
    """Remove events older than retention window periodically."""
    while True:
        time.sleep(5)
        cutoff = datetime.utcnow() - timedelta(minutes=EVENT_RETENTION_MINUTES)
        cutoff_s = cutoff.isoformat()
        with _lock:
            while events and events[0][0] < cutoff_s:
                events.popleft()

# start background threads
threading.Thread(target=persist_periodically, daemon=True).start()
threading.Thread(target=cleanup_old_events, daemon=True).start()

@app.route("/")
def index():
    return send_from_directory('.', 'map.html')

@app.route("/ingest", methods=["POST"])
def ingest():
    """
    Accepts JSON single event or list of events with fields:
    timestamp (ISO str), lat, lon, continent, region, country, city, ip, status ("allowed"/"blocked")
    """
    payload = request.get_json()
    if payload is None:
        return jsonify({"error":"no json"}), 400

    batch = payload if isinstance(payload, list) else [payload]
    added = 0
    with _lock:
        for ev in batch:
            # Validate minimal fields
            try:
                ts = ev.get("timestamp") or datetime.utcnow().isoformat()
                lat = float(ev["lat"]);
                lon = float(ev["lon"]);
                continent = ev.get("continent", "Unknown")
                region = ev.get("region", "Unknown")
                country = ev.get("country", "Unknown")
                city = ev.get("city", "Unknown")
                ip = ev.get("ip", "0.0.0.0")
                status = ev.get("status", "blocked").lower()
            except Exception as e:
                print(f"Skipped event due to error: {e}, event: {ev}")
                continue

            events.append((ts, lat, lon, continent, region, country, city, ip, status))
            added += 1
            # increment repeat counter for blocked events (simple logic)
            if status != "allowed":
                repeat_counts[ip] += 1

    return jsonify({"ingested": added}), 200

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

def make_aggregates(level, time_minutes=15, status_filter=None, top_k=200, bbox=None):
    """
    level: one of ["continent","region","country","city","point"]
    time_minutes: time window
    status_filter: None|"allowed"|"blocked"
    bbox: optional bounding box [minLat,minLon,maxLat,maxLon] to limit results
    Returns GeoJSON FeatureCollection with properties: count, allowed_count, blocked_count, label
    """
    cutoff = datetime.utcnow() - timedelta(minutes=time_minutes)
    cutoff_s = cutoff.isoformat()
    agg = {}  # kdict of stats 
    with _lock:
        for ts, lat, lon, continent, region, country, city, ip, status in list(events):
            if ts < cutoff_s:
                continue
            if status_filter and status.lower() != status_filter.lower():
                continue
            if bbox:
                minLat, minLon, maxLat, maxLon = bbox
                if not (minLat <= lat <= maxLat and minLon <= lon <= maxLon):
                    continue

            if level == "continent":
                key = continent
                rep = (lat, lon)
            elif level == "region":
                key = f"{continent}||{region}"
                rep = (lat, lon)
            elif level == "country":
                key = f"{continent}||{region}||{country}"
                rep = (lat, lon)
            elif level == "city":
                key = f"{continent}||{region}||{country}||{city}"
                rep = (lat, lon)
            elif level == "point":
                key = f"{round(lat,4)}||{round(lon,4)}"
                rep = (lat, lon)
            else:
                key = continent
                rep = (lat, lon)

            stats = agg.get(key)
            if not stats:
                stats = {
                    "count": 0, "allowed": 0, "blocked": 0,
                    "lat": rep[0], "lon": rep[1], "label": key,
                    "ips": defaultdict(int),
                    "site": CITY_TO_CAMPUS.get(city, "Unknown")
                }
                agg[key] = stats

            stats["count"] += 1
            stats["ips"][ip] += 1
            if status == "allowed":
                stats["allowed"] += 1
            else:
                stats["blocked"] += 1

    items = list(agg.items())
    items.sort(key=lambda kv: kv[1]["count"], reverse=True)
    items = items[:top_k]

    features = []
    rank = 0
    for key, s in items:
        rank += 1
        total = s["count"]
        allowed_ratio = s["allowed"] / total if total else 0
        suspicious_score = s["blocked"] / total if total else 0
        top_ips = sorted(s["ips"].items(), key=lambda x: x[1], reverse=True)[:3]
        campus = s["site"]
        color = CAMPUS_COLORS.get(campus, "#999999")
        is_suspicious = suspicious_score >= 0.7 or s["blocked"] >= 20

        features.append({
            "type": "Feature",
            "properties": {
                "label": s.get("label"),
                "count": s["count"],
                "allowed": s["allowed"],
                "blocked": s["blocked"],
                "allowed_ratio": round(allowed_ratio, 3),
                "suspicious_score": round(suspicious_score, 3),
                "rank": rank,
                "top_ips": [ip for ip, _ in top_ips],
                "campus": campus,
                "color": color,
                "suspicious": is_suspicious
            },
            "geometry": {
                "type": "Point",
                "coordinates": [s["lon"], s["lat"]]
            }
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {"level": level, "time_minutes": time_minutes}
    }


@app.route("/data")
def data():
    """
    Query params:
      level (continent|region|country|city|point) default=continent
      minutes (int) time window default=15
      status (allowed|blocked|all) default=all
      top_k (int) default=200
    """
    level = request.args.get("level", "continent")
    minutes = int(request.args.get("minutes", "15"))
    status = request.args.get("status", "all")
    top_k = int(request.args.get("top_k", "200"))
    bbox_param = request.args.get("bbox")
    bbox = None
    if bbox_param:
        try:
            parts = [float(x) for x in bbox_param.split(",")]
            if len(parts) == 4:
                bbox = parts
        except:
            bbox = None

    status_filter = None if status == "all" else status

    gj = make_aggregates(level=level, time_minutes=minutes, status_filter=status_filter, top_k=top_k, bbox=bbox)
    return jsonify(gj)

@app.route("/stats")
def stats():
    with _lock:
        return jsonify({"events_stored": len(events), "unique_repeat_counts": len([k for k,v in repeat_counts.items() if v>0])})
    
@app.route("/alerts")
def alerts():
    """
    Returns recent alerts.

    Modes:
      - Raw feed (default): one row per event (for table)
      - Aggregate by IP (?aggregate=ip): groups attempts by IP
      - GeoJSON (?geojson=1): returns FeatureCollection (for Leaflet)

    Query params:
      minutes (default=15)
      ip (optional filter by IP)
      limit (default=200 for raw, 100 for aggregate)
    """
    minutes = int(request.args.get("minutes", "15"))
    ip_filter = request.args.get("ip")
    limit = int(request.args.get("limit", "200"))
    aggregate = request.args.get("aggregate")
    geojson_mode = request.args.get("geojson") == "1"

    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
    cutoff_s = cutoff.isoformat()

    rows = []
    with _lock:
        for ts, lat, lon, continent, region, country, city, ip, status in list(events):
            if ts < cutoff_s:
                continue
            if ip_filter and ip != ip_filter:
                continue

            rows.append({
                "timestamp": ts,
                "ip": ip,
                "status": status,
                "city": city,
                "country": country,
                "region": region,
                "continent": continent,
                "lat": lat,
                "lon": lon,
                "campus": CITY_TO_CAMPUS.get(city, "Unknown"),
                "repeat_count": repeat_counts[ip],
                "suspicious": (status == "blocked")
            })

    # Mode 1: Aggregate by IP 
    if aggregate == "ip":
        grouped = {}
        for r in rows:
            ip = r["ip"]
            if ip not in grouped:
                grouped[ip] = {
                    "ip": ip,
                    "city": r["city"],
                    "count": 0,
                    "blocked": 0,
                    "allowed": 0,
                    "last_seen": r["timestamp"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "campus": r["campus"]
                }
            grouped[ip]["count"] += 1
            if r["status"] == "blocked":
                grouped[ip]["blocked"] += 1
            else:
                grouped[ip]["allowed"] += 1
            if r["timestamp"] > grouped[ip]["last_seen"]:
                grouped[ip]["last_seen"] = r["timestamp"]

        alerts = []
        for ip, data in grouped.items():
            data["suspicious"] = (
                data["blocked"] / data["count"] >= 0.7 or
                data["blocked"] >= 10
            )
            alerts.append(data)

        alerts.sort(key=lambda x: x["count"], reverse=True)
        return jsonify({"alerts": alerts[: min(limit, 100)]})

    # Mode 2: GeoJSON for map 
    if geojson_mode:
        features = []
        for r in rows[-limit:]:
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                "properties": r
            })
        return jsonify({"type": "FeatureCollection", "features": features})

    # Mode 3: Raw feed (default) 
    return jsonify({"alerts": rows[-limit:]})

if __name__ == "__main__":
    # on startup load persisted events if exists
    if os.path.exists(PERSIST_FILE):
        try:
            with open(PERSIST_FILE, "r") as f:
                dump = json.load(f)
            with _lock:
                events.extend(dump[-2000:])  # limit restore
            print("Loaded persisted events:", len(events))
        except Exception as e:
            print("Error loading persisted:", e)
    app.run(host="0.0.0.0", port=5000, debug=True)

