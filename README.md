# 🌆 Real-Time Mobility Heatmap

Stream live GPS feeds into **Kafka**, process them with **PySpark Structured Streaming**, store results in **MongoDB**, and visualize on a **Leaflet** web map. Includes ready-to-run producers for **MBTA (Boston transit)** and **OpenSky (aircraft)**.

![stack](https://img.shields.io/badge/Stack-Kafka%20%E2%86%92%20PySpark%20%E2%86%92%20MongoDB%20%E2%86%92%20Leaflet-0a84ff)
![python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)
![spark](https://img.shields.io/badge/Spark-3.5.1-FD4D0C?logo=apachespark&logoColor=white)
![kafka](https://img.shields.io/badge/Kafka-3.x-231F20?logo=apachekafka&logoColor=white)
![mongodb](https://img.shields.io/badge/MongoDB-6.x-13AA52?logo=mongodb&logoColor=white)
![leaflet](https://img.shields.io/badge/Leaflet-UI-199900?logo=leaflet&logoColor=white)
![license](https://img.shields.io/badge/License-MIT-green)

---

## 🧭 System Design

```mermaid
flowchart LR
  A["Producers (MBTA, OpenSky, Simulator)"] -->|JSON events| B["Kafka Topic (mobility.positions.v1)"]
  B -->|Structured Streaming| C["PySpark Job (H3 snap + 5-min windows)"]
  C -->|Upsert| D["MongoDB (tiles, positions_latest)"]
  D --> E["Flask API (/api/tiles/latest, /api/positions/latest)"]
  E --> F["Leaflet UI (hex heatmap + markers)"]

⏱️ Streaming Timing (micro-batch)
sequenceDiagram
autonumber
participant P as Producer
participant K as Kafka Topic
participant S as Spark (Streaming)
participant M as MongoDB
participant W as Web UI

P->>K: Publish GPS JSON (key=vehicleId)
loop Every ~2s (trigger)
  S->>K: Poll new records (latest)
  K-->>S: Batch Δt of events
  S->>S: H3 cell + 5-min window aggregate
  S->>M: Upsert tiles & positions_latest
  W->>M: GET tiles/positions (REST)
  M-->>W: GeoJSON (hexes & markers)
end


🖼️ Screenshot
Replace the path below with your own image file.

Example after you add your image:
![Live heatmap](heatmap.png)


✨ Features
🔄 Live ingest from MBTA / OpenSky → Kafka

⚡ PySpark Structured Streaming micro-batches (low latency)

🧭 H3 hex tiles (5-min windows + TTL cleanup)

📍 Latest vehicle positions per provider (idempotent upserts)

🗺️ Leaflet UI with hex intensity + markers (auto refresh)

📁 Project Layout
graphql
Copy code
.
├─ heatmap_stream.py          # PySpark streaming job (Kafka -> MongoDB)
├─ app.py                     # Flask + Leaflet viewer (MongoDB -> browser)
├─ producers/
│  ├─ mbta_to_kafka.py        # MBTA vehicles -> Kafka (free API key)
│  └─ opensky_to_kafka.py     # OpenSky aircraft -> Kafka (no key)
├─ assets/                    # screenshots & images (add yours here)
├─ README.md
└─ LICENSE
🚀 Quick Start
✅ Tested on WSL2 (Ubuntu). Works on Linux/macOS with minor path tweaks.

0) Prereqs
Java 17, Spark 3.5.1

Kafka (single-node KRaft) on localhost:9092

MongoDB on 127.0.0.1:27017

Python 3.8+ virtualenv

Install Python deps:

bash
Copy code
python3 -m venv ~/venv-heatmap && source ~/venv-heatmap/bin/activate
pip install --upgrade pip
pip install pyspark==3.5.1 h3 pymongo flask requests kafka-python
Create the Kafka topic:

bash
Copy code
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic mobility.positions.v1 --partitions 3 --replication-factor 1
1) Start a real data producer
MBTA (Boston transit)

bash
Copy code
export MBTA_API_KEY=YOUR_KEY_HERE
export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=mobility.positions.v1
python producers/mbta_to_kafka.py
OpenSky (aircraft, worldwide)

bash
Copy code
export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=mobility.positions.v1
python producers/opensky_to_kafka.py
You should see logs like: “Fetched N vehicles / Sent N messages to Kafka.”

2) Run the PySpark streaming job
bash
Copy code
export PYSPARK_PYTHON=~/venv-heatmap/bin/python
export PYSPARK_DRIVER_PYTHON=~/venv-heatmap/bin/python

export MONGO_URI="mongodb://127.0.0.1:27017"
export MONGO_DB="mobility"
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_TOPIC="mobility.positions.v1"

$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  heatmap_stream.py
💡 For snappier updates, in code use
.writeStream.trigger(processingTime="2 seconds").
Add print(f"[epoch {epoch_id}] rows: {df.count()}", flush=True) inside foreach_batch_func for a heartbeat.

3) Create MongoDB indexes (once)
js
Copy code
// in mongosh
use mobility
db.tiles.createIndex({ city:1, grid:1, windowStart:-1 })
db.tiles.createIndex({ cellId:1, windowStart:-1 })
db.tiles.createIndex({ centroid:"2dsphere" })
db.tiles.createIndex({ staleAt:1 }, { expireAfterSeconds:0 })

db.positions_latest.createIndex({ provider:1, vehicleId:1 }, { unique:true })
db.positions_latest.createIndex({ loc:"2dsphere" })
db.positions_latest.createIndex({ ts:-1 })
4) Run the UI
bash
Copy code
python app.py
Open http://localhost:5000/

🔶 Hexes = latest 5-min H3 tiles (color by count)

📍 Dots = latest positions per vehicle

🔁 Auto-refresh every 5s

⚙️ Configuration
Set via environment variables (defaults in parentheses):

Streaming

H3_RES (8) — H3 resolution (7–9 good for cities)

TILE_MINUTES (5) — window size

TTL_MINUTES (45) — TTL for old tiles

Kafka

KAFKA_BOOTSTRAP (localhost:9092)

KAFKA_TOPIC (mobility.positions.v1)

Mongo

MONGO_URI (mongodb://127.0.0.1:27017)

MONGO_DB (mobility)

UI

REFRESH_MS (5000)

🧱 Data Model
Kafka message (JSON)

json
Copy code
{
  "provider": "mbta",
  "vehicleId": "BUS_1432",
  "lat": 42.355,
  "lon": -71.060,
  "speedKmh": 21.4,
  "bearing": 120,
  "accuracyM": 8,
  "ts": "2025-10-04T10:22:05Z"
}
MongoDB

mobility.tiles — one doc per (H3 cell, windowStart) (with count, avgSpeedKmh, centroid, staleAt)

mobility.positions_latest — one doc per vehicle (ts, loc)

🛠️ Troubleshooting
Failed to find data source: kafka → Run with
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

Tiles empty but positions exist → Ensure ISO parse:
to_timestamp(ts, "yyyy-MM-dd'T'HH:mm:ss'Z'"), and send fresh timestamps (watermark drops very old events).

Producer NoBrokersAvailable → Start Kafka and verify with
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Flask /api/tiles/latest error → Use the version-agnostic h3_boundary_geojson helper in app.py (works with h3 3.x/4.x).

WSL tip → Keep MongoDB data under Linux paths (e.g., ~/mongo-data), not /mnt/c/....

🧭 Roadmap
⏱️ Merge last N minutes of tiles (/api/tiles/range?minutes=15)

🟢 Toggle heat by count vs avgSpeedKmh

🔔 WebSocket push via MongoDB Change Streams

🐳 Docker Compose dev stack