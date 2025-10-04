#!/usr/bin/env python3
import os, time, json, sys, logging, requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [producer] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("producer")

MBTA_API = "https://api-v3.mbta.com/vehicles"
API_KEY   = os.getenv("MBTA_API_KEY", "")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC     = os.getenv("KAFKA_TOPIC", "mobility.positions.v1")
PROVIDER  = "mbta"

# ---- requests session with retries ----
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retry))
if API_KEY:
    session.headers.update({"x-api-key": API_KEY})

def now_utc_iso(): return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---- Kafka producer ----
log.info(f"Connecting to Kafka at {BOOTSTRAP}, topic={TOPIC}")
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    request_timeout_ms=10000,
    api_version_auto_timeout_ms=10000
)

def fetch():
    params = {
        "fields[vehicle]": "label,latitude,longitude,bearing,speed,current_status,updated_at",
        "page[limit]": "200"
    }
    r = session.get(MBTA_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

while True:
    try:
        log.info("Polling MBTA /vehicles …")
        data = fetch()
        items = data.get("data", [])
        log.info(f"Fetched {len(items)} vehicles")

        sent = 0
        for v in items:
            a = v.get("attributes") or {}
            lat, lon = a.get("latitude"), a.get("longitude")
            if lat is None or lon is None:
                continue

            ts = a.get("updated_at") or now_utc_iso()
            try:
                msg = {
                    "provider": PROVIDER,
                    "vehicleId": (a.get("label") or v.get("id") or "unknown"),
                    "lat": float(lat), "lon": float(lon),
                    "speedKmh": (float(a["speed"])*3.6 if isinstance(a.get("speed"), (float,int)) else None),
                    "bearing": a.get("bearing"),
                    "accuracyM": None,
                    "ts": ts if ts.endswith("Z") else now_utc_iso()
                }
            except Exception as e:
                log.warning(f"skip malformed record: {e} (raw={a})")
                continue

            producer.send(TOPIC, key=str(msg["vehicleId"]), value=msg)
            sent += 1

        producer.flush()
        log.info(f"Sent {sent} messages to Kafka")
        time.sleep(3)

    except KeyboardInterrupt:
        log.info("Interrupted, exiting.")
        break
    except requests.HTTPError as e:
        log.error(f"HTTP error {e.response.status_code}: {e}", exc_info=False)
        time.sleep(5)
    except requests.RequestException as e:
        log.error(f"Network error: {e}", exc_info=False)
        time.sleep(5)
    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        time.sleep(5)
