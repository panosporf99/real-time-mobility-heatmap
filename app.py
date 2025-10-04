#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime
from flask import Flask, jsonify, Response
from pymongo import MongoClient
import h3

# ---- Config via env (defaults match the user’s setup) ----
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME   = os.getenv("MONGO_DB", "mobility")
REFRESH_MS = int(os.getenv("REFRESH_MS", "5000"))  # UI refresh interval

app = Flask(__name__)
db = MongoClient(MONGO_URI)[DB_NAME]

# ---- H3 helpers that work for both h3 3.x and 4.x ----
def h3_boundary_geojson(cell_id):
    """
    Return a GeoJSON-style closed ring: [[lng, lat], ..., [lng, lat]]
    Works with h3 3.x and 4.x.
    """
    try:
        # h3 <= 3.x
        if hasattr(h3, "h3_to_geo_boundary"):
            ring = h3.h3_to_geo_boundary(cell_id, geo_json=True)  # list of {"lat":..,"lng":..}
            coords = [[pt["lng"], pt["lat"]] for pt in ring]
        else:
            # h3 >= 4.x (no geo_json kw)
            ring = h3.cell_to_boundary(cell_id)  # list of (lat, lng)
            coords = [[lng, lat] for (lat, lng) in ring]
    except TypeError:
        # Some 4.x builds throw if geo_json is passed; fall back to non-geo_json form
        ring = h3.cell_to_boundary(cell_id)      # list of (lat, lng)
        coords = [[lng, lat] for (lat, lng) in ring]

    # close polygon
    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords

# --------------------- API ---------------------

@app.get("/api/tiles/latest")
def api_tiles_latest():
    """
    Returns a GeoJSON FeatureCollection of H3 hex polygons for the most recent
    5-minute window in db.tiles (with properties: count, avgSpeedKmh, windowStart/End).
    """
    latest = db.tiles.find_one(sort=[("windowStart", -1)])
    if not latest:
        return jsonify({"type": "FeatureCollection", "features": []})

    start = latest["windowStart"]
    features = []
    for doc in db.tiles.find({"windowStart": start}):
        cell = doc["cellId"]
        geom = {"type": "Polygon", "coordinates": [h3_boundary_geojson(cell)]}
        props = {
            "cellId": cell,
            "count": int(doc.get("count", 0)),
            "avgSpeedKmh": float(doc.get("avgSpeedKmh", 0.0)),
            "windowStart": doc["windowStart"].isoformat(),
            "windowEnd": doc["windowEnd"].isoformat(),
        }
        features.append({"type": "Feature", "geometry": geom, "properties": props})

    return jsonify({"type": "FeatureCollection", "features": features})

@app.get("/api/positions/latest")
def api_positions_latest():
    """
    Returns a GeoJSON FeatureCollection of Point features for each vehicle’s
    most recent position in db.positions_latest.
    """
    features = []
    for doc in db.positions_latest.find({}):
        lon, lat = doc["loc"]["coordinates"]
        geom = {"type": "Point", "coordinates": [lon, lat]}
        props = {
            "provider": doc.get("provider"),
            "vehicleId": doc.get("vehicleId"),
            "ts": doc.get("ts").isoformat() if isinstance(doc.get("ts"), datetime) else str(doc.get("ts")),
        }
        features.append({"type": "Feature", "geometry": geom, "properties": props})

    return jsonify({"type": "FeatureCollection", "features": features})

# --------------------- Single-file UI ---------------------

@app.get("/")
def index():
    # Default map view shows Boston (MBTA feed). The map auto-fits to the latest tiles once loaded.
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Real-Time Mobility Heatmap</title>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    html, body, #map {{ height: 100%; margin: 0; }}
    .legend {{ background: white; padding:6px 8px; font: 12px/14px Arial; }}
    .legend i {{ width: 14px; height: 14px; float: left; margin-right: 6px; opacity: 0.8; }}
    .toast {{
      position: absolute; top: 10px; left: 50%; transform: translateX(-50%);
      background: rgba(0,0,0,0.7); color: #fff; padding: 6px 10px; border-radius: 6px; font: 12px Arial;
      z-index: 9999; display: none;
    }}
  </style>
</head>
<body>
<div id="map"></div>
<div id="toast" class="toast">Waiting for data…</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
  const REFRESH_MS = {REFRESH_MS};

  const map = L.map('map').setView([42.3601, -71.0589], 12); // Boston default
  L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
    maxZoom: 19, attribution: '&copy; OpenStreetMap'
  }}).addTo(map);

  const tilesLayer = L.geoJSON(null, {{
    style: f => ({{
      color: '#555', weight: 1, fillOpacity: 0.6,
      fillColor: colorByCount(f.properties.count)
    }})
  }}).addTo(map);

  const positionsLayer = L.layerGroup().addTo(map);

  function colorByCount(c){{
    return c > 50 ? '#800026' :
           c > 20 ? '#BD0026' :
           c > 10 ? '#E31A1C' :
           c > 5  ? '#FC4E2A' :
           c > 2  ? '#FD8D3C' :
           c > 0  ? '#FEB24C' : '#FFEDA0';
  }}

  function setToast(msg) {{
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.style.display = 'block';
    setTimeout(()=> t.style.display='none', 1500);
  }}

  async function refresh(){{
    try {{
      const [tRes, pRes] = await Promise.all([
        fetch('/api/tiles/latest'),
        fetch('/api/positions/latest')
      ]);
      const tiles = await tRes.json();
      const pts   = await pRes.json();

      tilesLayer.clearLayers();
      if (tiles.features && tiles.features.length) {{
        tilesLayer.addData(tiles);
        const b = tilesLayer.getBounds();
        if (b.isValid()) map.fitBounds(b, {{ maxZoom: 14 }});
      }}

      positionsLayer.clearLayers();
      (pts.features || []).forEach(f => {{
        const [lng, lat] = f.geometry.coordinates;
        const m = L.circleMarker([lat, lng], {{ radius: 5 }});
        m.bindPopup(`<b>${{f.properties.provider}}</b><br/>${{f.properties.vehicleId}}<br/>${{f.properties.ts}}`);
        positionsLayer.addLayer(m);
      }});

      if ((!tiles.features || tiles.features.length === 0) && (!pts.features || pts.features.length === 0)) {{
        setToast('Waiting for data…');
      }}
    }} catch (e) {{
      console.error(e);
      setToast('Error fetching data (check server logs).');
    }}
  }}

  refresh();
  setInterval(refresh, REFRESH_MS);
</script>
</body>
</html>"""
    return Response(html, mimetype="text/html")

if __name__ == "__main__":
    # Run on 127.0.0.1 so it’s reachable from Windows browsers as http://localhost:5000/
    app.run(host="127.0.0.1", port=5000, debug=True)
