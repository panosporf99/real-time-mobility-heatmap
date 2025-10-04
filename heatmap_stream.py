#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, udf, window, count, avg, lit, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# H3 + Mongo
import h3  # works with both h3 3.x and 4.x (see UDF below)
from pymongo import MongoClient, UpdateOne


# ------------------ Configuration via environment ------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.getenv("MONGO_DB", "mobility")
CITY = os.getenv("CITY", "ath")

# H3 resolution (typical 7–9 for city heatmaps)
H3_RES = int(os.getenv("H3_RES", "8"))

# Streaming/window settings
TILE_MIN = int(os.getenv("TILE_MINUTES", "5"))   # tile window size, minutes
TTL_MIN = int(os.getenv("TTL_MINUTES", "45"))    # tile TTL after window end, minutes

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mobility.positions.v1")

# Checkpoint
CHECKPOINT_DIR = os.getenv("CHECKPOINT", "/tmp/heatmap-checkpoint")


# ------------------ Spark session ------------------
spark = (
    SparkSession.builder
    .appName("PySpark-RealTimeCityHeatmap")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ------------------ Input schema ------------------
schema = StructType([
    StructField("provider", StringType(), True),
    StructField("vehicleId", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speedKmh", DoubleType(), True),
    StructField("bearing", IntegerType(), True),
    StructField("accuracyM", IntegerType(), True),
    StructField("ts", StringType(), True),  # ISO string, e.g. "2025-09-26T12:45:10Z"
])


# ------------------ H3 UDF (supports h3 3.x and 4.x) ------------------
def to_h3(lat: float, lon: float) -> str:
    if lat is None or lon is None:
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    if hasattr(h3, "geo_to_h3"):           # h3 <= 3.x
        return h3.geo_to_h3(lat, lon, H3_RES)
    else:                                   # h3 >= 4.x
        return h3.latlng_to_cell(lat, lon, H3_RES)

udf_to_h3 = udf(to_h3, StringType())


# ------------------ Read from Kafka ------------------
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw
    .select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .withColumn("eventTs", to_timestamp(col("ts")))
)

# Basic sanity filters and H3 cell assignment
points = (
    events
    .filter(
        col("provider").isNotNull() &
        col("vehicleId").isNotNull() &
        col("lat").between(-90, 90) &
        col("lon").between(-180, 180) &
        col("eventTs").isNotNull()
    )
    .withColumn("cellId", udf_to_h3(col("lat"), col("lon")))
    .filter(col("cellId").isNotNull())
    .withWatermark("eventTs", "10 minutes")
)


# ------------------ Tiles aggregation (streaming) ------------------
tiles = (
    points
    .groupBy(
        window(col("eventTs"), f"{TILE_MIN} minutes").alias("w"),
        col("cellId")
    )
    .agg(
        count(lit(1)).alias("count"),
        avg(col("speedKmh")).alias("avgSpeedKmh"),
        avg(col("lon")).alias("avgLon"),
        avg(col("lat")).alias("avgLat"),
    )
    .select(
        col("cellId"),
        col("w.start").alias("windowStart"),
        col("w.end").alias("windowEnd"),
        col("count"),
        col("avgSpeedKmh"),
        col("avgLon"),
        col("avgLat"),
    )
)

# Mark partitions for foreachBatch fan-out
tiles_marked = tiles.withColumn("__part", lit("tiles"))

# Latest-per-vehicle will be computed inside foreachBatch; here just pass raw needed cols
latest_raw = (
    points
    .select("provider", "vehicleId", "eventTs", "lat", "lon")
    .withColumn("__part", lit("latest_raw"))
)

# Union both logical outputs so foreachBatch can write both without two separate queries
unioned = tiles_marked.unionByName(latest_raw, allowMissingColumns=True)


# ------------------ Batch writer ------------------
def foreach_batch_func(df: DataFrame, epoch_id: int):
    """Runs on each micro-batch. Tiles are aggregated already; latest is reduced here."""
    # Split the union back into its logical parts
    tiles_df = df.where(col("__part") == lit("tiles")).drop("__part")
    latest_raw_df = df.where(col("__part") == lit("latest_raw")).drop("__part")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # ---- 1) Upsert tiles (TTL via staleAt) ----
    if tiles_df is not None and len(tiles_df.columns) > 0:
        ops = []
        # Stream rows to the driver without blowing memory
        for r in tiles_df.toLocalIterator():
            windowStart = r["windowStart"]
            windowEnd = r["windowEnd"]
            cellId = r["cellId"]

            count_val = int(r["count"] or 0)
            avg_speed = float(r["avgSpeedKmh"] or 0.0)
            avg_lat = float(r["avgLat"] or 0.0)
            avg_lon = float(r["avgLon"] or 0.0)

            _id = f"{CITY}|h3r{H3_RES}|{cellId}|{windowStart.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            staleAt = windowEnd + timedelta(minutes=TTL_MIN)

            doc = {
                "_id": _id,
                "city": CITY,
                "grid": f"h3r{H3_RES}",
                "cellId": cellId,
                "windowStart": windowStart,
                "windowEnd": windowEnd,
                "count": count_val,
                "avgSpeedKmh": avg_speed,
                "centroid": {"type": "Point", "coordinates": [avg_lon, avg_lat]},
                "staleAt": staleAt,
            }
            ops.append(UpdateOne({"_id": _id}, {"$set": doc}, upsert=True))

            # Flush in chunks to keep op list bounded
            if len(ops) >= 1000:
                db["tiles"].bulk_write(ops, ordered=False)
                ops.clear()

        if ops:
            db["tiles"].bulk_write(ops, ordered=False)

    # ---- 2) Compute latest per (provider, vehicleId) within this micro-batch, then upsert ----
    if latest_raw_df is not None and len(latest_raw_df.columns) > 0:
        latest_ts = (
            latest_raw_df.groupBy("provider", "vehicleId")
            .agg(spark_max("eventTs").alias("eventTs"))
        )
        latest_df = (
            latest_ts.join(latest_raw_df, ["provider", "vehicleId", "eventTs"], "left")
            .select("provider", "vehicleId", "eventTs", "lat", "lon")
        )

        ops2 = []
        for r in latest_df.toLocalIterator():
            provider = r["provider"]
            vehicleId = r["vehicleId"]
            ts = r["eventTs"]
            lat = float(r["lat"])
            lon = float(r["lon"])

            filt = {"_id": f"{provider}|{vehicleId}"}
            # Only update if incoming ts is newer (or doc doesn't exist)
            ops2.append(UpdateOne(
                {**filt, "$or": [{"ts": {"$exists": False}}, {"ts": {"$lt": ts}}]},
                {"$set": {
                    "provider": provider,
                    "vehicleId": vehicleId,
                    "ts": ts,
                    "loc": {"type": "Point", "coordinates": [lon, lat]}
                }},
                upsert=True
            ))

            if len(ops2) >= 1000:
                db["positions_latest"].bulk_write(ops2, ordered=False)
                ops2.clear()

        if ops2:
            db["positions_latest"].bulk_write(ops2, ordered=False)

    client.close()


# ------------------ Start the stream ------------------
query = (
    unioned.writeStream
    .outputMode("update")  # tiles are aggregations; latest_raw is append-safe in update mode
    .option("checkpointLocation", CHECKPOINT_DIR)
    .foreachBatch(foreach_batch_func)
    .start()
)

query.awaitTermination()
