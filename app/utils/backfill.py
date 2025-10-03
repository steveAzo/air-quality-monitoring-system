"""
Backfill script: programmatically fetch OpenAQ metadata and measurements and upsert into DB.
Run as: python -m app.utils.backfill
"""
import sys
import time
from app.services import openqq as oa
from app.db import SessionLocal, engine, Base
from app.crud import openaq as crud


def backfill_locations(country="GH", limit=1000):
    db = SessionLocal()
    try:
        page = 1
        while True:
            results = oa.fetch_locations(country=country, limit=limit, page=page)
            if not results:
                break
            print(f"Fetched {len(results)} locations (page {page})")
            for loc in results:
                crud.upsert_location(db, loc)
                # upsert sensors for this location (if present)
                try:
                    sensors = loc.get("sensors")
                    # Sometimes sensors are in location detail only; if empty, call detail endpoint
                    if not sensors:
                        detail = oa.fetch_location_detail(loc["id"])
                        sensors = detail.get("sensors", [])
                except Exception:
                    sensors = []
                for s in sensors:
                    crud.upsert_sensor(db, s, location_id=loc["id"])
                # optionally backfill recent measurements for each sensor (light)
            page += 1
            # avoid hammering API
            time.sleep(1)
    finally:
        db.close()

def backfill_measurements_for_sensor(sensor_id: int, pages: int = 5, page_size: int = 100):
    db = SessionLocal()
    try:
        page = 1
        while page <= pages:
            res = oa.fetch_measurements_by_sensor(sensor_id=sensor_id, limit=page_size, page=page)
            if not res:
                break
            for r in res:
                m = {
                    "sensor_id": r.get("sensor_id"),
                    "location_id": r.get("location_id"),
                    "timestamp": r.get("timestamp"),
                    "value": r.get("value"),
                    "coordinates": r.get("coordinates"),
                    "flag_info": r.get("flag_info"),
                    "raw": r
                }
                crud.insert_measurement(db, m)
            page += 1
    finally:
        db.close()

if __name__ == "__main__":
    # Create tables if not exist
    Base.metadata.create_all(bind=engine)
    # Backfill locations and sensors
    print("Starting backfill of locations for GH")
    backfill_locations(country="GH", limit=100)
    print("Backfill complete. You can backfill measurements per sensor with backfill_measurements_for_sensor(sensor_id)")
