# fixed_backfill_v2.py
import time
from datetime import datetime, timedelta
from app.db import SessionLocal
from app.services import openqq as oa
from app.crud import openaq as crud

def backfill_historical_with_fixed_format(sensor_id: int, location_id: int, days_back: int = 365):
    """Backfill using the corrected API format"""
    db = SessionLocal()
    try:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"🔍 Backfilling sensor {sensor_id} (location {location_id})")
        print(f"   From {start_date.date()} to {end_date.date()}")
        
        page = 1
        total_saved = 0
        
        while True:
            try:
                measurements = oa.fetch_measurements_by_sensor(
                    sensor_id=sensor_id,
                    datetime_from=start_date.isoformat() + 'Z',
                    datetime_to=end_date.isoformat() + 'Z',
                    limit=1000,
                    page=page
                )
                
                if not measurements:
                    print(f"❌ No more data for sensor {sensor_id}")
                    break
                
                print(f"📄 Page {page}: Got {len(measurements)} measurements")
                
                page_saved = 0
                for i, m in enumerate(measurements[:3]):  # Show first 3
                    print(f"   Sample {i}: timestamp={m.get('timestamp')}, value={m.get('value')}")
                
                for m in measurements:
                    # Ensure we have the correct location_id (sometimes API doesn't return it)
                    if not m.get('location_id'):
                        m['location_id'] = location_id
                    
                    measurement_data = {
                        "sensor_id": sensor_id,
                        "location_id": m.get("location_id"),
                        "timestamp": m.get("timestamp"),
                        "value": m.get("value"),
                        "parameter_name": m.get("parameter_name", "pm25"),
                        "coordinates": m.get("coordinates"),
                        "raw": m.get("raw")
                    }
                    
                    try:
                        result = crud.insert_measurement(db, measurement_data)
                        page_saved += 1
                        total_saved += 1
                    except Exception as e:
                        print(f"   ❌ Save failed: {e}")
                        continue
                
                print(f"   💾 Saved {page_saved} measurements from page {page}")
                
                page += 1
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"❌ Error on page {page}: {e}")
                break
        
        db.commit()
        print(f"✅ Saved {total_saved} total measurements for sensor {sensor_id}")
        return total_saved
        
    finally:
        db.close()

def backfill_key_locations_v2():
    """Backfill key locations with corrected format"""
    locations = [
        (1236045, 6530278),  # Physics Dept, PM2.5 sensor
        (9764, 30469),       # US Embassy, PM2.5 sensor
        (3, 5),              # NMA - Nima, PM2.5 sensor
    ]
    
    total_saved = 0
    for location_id, sensor_id in locations:
        print(f"\n🎯 Backfilling location {location_id}, sensor {sensor_id}")
        saved = backfill_historical_with_fixed_format(sensor_id, location_id, days_back=180)  # 6 months
        total_saved += saved
    
    print(f"\n🎉 TOTAL: Saved {total_saved} measurements across all locations")

if __name__ == "__main__":
    backfill_key_locations_v2()