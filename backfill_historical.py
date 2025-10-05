# app/utils/backfill_historical.py
import time
from datetime import datetime, timedelta
from app.db import SessionLocal
from app.services import openqq as oa
from app.crud import openaq as crud
from app.models.openaq import Sensor
import json

def get_pm25_sensors_for_location(location_id: int):
    """Get all PM2.5 sensors for a location"""
    db = SessionLocal()
    try:
        sensors = db.query(Sensor).filter(
            Sensor.location_id == location_id,
            Sensor.parameter_name == 'pm25'
        ).all()
        return sensors
    finally:
        db.close()

def backfill_historical_measurements(sensor_id: int, location_id: int, days_back: int = 365):
    """Backfill historical measurements for a specific sensor with corrected format"""
    db = SessionLocal()
    try:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"Backfilling sensor {sensor_id} (location {location_id}) from {start_date.date()} to {end_date.date()}")
        
        page = 1
        total_saved = 0
        
        while True:
            try:
                # Fetch measurements from OpenAQ
                measurements = oa.fetch_measurements_by_sensor(
                    sensor_id=sensor_id,
                    datetime_from=start_date.isoformat() + 'Z',
                    datetime_to=end_date.isoformat() + 'Z',
                    limit=1000,
                    page=page
                )
                
                if not measurements:
                    print(f"No more data for sensor {sensor_id}")
                    break
                
                print(f"Page {page}: Got {len(measurements)} measurements")
                
                # Show first 3 samples for debugging
                for i, m in enumerate(measurements[:3]):
                    print(f"   Sample {i}: timestamp={m.get('timestamp')}, value={m.get('value')}")
                
                page_saved = 0
                for m in measurements:
                    # Ensure we have the correct location_id (sometimes API doesn't return it)
                    if not m.get('location_id'):
                        m['location_id'] = location_id
                    
                    # Create measurement record with corrected data
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
                        # Insert into database
                        crud.insert_measurement(db, measurement_data)
                        page_saved += 1
                        total_saved += 1
                    except Exception as e:
                        print(f"Save failed: {e}")
                        continue
                
                print(f"Saved {page_saved} measurements from page {page}")
                
                page += 1
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"Error on page {page}: {e}")
                break
        
        db.commit()
        print(f"Saved {total_saved} total measurements for sensor {sensor_id}")
        return total_saved
        
    finally:
        db.close()

def backfill_historical_pm25():
    """Backfill PM2.5 data for key locations"""
    key_locations = [
        (1236045, "Physics Department-UG-Accra"),
        (3025585, "Graphic Road"),
    ]
    
    total_saved_all = 0
    
    for location_id, location_name in key_locations:
        print(f"\n🎯 Backfilling {location_name} (ID: {location_id})")
        
        sensors = get_pm25_sensors_for_location(location_id)
        print(f"   🔎 Found {len(sensors)} PM2.5 sensors in DB")
        
        if not sensors:
            print(f"   ⚠️ No PM2.5 sensors found for {location_name}.")
            print("   👉 Did you run the locations/sensors backfill first?")
            continue
        
        location_saved = 0
        for s in sensors:
            print(f"   📡 Sensor {s.id}: {s.parameter_name} at {s.location_id}")
            saved = backfill_historical_measurements(
                sensor_id=s.id,
                location_id=location_id,
                days_back=180
            )
            print(f"   ✅ Sensor {s.id}: {saved} measurements saved")
            location_saved += saved
        
        print(f"📊 {location_name}: {location_saved} total measurements saved")
        total_saved_all += location_saved
    
    print("\n🎉 HISTORICAL BACKFILL COMPLETE!")
    print(f"   Total measurements saved: {total_saved_all}")
    print(f"   Locations processed: {len(key_locations)}")


if __name__ == "__main__":
    # Run backfill for all key locations
    backfill_historical_pm25()
    
    # Example: To add a new location, uncomment and run:
    # backfill_specific_location(947129, "ARJWQ6WV", days_back=180)