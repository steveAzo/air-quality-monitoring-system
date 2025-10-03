# debug_backfill_live.py
import time
from datetime import datetime, timedelta
from app.db import SessionLocal
from app.services import openqq as oa
from app.crud import openaq as crud
from app.models.openaq import Sensor, Measurement
from sqlalchemy import func

def debug_backfill_live():
    """Backfill with live debugging of what's actually being saved"""
    
    location_id = 1236045  # Focus on one location
    sensor_id = 6530278    # The PM2.5 sensor for this location
    
    db = SessionLocal()
    try:
        # Check BEFORE backfill
        before_count = db.query(Measurement).filter(
            Measurement.location_id == location_id,
            Measurement.parameter_name == 'pm25'
        ).count()
        print(f"📊 BEFORE: {before_count} PM2.5 measurements for location {location_id}")
        
        # Do a small test backfill (just 1 page)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)  # Just 30 days for testing
        
        print(f"🔍 Backfilling sensor {sensor_id} from {start_date} to {end_date}")
        
        measurements = oa.fetch_measurements_by_sensor(
            sensor_id=sensor_id,
            datetime_from=start_date.isoformat() + 'Z',
            datetime_to=end_date.isoformat() + 'Z',
            limit=10,  # Just 10 for testing
            page=1
        )
        
        print(f"📄 API returned {len(measurements)} measurements")
        
        saved_count = 0
        for i, m in enumerate(measurements):
            print(f"\n📝 Processing measurement {i+1}:")
            print(f"   Timestamp: {m.get('timestamp')}")
            print(f"   Value: {m.get('value')}")
            print(f"   Location ID in API: {m.get('location_id')}")
            print(f"   Sensor ID in API: {m.get('sensor_id')}")
            
            # Create measurement record
            measurement_data = {
                "sensor_id": sensor_id,
                "location_id": location_id,  # Explicitly set location_id
                "timestamp": m.get("timestamp"),
                "value": m.get("value"),
                "parameter_name": "pm25",
                "coordinates": m.get("coordinates"),
                "raw": m
            }
            
            try:
                # Try to insert
                result = crud.insert_measurement(db, measurement_data)
                saved_count += 1
                print(f"   ✅ SAVED to database")
                
                # Immediately verify it was saved
                verify = db.query(Measurement).filter(
                    Measurement.sensor_id == sensor_id,
                    Measurement.timestamp == m.get("timestamp")
                ).first()
                
                if verify:
                    print(f"   ✅ VERIFIED in database: ID {verify.id}")
                else:
                    print(f"   ❌ NOT FOUND in database after save!")
                    
            except Exception as e:
                print(f"   ❌ Save failed: {e}")
        
        # Commit explicitly
        db.commit()
        print(f"\n💾 Explicit commit done")
        
        # Check AFTER backfill
        after_count = db.query(Measurement).filter(
            Measurement.location_id == location_id,
            Measurement.parameter_name == 'pm25'
        ).count()
        
        print(f"📊 AFTER: {after_count} PM2.5 measurements for location {location_id}")
        print(f"📈 Net change: {after_count - before_count} measurements")
        
        # Show what's actually in the database now
        recent = db.query(Measurement).filter(
            Measurement.location_id == location_id,
            Measurement.parameter_name == 'pm25'
        ).order_by(Measurement.timestamp.desc()).limit(5).all()
        
        print(f"\n🕒 Recent PM2.5 measurements in DB:")
        for m in recent:
            print(f"   - {m.timestamp}: {m.value} (sensor: {m.sensor_id})")
            
    finally:
        db.close()

if __name__ == "__main__":
    debug_backfill_live()