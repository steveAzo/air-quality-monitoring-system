# debug_measurements.py
from app.db import SessionLocal
from app.models.openaq import Measurement, Sensor
from sqlalchemy import func

def debug_measurements():
    db = SessionLocal()
    try:
        print("=== DEBUGGING MEASUREMENTS ===")
        
        # Check total measurements
        total_measurements = db.query(Measurement).count()
        print(f"📊 Total measurements in database: {total_measurements}")
        
        # Check measurements by location
        location_counts = db.query(
            Measurement.location_id, 
            func.count(Measurement.id)
        ).group_by(Measurement.location_id).all()
        
        print(f"📍 Measurements by location:")
        for loc_id, count in location_counts:
            print(f"   - Location {loc_id}: {count} measurements")
        
        # Specifically check PM2.5 for location 1236045
        pm25_count = db.query(Measurement).filter(
            Measurement.location_id == 1236045,
            Measurement.parameter_name == 'pm25'
        ).count()
        
        print(f"🔍 PM2.5 measurements for location 1236045: {pm25_count}")
        
        # Check what sensors exist for location 1236045
        sensors = db.query(Sensor).filter(Sensor.location_id == 1236045).all()
        print(f"📡 Sensors for location 1236045: {len(sensors)}")
        for sensor in sensors:
            print(f"   - Sensor {sensor.id}: {sensor.parameter_name} ({sensor.name})")
            
        # Check recent PM2.5 measurements
        recent_pm25 = db.query(Measurement).filter(
            Measurement.location_id == 1236045,
            Measurement.parameter_name == 'pm25'
        ).order_by(Measurement.timestamp.desc()).limit(5).all()
        
        print(f"🕒 Recent PM2.5 measurements:")
        for m in recent_pm25:
            print(f"   - {m.timestamp}: {m.value}")
            
    finally:
        db.close()

if __name__ == "__main__":
    debug_measurements()