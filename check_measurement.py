# check_measurements_final.py
from app.db import SessionLocal
from app.models.openaq import Measurement
from sqlalchemy import func

def check_measurements_final():
    db = SessionLocal()
    try:
        total_measurements = db.query(Measurement).count()
        latest_measurements = db.query(Measurement).order_by(Measurement.timestamp.desc()).limit(10).all()
        
        print(f"🎉 FINAL VERIFICATION:")
        print(f"📊 Total measurements in database: {total_measurements}")
        print(f"🕒 Latest 10 measurements:")
        for m in latest_measurements:
            print(f"   - {m.parameter_name}: {m.value} at {m.timestamp} (Sensor: {m.sensor_id})")
        
        # Count by parameter type
        param_counts = db.query(Measurement.parameter_name, func.count(Measurement.id)).group_by(Measurement.parameter_name).all()
        print(f"\n📈 Measurements by parameter:")
        for param, count in param_counts:
            print(f"   - {param}: {count}")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_measurements_final()