# verify_backfill.py
from app.db import SessionLocal
from app.models.openaq import Measurement
from sqlalchemy import func

def verify_backfill():
    db = SessionLocal()
    try:
        print("=== VERIFYING BACKFILL RESULTS ===")
        
        # Check total measurements
        total = db.query(Measurement).count()
        print(f"📊 Total measurements in database: {total}")
        
        # Check PM2.5 measurements for location 1236045
        pm25_count = db.query(Measurement).filter(
            Measurement.location_id == 1236045,
            Measurement.parameter_name == 'pm25'
        ).count()
        
        print(f"🔍 PM2.5 measurements for location 1236045: {pm25_count}")
        
        # Check date range
        oldest = db.query(Measurement.timestamp).filter(
            Measurement.location_id == 1236045,
            Measurement.parameter_name == 'pm25'
        ).order_by(Measurement.timestamp.asc()).first()
        
        newest = db.query(Measurement.timestamp).filter(
            Measurement.location_id == 1236045,
            Measurement.parameter_name == 'pm25'
        ).order_by(Measurement.timestamp.desc()).first()
        
        print(f"📅 Date range: {oldest[0]} to {newest[0]}")
        
        # Sample some data
        samples = db.query(Measurement).filter(
            Measurement.location_id == 1236045,
            Measurement.parameter_name == 'pm25'
        ).order_by(Measurement.timestamp.desc()).limit(5).all()
        
        print(f"🕒 Recent samples:")
        for m in samples:
            print(f"   - {m.timestamp}: {m.value}")
            
    finally:
        db.close()

if __name__ == "__main__":
    verify_backfill()