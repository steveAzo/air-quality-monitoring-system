from sqlalchemy.orm import Session
from datetime import datetime
from app.models.openaq import Location, Sensor, Measurement
from typing import Dict, Any, List, Optional
import json

def upsert_location(db: Session, loc: Dict[str, Any]) -> Location:
    loc_id = int(loc["id"])
    obj = db.query(Location).filter(Location.id == loc_id).first()
    
    # Extract country code from the country dict
    country_data = loc.get("country", {})
    country_name = country_data.get("name") if isinstance(country_data, dict) else loc.get("country")
    country_code = country_data.get("code") if isinstance(country_data, dict) else loc.get("country_code")
    
    payload = {
        "id": loc_id,
        "name": loc.get("name"),
        "city": loc.get("city"),
        "country": country_name,  # Extract string from dict
        "country_code": country_code,  # Extract code from dict
        "latitude": loc.get("coordinates", {}).get("latitude"),
        "longitude": loc.get("coordinates", {}).get("longitude"),
        "is_mobile": loc.get("isMobile") or False,
        "is_monitor": loc.get("isMonitor") or False,
        "first_measurement": loc.get("first_measurement", {}).get("utc"),
        "last_measurement": loc.get("last_measurement", {}).get("utc"),
        "raw": json.dumps(loc) if loc else None  # Convert dict to JSON string
    }
    
    if obj:
        for k, v in payload.items():
            setattr(obj, k, v)
    else:
        obj = Location(**payload)
        db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def upsert_sensor(db: Session, sensor: Dict[str, Any], location_id: int) -> Sensor:
    sid = int(sensor["id"])
    obj = db.query(Sensor).filter(Sensor.id == sid).first()
    param = sensor.get("parameter") or {}
    
    payload = {
        "id": sid,
        "location_id": location_id,
        "name": sensor.get("name"),
        "parameter_id": param.get("id"),
        "parameter_name": param.get("name"),
        "parameter_unit": param.get("units"),
        "raw": json.dumps(sensor) if sensor else None  # Convert dict to JSON string
    }
    
    if obj:
        for k, v in payload.items():
            setattr(obj, k, v)
    else:
        obj = Sensor(**payload)
        db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def insert_measurement(db: Session, m: Dict[str, Any]) -> Measurement:
    sensor_id = int(m["sensor_id"])
    ts = m["timestamp"]
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            ts = None 
    
    existing = db.query(Measurement).filter(Measurement.sensor_id == sensor_id, Measurement.timestamp == ts).first()
    
    payload = {
        "sensor_id": sensor_id,
        "location_id": m.get("location_id"),
        "timestamp": ts,
        "value": m.get("value"),
        "coordinates": json.dumps(m.get("coordinates")) if m.get("coordinates") else None,
        "parameter_name": m.get("parameter_name"),
        "raw": json.dumps(m.get("raw")) if m.get("raw") else None
    }
    
    if existing:
        for k, v in payload.items():
            setattr(existing, k, v)
        db.commit()
        db.refresh(existing)
        return existing
    else:
        rec = Measurement(**payload)
        db.add(rec)
        db.commit()
        db.refresh(rec)
        return rec

# Query helpers

def get_locations(db: Session, country_code: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[Location]:
    q = db.query(Location)
    if country_code:
        q = q.filter(Location.country_code == country_code)
    return q.order_by(Location.last_measurement.desc()).offset(offset).limit(limit).all()

def get_location(db: Session, location_id: int) -> Optional[Location]:
    return db.query(Location).filter(Location.id == location_id).first()

def get_sensors_by_location(db: Session, location_id: int) -> List[Sensor]:
    return db.query(Sensor).filter(Sensor.location_id == location_id).all()

def get_measurements_by_sensor(db: Session, sensor_id: int, start: Optional[str] = None, end: Optional[str] = None, limit: int = 100) -> List[Measurement]:
    q = db.query(Measurement).filter(Measurement.sensor_id == sensor_id).order_by(Measurement.timestamp.desc())
    if start:
        q = q.filter(Measurement.timestamp >= start)
    if end:
        q = q.filter(Measurement.timestamp <= end)
    return q.limit(limit).all()
