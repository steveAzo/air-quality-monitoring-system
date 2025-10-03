from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.db import get_db
from app.crud import openaq as crud
from app.schemas.openaq import LocationSchema, SensorSummary, MeasurementSchema
from app.models.openaq import Measurement, Sensor, Location
from datetime import datetime, timedelta

router = APIRouter(prefix="/openaq", tags=["OpenAQ"])


@router.get("/locations", response_model=List[LocationSchema])
def list_locations(country: Optional[str] = Query("GH"), limit: int = 100, page: int = 1, db: Session = Depends(get_db)):
    offset = (page - 1) * limit
    locs = crud.get_locations(db, country_code=country, limit=limit, offset=offset)
    # Map to Pydantic-friendly shape
    out = []
    for l in locs:
        sensors = []
        for s in l.sensors:
            sensors.append({
                "sensor_id": s.id,
                "name": s.name,
                "parameter": {
                    "id": s.parameter_id,
                    "name": s.parameter_name,
                    "units": s.parameter_unit
                },
            })
        out.append({
            "id": l.id,
            "name": l.name,
            "locality": None,
            "city": l.city,
            "country": l.country,
            "country_code": l.country_code,
            "coordinates": {"latitude": l.latitude, "longitude": l.longitude} if l.latitude else None,
            "isMobile": l.is_mobile,
            "isMonitor": l.is_monitor,
            "sensors": sensors,
            "first_measurement": l.first_measurement.isoformat() if l.first_measurement else None,
            "last_measurement": l.last_measurement.isoformat() if l.last_measurement else None
        })
    return out

@router.get("/locations/{location_id}", response_model=LocationSchema)
def get_location(location_id: int, db: Session = Depends(get_db)):
    loc = crud.get_location(db, location_id)
    if not loc:
        raise HTTPException(status_code=404, detail="Location not found")
    sensors = []
    for s in loc.sensors:
        sensors.append({
            "sensor_id": s.id,
            "name": s.name,
            "parameter": {"id": s.parameter_id, "name": s.parameter_name, "units": s.parameter_unit},
            "latest": None
        })
    return {
        "id": loc.id,
        "name": loc.name,
        "locality": None,
        "city": loc.city,
        "country": loc.country,
        "country_code": loc.country_code,
        "coordinates": {"latitude": loc.latitude, "longitude": loc.longitude} if loc.latitude else None,
        "isMobile": loc.is_mobile,
        "isMonitor": loc.is_monitor,
        "sensors": sensors,
        "first_measurement": loc.first_measurement.isoformat() if loc.first_measurement else None,
        "last_measurement": loc.last_measurement.isoformat() if loc.last_measurement else None
    }

@router.get("/locations/{location_id}/sensors", response_model=List[SensorSummary])
def get_sensors(location_id: int, db: Session = Depends(get_db)):
    sensors = crud.get_sensors_by_location(db, location_id)
    out = []
    for s in sensors:
        out.append({
            "sensor_id": s.id,
            "name": s.name,
            "parameter": {"id": s.parameter_id, "name": s.parameter_name, "units": s.parameter_unit},
            "latest": None
        })
    return out

@router.get("/sensors/{sensor_id}/measurements", response_model=List[MeasurementSchema])
def get_measurements(sensor_id: int, start: Optional[str] = None, end: Optional[str] = None, limit: int = 100, db: Session = Depends(get_db)):
    ms = crud.get_measurements_by_sensor(db, sensor_id=sensor_id, start=start, end=end, limit=limit)
    
    # Get sensor info for parameter details
    sensor = db.query(Sensor).filter(Sensor.id == sensor_id).first()
    
    out = []
    for m in ms:
        # Parse coordinates from JSON string to dict if needed
        coordinates = m.coordinates
        if isinstance(coordinates, str):
            try:
                import json
                coordinates = json.loads(coordinates)
            except:
                coordinates = None
        
        out.append({
            "timestamp": m.timestamp.isoformat() if m.timestamp else None,
            "value": m.value,
            "parameter": {
                "id": sensor.parameter_id if sensor else None,
                "name": m.parameter_name or (sensor.parameter_name if sensor else None),
                "units": sensor.parameter_unit if sensor else None
            },
            "sensor_id": m.sensor_id,
            "location_id": m.location_id,
            "coordinates": coordinates,  # Use parsed coordinates
        })
    return out

@router.post("/admin/refresh/location/{location_id}")
def refresh_location(location_id: int):
    from app.services import openaq as oa
    from app.db import SessionLocal
    from app.workers.schedular import refresh_latest_for_location
    
    db = SessionLocal()
    try:
        # Use the working function from your scheduler
        refresh_latest_for_location(location_id)
        return {"status": "ok", "message": f"Refreshed location {location_id}"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()

@router.get("/locations/{location_id}", response_model=LocationSchema)
def get_location(location_id: int, db: Session = Depends(get_db)):
    loc = crud.get_location(db, location_id)
    if not loc:
        raise HTTPException(status_code=404, detail="Location not found")
    
    sensors = []
    for s in loc.sensors:
        # Get latest measurement for this sensor
        latest_measurement = db.query(Measurement).filter(
            Measurement.sensor_id == s.id
        ).order_by(Measurement.timestamp.desc()).first()
        
        sensors.append({
            "sensor_id": s.id,
            "name": s.name,
            "parameter": {"id": s.parameter_id, "name": s.parameter_name, "units": s.parameter_unit},
            "first_seen": s.first_seen.isoformat() if s.first_seen else None,
            "last_seen": s.last_seen.isoformat() if s.last_seen else None,
            "latest": {
                "value": latest_measurement.value if latest_measurement else None,
                "timestamp": latest_measurement.timestamp.isoformat() if latest_measurement else None
            } if latest_measurement else None
        })
    
    return {
        "id": loc.id,
        "name": loc.name,
        # ... rest of your fields
        "sensors": sensors
    }


@router.get("/locations/{location_id}/latest")
def get_location_latest(location_id: int, db: Session = Depends(get_db)):
    """Get latest measurements for all sensors at a location"""
    sensors = crud.get_sensors_by_location(db, location_id)
    
    latest_measurements = []
    for sensor in sensors:
        latest = db.query(Measurement).filter(
            Measurement.sensor_id == sensor.id
        ).order_by(Measurement.timestamp.desc()).first()
        
        if latest:
            latest_measurements.append({
                "sensor_id": sensor.id,
                "parameter_name": sensor.parameter_name,
                "parameter_units": sensor.parameter_unit,
                "value": latest.value,
                "timestamp": latest.timestamp.isoformat(),
                "coordinates": latest.coordinates
            })
    
    return {"location_id": location_id, "latest_measurements": latest_measurements}


@router.get("/sensors/{sensor_id}/measurements", response_model=List[MeasurementSchema])
def get_measurements(sensor_id: int, start: Optional[str] = None, end: Optional[str] = None, limit: int = 100, db: Session = Depends(get_db)):
    ms = crud.get_measurements_by_sensor(db, sensor_id=sensor_id, start=start, end=end, limit=limit)
    
    # Get sensor info for parameter details
    sensor = db.query(Sensor).filter(Sensor.id == sensor_id).first()
    
    out = []
    for m in ms:
        # Handle NULL timestamps safely
        timestamp_str = None
        if m.timestamp:
            try:
                timestamp_str = m.timestamp.isoformat()
            except Exception:
                timestamp_str = None
        
        out.append({
            "timestamp": timestamp_str,  # This can be None now
            "value": m.value,
            "parameter": {
                "id": sensor.parameter_id if sensor else None,
                "name": m.parameter_name or (sensor.parameter_name if sensor else None),
                "units": sensor.parameter_unit if sensor else None
            },
            "sensor_id": m.sensor_id,
            "location_id": m.location_id,
            "coordinates": m.coordinates,
        })
    return out

@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Get system statistics"""
    from sqlalchemy import func
    
    location_count = db.query(Location).count()
    sensor_count = db.query(Sensor).count()
    measurement_count = db.query(Measurement).count()
    
    # Latest measurement timestamp
    latest_measurement = db.query(Measurement).order_by(Measurement.timestamp.desc()).first()
    
    return {
        "locations": location_count,
        "sensors": sensor_count,
        "measurements": measurement_count,
        "last_updated": latest_measurement.timestamp.isoformat() if latest_measurement else None,
        "parameters_measured": db.query(Measurement.parameter_name).distinct().count()
    }


@router.get("/location/search")
def search_locations(
    q: Optional[str] = Query(None, description="Search by location name"),
    city: Optional[str] = Query(None, description="Filter by city"),
    parameter: Optional[str] = Query(None, description="Filter by measured parameter (e.g., pm25, temperature)"),
    has_recent_data: Optional[bool] = Query(None, description="Only locations with recent measurements"),
    limit: int = Query(50, description="Number of results to return"),
    db: Session = Depends(get_db)
):
    """Search and filter locations"""
    query = db.query(Location)
    
    # Text search
    if q:
        query = query.filter(
            Location.name.ilike(f"%{q}%") | 
            Location.city.ilike(f"%{q}%")
        )
    
    # City filter
    if city:
        query = query.filter(Location.city.ilike(f"%{city}%"))
    
    # Parameter filter
    if parameter:
        locations_with_param = db.query(Sensor.location_id).filter(
            Sensor.parameter_name.ilike(f"%{parameter}%")
        ).distinct()
        query = query.filter(Location.id.in_(locations_with_param))
    
    # Recent data filter
    if has_recent_data:
        # Find locations with measurements in the last 7 days
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        recent_locations = db.query(Measurement.location_id).filter(
            Measurement.timestamp >= seven_days_ago
        ).distinct()
        query = query.filter(Location.id.in_(recent_locations))
    
    locations = query.limit(limit).all()
    
    return [{
        "id": loc.id, 
        "name": loc.name, 
        "city": loc.city,
        "country_code": loc.country_code,
        "coordinates": {
            "latitude": loc.latitude,
            "longitude": loc.longitude
        } if loc.latitude else None,
        "sensor_count": len(loc.sensors)
    } for loc in locations]