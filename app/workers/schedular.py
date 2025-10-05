"""
Scheduler: polls OpenAQ latest endpoints and upserts to DB on a schedule.
Started automatically on FastAPI startup event in main.py
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
import logging
import time
from app.services import openqq as oa
from app.db import SessionLocal
from app.crud import openaq as crud
from app.models.openaq import Sensor, Measurement

logger = logging.getLogger("scheduler")
scheduler = BackgroundScheduler()

# Add at the top of your scheduler
sensor_cache = {}  
def get_sensor_parameter_name(sensor_id: int, db) -> str:
    """Get parameter name from cache or fetch from API"""
    
    # Check cache first
    if sensor_id in sensor_cache:
        logger.info("Found parameter name in cache: %s", sensor_cache[sensor_id])
        return sensor_cache[sensor_id]
    
    logger.info("Parameter name not in cache for sensor %s, searching...", sensor_id)
    
    try:
        # Try to get from database first
        sensor = db.query(Sensor).filter(Sensor.id == sensor_id).first()
        if sensor and sensor.parameter_name:
            logger.info("Found parameter name in DB: %s", sensor.parameter_name)
            sensor_cache[sensor_id] = sensor.parameter_name  # Add to cache
            return sensor.parameter_name
        else:
            logger.info("Sensor not found in DB or has no parameter name")
        
        # If not in DB, fetch from API
        logger.info("Fetching sensor details from OpenAQ API for sensor %s...", sensor_id)
        sensor_detail = oa.fetch_sensor_detail(sensor_id)
        parameter = sensor_detail.get("parameter", {})
        parameter_name = parameter.get("name")
        
        if parameter_name:
            logger.info("Found parameter name from API: %s", parameter_name)
            sensor_cache[sensor_id] = parameter_name  # Add to cache
            return parameter_name
        else:
            logger.warning("No parameter name found in API response for sensor %s", sensor_id)
            sensor_cache[sensor_id] = "unknown"  # Cache even unknown values
            return "unknown"
            
    except Exception as e:
        logger.warning("Failed to get parameter name for sensor %s: %s", sensor_id, e)
        sensor_cache[sensor_id] = "unknown"  # Cache even errors
        return "unknown"

def refresh_latest_for_location(location_id: int):
    logger.info("refresh_latest_for_location called for location_id: %s", location_id)
    db = SessionLocal()
    try:
        logger.info("Calling OpenAQ API for location %s...", location_id)
        latest_results = oa.fetch_location_latest(location_id)
        logger.info("API returned %d results for location %s", len(latest_results), location_id)
        
        if not latest_results:
            logger.warning("No results returned from API for location %s", location_id)
            return
            
        measurements_saved = 0
        for i, measurement_data in enumerate(latest_results):
            try:
                logger.info("Processing result %d/%d for location %s", 
                           i+1, len(latest_results), location_id)
                
                # CORRECT FIELD NAME: sensorsId (camelCase)
                sensor_id = measurement_data.get("sensorsId")
                logger.info("Sensor ID: %s", sensor_id)
                
                if not sensor_id:
                    logger.warning("No sensor_id found in measurement data, skipping")
                    continue
                
                # Get parameter name
                logger.info("Getting parameter name for sensor %s...", sensor_id)
                parameter_name = get_sensor_parameter_name(sensor_id, db)
                logger.info("Parameter name: %s", parameter_name)
                
                # Create measurement record with parameter name
                # Note: timestamp is in measurement_data["datetime"]["utc"]
                measurement = {
                    "sensor_id": sensor_id,
                    "location_id": location_id,
                    "timestamp": measurement_data.get("datetime", {}).get("utc"),  # ← FIX timestamp too!
                    "value": measurement_data.get("value"),
                    "coordinates": measurement_data.get("coordinates"),
                    "parameter_name": parameter_name,
                    "raw": measurement_data
                }
                
                logger.info("Attempting to save measurement: sensor_id=%s, value=%s, timestamp=%s", 
                           sensor_id, measurement_data.get("value"), measurement_data.get("datetime", {}).get("utc"))
                
                # Save measurement
                crud.insert_measurement(db, measurement)
                measurements_saved += 1
                logger.info("Successfully saved measurement %d", i+1)
                
            except Exception as e:
                logger.exception("Error processing measurement %d: %s", i+1, e)
                continue
        
        logger.info("Saved %d/%d measurements for location %s", measurements_saved, len(latest_results), location_id)
        logger.info("Sensor cache size: %d entries", len(sensor_cache))
        
    except Exception as e:
        logger.exception("Error fetching latest for location %s: %s", location_id, e)
    finally:
        db.close()
        
def refresh_all_locations():
    logger.info("STARTING refresh_all_locations job")
    db = SessionLocal()
    try:
        from app.crud.openaq import get_locations
        logger.info("Fetching locations from database...")
        locs = get_locations(db, country_code="GH", limit=1000)
        logger.info("Found %d locations to process", len(locs))
        
        if not locs:
            logger.warning("No locations found in database!")
            return
            
        locations_processed = 0
        locations_with_data = 0
        
        for i, loc in enumerate(locs):
            try:
                logger.info("Processing location %d/%d: %s (ID: %s)", 
                           i+1, len(locs), loc.name, loc.id)
                refresh_latest_for_location(loc.id)
                locations_processed += 1
                
                # Check if any measurements were added for this location
                temp_db = SessionLocal()
                try:
                    measurement_count = temp_db.query(Measurement).filter(
                        Measurement.location_id == loc.id
                    ).count()
                    if measurement_count > 0:
                        locations_with_data += 1
                        logger.info("Location %s has %d measurements", loc.id, measurement_count)
                    else:
                        logger.warning("Location %s has NO measurements", loc.id)
                finally:
                    temp_db.close()
                
                # Add delay between API calls
                if i < len(locs) - 1:
                    time.sleep(2)
                    
            except Exception as e:
                logger.exception("Failed to refresh location %s: %s", loc.id, e)
        
        logger.info("refresh_all_locations COMPLETE: Processed %d/%d locations, %d have data", 
                   locations_processed, len(locs), locations_with_data)
        
    except Exception as e:
        logger.exception("FATAL ERROR in refresh_all_locations: %s", e)
    finally:
        db.close()

def start_scheduler(interval_minutes: int = 120):
    # Remove existing jobs if any
    scheduler.remove_all_jobs()
    # Schedule the refresh_all_locations job
    scheduler.add_job(refresh_all_locations, IntervalTrigger(minutes=interval_minutes), next_run_time=datetime.utcnow())
    scheduler.start()
    logger.info("Scheduler started: refresh_all_locations every %s minutes", interval_minutes)
