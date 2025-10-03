from fastapi import APIRouter, HTTPException, Query
from app.services.openaq import (
    get_locations,
    get_latest_air_quality,
    get_historical_air_quality,
    get_location_latest_measurements,
    get_location_historical_data
)
import requests
import os 
from typing import Optional

router = APIRouter(prefix="/airquality", tags=["Air Quality"])

@router.get("/locations")
async def get_locations_endpoint(
    country: str = "GH",
    limit: int = 50,
    page: int = 1
):
    """Get locations from OpenAQ API"""
    return get_locations(country=country, limit=limit, page=page)

@router.get("/latest")
async def latest_air_quality(city: str = None):
    """Get latest air quality measurements for Ghana"""
    return get_latest_air_quality(city=city)

@router.get("/historical")
async def historical_air_quality(days: int = 7, location_id: int = None):
    """Get historical air quality data"""
    return get_historical_air_quality(days=days, location_id=location_id)

@router.get("/locations/{location_id}/latest")
async def get_single_location_latest(location_id: int):
    """Get latest measurements for a specific location"""
    measurements = get_location_latest_measurements(location_id)
    if not measurements:
        raise HTTPException(status_code=404, detail="No measurements found for this location")
    return measurements

@router.get("/locations/{location_id}/historical")
async def get_single_location_historical(location_id: int, days: int = 7):
    """Get historical data for a specific location"""
    measurements = get_location_historical_data(location_id, days)
    if not measurements:
        raise HTTPException(status_code=404, detail="No historical data found for this location")
    return measurements

@router.get("/debug-sensor")
async def debug_sensor(sensor_id: int = 5):
    """Debug endpoint to test sensor measurements"""
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}
    
    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
    params = {"limit": 5, "sort": "desc"}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        return {
            "sensor_id": sensor_id,
            "status_code": response.status_code,
            "response": response.json() if response.status_code == 200 else response.text
        }
    except Exception as e:
        return {"error": str(e)}



@router.get("/by-location/{location_id}")
def get_sensors_by_location(location_id: int):
    """Get all sensors for a specific location"""
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}

    url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        sensors = []
        for result in data.get("results", []):
            sensors.append({
                "sensor_id": result["id"],
                "name": result["name"],
                "parameter": result.get("parameter"),
                "first_seen": result.get("datetimeFirst"),
                "last_seen": result.get("datetimeLast"),
                "latest": result.get("latest"),
            })

        return {"location_id": location_id, "sensors": sensors}

    except Exception as e:
        return {"error": f"Error getting sensors for location {location_id}: {str(e)}"}


@router.get("/{sensor_id}/measurements")
def get_measurements_by_sensor(
    sensor_id: int,
    limit: int = Query(100, description="Number of results to return"),
    page: int = Query(1, description="Page number"),
    datetime_from: Optional[str] = Query(None, description="Start datetime (ISO8601)"),
    datetime_to: Optional[str] = Query(None, description="End datetime (ISO8601)")
):
    """Get measurements for a specific sensor"""
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}

    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

    params = {"limit": limit, "page": page}
    if datetime_from:
        params["datetime_from"] = datetime_from
    if datetime_to:
        params["datetime_to"] = datetime_to

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        measurements = []
        for result in data.get("results", []):
            measurements.append({
                "timestamp": result.get("period", {}).get("datetimeFrom"),
                "value": result["value"],
                "parameter": result["parameter"],
                "coordinates": result.get("coordinates"),
                "flag_info": result.get("flagInfo")
            })

        return {"sensor_id": sensor_id, "measurements": measurements}

    except Exception as e:
        return {"error": f"Error getting measurements for sensor {sensor_id}: {str(e)}"}
