import os
import requests
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

BASE = "https://api.openaq.org/v3"
API_KEY = os.getenv("OPENAQ_API_KEY", None)
print("API_KEY:", API_KEY)
HEADERS = {"X-API-Key": API_KEY} if API_KEY else {}

def fetch_locations(country: str = "GH", limit: int = 1000, page: int = 1) -> List[Dict[str, Any]]:
    url = f"{BASE}/locations"
    params = {"iso": country, "limit": limit, "page": page}
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

def fetch_location_detail(location_id: int) -> Dict[str, Any]:
    url = f"{BASE}/locations/{location_id}"
    r = requests.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    return r.json().get("results", [])[0]

def fetch_location_sensors(location_id: int) -> List[Dict[str, Any]]:
    # the location detail contains sensors array; this is a helper wrapper
    detail = fetch_location_detail(location_id)
    return detail.get("sensors", [])

def fetch_location_latest(location_id: int) -> List[Dict[str, Any]]:
    url = f"{BASE}/locations/{location_id}/latest"
    r = requests.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    return r.json().get("results", [])

def fetch_measurements_by_sensor(sensor_id: int, limit: int = 100, page: int = 1, datetime_from: Optional[str] = None, datetime_to: Optional[str] = None) -> List[Dict[str, Any]]:
    url = f"{BASE}/sensors/{sensor_id}/measurements"
    params = {"limit": limit, "page": page}
    if datetime_from:
        params["datetime_from"] = datetime_from
    if datetime_to:
        params["datetime_to"] = datetime_to
    
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    
    # Transform the data to match our expected format
    results = []
    for result in data.get("results", []):
        # Extract timestamp from period.datetimeFrom
        period = result.get("period", {})
        timestamp = period.get("datetimeFrom") or period.get("utc")
        
        # Handle both string and dict timestamp formats
        if isinstance(timestamp, dict):
            timestamp = timestamp.get("utc")
        
        transformed_result = {
            "sensor_id": sensor_id,
            "location_id": result.get("location", {}).get("id") if isinstance(result.get("location"), dict) else result.get("location_id"),
            "timestamp": timestamp,
            "value": result.get("value"),
            "parameter_name": result.get("parameter", {}).get("name") if isinstance(result.get("parameter"), dict) else "pm25",
            "coordinates": result.get("coordinates"),
            "raw": result
        }
        results.append(transformed_result)
    
    return results

def fetch_sensor_detail(sensor_id: int) -> Dict[str, Any]:
    """Get detailed sensor information including parameter name"""
    url = f"{BASE}/sensors/{sensor_id}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])[0]  # Returns the first result
