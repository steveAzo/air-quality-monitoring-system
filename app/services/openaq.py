import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_locations(country: str = "GH", limit: int = 50, page: int = 1):
    """Get locations from OpenAQ API with filters"""
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}
    
    url = "https://api.openaq.org/v3/locations"
    
    params = {
        "limit": limit,
        "page": page,
    }
    
    if country:
        params["iso"] = country
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        locations = []
        for location in data.get("results", []):
            locations.append({
                "id": location.get("id"),
                "name": location.get("name"),
                "locality": location.get("locality"),
                "city": location.get("city"),
                "country": location.get("country", {}).get("name"),
                "country_code": location.get("country", {}).get("code"),
                "coordinates": location.get("coordinates"),
                "isMobile": location.get("isMobile"),
                "isMonitor": location.get("isMonitor"),
                "sensors": [
                    {
                        "id": sensor.get("id"),
                        "name": sensor.get("name"),
                        "parameter": sensor.get("parameter")
                    }
                    for sensor in location.get("sensors", [])
                ],
                "first_measurement": location.get("datetimeFirst"),
                "last_measurement": location.get("datetimeLast")
            })
        
        return {
            "meta": data.get("meta", {}),
            "locations": locations,
            "filters_used": params
        }
        
    except Exception as e:
        print(f"Error fetching locations: {e}")
        return {"locations": [], "meta": {}}

def get_location_latest_measurements(location_id: int):
    """Get latest measurements for a specific location"""
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}
    
    url = f"https://api.openaq.org/v3/locations/{location_id}/latest"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        measurements = []
        for result in data.get("results", []):
            measurements.append({
                "timestamp": result.get("datetime", {}).get("utc", ""),
                "value": result["value"],
                "sensor_id": result["sensorsId"],
                "location_id": result["locationsId"],
                "coordinates": result.get("coordinates")
            })
        
        return measurements
        
    except Exception as e:
        print(f"Error getting latest measurements for location {location_id}: {e}")
        return []

def get_location_historical_data(location_id: int, days: int = 7):
    """Get historical data for a specific location using daily averages"""
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}
    
    date_from = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    # First, get sensors for this location
    sensors_url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"
    sensors_response = requests.get(sensors_url, headers=headers)
    
    if sensors_response.status_code != 200:
        return []
    
    sensors_data = sensors_response.json()
    pm25_sensors = [
        sensor for sensor in sensors_data.get("results", [])
        if sensor.get("parameter", {}).get("name") == "pm25"
    ]
    
    if not pm25_sensors:
        return []
    
    all_measurements = []
    
    for sensor in pm25_sensors:
        try:
            sensor_id = sensor["id"]
            url = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
            params = {
                "date_from": date_from,
                "limit": days,
                "sort": "desc"
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                for entry in data.get("results", []):
                    period = entry.get("period", {})
                    all_measurements.append({
                        "timestamp": period.get("datetimeFrom", {}).get("utc", ""),
                        "pm25": entry["value"],
                        "unit": entry.get("parameter", {}).get("units", "µg/m³"),
                        "sensor_id": sensor_id,
                        "location_id": location_id,
                        "is_daily_average": True
                    })
                    
        except Exception as e:
            print(f"Error getting historical data for sensor {sensor['id']}: {e}")
            continue
    
    return all_measurements

def get_latest_air_quality(city: str = None):
    """Get latest air quality measurements for locations in Ghana"""
    locations_data = get_locations(country="GH", limit=50)
    locations = locations_data["locations"]
    
    all_measurements = []
    
    for location in locations:
        try:
            location_id = location["id"]
            measurements = get_location_latest_measurements(location_id)
            
            if measurements:
                for measurement in measurements:
                    measurement.update({
                        "location_name": location["name"],
                        "city": location.get("city", "Accra"),
                        "country": location["country"],
                        "coordinates": location["coordinates"]
                    })
                
                all_measurements.extend(measurements)
                
        except Exception as e:
            print(f"Error getting measurements for location {location['id']}: {e}")
            continue
    
    # Cache the results
    os.makedirs("data", exist_ok=True)
    with open("data/cache.json", "w") as f:
        json.dump(all_measurements, f, indent=2)
    
    print(f"Retrieved {len(all_measurements)} latest measurements from {len(locations)} locations")
    return all_measurements

def get_historical_air_quality(days: int = 7, location_id: int = None):
    """Get historical air quality data"""
    if location_id:
        return get_location_historical_data(location_id, days)
    else:
        return get_all_historical_data(days)

def get_all_historical_data(days: int = 7):
    """Get historical data for all locations in Ghana"""
    locations_data = get_locations(country="GH", limit=20)
    locations = locations_data["locations"]
    
    all_historical_data = []
    
    for location in locations[:10]:
        historical_data = get_location_historical_data(location["id"], days)
        
        for data in historical_data:
            data.update({
                "location_name": location["name"],
                "city": location.get("city", "Accra"),
                "country": location["country"],
                "coordinates": location["coordinates"]
            })
        
        all_historical_data.extend(historical_data)
    
    return all_historical_data


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
                "parameter": result.get("parameter"),  # e.g. PM2.5, NO₂, etc.
                "first_seen": result.get("datetimeFirst"),
                "last_seen": result.get("datetimeLast"),
                "latest": result.get("latest"),  # might include last value info
            })
        
        return sensors
    
    except Exception as e:
        print(f"Error getting sensors for location {location_id}: {e}")
        return []


def get_measurements_by_sensor(sensor_id: int, limit: int = 100, page: int = 1, datetime_from: str = None, datetime_to: str = None):
    """Get measurements for a specific sensor"""
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}
    
    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
    
    params = {
        "limit": limit,
        "page": page
    }
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
                "parameter": result["parameter"],  # this gives meaning to the value
                "coordinates": result.get("coordinates"),
                "flag_info": result.get("flagInfo")
            })
        
        return measurements
    
    except Exception as e:
        print(f"Error getting measurements for sensor {sensor_id}: {e}")
        return []
