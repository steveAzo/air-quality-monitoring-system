import earthaccess
import xarray as xr
from fastapi import APIRouter, HTTPException
from typing import List
from datetime import datetime
from dotenv import load_dotenv
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# FastAPI router
router = APIRouter(tags=["Weather"])

def get_latest_weather(lat: float = 5.58389, lon: float = -0.19968):
    """
    Fetch latest MERRA-2 weather and aerosol data for a given location (default: Accra).
    Returns temperature (T2M), wind speed (U10M), wind direction (V10M), humidity (QV2M),
    ozone (TO3), and PM2.5 (calculated from aerosol components).
    """
    try:
        # Authenticate with earthaccess
        earthaccess.login(strategy="netrc")
        logger.info("Authenticated with NASA Earthdata Login")

        # Define collections: surface variables (M2T1NXSLV) and aerosols (M2T1NXAER)
        collections = [
            {"short_name": "M2T1NXSLV", "variables": ["T2M", "U10M", "V10M", "QV2M", "TO3"]},  # Temp, wind speed, wind direction, humidity, ozone
            {"short_name": "M2T1NXAER", "variables": ["DUSMASS25", "BCSMASS", "OCSMASS", "SO4SMASS", "SSSMASS25"]}  # PM2.5 components
        ]

        # Use earlier dates due to MERRA-2 latency (e.g., early August 2025)
        start_date = "2025-08-01"
        end_date = "2025-08-15"
        logger.info(f"Querying MERRA-2 for {start_date} to {end_date}, lat: {lat}, lon: {lon}")

        # Search data for each collection
        data = {}
        for collection in collections:
            results = earthaccess.search_data(
                short_name=collection["short_name"],
                cloud_hosted=True,
                bounding_box=(lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5),  # Wider box
                temporal=(start_date, end_date)
            )

            if not results:
                logger.error(f"No data found for {collection['short_name']}")
                raise ValueError(f"No data found for {collection['short_name']}")

            logger.info(f"Found {len(results)} files for {collection['short_name']}")

            # Open the latest file
            files = earthaccess.open([results[-1]])  # Use the most recent file
            ds = xr.open_dataset(files[0], engine="h5netcdf")
            
            # Log all available variables
            logger.info(f"Dataset {collection['short_name']} variables: {list(ds.data_vars)}")
            
            # Select data at the nearest point
            point_data = ds.sel(lat=lat, lon=lon, method="nearest")

            # Extract variables
            for var in collection["variables"]:
                if var in ds.data_vars:
                    data[var] = float(point_data[var].values[-1])
                else:
                    logger.warning(f"Variable {var} not found in dataset")
                    data[var] = None
            ds.close()

        if not data:
            raise ValueError("No variables extracted from MERRA-2 data")

        # Calculate PM2.5 by summing aerosol components (if available)
        pm25_components = ["DUSMASS25", "BCSMASS", "OCSMASS", "SO4SMASS", "SSSMASS25"]
        pm25 = sum(data.get(var, 0) for var in pm25_components if data.get(var) is not None)
        if pm25 == 0:
            pm25 = None  # Set to None if no components are available

        # Format response
        return [
            {
                "timestamp": str(point_data["time"].values[-1]),
                "temperature": data.get("T2M", None),  # Kelvin
                "wind_speed": data.get("U10M", None),  # m/s
                "wind_direction": data.get("V10M", None),  # m/s (meridional wind)
                "humidity": data.get("QV2M", None),  # kg/kg (specific humidity)
                "ozone": data.get("TO3", None),  # Dobson units
                "pm25": pm25  # µg/m³ (sum of aerosol components)
            }
        ]

    except Exception as e:
        logger.error(f"Error in get_latest_weather: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch MERRA-2 data: {str(e)}")

