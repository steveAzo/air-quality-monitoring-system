# from fastapi import APIRouter, HTTPException
# from typing import List
# from app.models.schemas import WeatherData
# from app.services.merra2 import get_latest_weather

# router = APIRouter(tags=["Weather"])

# @router.get("/weather/latest", response_model=List[WeatherData])
# async def latest_weather(lat: float = 5.58389, lon: float = -0.19968):
#     """
#     FastAPI endpoint to fetch latest MERRA-2 weather data for a given location.
#     """
#     try:
#         return get_latest_weather(lat, lon)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from app.db import get_db
from app.schemas.weather import WeatherDataOut
from app.crud.weather import weather_crud
from app.services.merra2 import get_latest_weather

router = APIRouter(tags=["Weather"])

@router.get("/weather/latest", response_model=List[WeatherDataOut])
async def fetch_latest_weather_from_nasa(
    lat: float = Query(5.58389, description="Latitude"),
    lon: float = Query(-0.19968, description="Longitude"),
    db: Session = Depends(get_db)
):
    """
    Fetch latest MERRA-2 weather data from NASA and save to database.
    This endpoint will be slower as it calls the NASA API.
    """
    try:
        return get_latest_weather(lat, lon, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/weather/db/latest", response_model=Optional[WeatherDataOut])
async def get_latest_weather_from_db(
    lat: float = Query(5.58389, description="Latitude"),
    lon: float = Query(-0.19968, description="Longitude"),
    db: Session = Depends(get_db)
):
    """
    Get latest weather data from our database (fast).
    """
    try:
        weather_data = weather_crud.get_latest_weather_data(db, lat, lon)
        if not weather_data:
            raise HTTPException(status_code=404, detail="No weather data found for this location")
        return weather_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch weather data: {str(e)}")

@router.get("/weather/db/history", response_model=List[WeatherDataOut])
async def get_weather_history(
    lat: float = Query(5.58389, description="Latitude"),
    lon: float = Query(-0.19968, description="Longitude"),
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Number of records to return"),
    db: Session = Depends(get_db)
):
    """
    Get weather data history for a location from our database.
    """
    try:
        weather_data = weather_crud.get_weather_data(db, lat, lon, skip, limit)
        if not weather_data:
            raise HTTPException(status_code=404, detail="No weather data found for this location")
        return weather_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch weather history: {str(e)}")

@router.get("/weather/db/{weather_id}", response_model=WeatherDataOut)
async def get_weather_by_id(
    weather_id: int,
    db: Session = Depends(get_db)
):
    """
    Get specific weather data by ID from our database.
    """
    try:
        weather_data = weather_crud.get_weather_data_by_id(db, weather_id)
        if not weather_data:
            raise HTTPException(status_code=404, detail="Weather data not found")
        return weather_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch weather data: {str(e)}")

@router.get("/weather/db/range", response_model=List[WeatherDataOut])
async def get_weather_in_range(
    lat: float = Query(5.58389, description="Latitude"),
    lon: float = Query(-0.19968, description="Longitude"),
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Number of records to return"),
    db: Session = Depends(get_db)
):
    """
    Get weather data for a location within a specific date range.
    """
    try:
        weather_data = weather_crud.get_weather_data_in_range(
            db, lat, lon, start_date, end_date, skip, limit
        )
        if not weather_data:
            raise HTTPException(status_code=404, detail="No weather data found for this location and date range")
        return weather_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch weather data: {str(e)}")

@router.delete("/weather/db/{weather_id}")
async def delete_weather_data(
    weather_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete weather data by ID from our database.
    """
    try:
        success = weather_crud.delete_weather_data(db, weather_id)
        if not success:
            raise HTTPException(status_code=404, detail="Weather data not found")
        return {"message": "Weather data deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete weather data: {str(e)}")