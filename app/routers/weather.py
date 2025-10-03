from fastapi import APIRouter, HTTPException
from typing import List
from app.models.schemas import WeatherData
from app.services.merra2 import get_latest_weather

router = APIRouter(tags=["Weather"])

@router.get("/weather/latest", response_model=List[WeatherData])
async def latest_weather(lat: float = 5.58389, lon: float = -0.19968):
    """
    FastAPI endpoint to fetch latest MERRA-2 weather data for a given location.
    """
    try:
        return get_latest_weather(lat, lon)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))