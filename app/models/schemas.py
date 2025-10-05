from pydantic import BaseModel
from typing import List
from datetime import datetime

class AirQualityData(BaseModel):
    timestamp: str
    pm25: float
    unit: str
    location: str


class WeatherData(BaseModel):
    timestamp: str
    temperature: float | None = None
    wind_speed: float | None = None
    wind_direction: float | None = None
    humidity: float | None = None
    ozone: float | None = None
    pm25: float | None = None


class PredictInput(BaseModel):
    pm25: float
    wind_speed: float
    temperature: float

class PredictOutput(BaseModel):
    risk_category: str
    probability: float
    advice: str
    twi_advice: str