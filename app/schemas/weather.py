from pydantic import BaseModel
from datetime import datetime

class WeatherDataBase(BaseModel):
    timestamp: datetime
    lat: float
    lon: float
    temperature: float | None = None
    wind_speed: float | None = None
    wind_direction: float | None = None
    humidity: float | None = None
    ozone: float | None = None
    pm25: float | None = None

class WeatherDataCreate(WeatherDataBase):
    pass

class WeatherDataOut(WeatherDataBase):
    id: int

    class Config:
        orm_mode = True
