from sqlalchemy import Column, Integer, Float, String, DateTime
from app.db import Base
from datetime import datetime

class WeatherData(Base):
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    temperature = Column(Float, nullable=True)
    wind_speed = Column(Float, nullable=True)
    wind_direction = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    ozone = Column(Float, nullable=True)
    pm25 = Column(Float, nullable=True)
