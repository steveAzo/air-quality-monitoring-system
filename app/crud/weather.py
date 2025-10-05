from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from app.models.weather import WeatherData
from app.schemas.weather import WeatherDataCreate

class WeatherCRUD:
    def create_weather_data(self, db: Session, weather_data: WeatherDataCreate) -> WeatherData:
        """Create new weather data record"""
        db_weather = WeatherData(
            timestamp=weather_data.timestamp,
            lat=weather_data.lat,
            lon=weather_data.lon,
            temperature=weather_data.temperature,
            wind_speed=weather_data.wind_speed,
            wind_direction=weather_data.wind_direction,
            humidity=weather_data.humidity,
            ozone=weather_data.ozone,
            pm25=weather_data.pm25
        )
        db.add(db_weather)
        db.commit()
        db.refresh(db_weather)
        return db_weather

    def get_weather_data(
        self, 
        db: Session, 
        lat: float, 
        lon: float,
        skip: int = 0, 
        limit: int = 100
    ) -> List[WeatherData]:
        """Get weather data for specific location"""
        return db.query(WeatherData)\
            .filter(WeatherData.lat == lat, WeatherData.lon == lon)\
            .order_by(WeatherData.timestamp.desc())\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_latest_weather_data(self, db: Session, lat: float, lon: float) -> Optional[WeatherData]:
        """Get latest weather data for specific location"""
        return db.query(WeatherData)\
            .filter(WeatherData.lat == lat, WeatherData.lon == lon)\
            .order_by(WeatherData.timestamp.desc())\
            .first()

    def get_weather_data_by_id(self, db: Session, weather_id: int) -> Optional[WeatherData]:
        """Get weather data by ID"""
        return db.query(WeatherData).filter(WeatherData.id == weather_id).first()

    def get_weather_data_in_range(
        self,
        db: Session,
        lat: float,
        lon: float,
        start_date: datetime,
        end_date: datetime,
        skip: int = 0,
        limit: int = 100
    ) -> List[WeatherData]:
        """Get weather data for location within date range"""
        return db.query(WeatherData)\
            .filter(
                WeatherData.lat == lat,
                WeatherData.lon == lon,
                WeatherData.timestamp >= start_date,
                WeatherData.timestamp <= end_date
            )\
            .order_by(WeatherData.timestamp.desc())\
            .offset(skip)\
            .limit(limit)\
            .all()

    def delete_weather_data(self, db: Session, weather_id: int) -> bool:
        """Delete weather data by ID"""
        weather_data = db.query(WeatherData).filter(WeatherData.id == weather_id).first()
        if weather_data:
            db.delete(weather_data)
            db.commit()
            return True
        return False

# Create instance for easy import
weather_crud = WeatherCRUD()