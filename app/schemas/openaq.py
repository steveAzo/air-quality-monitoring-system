from pydantic import BaseModel
from typing import Optional, List, Any

class Coordinates(BaseModel):
    latitude: float
    longitude: float

class ParameterSchema(BaseModel):
    id: Optional[int]
    name: Optional[str]
    units: Optional[str]
    displayName: Optional[str] = None

class SensorSummary(BaseModel):
    sensor_id: int
    name: Optional[str]
    parameter: Optional[ParameterSchema]
    latest: Optional[Any] = None

class LocationSchema(BaseModel):
    id: int
    name: Optional[str]
    locality: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    coordinates: Optional[Coordinates] = None
    isMobile: Optional[bool] = False
    isMonitor: Optional[bool] = False
    sensors: Optional[List[SensorSummary]] = []
    first_measurement: Optional[str] = None
    last_measurement: Optional[str] = None

class MeasurementSchema(BaseModel):
    timestamp: Optional[str] = None
    value: Optional[float]
    parameter: Optional[ParameterSchema] = None
    sensor_id: int
    location_id: Optional[int]
    coordinates: Optional[Coordinates] = None
    flag_info: Optional[dict] = None
