from sqlalchemy import Column, BigInteger, String, Float, Boolean, TIMESTAMP, JSON, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base

class Location(Base):
    __tablename__ = "locations"

    id = Column(BigInteger, primary_key=True, index=True)
    name = Column(String)
    city = Column(String, nullable=True)
    country = Column(String)
    country_code = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    is_mobile = Column(Boolean, default=False)
    is_monitor = Column(Boolean, default=False)
    first_measurement = Column(TIMESTAMP, nullable=True)
    last_measurement = Column(TIMESTAMP, nullable=True)
    raw = Column(JSON)

    sensors = relationship("Sensor", back_populates="location")


class Sensor(Base):
    __tablename__ = "sensors"

    id = Column(BigInteger, primary_key=True, index=True)
    location_id = Column(BigInteger, ForeignKey("locations.id"))
    name = Column(String)
    parameter_id = Column(BigInteger)
    parameter_name = Column(String)
    parameter_unit = Column(String)  
    raw = Column(JSON)

    location = relationship("Location", back_populates="sensors")


class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    sensor_id = Column(BigInteger, ForeignKey("sensors.id"))
    location_id = Column(BigInteger, ForeignKey("locations.id"))
    timestamp = Column(TIMESTAMP, index=True)
    parameter_name = Column(String)
    value = Column(Float)
    coordinates = Column(JSON, nullable=True)
    raw = Column(JSON)
