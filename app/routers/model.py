# app/routers/ml.py
from fastapi import APIRouter, Depends, HTTPException
from app.models.schemas import PredictInput, PredictOutput
import numpy as np
from datetime import datetime, timedelta
from app.services.ml_model import AirQualityPredictor
from sqlalchemy.orm import Session
from app.db import get_db
from app.crud import openaq as crud  # Fix this import!
from app.models.openaq import Measurement, Sensor  

model_cache = {}
router = APIRouter(tags=["ML Model"])

@router.post("/ml/train/{location_id}")
def train_model(location_id: int, db: Session = Depends(get_db)):
    """Train ML model for a specific location"""
    try:
        predictor = AirQualityPredictor()
        results = predictor.train(location_id, db)
        
        # Save trained model
        model_cache[location_id] = predictor
        
        return {
            "status": "success",
            "location_id": location_id,
            "model_performance": results,
            "message": f"Model trained for location {location_id}"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/locations/{location_id}/forecast")
def get_forecast(location_id: int, db: Session = Depends(get_db)):
    """Get 24-hour PM2.5 forecast for a location"""
    if location_id not in model_cache:
        raise HTTPException(status_code=400, detail="Model not trained for this location. Please train it first.")
    
    # Get current data for the location
    current_data = get_current_pm25_stats(location_id, db)
    
    # Generate forecast
    predictor = model_cache[location_id]
    forecast = predictor.predict_next_24h(current_data)
    
    return {
        "location_id": location_id,
        "generated_at": datetime.utcnow().isoformat(),
        "forecast": forecast,
        "current_conditions": current_data
    }

def get_current_pm25_stats(location_id: int, db: Session):
    """Get current PM2.5 statistics for feature engineering"""
    # Get PM2.5 sensors for this location
    pm25_sensors = db.query(Sensor).filter(
        Sensor.location_id == location_id,
        Sensor.parameter_name == 'pm25'
    ).all()
    
    if not pm25_sensors:
        raise HTTPException(status_code=404, detail="No PM2.5 sensors found for this location")
    
    # Get recent PM2.5 measurements (last 24 hours)
    twenty_four_hours_ago = datetime.utcnow() - timedelta(hours=24)
    
    recent_data = db.query(Measurement).filter(
        Measurement.location_id == location_id,
        Measurement.parameter_name == 'pm25',
        Measurement.timestamp >= twenty_four_hours_ago
    ).order_by(Measurement.timestamp.desc()).limit(100).all()
    
    # Calculate stats
    values = [m.value for m in recent_data if m.value is not None]
    
    if not values:
        # If no recent data, use some default values
        return {
            'current': 10.0,
            '6h_avg': 10.0,
            '24h_avg': 10.0,
            '6h_ago': 10.0,
            '24h_ago': 10.0
        }
    
    return {
        'current': values[0] if values else 10.0,  # Most recent
        '6h_avg': np.mean(values[:6]) if len(values) >= 6 else np.mean(values),
        '24h_avg': np.mean(values),
        '6h_ago': values[6] if len(values) > 6 else values[-1] if values else 10.0,
        '24h_ago': values[-1] if values else 10.0
    }