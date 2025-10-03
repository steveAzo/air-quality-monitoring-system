# app/services/ml_model.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from datetime import datetime, timedelta
from sqlalchemy import text  # Add this import

class AirQualityPredictor:
    def __init__(self):
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.is_trained = False
    
    def prepare_features(self, df):
        """Create time-based features for ML model"""
        df = df.copy()
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        # Rolling averages (trend features)
        df['pm25_6h_avg'] = df['value'].rolling(6, min_periods=1).mean()
        df['pm25_24h_avg'] = df['value'].rolling(24, min_periods=1).mean()
        
        # Lag features
        df['pm25_lag_1h'] = df['value'].shift(1)
        df['pm25_lag_6h'] = df['value'].shift(6)
        df['pm25_lag_24h'] = df['value'].shift(24)
        
        return df.dropna()
    
    def train(self, location_id: int, db):
        """Train model on historical data for a location"""
        # Get historical PM2.5 data for this location using SQLAlchemy text()
        query = text("""
        SELECT m.timestamp, m.value, m.parameter_name
        FROM measurements m
        JOIN sensors s ON m.sensor_id = s.id
        WHERE s.location_id = :location_id AND m.parameter_name = 'pm25'
        ORDER BY m.timestamp
        """)
        
        # Convert to pandas DataFrame
        df = pd.read_sql(query, db.connection(), params={"location_id": location_id})
        
        if len(df) < 100:  # Need sufficient data
            raise ValueError(f"Insufficient data for location {location_id}. Only {len(df)} samples found.")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Prepare features
        feature_df = self.prepare_features(df)
        
        # Create target (predict next hour's PM2.5)
        feature_df['target'] = feature_df['value'].shift(-1)
        feature_df = feature_df.dropna()
        
        if len(feature_df) < 50:
            raise ValueError(f"Not enough data after feature engineering: {len(feature_df)} samples")
        
        # Feature columns
        feature_cols = ['hour', 'day_of_week', 'month', 'is_weekend', 
                       'pm25_6h_avg', 'pm25_24h_avg', 'pm25_lag_1h', 
                       'pm25_lag_6h', 'pm25_lag_24h']
        
        X = feature_df[feature_cols]
        y = feature_df['target']
        
        # Train model
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
        self.model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        
        self.is_trained = True
        self.feature_cols = feature_cols
        
        return {
            "mae": round(mae, 2),
            "rmse": round(rmse, 2),
            "training_samples": len(X_train),
            "test_samples": len(X_test)
        }
    
    def predict_next_24h(self, current_data):
        """Predict PM2.5 for next 24 hours"""
        if not self.is_trained:
            raise ValueError("Model not trained")
        
        predictions = []
        current_time = datetime.utcnow()
        
        for hours_ahead in range(1, 25):
            pred_time = current_time + timedelta(hours=hours_ahead)
            
            features = {
                'hour': pred_time.hour,
                'day_of_week': pred_time.weekday(),
                'month': pred_time.month,
                'is_weekend': 1 if pred_time.weekday() in [5, 6] else 0,
                'pm25_6h_avg': current_data.get('6h_avg', current_data['current']),
                'pm25_24h_avg': current_data.get('24h_avg', current_data['current']),
                'pm25_lag_1h': current_data['current'],
                'pm25_lag_6h': current_data.get('6h_ago', current_data['current']),
                'pm25_lag_24h': current_data.get('24h_ago', current_data['current'])
            }
            
            feature_df = pd.DataFrame([features])[self.feature_cols]
            prediction = self.model.predict(feature_df)[0]
            
            predictions.append({
                'timestamp': pred_time.isoformat(),
                'predicted_pm25': max(0, round(prediction, 2)),
                'confidence': 'medium'
            })
        
        return predictions