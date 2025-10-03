# test_original_dates.py
from app.services import openqq as oa
from datetime import datetime

def test_original_dates():
    sensor_id = 30469
    
    # Use the exact dates from the successful run
    start_date = datetime(2024, 10, 2, 22, 39, 34)  # 2024-10-02
    end_date = datetime(2025, 10, 2, 22, 39, 34)    # 2025-10-02
    
    print(f"🔍 Testing original successful dates:")
    print(f"   From: {start_date}")
    print(f"   To: {end_date}")
    
    try:
        measurements = oa.fetch_measurements_by_sensor(
            sensor_id=sensor_id,
            datetime_from=start_date.isoformat() + 'Z', 
            datetime_to=end_date.isoformat() + 'Z',
            limit=10,
            page=1
        )
        
        print(f"📄 Got {len(measurements)} measurements")
        if measurements:
            for i, m in enumerate(measurements[:3]):
                print(f"   Sample {i}: {m.get('timestamp')} - {m.get('value')}")
        else:
            print("❌ No data with original dates either!")
            
    except Exception as e:
        print(f"💥 Error: {e}")

if __name__ == "__main__":
    test_original_dates()