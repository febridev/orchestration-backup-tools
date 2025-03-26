from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def convert_utc7_to_utc0(dt_string):
    # Parse string input menjadi objek datetime
    dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f")
    
    # Set timezone ke UTC+7
    dt = dt.replace(tzinfo=ZoneInfo("Asia/Jakarta"))
    print(dt)
    
    # Konversi ke UTC
    dt_utc = dt.astimezone(ZoneInfo("UTC"))
    print(dt_utc)
    
    # Format hasil sesuai dengan yang diinginkan
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# Contoh penggunaan
input_time = "2024-01-15 00:00:00.000"
result = convert_utc7_to_utc0(input_time)
print(result)
