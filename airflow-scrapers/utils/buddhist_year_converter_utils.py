import sys
import os
import pendulum
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_bangkok_date_info(timezone: str = "Asia/Bangkok"):
    now = pendulum.now(tz=timezone)
    current_date = now.date()
    current_month = now.month
    buddhist_year = now.year + 543
    return current_date, current_month, buddhist_year