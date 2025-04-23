import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pendulum 
from pendulum.tz.timezone import Timezone

def convert_current_time_to_bkk_timezone_and_buddhist_year(datetime):
    """Returns buddhist year, month, date"""
   
    buddhist_year=datetime.year+543
    month=datetime.month
    day=datetime.day

    return buddhist_year, month, day
