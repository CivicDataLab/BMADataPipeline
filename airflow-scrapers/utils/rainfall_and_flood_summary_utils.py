import sys
import os
import math
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

def compute_flood_drops(floods):
    """Returns a list of positive drops between consecutie readings."""
    drops=[]
    for i in range(1, len(floods)):
        previous=floods[i-1]
        current=floods[i]
        if previous> current:
            drops.append(previous-current)
    return drops

def compute_drain_rate(drops):
    """Average drop per interval, or None if no drops"""
    if not drops:
        return 0
    total=0.0
    for d in drops:
        total+=d
    return total/len(drops)


def estimate_flood_duration(max_flood, drain_rate):
    """Estimates minutes to drain from max_flood at given rate(5-min intervals)"""
    if drain_rate is None or drain_rate<=0 or max_flood is None:
        return 0
    # round it up for steps
    steps_needed=math.ceil(max_flood/drain_rate)
    return steps_needed*5

