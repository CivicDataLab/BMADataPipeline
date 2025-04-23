import os
import logging
import json
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import Tuple,List
from dotenv import load_dotenv
from geopy.distance import geodesic
load_dotenv()

from sqlalchemy import create_engine, select,MetaData
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


def setup_engine_and_metadata():
    db_host = os.getenv("POSTGRES_HOST")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    if not all([db_host, db_user, db_password, db_name]):
        raise ValueError("Missing one or more environment variables")
    
    engine=create_engine(
         f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode=require'
    )
    metadata=MetaData()
    metadata.reflect(bind=engine)
    return engine,metadata
def get_sensors(engine,metadata, sensor_table:str):
    sensor_table=metadata.tables.get(sensor_table)
    if sensor_table is None:
        raise ValueError("Risk Points or Rainfall Sensor not found in the database")
    with engine.connect() as conn:
        # get all sensor rows
        sensor_stmt=select(sensor_table)
        sensors=conn.execute(sensor_stmt).mappings().all()
        return  sensors

def get_distance_between_riskpoint_and_sensors(risk_point:dict,sensor_table:str,sensor_rows:list):
    if risk_point is None:
        raise ValueError("Risk point data is missing")
    
    if sensor_rows is None:
        raise ValueError("Sensor rows are empty")
    
    try:
        risk_coords=(float(risk_point["lat"]), float(risk_point["long"]))
    except (KeyError, ValueError)as e:
        raise ValueError(f"Invalid risk point coordinates: {str(e)}")
    
    min_distance=float("inf")
    closest_sensor_id=None
    closest_sensor_code=None
    sensor_coords=None
    for sensor in sensor_rows:
        try:
            if sensor_table=='flood_sensor':
                sensor_coords=(float(sensor["lat"]),float(sensor["long"]))
            elif sensor_table=='rainfall_sensor':

                sensor_coords=(float(sensor["latitude"]),float(sensor["longitude"]))
            distance=geodesic(risk_coords, sensor_coords).meters
            if distance<min_distance:
                min_distance=round(distance,3)
                closest_sensor_id=sensor["id"]
                closest_sensor_code=sensor["code"]

        except (KeyError, ValueError):
            # skip sensors with bad coordinates
            continue
    if closest_sensor_id is None:
        raise ValueError("No valid sensor found to compute ")
    logging.info(f"Closest sensor id is :{closest_sensor_id} for {closest_sensor_code} and {min_distance} for {risk_point.objectid}")
    return closest_sensor_id, closest_sensor_code,min_distance

