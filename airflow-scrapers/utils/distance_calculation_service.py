import os
import logging
import json
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import Tuple,List
from dotenv import load_dotenv
from geopy.distance import geodesic
# from utils.budget import get_mis_budget_by_fy
load_dotenv()

from sqlalchemy import create_engine, select,MetaData,Table
from sqlalchemy.orm import Session
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
def get_riskpoint_and_sensors(engine,metadata,risk_point_id:int, sensor_table:str):
   

    riskpoint_table=metadata.tables.get('risk_points')
    sensor_table=metadata.tables.get(sensor_table)
    if riskpoint_table is None or sensor_table is None:
        raise ValueError("Risk Points or Rainfall Sensor not found in the database")
    with engine.connect() as conn:
        # get one risk point by id
        risk_stmt=select(riskpoint_table).where(riskpoint_table.c.objectid==risk_point_id)
        riskpoint=conn.execute(risk_stmt).mappings().first()
        # get all sensor rows
        rainfall_stmt=select(sensor_table)
        rainfall_sensors=conn.execute(rainfall_stmt).mappings().all()
        return riskpoint, rainfall_sensors

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



def main():
    engine, metadata=setup_engine_and_metadata()
    riskpoint, rainfall_sensors=get_riskpoint_and_sensors(engine,metadata,720, 'rainfall_sensor')
    sensor_id, sensor_code, min_distance=get_distance_between_riskpoint_and_sensors(riskpoint,'rainfall_sensor', rainfall_sensors)

if __name__=="__main__":
    main()
