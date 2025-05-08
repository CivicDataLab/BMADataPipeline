import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

from airflow.decorators import dag, task
import logging
import pendulum
from utils.db_utils import setup_engine_and_metadata
from utils.rainfall_and_flood_summary_utils import compute_drain_rate, compute_flood_drops,  estimate_flood_duration
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

@dag(
    dag_id='rainfall_and_flood_summary_scraper',
    schedule='0 * * * *',  # every 1 hour 
    start_date=pendulum.datetime(2025, 4, 16, tz="Asia/Bangkok"),
    catchup=False,
    tags=['api', 'bangkok', 'flood_sensor'],
)
def road_flood_sensor_pipeline():

    @task()
    def calculate_max_flood_rf_sensor():
        engine, metadata = setup_engine_and_metadata()
        fs_streaming = metadata.tables['flood_sensor_streaming_data']
        rs_streaming = metadata.tables['rainfall_sensor_streaming_data']
        summary_tbl = metadata.tables['rainfall_and_flood_summary_2']
        risk_tbl    = metadata.tables['risk_points']

        now = pendulum.now(tz='Asia/Bangkok').start_of('hour')
        one_hour_ago = now.subtract(hours=1)
        try:

            with engine.begin() as conn:
                codes = conn.execute(
                    select(fs_streaming.c.sensor_name).distinct()
                ).scalars().all()
                

                for code in codes:
                    # get last 12 flood readings
                    floods = conn.execute(
                        select(fs_streaming.c.value)
                        .where((fs_streaming.c.sensor_name == code)
                            # URGENT THIS SEEMS TO BE A TIMEZONE ISSUE (INVESTIGATE LATER)
                            # IF UNABLE TO SOLVE DO NAIVE QUERYING IN OPTIMISTIC MANNER
                                # (fs_streaming.c.record_time>=one_hour_ago)&
                                # (fs_streaming.c.record_time<now)
                                )
                        .order_by(fs_streaming.c.record_time.desc())
                        .limit(12)
                    ).scalars().all()
                    max_flood = max(floods) if floods else None
                    logging.info(f"The max flood is : {max_flood}")
                     # find all risk points tied to this flood sensor
                    risk_rows = conn.execute(
                        select(
                            risk_tbl.c.objectid,
                            risk_tbl.c.risk_name,
                            risk_tbl.c.group_t.label('category'),
                            risk_tbl.c.district_t.label('district'),
                            risk_tbl.c.closest_rainfall_sensor_code.label('rain_code')
                        )
                        .where(risk_tbl.c.closest_road_flood_sensor_code == code)
                    ).all()

                    for objectid, risk_name, category, district, rain_code in risk_rows:
                        rf1hr = None
                        if rain_code:
                            rf1hr = conn.execute(
                                select(rs_streaming.c.rf1hr)
                                .where(rs_streaming.c.code == rain_code)
                                .order_by(rs_streaming.c.site_time.desc())
                                .limit(1)
                            ).scalar()

                        stmt = pg_insert(summary_tbl).values(
                            risk_id=objectid,
                            risk_name=risk_name,
                            category=category,
                            district=district,
                            flood_sensor_code=code,
                            rainfall_sensor_code=rain_code,
                            flood_code=code,
                            hour=now,
                            max_flood=max_flood,
                            flood_duration_minutes=0,
                            rf1hr=rf1hr
                        )
                        conn.execute(stmt)
        except (TypeError, ValueError, ConnectionError) as e:
            logging.error(f"The error is :{str(e)} ")

    @task()
    def calculate_drain_rate_and_estimated_flood_duration():
        engine, metadata=setup_engine_and_metadata()
        fs_streaming = metadata.tables['flood_sensor_streaming_data']
        summary_tbl = metadata.tables['rainfall_and_flood_summary_2']
        

        now = pendulum.now(tz='Asia/Bangkok').start_of('hour')
        one_hour_ago = now.subtract(hours=1)
        try:

            with engine.begin() as conn:
                rows=conn.execute(
                    select(
                        summary_tbl.c.risk_id,
                        summary_tbl.c.flood_sensor_code,
                        summary_tbl.c.max_flood

                    )
                    .where(summary_tbl.c.hour==now)
                ).all()

                for risk_id, code, max_flood in rows:
                    # fetch the last 12 flood readings in the past hour
                    floods=conn.execute(
                        select(fs_streaming.c.value)
                        .where(
                            (fs_streaming.c.sensor_name==code)
                            # (fs_streaming.c.record_time>=one_hour_ago)&
                            # (fs_streaming.c.record_time<now)
                            )
                        .order_by(fs_streaming.c.record_time.desc())
                        .limit(12)
                    ).scalars().all()

                    drops=compute_flood_drops(floods)
                    drain_rate=compute_drain_rate(drops)
                    duration=estimate_flood_duration(max_flood, drain_rate)
                    logging.info(f"The drop {drops}, {drain_rate} and {duration}")
                    conn.execute(
                        update(summary_tbl)
                        .where(
                            (summary_tbl.c.risk_id==risk_id)
                            # (summary_tbl.c.hour==now)
                        )
                        .values(
                            flood_duration_minutes=duration,
                            flood_drain_rate_in_cm_minutes=drain_rate
                    
                        )
                    )

        except Exception as e:
            logging.error(f"Some error occured in rain_fall_summary scraper :{str(e)}") 



    calculate_max_flood_rf_sensor() >>calculate_drain_rate_and_estimated_flood_duration()

road_flood_sensor_pipeline()
