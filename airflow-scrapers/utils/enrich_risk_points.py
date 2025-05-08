import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from utils.db_utils import setup_engine_and_metadata
from sqlalchemy import update
from dotenv import load_dotenv
load_dotenv()


def enrich_riskpoints_with_flood_sensor():
        engine, metadata = setup_engine_and_metadata()
        riskpoint_table = metadata.tables.get("risk_points")

        csv_path=os.getenv("RISK_MAPPED_FLOOD_SENSOR_CSV_PATH")
        df = pd.read_csv(csv_path)

        # only those with a valid sensor_id
        mapped_df = df[df["sensor_id"].notna()]
        mapped_ids = mapped_df["risk_id"].astype(int).tolist()

        with engine.begin() as conn:
            # set unmapped risk points to NULL
            conn.execute(
                update(riskpoint_table)
                .where(~riskpoint_table.c.objectid.in_(mapped_ids))
                .values(
                    road_flood_sensor_id=None,
                    closest_road_flood_sensor_code=None,
                    closest_road_flood_sensor_distance=None
                )
            )

            # update mapped risk points
            for _, row in mapped_df.iterrows():
                sensor_id = int(row["sensor_id"])
                code = row["code"] if pd.notna(row["code"]) else None
                distance = float(row["distance_to_sensor_m"]) if pd.notna(row["distance_to_sensor_m"]) else None

                try:
                    conn.execute(
                        update(riskpoint_table)
                        .where(riskpoint_table.c.objectid == int(row["risk_id"]))
                        .values(
                            road_flood_sensor_id=sensor_id,
                            closest_road_flood_sensor_code=code,
                            closest_road_flood_sensor_distance=distance
                        )
                    )
                except Exception as e:
                    logging.warning(f"Failed to update risk_point {row['risk_id']} due to: {e}")