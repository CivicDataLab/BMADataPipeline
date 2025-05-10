# 2024 October to 2025 April For canal dredging CSV
# 2024 October to 2025 April for Sewarage dredging

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
import requests
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from models.models import CanalDredgingProgress, SewerageDredgingProgress
from utils.db_utils import setup_engine_and_metadata
from utils.canals_translation_map_utils import thai_to_column_mapping
from utils.buddhist_year_converter_utils import get_bangkok_date_info
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
load_dotenv()

def transform_canal_dredging_data(items, current_date):
    """
    Cast & clean each field per its target column type.
    """
    out = []
    for item in items:
        rec = {}
        for thai_key, eng_col in thai_to_column_mapping.items():
            raw = item.get(thai_key, "")

            # integer fields ending in "_m"
            if eng_col.endswith("_m"):
                try:
                    rec[eng_col] = int(raw.replace(",", "")) if raw not in ("", "-") else 0
                except:
                    rec[eng_col] = 0

            # percentage fields
            elif eng_col.startswith("percent_") or eng_col.startswith("total_percent"):
                try:
                    rec[eng_col] = float(raw) if raw not in ("", "-") else 0.0
                except:
                    rec[eng_col] = 0.0

            # year
            elif eng_col == "buddhist_year":
                try:
                    rec[eng_col] = int(raw) if raw else None
                except:
                    rec[eng_col] = None

            # day_period
            elif eng_col == "day_period":
                rec[eng_col] = str(raw) if raw else None

            # record_date
            elif eng_col == "record_date":
                rec[eng_col] = str(raw) if raw else str(current_date)

            # other integer counts
            # elif eng_col in {
            #     "record_number",
            #     "total_dredged_canals",
            #     "planned_dredging_canals",
            #     "additional_budgeted_canals",
            #     "annual_budgeted_canals",
            #     "regularly_maintained_canals",
            #     "periodically_flowed_canals",
            #     "total_canals_assigned",
            # }:
            #     try:
            #         rec[eng_col] = int(raw) if raw not in ("", "-") else 0
            #     except:
            #         rec[eng_col] = 0

            # everything else
            else:
                rec[eng_col] = raw if raw not in ("", "-") else None

        out.append(rec)

    if not out:
        raise ValueError("No data transformed; check the API response.")
    logging.info(f"Transformed {len(out)} records")
    return out


def main():
    # 1) DB setup
    engine, _ = setup_engine_and_metadata()
    Session   = sessionmaker(bind=engine)
    session   = Session()

    # 2) fetch‐range info
    keyid    = os.getenv("BMA_WEATHER_API_KEY")
    base_url = os.getenv("BMA_WEATHER_API_URL_NEW")
    if not all([keyid, base_url]):
        raise ValueError("Missing BMA_WEATHER_API_KEY or BMA_WEATHER_API_URL_NEW")

    current_date, _, _ = get_bangkok_date_info()

    api_url = f"{base_url}/ProgressReport/DDS"
    all_items = []

    # Oct 2567 (2024) → Dec 2567
    for year, months in [(2567, range(10, 13)), (2568, range(1, 6))]:
        for month in months:
            for period in ["01-15", "16-30"]:
                resp = requests.get(
                    api_url,
                    params={"ReportType": "01", "Period": period, "Month": month, "Year": year},
                    headers={"KeyId": keyid},
                )
                resp.raise_for_status()
                data = resp.json()
                items = [data] if isinstance(data, dict) else data

                for item in items:
                    # inject metadata fields so transform can pick them up
                    # item["Year"]   = year
                    # item["Month"]  = month
                    # item["Period"] = period
                    all_items.append(item)

    logging.info(f"Fetched a total of {len(all_items)} raw items")

    # 3) transform & clean
    cleaned = transform_canal_dredging_data(all_items, current_date)

    # 4) bulk insert
    try:
        session.bulk_insert_mappings(SewerageDredgingProgress, cleaned)
        session.commit()
        logging.info(f"Inserted {len(cleaned)} rows into canal_dredging_progress")
    except Exception:
        session.rollback()
        logging.exception("Bulk insert failed")
        raise
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()