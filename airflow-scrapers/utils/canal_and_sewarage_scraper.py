# 2024 October to 2025 April For canal dredging CSV
# 2024 October to 2025 April for Sewarage dredging

import requests
import json
import csv
import os
from dotenv import load_dotenv
load_dotenv()

def main():
    base_url=os.getenv("BMA_WEATHER_API_URL_NEW")
    url = f"{base_url}/ProgressReport/DDS"
    all_records = []
    keyid=os.getenv("BMA_WEATHER_API_KEY")
    # scrape from October 2567 to April 2568
    for year, months in [(2567, range(10, 13)), (2568, range(1, 5))]:
        for month in months:
            for period in ["01-15", "16-30"]:
                params = {
                    "ReportType": "01",
                    "Period": period,
                    "Month": month,
                    "Year": year
                }
                resp = requests.get(url, params=params, headers={"KeyId":keyid})
                resp.raise_for_status()
                data = resp.json()

                # normalize to list of dicts
                if isinstance(data, dict):
                    items = [data]
                else:
                    items = data

                for item in items:
                    # include metadata columns
                    item["Year"] = year
                    item["Month"] = month
                    item["Period"] = period
                    all_records.append(item)

    if not all_records:
        print("No data fetched.")
        return

    # determine CSV headers as the union of all keys
    fieldnames = set()
    for rec in all_records:
        fieldnames.update(rec.keys())

    # put Year, Month, Period first if they exist
    ordered_fields = [f for f in ("Year", "Month", "Period") if f in fieldnames]
    ordered_fields += [f for f in fieldnames if f not in ordered_fields]

    # write out the CSV
    with open("sewarage_progress_report_oct2567_apr2568.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=ordered_fields)
        writer.writeheader()
        writer.writerows(all_records)

    print(f"Saved {len(all_records)} records to progress_report_oct2567_apr2568.csv")

if __name__ == "__main__":
    main()
