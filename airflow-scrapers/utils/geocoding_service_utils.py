import requests
import json
import logging
from ratelimit import limits, sleep_and_retry
from retrying import retry
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


def get_bma_mis_budget_data(url: str, params: Optional[Dict[str, any]] = None) -> Any:
    username = os.getenv("BMA_API_USERNAME")
    password = os.getenv("BMA_API_PASSWORD")

    return {
        "username": username,
        "password": password
    }


def main():
    print(get_bma_mis_budget_data(
        "https://connect.bangkok.go.th/misbudget/bmabudget?source_id='01'&book_id='0'&fiscal_year='67'&department_id='11000000'&exp_object_id='05'"))


if __name__ == "__main__":
    main()
