import os
import httpx
import logging
from typing import Any
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


async def get_mis_budget_by_fy(
        fiscal_year: str,
        department_id: str ,
        exp_object_id: str ,
        source_id: str ,
        book_id: str):

    url = os.getenv("BMA_MIS_API_URL")
    username = os.getenv("BMA_MIS_API_USERNAME")
    password = os.getenv("BMA_MIS_API_PASSWORD")

    # check for empty string error
    if not all([url, username, password]):
        raise ValueError(
            "Missing required environment variables for authentication")

    async with httpx.AsyncClient(auth=(str(username), str(password)), timeout=15.0) as budget_client:
        params = {
            "source_id": source_id,
            "book_id": book_id,
            "fiscal_year": fiscal_year,
            "department_id": department_id,
            "exp_object_id": exp_object_id
        }
        logging.info("Initialising API call")
        response = await budget_client.get(url=str(url), params=params)
        response_count = len(response.json())
        response.raise_for_status()
    response.raise_for_status()

    return {
        "message": f"Successfully fetched {response_count} budget details for projects",
        "data": response.json()
    }
