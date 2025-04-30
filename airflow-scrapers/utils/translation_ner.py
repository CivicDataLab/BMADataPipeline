import os
import logging
import json
from typing import Any, List
import anthropic

import asyncio
import httpx
from dotenv import load_dotenv
from budget import get_mis_budget_by_fy
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# CONSTANT VARIABLES
DEPARTMENT_ID = '11000000'
EXP_OBJECT_ID = '05'  # Land, Durable Equipment and Construction
SOURCE_ID = '01'
BOOK_ID = '0'


def build_translation_and_entity_extraction_prompt(thai_project_details: str) -> str:
    prompt = f"""You are a multilingual assistant that translates Thai text into English and then performs named entity recognition (NER) to extract geolocation-related information, specifically tailored for Bangkok, Thailand.

Given a Thai sentence, perform the following two tasks:

1. Translate the sentence into English, preserving meaning and structure.
2. Extract key geolocation entities from the translated sentence. Focus on the following:
   - District (เขต / khet)
   - Sub-district (แขวง / khwaeng)
   - Road names, especially major roads
   - Canals (Khlong), landmarks, and any other address-like elements that may help geocoding
   - Geocoding lat,long of the Khlong, Road names or subdistricts or district as per the best case availability in that order of preference.
   - geocoding_of which entity whose lat/long has been calculated.

Return your response in the following JSON format:

{{
  "translation": "<English translation of the sentence>",
  "entities": {{
    "district": "<District name, or null>",
    "sub_district": "<Sub-district name, or null>",
    "roads": ["<Road name>", "..."],
    "canals": ["<Canal name>", "..."],
    "landmarks_or_other": ["<Landmark or other addressable entities>", "..."]
  }},
  "geocoding_of":"<landmarks_or_other,canals,roads,sub_district,sub_district>",
  "lat":"lattitude",
  "lon":"longitude"
}}

If an entity is not present in the text, set it to null or an empty list [] as appropriate.

Input (Thai): {thai_project_details}
"""
    return prompt


def translate_and_entity_extract(thai_project_details: str) -> Any:
    claude_api_key = os.getenv("CLAUDE_API_KEY")
    default_model_id = os.getenv("CLAUDE_MODEL_ID")
    client = anthropic.Anthropic(api_key=claude_api_key)
    translation_ner_prompt = build_translation_and_entity_extraction_prompt(
        thai_project_details)

    logging.info("The thai project details is: %s", thai_project_details)
    message = client.messages.create(
        model=str(default_model_id),
        max_tokens=256,
        temperature=0,
        messages=[
            {"role": "user", "content": translation_ner_prompt}
        ],
        stream=False
    )
    # extract the JSON object
    response = message.content[0].text  # type: ignore

    return response


async def fetch_all_budget_data() -> Any:

    fy67_data = await get_mis_budget_by_fy(67, DEPARTMENT_ID, EXP_OBJECT_ID, SOURCE_ID, BOOK_ID)

    fy68_data = await get_mis_budget_by_fy(68, DEPARTMENT_ID, EXP_OBJECT_ID, SOURCE_ID, BOOK_ID)

    combined_data = fy67_data["data"]+fy68_data["data"]
    return {
        "message": f"Successfully fetched {len(combined_data)} budget records",
        "data": combined_data
    }


async def augment_with_translation_and_entities(projects: List[dict]) -> Any:
    augmented = []
    for project in projects:
        await asyncio.sleep(1)
        detail = project.get("DETAIL", "")
        if not detail:
            logging.warning("No DETAIL in project: %s", project)
            project["translation_ner"] = None
        else:
            try:
                logging.info("Processing: %s", detail)
                # Claude API is sync, so call via thread executor to avoid blocking
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, translate_and_entity_extract, detail)
                project["translation_ner"] = result
            except Exception as e:
                logging.error("Failed to process: %s", detail)
                project["translation_ner"] = {"error": str(e)}
        augmented.append(project)
    return augmented


def clean_translation_ner_field(projects: list[dict]) -> list[dict]:
    cleaned_projects = []

    for project in projects:
        raw_value = project.get("translation_ner")

        if isinstance(raw_value, str):
            try:
                # Try to parse the stringified JSON
                cleaned_value = json.loads(raw_value)
                project["translation_ner"] = cleaned_value
            except json.JSONDecodeError as e:
                logging.warning(
                    "Failed to parse 'translation_ner' field: %s", e)
                project["translation_ner"] = {"error": "Invalid JSON format"}
        else:
            # If already a dict or null, just leave it as is
            project["translation_ner"] = raw_value

        cleaned_projects.append(project)

    return cleaned_projects
# ad hoc testing script


async def main():
    logging.info("Fetching budget data for FY67 and FY68")
    projects = await fetch_all_budget_data()
    # # print(projects)

    logging.info("Augmenting with Claude translation and entities...")
    enriched_projects = await augment_with_translation_and_entities(projects["data"])

    logging.info("Total enriched projects: %d", len(enriched_projects))

    # # print one samplerecord
    print(json.dumps(enriched_projects[0], ensure_ascii=False, indent=2))

    with open("augmented_projects_allowances.json", "w", encoding="utf-8") as f:
        json.dump(enriched_projects, f, ensure_ascii=False, indent=2)

    # with open("augmented_projects_others.json", "r", encoding="utf-8") as f:
    #     projects = json.load(f)
    # cleaned_projects = clean_translation_ner_field(projects)

    # print(len(cleaned_projects))

    # with open("cleaned_projects_others.json", "w", encoding="utf-8") as f:
    #     json.dump(cleaned_projects, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
u
