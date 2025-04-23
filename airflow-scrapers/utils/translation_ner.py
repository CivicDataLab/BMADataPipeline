import os
import logging
import json
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import anthropic
from dotenv import load_dotenv

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

claude_api_key = os.getenv("CLAUDE_API_KEY")
model_id = os.getenv("CLAUDE_MODEL_ID")
client = anthropic.Anthropic(api_key=claude_api_key)

def extract_entities_from_thai_text(thai_text: str) -> dict:
    """
    Translates Thai text to English and extracts named geolocation entities using Claude API.

    Args:
        thai_text (str): Original Thai sentence to process.

    Returns:
        dict: Dictionary with translation, entities, geocoding_of, lat, lon
    """
    prompt = f"""You are a multilingual assistant that translates Thai text into English and then performs named entity recognition (NER) to extract geolocation-related information, specifically tailored for Bangkok, Thailand.

Given a Thai sentence, perform the following two tasks:

1. Translate the sentence into English, preserving meaning and structure.
2. Extract key geolocation entities from the translated sentence. Focus on the following:
   - District (เขต / khet)
   - Sub-district (แขวง / khwaeng)
   - Road names, especially major roads
   - Canals (Khlong), landmarks, and any other address-like elements
   - Geocoding lat,long of the most specific available entity, in order of preference:
     landmarks_or_other → canals → roads → sub_district → district
   - geocoding_of field should note which entity was used for lat/lon

Respond in the following JSON format:
{{
  "translation": "<English translation of the sentence>",
  "entities": {{
    "district": "<District name, or null>",
    "sub_district": "<Sub-district name, or null>",
    "roads": ["<Road name>", "..."],
    "canals": ["<Canal name>", "..."],
    "landmarks_or_other": ["<Landmark or other addressable entities>", "..."]
  }},
  "geocoding_of": "<string>",
  "lat": "<latitude>",
  "lon": "<longitude>"
}}

Input (Thai): {thai_text}
"""

    try:
        logging.info("Sending request to Claude for NER and translation...")
        message = client.messages.create(
            model=model_id,
            max_tokens=256,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
            stream=False
        )
        raw_response = message.content[0].text  # type: ignore
        result = json.loads(raw_response)
        return result
    except Exception as e:
        logging.error(f"Failed to extract entities from Thai text: {e}")
        return {
            "translation": None,
            "entities": {},
            "geocoding_of": None,
            "lat": None,
            "lon": None,
            "error": str(e)
        }