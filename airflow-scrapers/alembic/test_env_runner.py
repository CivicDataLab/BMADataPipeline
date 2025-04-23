import os
from dotenv import load_dotenv
load_dotenv()
def get_database_url_from_env():

    return f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}?sslmode=require"

print(get_database_url_from_env())