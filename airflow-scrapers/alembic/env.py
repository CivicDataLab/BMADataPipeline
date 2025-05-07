import sys
import os
from logging.config import fileConfig
from alembic import context
from sqlalchemy import engine_from_config, pool

# ensure project root is on PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# import your Base metadata
from models.models import Base

target_metadata = Base.metadata

# build URL from .env
from dotenv import load_dotenv
load_dotenv()
url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}?sslmode=require"
config = context.config
config.set_main_option('sqlalchemy.url', url)
fileConfig(config.config_file_name)

# offline and online functions unchanged...

if context.is_offline_mode():
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix='sqlalchemy.', poolclass=pool.NullPool
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata,compare_type=True)
        with context.begin_transaction():
            context.run_migrations()