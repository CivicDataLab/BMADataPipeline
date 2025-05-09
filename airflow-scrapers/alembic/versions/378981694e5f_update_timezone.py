"""update timezone

Revision ID: 378981694e5f
Revises: ebe14453ce76
Create Date: 2025-05-08 15:52:12.444217

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '378981694e5f'
down_revision: Union[str, None] = 'ebe14453ce76'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('flood_sensor_streaming_data', 'record_time',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               type_=sa.DateTime(),
               existing_nullable=True)
    op.add_column('rainfall_sensor_streaming_data', sa.Column('rf12rh', sa.Float(), nullable=True))
    op.drop_column('rainfall_sensor_streaming_data', 'rf12hr')
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('rainfall_sensor_streaming_data', sa.Column('rf12hr', postgresql.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=True))
    op.drop_column('rainfall_sensor_streaming_data', 'rf12rh')
    op.alter_column('flood_sensor_streaming_data', 'record_time',
               existing_type=sa.DateTime(),
               type_=postgresql.TIMESTAMP(timezone=True),
               existing_nullable=True)
    # ### end Alembic commands ###
