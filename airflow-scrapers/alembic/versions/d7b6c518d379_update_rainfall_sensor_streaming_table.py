"""update rainfall sensor streaming table

Revision ID: d7b6c518d379
Revises: d7777e4d2e5a
Create Date: 2025-04-30 08:37:06.008731

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7b6c518d379'
down_revision: Union[str, None] = 'd7777e4d2e5a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('rainfall_sensor_streaming_data',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('code', sa.String(length=255), nullable=True),
    sa.Column('sensor_id', sa.Integer(), nullable=True),
    sa.Column('rf5min', sa.Float(), nullable=True),
    sa.Column('rf15min', sa.Float(), nullable=True),
    sa.Column('rf30min', sa.Float(), nullable=True),
    sa.Column('rf1hr', sa.Float(), nullable=True),
    sa.Column('rf3hr', sa.Float(), nullable=True),
    sa.Column('rf6hr', sa.Float(), nullable=True),
    sa.Column('rf12hr', sa.Float(), nullable=True),
    sa.Column('rf24rh', sa.Float(), nullable=True),
    sa.Column('site_time', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['sensor_id'], ['rainfall_sensor.id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('rainfall_sensor_streaming_data')
    # ### end Alembic commands ###
