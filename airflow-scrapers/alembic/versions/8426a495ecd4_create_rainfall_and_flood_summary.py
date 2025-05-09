"""create rainfall and flood summary

Revision ID: 8426a495ecd4
Revises: 675f8ed2fd22
Create Date: 2025-05-06 23:59:05.033163

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8426a495ecd4'
down_revision: Union[str, None] = '675f8ed2fd22'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('rainfall_and_flood_summary',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('risk_id', sa.Integer(), nullable=True),
    sa.Column('risk_name', sa.String(length=255), nullable=True),
    sa.Column('category', sa.String(length=255), nullable=True),
    sa.Column('district', sa.String(length=255), nullable=True),
    sa.Column('flood_sensor_code', sa.String(length=255), nullable=True),
    sa.Column('flood_code', sa.String(length=255), nullable=True),
    sa.Column('hour', sa.DateTime(timezone=True), nullable=True),
    sa.Column('max_flood', sa.Float(), nullable=True),
    sa.Column('flood_duration_minutes', sa.Float(), nullable=True),
    sa.Column('rainfall_sensor_code', sa.String(length=255), nullable=True),
    sa.Column('rf1hr', sa.Float(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.drop_table('flood_data')
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('flood_data',
    sa.Column('oid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('flood_code', sa.VARCHAR(length=255), autoincrement=False, nullable=True),
    sa.Column('hour', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('hourly_max_flood', sa.NUMERIC(), autoincrement=False, nullable=True),
    sa.Column('hourly_flood_frequency', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('rain_code', sa.VARCHAR(length=255), autoincrement=False, nullable=True),
    sa.Column('rf1hr', sa.NUMERIC(), autoincrement=False, nullable=True),
    sa.Column('flood_duration_min', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('oid', name='flood_data_pkey')
    )
    op.drop_table('rainfall_and_flood_summary')
    # ### end Alembic commands ###
