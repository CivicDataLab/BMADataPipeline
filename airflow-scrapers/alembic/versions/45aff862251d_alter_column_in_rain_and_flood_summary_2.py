"""alter column in rain and flood summary 2

Revision ID: 45aff862251d
Revises: cd849a20fddb
Create Date: 2025-05-08 19:25:14.595723

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '45aff862251d'
down_revision: Union[str, None] = 'cd849a20fddb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('rainfall_and_flood_summary_2', sa.Column('flood_drain_rate_in_cm_minutes', sa.Float(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('rainfall_and_flood_summary_2', 'flood_drain_rate_in_cm_minutes')
    # ### end Alembic commands ###
