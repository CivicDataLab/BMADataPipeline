"""update rainfall sensor streaming table

Revision ID: e2c2b3e767b6
Revises: c14f566e43d3
Create Date: 2025-04-30 08:29:19.617222

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e2c2b3e767b6'
down_revision: Union[str, None] = 'c14f566e43d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
