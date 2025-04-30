"""update rainfall sensor streaming table

Revision ID: 78b6aa4b2070
Revises: e2c2b3e767b6
Create Date: 2025-04-30 08:30:46.458728

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '78b6aa4b2070'
down_revision: Union[str, None] = 'e2c2b3e767b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
