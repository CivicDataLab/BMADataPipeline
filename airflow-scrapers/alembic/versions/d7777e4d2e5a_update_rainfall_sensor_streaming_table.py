"""update rainfall sensor streaming table

Revision ID: d7777e4d2e5a
Revises: 6146a8da1815
Create Date: 2025-04-30 08:32:46.039956

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7777e4d2e5a'
down_revision: Union[str, None] = '6146a8da1815'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
