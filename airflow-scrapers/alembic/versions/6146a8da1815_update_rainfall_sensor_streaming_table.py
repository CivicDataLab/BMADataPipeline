"""update rainfall sensor streaming table

Revision ID: 6146a8da1815
Revises: 78b6aa4b2070
Create Date: 2025-04-30 08:32:06.577991

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6146a8da1815'
down_revision: Union[str, None] = '78b6aa4b2070'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
