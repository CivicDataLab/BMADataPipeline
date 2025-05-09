"""create bangkok_budget

Revision ID: 27ebbc3dc32b
Revises: 
Create Date: 2025-04-28 20:14:33.019626

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '27ebbc3dc32b'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('bangkok_budget',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('mis_id', sa.String(length=255), nullable=True),
    sa.Column('department_name', sa.String(length=255), nullable=True),
    sa.Column('sector_name', sa.String(length=255), nullable=True),
    sa.Column('program_name', sa.String(length=255), nullable=True),
    sa.Column('func_name', sa.String(length=255), nullable=True),
    sa.Column('expenditure_name', sa.String(length=255), nullable=True),
    sa.Column('subobject_name', sa.String(length=255), nullable=True),
    sa.Column('func_id', sa.String(length=50), nullable=True),
    sa.Column('func_year', sa.String(length=50), nullable=True),
    sa.Column('func_seq', sa.String(length=50), nullable=True),
    sa.Column('exp_object_id', sa.String(length=50), nullable=True),
    sa.Column('exp_subobject_id', sa.String(length=50), nullable=True),
    sa.Column('expenditure_id', sa.String(length=50), nullable=True),
    sa.Column('item_id', sa.String(length=50), nullable=True),
    sa.Column('detail', sa.Text(), nullable=True),
    sa.Column('approve_on_hand', sa.String(length=255), nullable=True),
    sa.Column('allot_approve', sa.String(length=255), nullable=True),
    sa.Column('allot_date', sa.String(length=50), nullable=True),
    sa.Column('agr_date', sa.String(length=50), nullable=True),
    sa.Column('open_date', sa.String(length=50), nullable=True),
    sa.Column('acc_date', sa.String(length=50), nullable=True),
    sa.Column('acc_amt', sa.String(length=255), nullable=True),
    sa.Column('sgn_date', sa.String(length=50), nullable=True),
    sa.Column('st_sgn_date', sa.String(length=50), nullable=True),
    sa.Column('end_sgn_date', sa.String(length=50), nullable=True),
    sa.Column('last_rcv_date', sa.String(length=50), nullable=True),
    sa.Column('vendor_type_id', sa.String(length=50), nullable=True),
    sa.Column('vendor_no', sa.String(length=50), nullable=True),
    sa.Column('vendor_description', sa.String(length=255), nullable=True),
    sa.Column('pay_total_amt', sa.String(length=255), nullable=True),
    sa.Column('fin_dept_amt', sa.String(length=255), nullable=True),
    sa.Column('net_amt', sa.Float(), nullable=True),
    sa.Column('pur_hire_status', sa.String(length=50), nullable=True),
    sa.Column('pur_hire_status_name', sa.String(length=255), nullable=True),
    sa.Column('contract_id', sa.String(length=50), nullable=True),
    sa.Column('purchasing_department', sa.String(length=255), nullable=True),
    sa.Column('contract_name', sa.Text(), nullable=True),
    sa.Column('contract_amount', sa.String(length=255), nullable=True),
    sa.Column('pur_hire_method', sa.String(length=255), nullable=True),
    sa.Column('egp_project_code', sa.String(length=50), nullable=True),
    sa.Column('egp_po_control_code', sa.String(length=50), nullable=True),
    sa.Column('original_text', sa.Text(), nullable=True),
    sa.Column('translation', sa.Text(), nullable=True),
    sa.Column('entities', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('geocoding_of', sa.String(length=255), nullable=True),
    sa.Column('lat', sa.Float(), nullable=True),
    sa.Column('lon', sa.Float(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('bangkok_budget')
    # ### end Alembic commands ###
