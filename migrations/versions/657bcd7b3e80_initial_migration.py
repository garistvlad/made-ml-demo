"""initial migration

Revision ID: 657bcd7b3e80
Revises: 
Create Date: 2020-12-27 22:03:58.541123

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '657bcd7b3e80'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('user',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('phone', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('phone')
    )
    op.create_table('score',
    sa.Column('score', sa.Float(), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], ),
    sa.PrimaryKeyConstraint('user_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('score')
    op.drop_table('user')
    # ### end Alembic commands ###
