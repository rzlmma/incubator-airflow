#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""add column to table task_instance

Revision ID: af395a16b21b
Revises: 17b9e5101476
Create Date: 2017-12-26 15:39:44.838409

"""

# revision identifiers, used by Alembic.
revision = 'af395a16b21b'
down_revision = '17b9e5101476'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('task_instance', sa.Column('is_success', sa.Boolean, default=False))


def downgrade():
    op.drop_column('task_instance', 'is_success')
