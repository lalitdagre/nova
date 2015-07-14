#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


from oslo_db.sqlalchemy import models
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import orm
from sqlalchemy import schema
from sqlalchemy import String
from sqlalchemy import Text


class _NovaAPIBase(models.ModelBase, models.TimestampMixin):
    pass


API_BASE = declarative_base(cls=_NovaAPIBase)


class CellMapping(API_BASE):
    """Contains information on communicating with a cell"""
    __tablename__ = 'cell_mappings'
    __table_args__ = (Index('uuid_idx', 'uuid'),
                      schema.UniqueConstraint('uuid',
                          name='uniq_cell_mappings0uuid'))

    id = Column(Integer, primary_key=True)
    uuid = Column(String(36), nullable=False)
    name = Column(String(255))
    transport_url = Column(Text())
    database_connection = Column(Text())


class InstanceMapping(API_BASE):
    """Contains the mapping of an instance to which cell it is in"""
    __tablename__ = 'instance_mappings'
    __table_args__ = (Index('project_id_idx', 'project_id'),
                      Index('instance_uuid_idx', 'instance_uuid'),
                      schema.UniqueConstraint('instance_uuid',
                          name='uniq_instance_mappings0instance_uuid'))

    id = Column(Integer, primary_key=True)
    instance_uuid = Column(String(36), nullable=False)
    cell_id = Column(Integer, ForeignKey('cell_mappings.id'),
            nullable=False)
    project_id = Column(String(255), nullable=False)


class HostMapping(API_BASE):
    """Contains mapping of a compute host to which cell it is in"""
    __tablename__ = "host_mappings"
    __table_args__ = (Index('host_idx', 'host'),
                      schema.UniqueConstraint('host',
                        name='uniq_host_mappings0host'))

    id = Column(Integer, primary_key=True)
    cell_id = Column(Integer, ForeignKey('cell_mappings.id'),
            nullable=False)
    host = Column(String(255), nullable=False)


# NOTE(dheeraj): Flavors and realted tables do not need SoftDeleteMixin but
# it cannot be removed without breaking lot of tests, so is being kept for now
class Flavors(API_BASE, models.SoftDeleteMixin):
    """Represents possible flavors for instances"""
    __tablename__ = 'flavors'
    __table_args__ = (
        schema.UniqueConstraint("flavorid", "deleted",
                                name="uniq_flavors0flavorid0deleted"),
        schema.UniqueConstraint("name", "deleted",
                                name="uniq_flavors0name0deleted"))

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    memory_mb = Column(Integer, nullable=False)
    vcpus = Column(Integer, nullable=False)
    root_gb = Column(Integer)
    ephemeral_gb = Column(Integer)
    flavorid = Column(String(255))
    swap = Column(Integer, nullable=False, default=0)
    rxtx_factor = Column(Float, default=1)
    vcpu_weight = Column(Integer)
    disabled = Column(Boolean, default=False)
    is_public = Column(Boolean, default=True)


class FlavorExtraSpecs(API_BASE, models.SoftDeleteMixin):
    """Represents additional specs as key/value pairs for a flavor"""
    __tablename__ = 'flavor_extra_specs'
    __table_args__ = (
        Index('flavor_extra_specs_flavor_id_key_idx', 'flavor_id', 'key'),
        schema.UniqueConstraint(
            'flavor_id', 'key', 'deleted',
            name='uniq_flavor_extra_specs0flavor_id0key0deleted'),
        {'mysql_collate': 'utf8_bin'},
    )

    id = Column(Integer, primary_key=True)
    key = Column(String(255))
    value = Column(String(255))
    flavor_id = Column(Integer, ForeignKey('flavors.id'),
              nullable=False)
    flavor = orm.relationship(
        Flavors, backref="extra_specs", foreign_keys=flavor_id,
        primaryjoin='and_('
                    'FlavorExtraSpecs.flavor_id == Flavors.id,'
                    'FlavorExtraSpecs.deleted == 0)')


class FlavorProjects(API_BASE, models.SoftDeleteMixin):
    """Represent projects associated to flavors"""
    __tablename__ = 'flavor_projects'
    __table_args__ = (schema.UniqueConstraint(
        'flavor_id', 'project_id', 'deleted',
        name='uniq_flavor_projects0flavor_id0project_id0deleted'),)

    id = Column(Integer, primary_key=True)
    flavor_id = Column(Integer, ForeignKey('flavors.id'),
                              nullable=False)
    project_id = Column(String(255))
    flavor = orm.relationship(
        Flavors, backref="projects", foreign_keys=flavor_id,
        primaryjoin='and_('
                    'FlavorProjects.flavor_id == Flavors.id,'
                    'FlavorProjects.deleted == 0)')
