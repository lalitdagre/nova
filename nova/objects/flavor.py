#    Copyright 2013 Red Hat, Inc
#
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

import copy
from nova import db
from nova import exception
from nova import objects
from nova.objects import base
from nova.objects import fields
from operator import itemgetter

from oslo_db.sqlalchemy import utils as sqlalchemyutils
from sqlalchemy import or_
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.expression import asc
from sqlalchemy.sql import true

from nova.db.sqlalchemy import api as db_api
from nova.db.sqlalchemy.api import require_context
from nova.db.sqlalchemy import api_models
from oslo_db import exception as db_exc


OPTIONAL_FIELDS = ['extra_specs', 'projects']


def _migrate_flavor_to_api(context, values, flavor_id):
    tvalues = copy.deepcopy(values)
    del tvalues['id']
    access = db.flavor_access_get_by_flavor_id(context, flavor_id)
    _flavor_create_db(context, tvalues, projects=access)


def _flavor_get_id_from_flavor_db(context, flavor_id, session=None):
    result = _flavor_get_id_from_flavor_query_db(context, flavor_id,
                                                     session=session).\
                    first()
    if not result:
        raise exception.FlavorNotFound(flavor_id=flavor_id)
    return result[0]


def _flavor_access_get_by_flavor_id_db(context, flavor_id):
    """Get flavor access list by flavor id."""
    session = db_api.get_api_session()
    flavor_id_subq = \
            _flavor_get_id_from_flavor_query_db(context, flavor_id, session)
    access_refs = _flavor_access_query_db(context, session).\
                        filter_by(flavor_id=flavor_id_subq).\
                        all()
    return access_refs


def _flavor_get_id_from_flavor_query_db(context, flavor_id, session=None):
    return db_api.model_query(context, api_models.Flavors,
                       (api_models.Flavors.id,),
                       read_deleted="no", session=session).\
                filter_by(flavorid=flavor_id)


def _flavor_access_query_db(context, session=None):
    return db_api.model_query(context, api_models.FlavorProjects,
                       session=session, read_deleted="no")


def _flavor_get_query_db(context, session=None, read_deleted=None):
    query = db_api.model_query(context, api_models.Flavors, session=session,
                              read_deleted=read_deleted).\
                              options(joinedload('extra_specs'))
    if not context.is_admin:
        the_filter = [api_models.Flavors.is_public == true()]
        the_filter.extend([
            api_models.Flavors.projects.any(project_id=context.project_id)
        ])
        query = query.filter(or_(*the_filter))
    return query


def _sort_flavor_list(dictlist, key, sort_dir):

    """_sort_flavor_list takes list containing dictionary and returns the sorted
    list which is sorted by key
    """
    asc = True if sort_dir is 'asc' else False
    return sorted(dictlist, key=itemgetter(key), reverse=(not asc))


def _flavor_list_by_marker(flavorlist, marker):
    if marker is None:
        return flavorlist
    try:
        mark = [i for i, _ in enumerate(flavorlist) if _['flavorid']
                                                     == marker][0] + 1
        return flavorlist[mark:]
    except Exception:
        raise exception.MarkerNotFound(marker)


def _limit_flavor_list(flavorlist, limit):

    """_limit_flavor_list takes flavor list and limit count. It returns the
    flavors till the limit as upperlimit of flavor list.
    """
    return flavorlist[:limit]


def _flavor_union_list(flavor_list1, flavor_list2):

    """_flavor_union_list takes two lists containing dictionary. It returns
    union of input lists by key as flavorid
    """
    flavor_list1_ids = [x["flavorid"] for x in flavor_list1]
    return flavor_list1 + [x for x in flavor_list2 if x["flavorid"]
                                           not in flavor_list1_ids]


def _flavor_access_add_db(context, flavor_id, project_id):
    """Add given tenant to the flavor access list."""
    session = db_api.get_api_session()
    flavor_id = _flavor_get_id_from_flavor_db(context, flavor_id, session)

    access_ref = api_models.FlavorProjects()
    access_ref.update({"flavor_id": flavor_id,
                       "project_id": project_id})
    try:
        access_ref.save(session)
    except db_exc.DBDuplicateEntry:
        raise exception.FlavorAccessExists(flavor_id=flavor_id,
                                            project_id=project_id)
    return access_ref


def _flavor_access_remove_db(context, flavor_id, project_id):
    """Remove given tenant from the flavor access list."""
    session = db_api.get_api_session()
    flavor_id = _flavor_get_id_from_flavor_db(context, flavor_id, session)

    count = _flavor_access_query_db(context, session).\
                    filter_by(flavor_id=flavor_id).\
                    filter_by(project_id=project_id).\
                    soft_delete(synchronize_session=False)
    if count == 0:
        raise exception.FlavorAccessNotFound(flavor_id=flavor_id,
                                             project_id=project_id)


def _flavor_extra_specs_get_query_db(context, flavor_id, session=None):
    flavor_id_subq = \
            _flavor_get_id_from_flavor_query_db(context, flavor_id)

    return db_api.model_query(context, api_models.FlavorExtraSpecs,
                       session=session, read_deleted="no").\
                filter_by(flavor_id=flavor_id_subq)


@require_context
def _flavor_extra_specs_delete_db(context, flavor_id, key):
    session = db_api.get_api_session()
    result = _flavor_extra_specs_get_query_db(context, flavor_id, session).\
                     filter(api_models.FlavorExtraSpecs.key == key).\
                     soft_delete(synchronize_session=False)
    # did not find the extra spec
    if result == 0:
        raise exception.FlavorExtraSpecsNotFound(
                extra_specs_key=key, flavor_id=flavor_id)


@require_context
def _flavor_extra_specs_update_or_create_db(context, flavor_id, specs,
                                               max_retries=10):
    for attempt in range(max_retries):
        try:
            session = db_api.get_api_session()
            with session.begin():
                flavor_id = _flavor_get_id_from_flavor_db(context,
                                                         flavor_id, session)
                spec_refs = db_api.model_query(context,
                                        api_models.FlavorExtraSpecs,
                                        session=session, read_deleted="no").\
                  filter_by(flavor_id=flavor_id).\
                  filter(api_models.FlavorExtraSpecs.key.in_(specs.keys())).\
                  all()

                existing_keys = set()
                for spec_ref in spec_refs:
                    key = spec_ref["key"]
                    existing_keys.add(key)
                    spec_ref.update({"value": specs[key]})

                for key, value in specs.items():
                    if key in existing_keys:
                        continue
                    spec_ref = api_models.FlavorExtraSpecs()
                    spec_ref.update({"key": key, "value": value,
                                     "flavor_id": flavor_id})
                    spec_ref.save(session)

            return specs
        except db_exc.DBDuplicateEntry:
            # a concurrent transaction has been committed,
            # try again unless this was the last attempt
            if attempt == max_retries - 1:
                raise exception.FlavorExtraSpecUpdateCreateFailed(
                                    id=flavor_id, retries=max_retries)


def _flavor_create_db(context, values, projects=None):

    """Create a new instance type. In order to pass in extra specs,
    the values dict should contain a 'extra_specs' key/value pair:

    {'extra_specs' : {'k1': 'v1', 'k2': 'v2', ...}}

    """
    specs = values.get('extra_specs')
    specs_refs = []
    if specs:
        for k, v in specs.items():
            specs_ref = api_models.FlavorExtraSpecs()
            specs_ref['key'] = k
            specs_ref['value'] = v
            specs_refs.append(specs_ref)

    values['extra_specs'] = specs_refs
    flavors_ref = api_models.Flavors()
    flavors_ref.update(values)
    if projects is None:
        projects = []

    session = db_api.get_api_session()
    with session.begin():
        try:
            flavors_ref.save(session)
        except db_exc.DBDuplicateEntry as e:
            if 'flavorid' in e.columns:
                raise exception.FlavorIdExists(
                                          flavor_id=values['flavorid'])
            raise exception.FlavorExists(name=values['name'])
        except Exception as e:
            raise db_exc.DBError(e)
        for project in set(projects):
            access_ref = api_models.FlavorProjects()
            access_ref.update({"flavor_id": flavors_ref.id,
                               "project_id": project})
            access_ref.save(session)

    specs = values.get('extra_specs')
    return db_api._dict_with_extra_specs(flavors_ref)


def _flavor_by_name_exist_in_db(context, f_name):
    try:
        db.flavor_get_by_name(context, f_name)
        return True
    except exception.FlavorNotFoundByName:
        return False


def _flavor_by_flavor_id_exist_in_db(context, f_id):
    try:
        db.flavor_get_by_flavor_id(context, f_id)
        return True
    except exception.FlavorNotFound:
        return False


@require_context
def _flavor_get_all_db(context, inactive=False, filters=None,
                      sort_key='flavorid', sort_dir='asc', limit=None,
                      marker=None):
    """Returns all flavors.
    """
    filters = filters or {}

    # FIXME(sirp): now that we have the `disabled` field for flavors, we
    # should probably remove the use of `deleted` to mark inactive.
    # `deleted` should mean truly deleted, e.g. we can safely purge
    # the record out of the database.

    read_deleted = "yes" if inactive else "no"
    session = db_api.get_api_session()
    query = _flavor_get_query_db(context, session,
                                 read_deleted=read_deleted)

    if 'min_memory_mb' in filters:
        query = query.filter(
                api_models.Flavors.memory_mb >= filters['min_memory_mb'])

    if 'min_root_gb' in filters:
        query = query.filter(
                api_models.Flavors.root_gb >= filters['min_root_gb'])

    if 'disabled' in filters:
        query = query.filter(
               api_models.Flavors.disabled == filters['disabled'])

    if 'is_public' in filters and filters['is_public'] is not None:
        the_filter = [api_models.Flavors.is_public == filters['is_public']]
        if filters['is_public'] and context.project_id is not None:
            the_filter.extend([
                api_models.Flavors.projects.any(
                   project_id=context.project_id, deleted=0)
            ])
        if len(the_filter) > 1:
            query = query.filter(or_(*the_filter))
        else:
            query = query.filter(the_filter[0])
    marker_row = None
    union_marker = 0
    if marker is not None:
        marker_row = _flavor_get_query_db(context, session,
                      read_deleted=read_deleted).\
                    filter_by(flavorid=marker).\
                    first()
        if not marker_row:
            union_marker = 1
            pass

    query = sqlalchemyutils.paginate_query(query, api_models.Flavors,
                                       limit,
                                       [sort_key, 'id'],
                                       marker=marker_row,
                                       sort_dir=sort_dir)
    flavors = query.all()
    nova_flavors = db.flavor_get_all(context, inactive=inactive,
                                   filters=filters, sort_key=sort_key,
                                   sort_dir=sort_dir, limit=limit,
                                   marker=None)
    api_flavors = [db_api._dict_with_extra_specs(i) for i in flavors]
    flavor_union = _flavor_union_list(api_flavors, nova_flavors)
    flavor_union = _sort_flavor_list(flavor_union, sort_key, sort_dir)
    if (union_marker == 1):
        flavor_union = _flavor_list_by_marker(flavor_union, marker)

    return _limit_flavor_list(flavor_union, limit)


def _flavor_destroy_db(context, name):
    """Marks specific flavor as deleted."""
    session = db_api.get_api_session()
    with session.begin():
        ref = db_api.model_query(context, api_models.Flavors,
                          session=session,
                          read_deleted="no").\
                    filter_by(name=name).\
                    first()
        if not ref:
            try:
                db.flavor_destroy(context, name)
                return
            except exception.FlavorNotFoundByName:
                raise

        ref.soft_delete(session=session)
        db_api.model_query(context, api_models.FlavorExtraSpecs,
                    session=session, read_deleted="no").\
                    filter_by(flavor_id=ref['id']).\
                    soft_delete()
        db_api.model_query(context, api_models.FlavorProjects,
                    session=session, read_deleted="no").\
                    filter_by(flavor_id=ref['id']).\
                    soft_delete()
        try:
            db.flavor_destroy(context, name)
        except exception.FlavorNotFoundByName:
            pass


@require_context
def _flavor_get_by_flavor_id_db(context, flavor_id,
                                         read_deleted):
    """Returns a dict describing specific flavor_id."""
    session = db_api.get_api_session()
    result = _flavor_get_query_db(context, session,
                        read_deleted=read_deleted).\
                        filter_by(flavorid=flavor_id).\
                        order_by(asc("deleted"), asc("id")).\
                        first()
    if not result:
        raise exception.FlavorNotFound(flavor_id=flavor_id)
    return db_api._dict_with_extra_specs(result)


@require_context
def _flavor_get_by_name_db(context, name):
    """Returns a dict describing specific flavor."""
    session = db_api.get_api_session()
    result = _flavor_get_query_db(context, session).\
                        filter_by(name=name).\
                        first()
    if not result:
        raise exception.FlavorNotFoundByName(flavor_name=name)
    return db_api._dict_with_extra_specs(result)


def _flavor_get_by_id_db(context, id):
    """Returns a dict describing specific flavor."""
    session = db_api.get_api_session()
    result = _flavor_get_query_db(context, session).\
                        filter_by(id=id).\
                        first()
    if not result:
        raise exception.FlavorNotFound(flavor_id=id)
    return db_api._dict_with_extra_specs(result)


@require_context
def _flavor_extra_specs_get_db(context, flavor_id):
    session = db_api.get_api_session()
    rows = _flavor_extra_specs_get_query_db(context, flavor_id, session).all()
    return {row['key']: row['value'] for row in rows}


@require_context
def _flavor_get_db(context, id):
    """Returns a dict describing specific flavor."""
    session = db_api.get_api_session()
    result = _flavor_get_query_db(context, session).\
                        filter_by(id=id).\
                        first()
    if not result:
        raise exception.FlavorNotFound(flavor_id=id)
    return db_api._dict_with_extra_specs(result)


# TODO(berrange): Remove NovaObjectDictCompat
@base.NovaObjectRegistry.register
class Flavor(base.NovaPersistentObject, base.NovaObject,
             base.NovaObjectDictCompat):
    # Version 1.0: Initial version
    # Version 1.1: Added save_projects(), save_extra_specs(), removed
    #              remoteable from save()
    VERSION = '1.1'

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(nullable=True),
        'memory_mb': fields.IntegerField(),
        'vcpus': fields.IntegerField(),
        'root_gb': fields.IntegerField(),
        'ephemeral_gb': fields.IntegerField(),
        'flavorid': fields.StringField(),
        'swap': fields.IntegerField(),
        'rxtx_factor': fields.FloatField(nullable=True, default=1.0),
        'vcpu_weight': fields.IntegerField(nullable=True),
        'disabled': fields.BooleanField(),
        'is_public': fields.BooleanField(),
        'extra_specs': fields.DictOfStringsField(),
        'projects': fields.ListOfStringsField(),
        }

    def __init__(self, *args, **kwargs):
        super(Flavor, self).__init__(*args, **kwargs)
        self._orig_extra_specs = {}
        self._orig_projects = []

    @staticmethod
    def _from_db_object(context, flavor, db_flavor, expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []
        flavor._context = context
        for name, field in flavor.fields.items():
            if name in OPTIONAL_FIELDS:
                continue
            value = db_flavor[name]
            if isinstance(field, fields.IntegerField):
                value = value if value is not None else 0
            flavor[name] = value

        if 'extra_specs' in expected_attrs:
            flavor.extra_specs = db_flavor['extra_specs']

        if 'projects' in expected_attrs:
            flavor._load_projects()

        flavor.obj_reset_changes()
        return flavor

    @base.remotable
    def _load_projects(self):
        self.projects = [x['project_id'] for x in
                         _flavor_access_get_by_flavor_id_db(self._context,
                                                           self.flavorid)]
        self.obj_reset_changes(['projects'])

    def obj_load_attr(self, attrname):
        # NOTE(danms): Only projects could be lazy-loaded right now
        if attrname != 'projects':
            raise exception.ObjectActionError(
                action='obj_load_attr', reason='unable to load %s' % attrname)

        self._load_projects()

    def obj_reset_changes(self, fields=None, recursive=False):
        super(Flavor, self).obj_reset_changes(fields=fields,
                recursive=recursive)
        if fields is None or 'extra_specs' in fields:
            self._orig_extra_specs = (dict(self.extra_specs)
                                      if self.obj_attr_is_set('extra_specs')
                                      else {})
        if fields is None or 'projects' in fields:
            self._orig_projects = (list(self.projects)
                                   if self.obj_attr_is_set('projects')
                                   else [])

    def obj_what_changed(self):
        changes = super(Flavor, self).obj_what_changed()
        if ('extra_specs' in self and
            self.extra_specs != self._orig_extra_specs):
            changes.add('extra_specs')
        if 'projects' in self and self.projects != self._orig_projects:
            changes.add('projects')
        return changes

    @classmethod
    def _obj_from_primitive(cls, context, objver, primitive):
        self = super(Flavor, cls)._obj_from_primitive(context, objver,
                                                      primitive)
        changes = self.obj_what_changed()
        if 'extra_specs' not in changes:
            # This call left extra_specs "clean" so update our tracker
            self._orig_extra_specs = (dict(self.extra_specs)
                                      if self.obj_attr_is_set('extra_specs')
                                      else {})
        if 'projects' not in changes:
            # This call left projects "clean" so update our tracker
            self._orig_projects = (list(self.projects)
                                   if self.obj_attr_is_set('projects')
                                   else [])
        return self

    @base.remotable_classmethod
    def get_by_id(cls, context, id):
        try:
            db_flavor = _flavor_get_db(context, id)
        except exception.FlavorNotFound:
            db_flavor = db.flavor_get(context, id)
        return cls._from_db_object(context, cls(context), db_flavor,
                                   expected_attrs=['extra_specs'])

    @base.remotable_classmethod
    def get_by_name(cls, context, name):
        try:
            db_flavor = _flavor_get_by_name_db(context, name)
        except exception.FlavorNotFoundByName:
            db_flavor = db.flavor_get_by_name(context, name)
        return cls._from_db_object(context, cls(context), db_flavor,
                                   expected_attrs=['extra_specs'])

    @base.remotable_classmethod
    def get_by_flavor_id(cls, context, flavor_id, read_deleted=None):
        try:
            db_flavor = _flavor_get_by_flavor_id_db(context,
                                                    flavor_id, read_deleted)
        except exception.FlavorNotFound:
            db_flavor = db.flavor_get_by_flavor_id(context, flavor_id,
                                                   read_deleted)
            _migrate_flavor_to_api(context, db_flavor, flavor_id)
        return cls._from_db_object(context, cls(context), db_flavor,
                                   expected_attrs=['extra_specs'])

    @base.remotable
    def add_access(self, project_id):
        if 'projects' in self.obj_what_changed():
            raise exception.ObjectActionError(action='add_access',
                                              reason='projects modified')
        _flavor_access_add_db(self._context, self.flavorid, project_id)
        self._load_projects()

    @base.remotable
    def remove_access(self, project_id):
        if 'projects' in self.obj_what_changed():
            raise exception.ObjectActionError(action='remove_access',
                                              reason='projects modified')
        _flavor_access_remove_db(self._context, self.flavorid, project_id)
        self._load_projects()

    @base.remotable
    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.obj_get_changes()
        expected_attrs = []
        for attr in OPTIONAL_FIELDS:
            if attr in updates:
                expected_attrs.append(attr)
        projects = updates.pop('projects', [])

        if (_flavor_by_flavor_id_exist_in_db(self._context,
                                              updates.get('flavorid'))):
            raise exception.FlavorIdExists(flavor_id=updates.get('flavorid'))
        if (_flavor_by_name_exist_in_db(self._context, updates.get('name'))):
            raise exception.FlavorExists(name=updates.get('name'))

        db_flavor = _flavor_create_db(self._context, updates,
                                          projects=projects)
        self._from_db_object(self._context, self, db_flavor,
                             expected_attrs=expected_attrs)

    @base.remotable
    def save_projects(self, to_add=None, to_delete=None):
        """Add or delete projects.

        :param:to_add: A list of projects to add
        :param:to_delete: A list of projects to remove
        """

        to_add = to_add if to_add is not None else []
        to_delete = to_delete if to_delete is not None else []

        for project_id in to_add:
            _flavor_access_add_db(self._context, self.flavorid, project_id)
        for project_id in to_delete:
            _flavor_access_remove_db(self._context, self.flavorid, project_id)
        self.obj_reset_changes(['projects'])

    @base.remotable
    def save_extra_specs(self, to_add=None, to_delete=None):
        """Add or delete extra_specs.

        :param:to_add: A dict of new keys to add/update
        :param:to_delete: A list of keys to remove
        """

        to_add = to_add if to_add is not None else {}
        to_delete = to_delete if to_delete is not None else []

        if to_add:
            _flavor_extra_specs_update_or_create_db(self._context,
                                                   self.flavorid,
                                                   to_add)
        for key in to_delete:
            _flavor_extra_specs_delete_db(self._context, self.flavorid, key)
        self.obj_reset_changes(['extra_specs'])

    def save(self):
        updates = self.obj_get_changes()
        projects = updates.pop('projects', None)
        extra_specs = updates.pop('extra_specs', None)
        if updates:
            raise exception.ObjectActionError(
                action='save', reason='read-only fields were changed')

        if extra_specs is not None:
            deleted_keys = (set(self._orig_extra_specs.keys()) -
                            set(extra_specs.keys()))
            added_keys = self.extra_specs
        else:
            added_keys = deleted_keys = None

        if projects is not None:
            deleted_projects = set(self._orig_projects) - set(projects)
            added_projects = set(projects) - set(self._orig_projects)
        else:
            added_projects = deleted_projects = None

        # NOTE(danms): The first remotable method we call will reset
        # our of the original values for projects and extra_specs. Thus,
        # we collect the added/deleted lists for both above and /then/
        # call these methods to update them.

        if added_keys or deleted_keys:
            self.save_extra_specs(self.extra_specs, deleted_keys)

        if added_projects or deleted_projects:
            self.save_projects(added_projects, deleted_projects)

    @base.remotable
    def destroy(self):
        _flavor_destroy_db(self._context, self.name)


@base.NovaObjectRegistry.register
class FlavorList(base.ObjectListBase, base.NovaObject):
    VERSION = '1.1'

    fields = {
        'objects': fields.ListOfObjectsField('Flavor'),
        }
    obj_relationships = {
        'objects': [('1.0', '1.0'), ('1.1', '1.1')]
        }

    @base.remotable_classmethod
    def get_all(cls, context, inactive=False, filters=None,
                sort_key='flavorid', sort_dir='asc', limit=None, marker=None):
        db_flavors = _flavor_get_all_db(context, inactive=inactive,
                                       filters=filters, sort_key=sort_key,
                                       sort_dir=sort_dir, limit=limit,
                                       marker=marker)
        return base.obj_make_list(context, cls(context), objects.Flavor,
                                  db_flavors, expected_attrs=['extra_specs'])
