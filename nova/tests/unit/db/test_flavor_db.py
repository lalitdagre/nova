# encoding=UTF8

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
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

"""Unit tests for the Flavor Objects DB API Methods."""

from nova import context
from nova import exception
from nova.objects import flavor as flavor_obj
from nova import test
from oslo_db import exception as db_exc


class ModelsObjectComparatorMixin(object):
    def _dict_from_object(self, obj, ignored_keys):
        if ignored_keys is None:
            ignored_keys = []
        if isinstance(obj, dict):
            obj_items = obj.items()
        else:
            obj_items = obj.iteritems()
        return {k: v for k, v in obj_items
                if k not in ignored_keys}

    def _assertEqualObjects(self, obj1, obj2, ignored_keys=None):
        obj1 = self._dict_from_object(obj1, ignored_keys)
        obj2 = self._dict_from_object(obj2, ignored_keys)

        self.assertEqual(len(obj1),
                         len(obj2),
                         "Keys mismatch: %s" %
                          str(set(obj1.keys()) ^ set(obj2.keys())))
        for key, value in obj1.items():
            self.assertEqual(value, obj2[key])

    def _assertEqualListsOfObjects(self, objs1, objs2, ignored_keys=None):
        obj_to_dict = lambda o: self._dict_from_object(o, ignored_keys)
        sort_key = lambda d: [d[k] for k in sorted(d)]
        conv_and_sort = lambda obj: sorted(map(obj_to_dict, obj), key=sort_key)

        self.assertEqual(conv_and_sort(objs1), conv_and_sort(objs2))

    def _assertEqualOrderedListOfObjects(self, objs1, objs2,
                                         ignored_keys=None):
        obj_to_dict = lambda o: self._dict_from_object(o, ignored_keys)
        conv = lambda objs: [obj_to_dict(obj) for obj in objs]

        self.assertEqual(conv(objs1), conv(objs2))

    def _assertEqualListsOfPrimitivesAsSets(self, primitives1, primitives2):
        self.assertEqual(len(primitives1), len(primitives2))
        for primitive in primitives1:
            self.assertIn(primitive, primitives2)

        for primitive in primitives2:
            self.assertIn(primitive, primitives1)


class BaseFlavorTestCase(test.TestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(BaseFlavorTestCase, self).setUp()
        self.ctxt = context.get_admin_context()
        self.user_ctxt = context.RequestContext('user', 'user')

    def _get_base_values(self):
        return {
            'name': 'fake_name',
            'memory_mb': 512,
            'vcpus': 1,
            'root_gb': 10,
            'ephemeral_gb': 10,
            'flavorid': 'fake_flavor',
            'swap': 0,
            'rxtx_factor': 0.5,
            'vcpu_weight': 1,
            'disabled': False,
            'is_public': True
        }

    def _create_flavor(self, values, projects=None):
        v = self._get_base_values()
        v.update(values)
        return flavor_obj._flavor_create_db(self.ctxt, v, projects)


class FlavorTestCase(BaseFlavorTestCase):

    def test_flavor_create(self):
        flavor = self._create_flavor({})
        ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                        'created_at', 'extra_specs']

        self.assertIsNotNone(flavor['id'])
        self._assertEqualObjects(flavor, self._get_base_values(),
                                 ignored_keys)

    def test_flavor_create_with_projects(self):
        projects = ['fake-project1', 'fake-project2']
        flavor = self._create_flavor({}, projects + ['fake-project2'])
        access = flavor_obj._flavor_access_get_by_flavor_id_db(self.ctxt,
                                                   flavor['flavorid'])
        self.assertEqual(projects, [x.project_id for x in access])

    def test_flavor_destroy(self):
        specs1 = {'a': '1', 'b': '2'}
        flavor1 = self._create_flavor({'name': 'name1', 'flavorid': 'a1',
                                       'extra_specs': specs1})
        specs2 = {'c': '4', 'd': '3'}
        flavor2 = self._create_flavor({'name': 'name2', 'flavorid': 'a2',
                                       'extra_specs': specs2})

        flavor_obj._flavor_destroy_db(self.ctxt, 'name1')

        self.assertRaises(exception.FlavorNotFound,
                         flavor_obj._flavor_get_db, self.ctxt, flavor1['id'])
        real_specs1 = flavor_obj._flavor_extra_specs_get_db(self.ctxt,
                                                         flavor1['flavorid'])
        self._assertEqualObjects(real_specs1, {})

        r_flavor2 = flavor_obj._flavor_get_db(self.ctxt, flavor2['id'])
        self._assertEqualObjects(flavor2, r_flavor2, 'extra_specs')

    def test_flavor_destroy_not_found(self):
        self.assertRaises(exception.FlavorNotFound,
                          flavor_obj._flavor_destroy_db, self.ctxt,
                          'nonexists')

    def test_flavor_create_duplicate_name(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorExists,
                          self._create_flavor,
                          {'flavorid': 'some_random_flavor'})

    def test_flavor_create_duplicate_flavorid(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorIdExists,
                          self._create_flavor,
                          {'name': 'some_random_name'})

    def test_flavor_create_with_extra_specs(self):
        extra_specs = dict(a='abc', b='def', c='ghi')
        flavor = self._create_flavor({'extra_specs': extra_specs})
        ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                        'created_at', 'extra_specs']

        self._assertEqualObjects(flavor, self._get_base_values(),
                                 ignored_keys)
        self._assertEqualObjects(extra_specs, flavor['extra_specs'])

    def test_flavor_get_all(self):
        # NOTE(boris-42): Remove base flavors
        for it in flavor_obj._flavor_get_all_db(self.ctxt):
            flavor_obj._flavor_destroy_db(self.ctxt, it['name'])

        flavors = [
            {'root_gb': 600, 'memory_mb': 100, 'disabled': True,
             'is_public': True, 'name': 'a1', 'flavorid': 'f1'},
            {'root_gb': 500, 'memory_mb': 200, 'disabled': True,
             'is_public': True, 'name': 'a2', 'flavorid': 'f2'},
            {'root_gb': 400, 'memory_mb': 300, 'disabled': False,
             'is_public': True, 'name': 'a3', 'flavorid': 'f3'},
            {'root_gb': 300, 'memory_mb': 400, 'disabled': False,
             'is_public': False, 'name': 'a4', 'flavorid': 'f4'},
            {'root_gb': 200, 'memory_mb': 500, 'disabled': True,
             'is_public': False, 'name': 'a5', 'flavorid': 'f5'},
            {'root_gb': 100, 'memory_mb': 600, 'disabled': True,
             'is_public': False, 'name': 'a6', 'flavorid': 'f6'}
        ]
        flavors = [self._create_flavor(it) for it in flavors]

        lambda_filters = {
            'min_memory_mb': lambda it, v: it['memory_mb'] >= v,
            'min_root_gb': lambda it, v: it['root_gb'] >= v,
            'disabled': lambda it, v: it['disabled'] == v,
            'is_public': lambda it, v: (v is None or it['is_public'] == v)
        }

        mem_filts = [{'min_memory_mb': x} for x in [100, 350, 550, 650]]
        root_filts = [{'min_root_gb': x} for x in [100, 350, 550, 650]]
        disabled_filts = [{'disabled': x} for x in [True, False]]
        is_public_filts = [{'is_public': x} for x in [True, False, None]]

        def assert_multi_filter_flavor_get(filters=None):
            if filters is None:
                filters = {}

            expected_it = flavors
            for name, value in filters.items():
                filt = lambda it: lambda_filters[name](it, value)
                expected_it = list(filter(filt, expected_it))

            real_it = flavor_obj._flavor_get_all_db(self.ctxt, filters=filters)
            self._assertEqualListsOfObjects(expected_it, real_it)

        # no filter
        assert_multi_filter_flavor_get()
        # test only with one filter
        for filt in mem_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in root_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in disabled_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in is_public_filts:
            assert_multi_filter_flavor_get(filt)

        # test all filters together
        for mem in mem_filts:
            for root in root_filts:
                for disabled in disabled_filts:
                    for is_public in is_public_filts:
                        filts = {}
                        for f in (mem, root, disabled, is_public):
                            filts.update(f)
                        assert_multi_filter_flavor_get(filts)

    def test_flavor_get_all_limit_sort(self):
        def assert_sorted_by_key_dir(sort_key, asc=True):
            sort_dir = 'asc' if asc else 'desc'
            results = flavor_obj._flavor_get_all_db(self.ctxt, sort_key='name',
                                        sort_dir=sort_dir)
            # Manually sort the results as we would expect them
            expected_results = sorted(results,
                                      key=lambda item: item['name'],
                                      reverse=(not asc))
            self.assertEqual(expected_results, results)

        def assert_sorted_by_key_both_dir(sort_key):
            assert_sorted_by_key_dir(sort_key, True)
            assert_sorted_by_key_dir(sort_key, False)

        for attr in ['memory_mb', 'root_gb', 'deleted_at', 'name', 'deleted',
                     'created_at', 'ephemeral_gb', 'updated_at', 'disabled',
                     'vcpus', 'swap', 'rxtx_factor', 'is_public', 'flavorid',
                     'vcpu_weight', 'id']:
            assert_sorted_by_key_both_dir(attr)

    def test_flavor_get_all_limit(self):
        limited_flavors = flavor_obj._flavor_get_all_db(self.ctxt, limit=2)
        self.assertEqual(2, len(limited_flavors))

    def test_flavor_get_all_list_marker(self):
        all_flavors = flavor_obj._flavor_get_all_db(self.ctxt)
        # Set the 3rd result as the marker
        marker_flavorid = all_flavors[2]['flavorid']
        marked_flavors = flavor_obj._flavor_get_all_db(self.ctxt,
                                                   marker=marker_flavorid)
        # We expect everything /after/ the 3rd result
        expected_results = all_flavors[3:]
        self.assertEqual(expected_results, marked_flavors)

    def test_flavor_get_all_marker_not_found(self):
        self.assertRaises(exception.MarkerNotFound,
                flavor_obj._flavor_get_all_db, self.ctxt, marker='invalid')

    def test_flavor_get(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            flavor_by_id = flavor_obj._flavor_get_db(self.ctxt, flavor['id'])
            self._assertEqualObjects(flavor, flavor_by_id)

    def test_flavor_get_non_public(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
                                      'is_public': False})

        # Admin can see it
        flavor_by_id = flavor_obj._flavor_get_db(self.ctxt, flavor['id'])
        self._assertEqualObjects(flavor, flavor_by_id)

        # Regular user can not
        self.assertRaises(exception.FlavorNotFound, flavor_obj._flavor_get_db,
                self.user_ctxt, flavor['id'])

        # Regular user can see it after being granted access
        flavor_obj._flavor_access_add_db(self.ctxt, flavor['flavorid'],
                self.user_ctxt.project_id)
        flavor_by_id = flavor_obj._flavor_get_db(self.user_ctxt, flavor['id'])
        self._assertEqualObjects(flavor, flavor_by_id)

    def test_flavor_get_by_name(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            flavor_by_name = flavor_obj._flavor_get_by_name_db(self.ctxt,
                                                               flavor['name'])
            self._assertEqualObjects(flavor, flavor_by_name)

    def test_flavor_get_by_name_not_found(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorNotFoundByName,
                          flavor_obj._flavor_get_by_name_db, self.ctxt,
                          'nonexists')

    def test_flavor_get_by_name_non_public(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
                                      'is_public': False})

        # Admin can see it
        flavor_by_name = flavor_obj._flavor_get_by_name_db(self.ctxt,
                                                           flavor['name'])
        self._assertEqualObjects(flavor, flavor_by_name)

        # Regular user can not
        self.assertRaises(exception.FlavorNotFoundByName,
                flavor_obj._flavor_get_by_name_db, self.user_ctxt,
                flavor['name'])

        # Regular user can see it after being granted access
        flavor_obj._flavor_access_add_db(self.ctxt, flavor['flavorid'],
                self.user_ctxt.project_id)
        flavor_by_name = flavor_obj._flavor_get_by_name_db(self.user_ctxt,
                                                           flavor['name'])
        self._assertEqualObjects(flavor, flavor_by_name)

    def test_flavor_get_by_flavor_id(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            params = (self.ctxt, flavor['flavorid'], None)
            flavor_by_flavorid = flavor_obj.\
                                _flavor_get_by_flavor_id_db(*params)
            self._assertEqualObjects(flavor, flavor_by_flavorid)

    def test_flavor_get_by_flavor_not_found(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorNotFound,
                          flavor_obj._flavor_get_by_flavor_id_db,
                          self.ctxt, 'nonexists', read_deleted=None)

    def test_flavor_get_by_flavor_id_non_public(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
                                      'is_public': False})

        # Admin can see it
        flavor_by_fid = flavor_obj._flavor_get_by_flavor_id_db(self.ctxt,
                                                   flavor['flavorid'],
                                                   read_deleted=None)
        self._assertEqualObjects(flavor, flavor_by_fid)

        # Regular user can not
        self.assertRaises(exception.FlavorNotFound,
                flavor_obj._flavor_get_by_flavor_id_db, self.user_ctxt,
                flavor['flavorid'], read_deleted=None)

        # Regular user can see it after being granted access
        flavor_obj._flavor_access_add_db(self.ctxt, flavor['flavorid'],
                self.user_ctxt.project_id)
        flavor_by_fid = flavor_obj._flavor_get_by_flavor_id_db(self.user_ctxt,
                                                   flavor['flavorid'],
                                                   read_deleted=None)
        self._assertEqualObjects(flavor, flavor_by_fid)

    def test_flavor_get_by_flavor_id_deleted(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123'})

        flavor_obj._flavor_destroy_db(self.ctxt, 'abc')

        flavor_by_fid = flavor_obj._flavor_get_by_flavor_id_db(self.ctxt,
                flavor['flavorid'], read_deleted='yes')
        self.assertEqual(flavor['id'], flavor_by_fid['id'])

    def test_flavor_get_by_flavor_id_deleted_and_recreat(self):
        # NOTE(wingwj): Aims to test difference between mysql and postgresql
        # for bug 1288636
        param_dict = {'name': 'abc', 'flavorid': '123'}

        self._create_flavor(param_dict)
        flavor_obj._flavor_destroy_db(self.ctxt, 'abc')

        # Recreate the flavor with the same params
        flavor = self._create_flavor(param_dict)

        flavor_by_fid = flavor_obj._flavor_get_by_flavor_id_db(self.ctxt,
                flavor['flavorid'], read_deleted='yes')
        self.assertEqual(flavor['id'], flavor_by_fid['id'])


class FlavorExtraSpecsTestCase(BaseFlavorTestCase):

    def setUp(self):
        super(FlavorExtraSpecsTestCase, self).setUp()
        values = ({'name': 'n1', 'flavorid': 'f1',
                   'extra_specs': dict(a='a', b='b', c='c')},
                  {'name': 'n2', 'flavorid': 'f2',
                   'extra_specs': dict(d='d', e='e', f='f')})

        # NOTE(boris-42): We have already tested flavor_create method
        #                 with extra_specs in FlavorTestCase.
        self.flavors = [self._create_flavor(v) for v in values]

    def test_flavor_extra_specs_get(self):
        for it in self.flavors:
            real_specs = flavor_obj._flavor_extra_specs_get_db(self.ctxt,
                                                               it['flavorid'])
            self._assertEqualObjects(it['extra_specs'], real_specs)

    def test_flavor_extra_specs_delete(self):
        for it in self.flavors:
            specs = it['extra_specs']
            key = list(specs.keys())[0]
            del specs[key]
            flavor_obj._flavor_extra_specs_delete_db(self.ctxt,
                                                     it['flavorid'], key)
            real_specs = flavor_obj._flavor_extra_specs_get_db(self.ctxt,
                                                              it['flavorid'])
            self._assertEqualObjects(it['extra_specs'], real_specs)

    def test_flavor_extra_specs_delete_failed(self):
        for it in self.flavors:
            self.assertRaises(exception.FlavorExtraSpecsNotFound,
                          flavor_obj._flavor_extra_specs_delete_db,
                          self.ctxt, it['flavorid'], 'dummy')

    def test_flavor_extra_specs_update_or_create(self):
        for it in self.flavors:
            current_specs = it['extra_specs']
            current_specs.update(dict(b='b1', c='c1', d='d1', e='e1'))
            params = (self.ctxt, it['flavorid'], current_specs)
            flavor_obj._flavor_extra_specs_update_or_create_db(*params)
            real_specs = flavor_obj._flavor_extra_specs_get_db(self.ctxt,
                                                              it['flavorid'])
            self._assertEqualObjects(current_specs, real_specs)

    def test_flavor_extra_specs_update_or_create_flavor_not_found(self):
        self.assertRaises(exception.FlavorNotFound,
                          flavor_obj._flavor_extra_specs_update_or_create_db,
                          self.ctxt, 'nonexists', {})

    def test_flavor_extra_specs_update_or_create_retry(self):

        def counted():
            def get_id(context, flavorid, session):
                get_id.counter += 1
                raise db_exc.DBDuplicateEntry
            get_id.counter = 0
            return get_id

        get_id = counted()
        self.stubs.Set(flavor_obj, '_flavor_get_id_from_flavor_db', get_id)
        self.assertRaises(exception.FlavorExtraSpecUpdateCreateFailed,
                          flavor_obj._flavor_extra_specs_update_or_create_db,
                          self.ctxt, 1, {}, 5)
        self.assertEqual(get_id.counter, 5)


class FlavorAccessTestCase(BaseFlavorTestCase):

    def _create_flavor_access(self, flavor_id, project_id):
        return flavor_obj._flavor_access_add_db(self.ctxt, flavor_id,
                                                project_id)

    def test_flavor_access_get_by_flavor_id(self):
        flavors = ({'name': 'n1', 'flavorid': 'f1'},
                   {'name': 'n2', 'flavorid': 'f2'})
        it1, it2 = tuple((self._create_flavor(v) for v in flavors))

        access_it1 = [self._create_flavor_access(it1['flavorid'], 'pr1'),
                      self._create_flavor_access(it1['flavorid'], 'pr2')]

        access_it2 = [self._create_flavor_access(it2['flavorid'], 'pr1')]

        for it, access_it in zip((it1, it2), (access_it1, access_it2)):
            params = (self.ctxt, it['flavorid'])
            real_access_it = flavor_obj.\
                            _flavor_access_get_by_flavor_id_db(*params)
            self._assertEqualListsOfObjects(access_it, real_access_it)

    def test_flavor_access_get_by_flavor_id_flavor_not_found(self):
        self.assertRaises(exception.FlavorNotFound,
                          flavor_obj._flavor_get_by_flavor_id_db,
                          self.ctxt, 'nonexists', read_deleted=None)

    def test_flavor_access_add(self):
        flavor = self._create_flavor({'flavorid': 'f1'})
        project_id = 'p1'

        access = self._create_flavor_access(flavor['flavorid'], project_id)
        # NOTE(boris-42): Check that flavor_access_add doesn't fail and
        #                 returns correct value. This is enough because other
        #                 logic is checked by other methods.
        self.assertIsNotNone(access['id'])
        self.assertEqual(access['flavor_id'], flavor['id'])
        self.assertEqual(access['project_id'], project_id)

    def test_flavor_access_add_to_non_existing_flavor(self):
        self.assertRaises(exception.FlavorNotFound,
                          self._create_flavor_access,
                          'nonexists', 'does_not_matter')

    def test_flavor_access_add_duplicate_project_id_flavor(self):
        flavor = self._create_flavor({'flavorid': 'f1'})
        params = (flavor['flavorid'], 'p1')

        self._create_flavor_access(*params)
        self.assertRaises(exception.FlavorAccessExists,
                          self._create_flavor_access, *params)

    def test_flavor_access_remove(self):
        flavors = ({'name': 'n1', 'flavorid': 'f1'},
                   {'name': 'n2', 'flavorid': 'f2'})
        it1, it2 = tuple((self._create_flavor(v) for v in flavors))

        access_it1 = [self._create_flavor_access(it1['flavorid'], 'pr1'),
                      self._create_flavor_access(it1['flavorid'], 'pr2')]

        access_it2 = [self._create_flavor_access(it2['flavorid'], 'pr1')]

        flavor_obj._flavor_access_remove_db(self.ctxt, it1['flavorid'],
                                access_it1[1]['project_id'])

        for it, access_it in zip((it1, it2), (access_it1[:1], access_it2)):
            params = (self.ctxt, it['flavorid'])
            real_access_it = flavor_obj.\
                            _flavor_access_get_by_flavor_id_db(*params)
            self._assertEqualListsOfObjects(access_it, real_access_it)

    def test_flavor_access_remove_flavor_not_found(self):
        self.assertRaises(exception.FlavorNotFound,
                          flavor_obj._flavor_access_remove_db,
                          self.ctxt, 'nonexists', 'does_not_matter')

    def test_flavor_access_remove_access_not_found(self):
        flavor = self._create_flavor({'flavorid': 'f1'})
        params = (flavor['flavorid'], 'p1')
        self._create_flavor_access(*params)
        self.assertRaises(exception.FlavorAccessNotFound,
                          flavor_obj._flavor_access_remove_db,
                          self.ctxt, flavor['flavorid'], 'p2')

    def test_flavor_access_removed_after_flavor_destroy(self):
        flavor1 = self._create_flavor({'flavorid': 'f1', 'name': 'n1'})
        flavor2 = self._create_flavor({'flavorid': 'f2', 'name': 'n2'})
        values = [
            (flavor1['flavorid'], 'p1'),
            (flavor1['flavorid'], 'p2'),
            (flavor2['flavorid'], 'p3')
        ]
        for v in values:
            self._create_flavor_access(*v)

        flavor_obj._flavor_destroy_db(self.ctxt, flavor1['name'])

        p = (self.ctxt, flavor1['flavorid'])
        self.assertEqual(0,
                       len(flavor_obj._flavor_access_get_by_flavor_id_db(*p)))
        p = (self.ctxt, flavor2['flavorid'])
        self.assertEqual(1,
                       len(flavor_obj._flavor_access_get_by_flavor_id_db(*p)))
        flavor_obj._flavor_destroy_db(self.ctxt, flavor2['name'])
        self.assertEqual(0,
                       len(flavor_obj._flavor_access_get_by_flavor_id_db(*p)))
