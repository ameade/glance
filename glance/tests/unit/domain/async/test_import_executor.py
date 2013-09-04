
# Copyright 2012 OpenStack Foundation.
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

import time

import mock

from glance.common import exception
from glance.domain.async import import_executor
import glance.tests.unit.utils as unit_utils
import glance.tests.utils as test_utils
from glance.tests import stubs


UUID1 = 'c80a1a6c-bd1f-41c5-90ee-81afedb1d58d'
TENANT1 = '6838eb7b-6ded-434a-882c-b344c77fe8df'


class FakeTaskRepo(object):
    def __init__(self, result=None):
        self.result = result

    def get(self, task_id):
        if isinstance(self.result, BaseException):
            raise self.result
        else:
            return self.result

    def save(self, task):
        self.saved_task = task


class FakeImageRepo(object):
    def __init__(self):
        pass

    def add(self, image):
        pass


class FakeImageFactory(object):
    def __init__(self, fake_image):
        self.fake_image = fake_image

    def new_image(self, *args, **kwargs):
        return self.fake_image


class FakeGateway(object):
    def __init__(self, task_repo, image_repo, image_factory):
        self.task_repo = task_repo
        self.image_repo = image_repo
        self.image_factory = image_factory

    def get_task_repo(self, context):
        return self.task_repo

    def get_repo(self, context):
        return self.image_repo

    def get_image_factory(self, context):
        return self.image_factory


class TestTaskImportExecutor(test_utils.BaseTestCase):

    def setUp(self):
        super(TestTaskImportExecutor, self).setUp()
        self.request = unit_utils.get_fake_request()
        self.task_id = UUID1
        self.fake_task = unit_utils.FakeTask(self.request, self.task_id)
        self.fake_task_repo = FakeTaskRepo()
        self.fake_image_repo = FakeImageRepo()
        self.fake_image_factory = FakeImageFactory(None)
        self.import_executor = import_executor.TaskImportExecutor(self.request,
            gateway=FakeGateway(self.fake_task_repo, self.fake_image_repo,
                                self.fake_image_factory))

#    def test_run(self):
#        called = {"execute": False}
#        def fake_execute(self, task):
#            called['execute'] = True
#
#        self.stubs.Set(import_executor.TaskImportExecutor, 'execute', fake_execute)
#        self.import_executor.run(self.fake_task)
#
#        count = 100
#        while not called['execute'] and count:
#            time.sleep(.01)
#            count-=1;
#
#        self.assertNotEqual(count, 0)

    def test_unpack_task_input(self):
        fake_input_json = '{"import_from": "blah", "import_from_format": "qcow2", "image_properties": {}}'
        self.fake_task.input = fake_input_json
        data = self.import_executor.unpack_task_input(self.fake_task)

        self.assertEqual(data,
                         {'import_from': 'blah',
                          'import_from_format': 'qcow2',
                          'image_properties': {}})

    def test_unpack_task_input_missing_import_format(self):
        fake_input_json = '{"input_from": "blah", "image_properties": {}}'
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_executor.unpack_task_input,
                          self.fake_task)

    def test_unpack_task_input_missing_import_from(self):
        fake_input_json = '{"import_from_format": "qcow2", \
                          "image_properties": {}}'
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_executor.unpack_task_input,
                          self.fake_task)

    def test_unpack_task_input_missing_image_props(self):
        fake_input_json = '{"import_from_format": "qcow2", \
                          "import_from": "blah"}'
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_executor.unpack_task_input,
                          self.fake_task)

    def test_unpack_task_input_invalid_json(self):
        fake_input_json = 'invalid'
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_executor.unpack_task_input,
                          self.fake_task)

    def test_create_image(self):
        fake_image = {
            "name": "test_name",
            "tags": ['tag1', 'tag2'],
            "foo": "bar"
        }
        with mock.patch.object(self.fake_image_factory, 'new_image') as \
            mock_new_image:
            mock_new_image.return_value = {}
            with mock.patch.object(self.fake_image_repo, 'add') as mock_add:
                self.import_executor.create_image(fake_image)

        mock_new_image.assert_called_once_with(extra_properties={'foo': 'bar'},
                                               tags=['tag1', 'tag2'],
                                               name='test_name')
        mock_add.assert_called_once_with({})

    def test_execute(self):
        with mock.patch.object(self.import_executor, 'unpack_task_input') as \
            unpack_mock:
            self.import_executor.execute(self.fake_task)

        unpack_mock.assert_called_once_with(self.fake_task)
        self.assertEqual(self.fake_task.message, None)

    def test_execute_bad_input(self):
        with mock.patch.object(self.import_executor, 'unpack_task_input') as \
            unpack_mock:
            with mock.patch.object(self.fake_task_repo, 'save') as mock_save:
                unpack_mock.side_effect = exception.Invalid()
                self.import_executor.execute(self.fake_task)

        unpack_mock.assert_called_once_with(self.fake_task)
        mock_save.assert_called_once_with(self.fake_task)
        self.assertNotEqual(self.fake_task.message, None)

