
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

from glance.domain.async import import_executor
import glance.tests.unit.utils as unit_utils
import glance.tests.utils as test_utils
from glance.tests import stubs


UUID1 = 'c80a1a6c-bd1f-41c5-90ee-81afedb1d58d'
TENANT1 = '6838eb7b-6ded-434a-882c-b344c77fe8df'


class TestTaskImportExecutor(test_utils.BaseTestCase):

    def setUp(self):
        super(TestTaskImportExecutor, self).setUp()
        self.request = unit_utils.get_fake_request()
        self.task_id = UUID1
        self.fake_task = unit_utils.FakeTask(self.request, self.task_id)
        self.import_executor = import_executor.TaskImportExecutor(self.request)

    def test_run(self):
        called = {"execute": False}
        def fake_execute(self, task):
            called['execute'] = True

        self.stubs.Set(import_executor.TaskImportExecutor, 'execute', fake_execute)
        self.import_executor.run(self.fake_task)

        count = 100
        while not called['execute'] and count:
            time.sleep(.01)
            count-=1;

        self.assertNotEqual(count, 0)
