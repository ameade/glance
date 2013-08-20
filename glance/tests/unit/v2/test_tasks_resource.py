# Copyright 2013 OpenStack Foundation.
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

import datetime
import json

from oslo.config import cfg
import testtools
import webob

import glance.api.v2.images
from glance.openstack.common import uuidutils
import glance.schema
import glance.store
from glance.tests.unit import base
import glance.tests.unit.utils as unit_test_utils
import glance.tests.utils as test_utils


class TestTasksController(test_utils.BaseTestCase):

    def setUp(self):
        super(TestTasksController, self).setUp()
        self.db = unit_test_utils.FakeDB()
        self.controller = glance.api.v2.tasks.TasksController(self.db,
                                                              self.policy,
                                                              self.notifier,
                                                              self.store)


class TestImagesDeserializer(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesDeserializer, self).setUp()
        self.deserializer = glance.api.v2.tasks.RequestDeserializer()


class TestTasksSerializer(test_utils.BaseTestCase):

    def setUp(self):
        super(TestTasksSerializer, self).setUp()
        self.serializer = glance.api.v2.tasks.ResponseSerializer()
