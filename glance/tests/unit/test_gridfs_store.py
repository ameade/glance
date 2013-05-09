# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 OpenStack Foundation
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

"""Tests the gridfs backend store"""

import __builtin__

from glance.openstack.common import uuidutils
from glance.store.gridfs import Store
from glance.tests.unit import base


class TestStore(base.IsolatedUnitTest):

    def setUp(self):
        """Establish a clean test environment"""
        super(TestStore, self).setUp()
        self.store = Store()

    def tearDown(self):
        """Clear the test environment"""
        super(TestStore, self).tearDown()

    def test_get_location_uri(self):
        image_id = uuidutils.generate_uuid()
        expected_location = "gridfs://%s" % image_id

        location = self.store.get_location_uri(image_id)

        self.assertEquals(expected_location, location)
