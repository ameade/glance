# Copyright 2012 OpenStack LLC.
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

from glance.api.v2.schemas import image
from glance.api.v2.schemas import image_access
from glance.api.v2.schemas import image_tags
from glance.common import wsgi
import glance.schema


class Controller(object):
    def __init__(self):
        self.access_schema = image_access.get_schema()
        self.tags_schema = image_tags.get_schema()
        self.image_schema = image.get_schema()

    def index(self, req):
        links = [
            {'rel': 'image', 'href': '/v2/schemas/image'},
            {'rel': 'access', 'href': '/v2/schemas/image/access'},
            {'rel': 'tags', 'href': '/v2/schemas/image/tags'},
        ]
        return {'links': links}

    def image(self, req):
        return self.image_schema.raw()

    def access(self, req):
        return self.access_schema.raw()

    def tags(self, req):
        return self.tags_schema.raw()


def create_resource():
    controller = Controller()
    return wsgi.Resource(controller)
