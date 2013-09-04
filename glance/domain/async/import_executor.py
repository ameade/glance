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

import json

import eventlet

from glance.common import exception
from glance.domain import async

class TaskImportExecutor(async.TaskExecutorInterface):

    def execute(self, task):
        #TODO unpack json input in task, catch exceptions and set task message
        task_repo = self.gateway.get_task_repo(self.request.context)
        try:
            input = self.unpack_task_input(task)

        # Create Image
            self.create_image(input.get('image_properties'))
        except Exception as e:
            task.message = unicode(e)
            task_repo.save(task)

        # Verify location string and open file into file object

        # image.set_data(file) <--this will set status to active

        # image_repo.save()

    def run(self, task):
        eventlet.spawn_n(self.execute, task)

    def unpack_task_input(self, task):
        input = None
        try:
            input = json.loads(task.input)
        except ValueError:
            raise exception.Invalid(_("Input contains invalid json"))

        for key in ["import_from", "import_from_format", "image_properties"]:
            if not key in input:
                msg = _("Input does not contain '%s' field") % key
                raise exception.Invalid(msg)

        return input

    def create_image(self, image_properties):
        _base_properties = ['checksum', 'created_at', 'container_format',
                            'disk_format', 'id', 'min_disk', 'min_ram', 'name',
                            'size', 'status', 'tags', 'updated_at', 'visibility',
                            'protected']
        image_factory = self.gateway.get_image_factory(self.request.context)
        image_repo = self.gateway.get_repo(self.request.context)

        image = {}
        properties = image_properties
        tags = properties.pop('tags', None)
        for key in _base_properties:
            try:
                image[key] = properties.pop(key)
            except KeyError:
                pass
        image.pop('image_id', None)
        image = image_factory.new_image(tags=tags, extra_properties=properties,
                                        **image)
        image_repo.add(image)
        return image
