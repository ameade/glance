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

from oslo.config import cfg

from glance.common import exception
from glance.openstack.common import timeutils
from glance.openstack.common import uuidutils
from glance.domain.async import import_executor


image_format_opts = [
    cfg.ListOpt('container_formats',
                default=['ami', 'ari', 'aki', 'bare', 'ovf'],
                help=_("Supported values for the 'container_format' "
                       "image attribute")),
    cfg.ListOpt('disk_formats',
                default=['ami', 'ari', 'aki', 'vhd', 'vmdk', 'raw', 'qcow2',
                         'vdi', 'iso'],
                help=_("Supported values for the 'disk_format' "
                       "image attribute")),
]


CONF = cfg.CONF
CONF.register_opts(image_format_opts)


class ImageFactory(object):
    _readonly_properties = ['created_at', 'updated_at', 'status', 'checksum',
                            'size']
    _reserved_properties = ['owner', 'is_public', 'locations',
                            'deleted', 'deleted_at', 'direct_url', 'self',
                            'file', 'schema']

    def _check_readonly(self, kwargs):
        for key in self._readonly_properties:
            if key in kwargs:
                raise exception.ReadonlyProperty(property=key)

    def _check_unexpected(self, kwargs):
        if kwargs:
            msg = 'new_image() got unexpected keywords %s'
            raise TypeError(msg % kwargs.keys())

    def _check_reserved(self, properties):
        if properties is not None:
            for key in self._reserved_properties:
                if key in properties:
                    raise exception.ReservedProperty(property=key)

    def new_image(self, image_id=None, name=None, visibility='private',
                  min_disk=0, min_ram=0, protected=False, owner=None,
                  disk_format=None, container_format=None,
                  extra_properties=None, tags=None, **other_args):
        self._check_readonly(other_args)
        self._check_unexpected(other_args)
        self._check_reserved(extra_properties)

        if image_id is None:
            image_id = uuidutils.generate_uuid()
        created_at = timeutils.utcnow()
        updated_at = created_at
        status = 'queued'

        return Image(image_id=image_id, name=name, status=status,
                     created_at=created_at, updated_at=updated_at,
                     visibility=visibility, min_disk=min_disk,
                     min_ram=min_ram, protected=protected,
                     owner=owner, disk_format=disk_format,
                     container_format=container_format,
                     extra_properties=extra_properties, tags=tags)


class Image(object):

    def __init__(self, image_id, status, created_at, updated_at, **kwargs):
        self.image_id = image_id
        self.status = status
        self.created_at = created_at
        self.updated_at = updated_at
        self.name = kwargs.pop('name', None)
        self.visibility = kwargs.pop('visibility', 'private')
        self.min_disk = kwargs.pop('min_disk', 0)
        self.min_ram = kwargs.pop('min_ram', 0)
        self.protected = kwargs.pop('protected', False)
        self.locations = kwargs.pop('locations', [])
        self.checksum = kwargs.pop('checksum', None)
        self.owner = kwargs.pop('owner', None)
        self.disk_format = kwargs.pop('disk_format', None)
        self.container_format = kwargs.pop('container_format', None)
        self.size = kwargs.pop('size', None)
        self.extra_properties = kwargs.pop('extra_properties', None) or {}
        self.tags = kwargs.pop('tags', None) or []
        if kwargs:
            message = "__init__() got unexpected keyword argument '%s'"
            raise TypeError(message % kwargs.keys()[0])

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        if (hasattr(self, '_status') and self._status == 'queued' and
                status in ('saving', 'active')):
            missing = [k for k in ['disk_format', 'container_format']
                       if not getattr(self, k)]
            if len(missing) > 0:
                if len(missing) == 1:
                    msg = _('Property %s must be set prior to saving data.')
                else:
                    msg = _('Properties %s must be set prior to saving data.')
                raise ValueError(msg % ', '.join(missing))

        self._status = status

    @property
    def visibility(self):
        return self._visibility

    @visibility.setter
    def visibility(self, visibility):
        if visibility not in ('public', 'private'):
            raise ValueError('Visibility must be either "public" or "private"')
        self._visibility = visibility

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, value):
        self._tags = set(value)

    def delete(self):
        if self.protected:
            raise exception.ProtectedImageDelete(image_id=self.image_id)
        self.status = 'deleted'

    def get_data(self):
        raise NotImplementedError()

    def set_data(self, data, size=None):
        raise NotImplementedError()


class ImageMembership(object):

    def __init__(self, image_id, member_id, created_at, updated_at,
                 id=None, status=None):
        self.id = id
        self.image_id = image_id
        self.member_id = member_id
        self.created_at = created_at
        self.updated_at = updated_at
        self.status = status

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        if status not in ('pending', 'accepted', 'rejected'):
            msg = _('Status must be "pending", "accepted" or "rejected".')
            raise ValueError(msg)
        self._status = status


class ImageMemberFactory(object):

    def new_image_member(self, image, member_id):
        created_at = timeutils.utcnow()
        updated_at = created_at

        return ImageMembership(image_id=image.image_id, member_id=member_id,
                               created_at=created_at, updated_at=updated_at,
                               status='pending')


class Task(object):
    def __init__(self, task_id, type, status, input, result, owner, message,
                 expires_at, created_at, updated_at, executor=None):
        if type not in ('import'):
            raise exception.InvalidTaskType(type)

        if status not in ('pending', 'success', 'failure', 'inprogress'):
            raise exception.InvalidTaskStatus(status)
        self.task_id = task_id
        self._status = status
        self.type = type
        self.input = input
        self.result = result
        self.owner = owner
        self.message = message
        self.expires_at = expires_at
        self.created_at = created_at
        self.updated_at = updated_at
        self._executor = executor

    @property
    def status(self):
        return self._status

    def run(self):
        self._status = 'processing'
        self._executor.run(self)

    def kill(self, message=None):
        pass

    def complete(self, result):
        self._status = 'success'
        self.result = result

    def fail(self, message):
        self._status = 'failure'
        self.message = message


class TaskFactory(object):
    def new_task(self, request, task_dict):
        if not request:
            raise TypeError('new_task() takes at least one argument'
                            ' (\'request\')')

        task_id = uuidutils.generate_uuid()
        type = task_dict['type']
        status = 'pending'
        input = task_dict['input']
        result = None
        owner = request.context.owner
        message = None
        expires_at = None  # depends on the expire policy ???
        created_at = timeutils.utcnow()
        updated_at = created_at
        executor = TaskExecutorFactory().new_task_executor(request, task_dict)
        return Task(task_id, type, status, input, result, owner, message,
                    expires_at, created_at, updated_at, executor)


class TaskExecutorFactory(object):
    def new_task_executor(self, request, task_dict):
       if task_dict['type'] == 'import':
           return import_executor.TaskImportExecutor(request)

       raise exception.InvalidTaskType(type=task_dict['type'])
