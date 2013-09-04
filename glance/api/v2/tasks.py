# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import copy
import json
import webob.exc
import urllib

from oslo.config import cfg

import glance.db
import glance.gateway
import glance.notifier
import glance.schema
import glance.store

from glance.api import policy
from glance.common import wsgi
from glance.common import exception
from glance.common import utils
from glance.openstack.common import timeutils
import glance.openstack.common.log as logging

LOG = logging.getLogger(__name__)

CONF = cfg.CONF


class TasksController(object):
    """Manages operations on tasks."""

    def __init__(self, db_api=None, policy_enforcer=None, notifier=None,
                 store_api=None):
        self.db_api = db_api or glance.db.get_api()
        self.db_api.setup_db_env()
        self.policy = policy_enforcer or policy.Enforcer()
        self.notifier = notifier or glance.notifier.Notifier()
        self.store_api = store_api or glance.store
        self.gateway = glance.gateway.Gateway(self.db_api, self.store_api,
                                              self.notifier, self.policy)

    @utils.mutating
    def create(self, req, task):
        try:
            task_factory = self.gateway.get_task_factory(req.context)
            task_repo = self.gateway.get_task_repo(req.context)
            try:
                task = task_factory.new_task(req, task, self.gateway)
                task_repo.add(task)
                task.run()
            except exception.Forbidden as e:
                raise webob.exc.HTTPForbidden(explanation=unicode(e))

            return task
        except Exception as e:
            LOG.exception("WTF")

    def index(self, req, marker=None, limit=None, sort_key='created_at',
              sort_dir='desc', filters=None):
        result = {}
        if filters is None:
            filters = {}
        filters['deleted'] = False

        if limit is None:
            limit = CONF.limit_param_default
        limit = min(CONF.api_limit_max, limit)

        task_repo = self.gateway.get_task_repo(req.context)
        try:
            tasks = task_repo.list(marker, limit, sort_key, sort_dir, filters)
            if len(tasks) != 0 and len(tasks) == limit:
                result['next_marker'] = tasks[-1].image_id
        except (exception.NotFound, exception.InvalidSortKey,
                exception.InvalidFilterRangeValue) as e:
            raise webob.exc.HTTPBadRequest(explanation=unicode(e))
        except exception.Forbidden as e:
            raise webob.exc.HTTPForbidden(explanation=unicode(e))
        result['tasks'] = tasks
        return result

    def get(self, req, task_id):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.get(task_id)

    def kill(self, req, task_id):
        pass


class RequestDeserializer(wsgi.JSONRequestDeserializer):
    _disallowed_properties = ['direct_url', 'self', 'file', 'schema']
    _base_properties = ['type', 'input']

    def _get_request_body(self, request):
        output = super(RequestDeserializer, self).default(request)
        if 'body' not in output:
            msg = _('Body expected in request.')
            raise webob.exc.HTTPBadRequest(explanation=msg)
        return output['body']

    def _validate_sort_dir(self, sort_dir):
        if sort_dir not in ['asc', 'desc']:
            msg = _('Invalid sort direction: %s') % sort_dir
            raise webob.exc.HTTPBadRequest(explanation=msg)

        return sort_dir

    def _get_filters(self, filters):
        status = filters.get('status', None)
        if status:
            if status not in ['pending', 'processing', 'success', 'failure']:
                msg = _('Invalid status value: %s') % status
                raise webob.exc.HTTPBadRequest(explanation=msg)

        type = filters.get('type', None)
        if type:
            if type not in ['import']:
                msg = _('Invalid type value: %s') % type
                raise webob.exc.HTTPBadRequest(explanation=msg)

        return filters

    def _validate_limit(self, limit):
        try:
            limit = int(limit)
        except ValueError:
            msg = _("limit param must be an integer")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        if limit < 0:
            msg = _("limit param must be positive")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        return limit

    @classmethod
    def _check_allowed(cls, task):
        for key in cls._disallowed_properties:
            if key in task:
                msg = "Attribute \'%s\' is read-only." % key
                raise webob.exc.HTTPForbidden(explanation=unicode(msg))

    def __init__(self, schema=None):
        super(RequestDeserializer, self).__init__()
        self.schema = schema or get_schema()

    def create(self, request):
        body = self._get_request_body(request)
        self._check_allowed(body)
        task = {}
        properties = body
        for key in self._base_properties:
            try:
                task[key] = properties.pop(key)
            except KeyError:
                pass
        return dict(task=task)

    def index(self, request):
        params = request.params.copy()
        limit = params.pop('limit', None)
        marker = params.pop('marker', None)
        sort_dir = params.pop('sort_dir', 'desc')
        query_params = {
            'sort_key': params.pop('sort_key', 'created_at'),
            'sort_dir': self._validate_sort_dir(sort_dir),
            'filters': self._get_filters(params)
        }

        if marker is not None:
            query_params['marker'] = marker

        if limit is not None:
            query_params['limit'] = self._validate_limit(limit)
        return query_params


class ResponseSerializer(wsgi.JSONResponseSerializer):
    def __init__(self, schema=None):
        super(ResponseSerializer, self).__init__()
        self.schema = schema or get_schema()

    def _format_task(self, task):
        task_view = {}
        attributes = ['type', 'status', 'input', 'result', 'owner', 'message']
        for key in attributes:
            task_view[key] = getattr(task, key)
        task_view['id'] = task.task_id
        task_view['expires_at'] = timeutils.isotime(task.expires_at)
        task_view['created_at'] = timeutils.isotime(task.created_at)
        task_view['updated_at'] = timeutils.isotime(task.updated_at)
        task_view['schema'] = '/v2/schemas/task'
        task_view = self.schema.filter(task_view)  # domain
        return task_view

    def create(self, response, task):
        response.status_int = 201
        self.get(response, task)

    def get(self, response, task):
        task_view = self._format_task(task)
        body = json.dumps(task_view, ensure_ascii=False)
        response.unicode_body = unicode(body)
        response.content_type = 'application/json'

    def index(self, response, result):
        params = dict(response.request.params)
        params.pop('marker', None)
        query = urllib.urlencode(params)
        body = {
            'tasks': [self._format_task(i) for i in result['tasks']],
            'first': '/v2/tasks',
            'schema': '/v2/schemas/tasks',
        }
        if query:
            body['first'] = '%s?%s' % (body['first'], query)
        if 'next_marker' in result:
            params['marker'] = result['next_marker']
            next_query = urllib.urlencode(params)
            body['next'] = '/v2/tasks?%s' % next_query
        response.unicode_body = unicode(json.dumps(body, ensure_ascii=False))
        response.content_type = 'application/json'


_TASK_SCHEMA = {
    "id": {
        "description": "An identifier for the task",
        "pattern": _('^([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}'
                     '-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}$'),
        "type": "string"
    },
    "type": {
        "description": _("The type of task represented by this content"),
        "enum": [
            "import",
            "export",
            "clone"
        ],
        "type": "string"
    },
    "status": {
        "description": _("The current status of this task"),
        "enum": [
            "queued",
            "processing",
            "success",
            "failure"
        ],
        "type": "string"
    },
    "input": {
        "description": _("The parameters required by task, JSON blob"),
        "type": "string"
    },
    "result": {
        "description": _("The result of current task, JSON blob"),
        "type": "string",
    },
    "owner": {
        "description": _("An identifier for the owner of this task"),
        "type": "string"
    },
    "message": {
        "description": _("Human-readable informative message only included \
when appropriate (usually on failure)"),
        "type": "string",
    },
    "expires_at": {
        "description": _("Datetime when this resource is subject to removal"),
        "type": "string"
    },
    "created_at": {
        "description": _("Datetime when this resource is created"),
        "type": "string"
    },
    "updated_at": {
        "description": _("Datetime when this resource is updated"),
        "type": "string"
    },
    'schema': {'type': 'string'}
}


def get_schema():
    properties = copy.deepcopy(_TASK_SCHEMA)
    schema = glance.schema.Schema('task', properties)
    return schema


def get_collection_schema():
    task_schema = get_schema()
    return glance.schema.CollectionSchema('tasks', task_schema)


def create_resource():
    """Task resource factory method"""
    deserializer = RequestDeserializer()
    serializer = ResponseSerializer()
    controller = TasksController()
    return wsgi.Resource(controller, deserializer, serializer)
