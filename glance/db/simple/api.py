# Copyright 2012 OpenStack, LLC
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
import functools
import logging
import uuid

from glance.common import exception


LOG = logging.getLogger(__name__)

DATA = {
    'images': {},
    'members': {},
    'tags': {},
}


def log_call(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        LOG.info('Calling %s: args=%s, kwargs=%s' %
                 (func.__name__, args, kwargs))
        output = func(*args, **kwargs)
        LOG.info('Returning %s: %s' % (func.__name__, output))
        return output
    return wrapped


def reset():
    global DATA
    DATA = {
        'images': {},
        'members': {},
        'tags': {},
    }


def configure_db(*args, **kwargs):
    pass


def get_session():
    return DATA


def _image_member_format(image_id, tenant_id, can_share):
    return {
        'image_id': image_id,
        'member': tenant_id,
        'can_share': can_share,
        'deleted': False,
    }


def _image_format(image_id, **values):
    dt = datetime.datetime.now()
    image = {
        'id': image_id,
        'name': 'image-name',
        'owner': None,
        'location': None,
        'status': 'queued',
        'is_public': False,
        'deleted': False,
        'created_at': dt,
        'updated_at': dt,
        'tags': [],
        'properties': [],
    }
    image.update(values)
    return image


@log_call
def image_get(context, image_id, session=None, force_show_deleted=False):
    try:
        image = DATA['images'][image_id]
    except KeyError:
        LOG.info('Could not find image %s' % image_id)
        raise exception.NotFound()

    if image['deleted'] and not (force_show_deleted or context.show_deleted):
        LOG.info('Unable to get deleted image')
        raise exception.NotFound()

    #NOTE(bcwaldon: this is a hack until we can get image members with
    # a direct db call
    image['members'] = DATA['members'].get(image_id, [])

    return image


@log_call
def image_get_all(context, filters=None, marker=None, limit=None,
                  sort_key='created_at', sort_dir='desc'):
    filters = filters or {}
    images_copy = DATA['images'].values()
    images = []
    # Do filtering
    if filters:
        for i, image in enumerate(images_copy):
            add = True
            for k, value in filters.iteritems():
                key = k
                if k.endswith('_min') or k.endswith('_max'):
                    key = key[0:-4]
                    try:
                        value = int(value)
                    except ValueError:
                        raise exception.InvalidFilterRangeValue()
                if k.endswith('_min'):
                    add = image.get(key) >= value
                elif k.endswith('_max'):
                    add = image.get(key) <= value
                elif image.get(k) is not None:
                    add = image.get(key) == value
                elif not key in ['deleted', 'is_public']:
                    raise exception.InvalidFilterKey(attr=k)
                if not add:
                    break

            if add:
                images.append(image)

    # Do Pagination
    reverse = False
    start = 0
    end = -1
    if images and not images[0].get(sort_key):
        raise exception.InvalidSortKey()
    keyfn = lambda x: (x[sort_key], x['created_at'], x['id'])
    reverse = sort_dir == 'desc'
    images.sort(key=keyfn, reverse=reverse)
    if marker is None:
        start = 0
    else:
        # Check that the image is accessible
        image_get(context, marker, force_show_deleted=filters.get('deleted'))

        for i, image in enumerate(images):
            if image['id'] == marker:
                start = i + 1
                break
        else:
            raise exception.NotFound()

    end = start + limit if limit else None
    return images[start:end]


@log_call
def image_member_find(context, image_id, tenant_id):
    image_get(context, image_id)

    for member in DATA['members'].get(image_id, []):
        if member['member'] == tenant_id:
            return member

    raise exception.NotFound()


@log_call
def image_member_create(context, values):
    member = _image_member_format(values['image_id'],
                                  values['member'],
                                  values['can_share'])
    global DATA
    DATA['members'].setdefault(values['image_id'], [])
    DATA['members'][values['image_id']].append(member)
    return member


@log_call
def image_create(context, image_values):
    image_id = image_values.get('id', str(uuid.uuid4()))
    image = _image_format(image_id, **image_values)
    global DATA
    DATA['images'][image_id] = image
    DATA['tags'][image_id] = image.pop('tags', [])
    return image


@log_call
def image_update(context, image_id, image_values):
    global DATA
    try:
        image = DATA['images'][image_id]
    except KeyError:
        raise exception.NotFound(image_id=image_id)

    image.update(image_values)
    DATA['images'][image_id] = image
    return image


@log_call
def image_destroy(context, image_id):
    global DATA
    try:
        DATA['images'][image_id]['deleted'] = True
    except KeyError:
        raise exception.NotFound()


@log_call
def image_tag_get_all(context, image_id):
    image_get(context, image_id)
    return DATA['tags'].get(image_id, [])


@log_call
def image_tag_get(context, image_id, value):
    tags = image_tag_get_all(context, image_id)
    if value in tags:
        return value
    else:
        raise exception.NotFound()


@log_call
def image_tag_set_all(context, image_id, values):
    global DATA
    DATA['tags'][image_id] = values


@log_call
def image_tag_create(context, image_id, value):
    global DATA
    DATA['tags'][image_id].append(value)
    return value


@log_call
def image_tag_delete(context, image_id, value):
    global DATA
    try:
        DATA['tags'][image_id].remove(value)
    except ValueError:
        raise exception.NotFound()


def is_image_mutable(context, image):
    """Return True if the image is mutable in this context."""
    # Is admin == image mutable
    if context.is_admin:
        return True

    # No owner == image not mutable
    if image['owner'] is None or context.owner is None:
        return False

    # Image only mutable by its owner
    return image['owner'] == context.owner


def is_image_sharable(context, image, **kwargs):
    """Return True if the image can be shared to others in this context."""
    # Only allow sharing if we have an owner
    if context.owner is None:
        return False

    # Is admin == image sharable
    if context.is_admin:
        return True

    # If we own the image, we can share it
    if context.owner == image['owner']:
        return True

    # Let's get the membership association
    if 'membership' in kwargs:
        membership = kwargs['membership']
        if membership is None:
            # Not shared with us anyway
            return False
    else:
        try:
            membership = image_member_find(context, image['id'], context.owner)
        except exception.NotFound:
            # Not shared with us anyway
            return False

    # It's the can_share attribute we're now interested in
    return membership['can_share']


def is_image_visible(context, image):
    """Return True if the image is visible in this context."""
    # Is admin == image visible
    if context.is_admin:
        return True

    # No owner == image visible
    if image['owner'] is None:
        return True

    # Image is_public == image visible
    if image['is_public']:
        return True

    # Perform tests based on whether we have an owner
    if context.owner is not None:
        if context.owner == image['owner']:
            return True

        # Figure out if this image is shared with that tenant
        try:
            tmp = image_member_find(context, image['id'], context.owner)
            return not tmp['deleted']
        except exception.NotFound:
            pass

    # Private image
    return False
