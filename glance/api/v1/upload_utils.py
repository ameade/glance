from oslo.config import cfg
from webob.exc import (HTTPError,
                       HTTPNotFound,
                       HTTPConflict,
                       HTTPBadRequest,
                       HTTPForbidden,
                       HTTPRequestEntityTooLarge,
                       HTTPInternalServerError,
                       HTTPServiceUnavailable)

from glance.common import exception
from glance.common import utils
import glance.openstack.common.log as logging
import glance.registry.client.v1.api as registry
import glance.store


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def initiate_deletion(req, location, id):
    """
    Deletes image data from the backend store.

    If CONF.delayed_delete is true then data
    deletion will be delayed.

    :param req: The WSGI/Webob Request object
    :param location: URL to the image data in a data store
    :param image_id: Opaque image identifier
    """
    if CONF.delayed_delete:
        glance.store.schedule_delayed_delete_from_backend(location, id)
    else:
        glance.store.safe_delete_from_backend(location, req.context, id)


def _kill(req, image_id):
    """
    Marks the image status to `killed`.

    :param req: The WSGI/Webob Request object
    :param image_id: Opaque image identifier
    """
    registry.update_image_metadata(req.context, image_id,
                                   {'status': 'killed'})


def safe_kill(req, image_id):
    """
    Mark image killed without raising exceptions if it fails.

    Since _kill is meant to be called from exceptions handlers, it should
    not raise itself, rather it should just log its error.

    :param req: The WSGI/Webob Request object
    :param image_id: Opaque image identifier
    """
    try:
        _kill(req, image_id)
    except Exception as e:
        LOG.error(_("Unable to kill image %(id)s: "
                    "%(exc)s") % ({'id': image_id,
                                   'exc': repr(e)}))


def upload_data_to_store(req, image_meta, image_data, store):
    """
    Upload image data to specified store.

    Upload image data to the store and cleans up on error.
    """
    image_id = image_meta['id']
    try:
        location, size, checksum = store.add(
            image_meta['id'],
            utils.CooperativeReader(image_data),
            image_meta['size'])

        def _kill_mismatched(image_meta, attr, actual):
            supplied = image_meta.get(attr)
            if supplied and supplied != actual:
                msg = _("Supplied %(attr)s (%(supplied)s) and "
                        "%(attr)s generated from uploaded image "
                        "(%(actual)s) did not match. Setting image "
                        "status to 'killed'.") % locals()
                LOG.error(msg)
                safe_kill(req, image_id)
                initiate_deletion(req, location, image_id)
                raise HTTPBadRequest(explanation=msg,
                                     content_type="text/plain",
                                     request=req)

        # Verify any supplied size/checksum value matches size/checksum
        # returned from store when adding image
        _kill_mismatched(image_meta, 'size', size)
        _kill_mismatched(image_meta, 'checksum', checksum)

        # Update the database with the checksum returned
        # from the backend store
        LOG.debug(_("Updating image %(image_id)s data. "
                  "Checksum set to %(checksum)s, size set "
                  "to %(size)d"), locals())
        update_data = {'checksum': checksum,
                       'size': size}
        image_meta = registry.update_image_metadata(req.context,
                                                    image_id,
                                                    update_data)

    except exception.Duplicate as e:
        msg = _("Attempt to upload duplicate image: %s") % e
        LOG.debug(msg)
        safe_kill(req, image_id)
        raise HTTPConflict(explanation=msg, request=req)

    except exception.Forbidden as e:
        msg = _("Forbidden upload attempt: %s") % e
        LOG.debug(msg)
        safe_kill(req, image_id)
        raise HTTPForbidden(explanation=msg,
                            request=req,
                            content_type="text/plain")

    except exception.StorageFull as e:
        msg = _("Image storage media is full: %s") % e
        LOG.error(msg)
        safe_kill(req, image_id)
        self.notifier.error('image.upload', msg)
        raise HTTPRequestEntityTooLarge(explanation=msg, request=req,
                                        content_type='text/plain')

    except exception.StorageWriteDenied as e:
        msg = _("Insufficient permissions on image storage media: %s") % e
        LOG.error(msg)
        safe_kill(req, image_id)
        self.notifier.error('image.upload', msg)
        raise HTTPServiceUnavailable(explanation=msg, request=req,
                                     content_type='text/plain')

    except exception.ImageSizeLimitExceeded as e:
        msg = _("Denying attempt to upload image larger than %d bytes."
                % CONF.image_size_cap)
        LOG.info(msg)
        safe_kill(req, image_id)
        raise HTTPRequestEntityTooLarge(explanation=msg, request=req,
                                        content_type='text/plain')

    except HTTPError as e:
        safe_kill(req, image_id)
        #NOTE(bcwaldon): Ideally, we would just call 'raise' here,
        # but something in the above function calls is affecting the
        # exception context and we must explicitly re-raise the
        # caught exception.
        raise e

    except Exception as e:
        LOG.exception(_("Failed to upload image"))
        safe_kill(req, image_id)
        raise HTTPInternalServerError(request=req)

    return image_meta, location
