from oslo.config import cfg

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
