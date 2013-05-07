from oslo.config import cfg

import glance.store


CONF = cfg.CONF


def initiate_deletion(req, location, id):
    if CONF.delayed_delete:
        glance.store.schedule_delayed_delete_from_backend(location, id)
    else:
        glance.store.safe_delete_from_backend(location, req.context, id)
