import mox

from glance.api.v1 import upload_utils
import glance.registry.client.v1.api as registry
import glance.store
from glance.tests.unit import base
import glance.tests.unit.utils as unit_test_utils


class TestUploadUtils(base.StoreClearingUnitTest):
    def setUp(self):
        super(TestUploadUtils, self).setUp()
        self.config(verbose=True, debug=True)
        self.mox = mox.Mox()

    def tearDown(self):
        super(TestUploadUtils, self).tearDown()
        self.mox.UnsetStubs()

    def test_initiate_delete(self):
        self.config(delayed_delete=False)
        req = unit_test_utils.get_fake_request()
        location = "file://foo/bar"
        id = unit_test_utils.UUID1

        self.mox.StubOutWithMock(glance.store, "safe_delete_from_backend")
        glance.store.safe_delete_from_backend(location, req.context, id)
        self.mox.ReplayAll()

        upload_utils.initiate_deletion(req, location, id)

        self.mox.VerifyAll()

    def test_initiate_delete_with_delayed_delete(self):
        self.config(delayed_delete=True)
        req = unit_test_utils.get_fake_request()
        location = "file://foo/bar"
        id = unit_test_utils.UUID1

        self.mox.StubOutWithMock(glance.store,
                                 "schedule_delayed_delete_from_backend")
        glance.store.schedule_delayed_delete_from_backend(location,
                                                          id)
        self.mox.ReplayAll()

        upload_utils.initiate_deletion(req, location, id)

        self.mox.VerifyAll()

    def test_safe_kill(self):
        req = unit_test_utils.get_fake_request()
        id = unit_test_utils.UUID1

        self.mox.StubOutWithMock(registry, "update_image_metadata")
        registry.update_image_metadata(req.context, id, {'status': 'killed'})
        self.mox.ReplayAll()

        upload_utils.safe_kill(req, id)

        self.mox.VerifyAll()

    def test_safe_kill_with_error(self):
        req = unit_test_utils.get_fake_request()
        id = unit_test_utils.UUID1

        self.mox.StubOutWithMock(registry, "update_image_metadata")
        registry.update_image_metadata(req.context,
                                       id,
                                       {'status': 'killed'}
                                       ).AndRaise(Exception())
        self.mox.ReplayAll()

        upload_utils.safe_kill(req, id)

        self.mox.VerifyAll()
