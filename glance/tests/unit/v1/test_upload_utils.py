import mox

from glance.api.v1 import upload_utils
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
