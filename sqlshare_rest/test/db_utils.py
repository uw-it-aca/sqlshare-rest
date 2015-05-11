from sqlshare_rest.test import CleanUpTestCase

from sqlshare_rest.util.db import _get_basic_settings, get_backend, BackendNotImplemented
from sqlshare_rest.backend.base import DBInterface


class TestBackendSettings(CleanUpTestCase):
    def test_invalid_backend(self):
        with self.settings(DATABASES = { 'default': { 'ENGINE': 'django.db.backends.not_implemented' } }):
            self.assertRaises(BackendNotImplemented, get_backend)

    def test_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            DBInterface()._not_implemented("Testing")
