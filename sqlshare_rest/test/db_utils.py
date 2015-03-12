from django.test import TestCase

from sqlshare_rest.util.db import _get_basic_settings, _get_backend, BackendNotImplemented
from sqlshare_rest.backend.base import DBInterface


class TestBackendSettings(TestCase):
    def test_get_mysql(self):
        _get_backend()

    def test_invalid_backend(self):
        with self.settings(DATABASES = { 'default': { 'ENGINE': 'django.db.backends.not_implemented' } }):
            self.assertRaises(BackendNotImplemented, _get_backend)

    def test_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            DBInterface()._not_implemented("Testing")
