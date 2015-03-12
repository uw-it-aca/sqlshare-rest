from django.test import TestCase
from sqlshare_rest.util.db import is_mysql, get_backend
import unittest

@unittest.skipUnless(is_mysql(), "Only test with mysql")
class TestMySQLBackend(TestCase):

    def test_create_user(self):
        backend = get_backend()
        user = backend.get_user("test_user_tcu1")
