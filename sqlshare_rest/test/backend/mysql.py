from django.test import TestCase
from sqlshare_rest.util.db import is_mysql, get_backend
import unittest

@unittest.skipUnless(is_mysql(), "Only test with mysql")
class TestMySQLBackend(TestCase):

    def test_create_user(self):
        self.remove_users.append("test_user_tcu1")
        backend = get_backend()
        user = backend.get_user("test_user_tcu1")

    def test_run_query(self):
        self.remove_users.append("test_user_trq1")
        backend = get_backend()
        user = backend.get_user("test_user_trq1")
        result = backend.run_query("select (5)", user)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0][0], 5)

        result = backend.run_query('select (10) union select ("a")', user)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0][0], '10')
        self.assertEquals(result[1][0], "a")

    def setUp(self):
        self.remove_users = []

    def tearDown(self):
        backend = get_backend()

        for user in self.remove_users:
            try:
                backend.remove_user(user)
            except Exception as ex:
                print "Error deleting user: ", ex
